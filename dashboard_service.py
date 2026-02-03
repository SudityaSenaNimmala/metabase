"""
Dashboard Auto-Clone Service
Runs 24/7, checks every hour for databases needing dashboards and creates them.
Provides a web UI with countdown timer and activity logs.
"""

import sys
import os
import json
import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, asdict
import requests

# Fix Windows console encoding
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except:
        pass

from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from db_identifier import DatabaseIdentifier, DatabaseInfo
from simple_clone import DashboardCloner

# =============================================================================
# Configuration
# =============================================================================

LOG_FILE = "dashboard_activity.json"
CONFIG_FILE = "auto_clone_config.json"
METABASE_CONFIG_FILE = "metabase_config.json"

# =============================================================================
# Activity Log
# =============================================================================

@dataclass
class ActivityLogEntry:
    timestamp: str
    database_name: str
    database_id: int
    db_type: str
    dashboard_name: str
    dashboard_id: int
    dashboard_url: str
    status: str  # "success", "failed", or "deleted"
    error_message: Optional[str] = None


class ActivityLog:
    """Manages the activity log for dashboard creation"""
    
    def __init__(self, log_file: str = LOG_FILE):
        self.log_file = log_file
        self.entries: List[ActivityLogEntry] = []
        self.load()
    
    def load(self):
        """Load existing log from file"""
        try:
            if os.path.exists(self.log_file):
                with open(self.log_file, 'r') as f:
                    data = json.load(f)
                    self.entries = [ActivityLogEntry(**entry) for entry in data.get('entries', [])]
        except Exception as e:
            logging.error(f"Failed to load activity log: {e}")
            self.entries = []
    
    def save(self):
        """Save log to file"""
        try:
            with open(self.log_file, 'w') as f:
                json.dump({
                    'entries': [asdict(e) for e in self.entries],
                    'last_updated': datetime.now().isoformat()
                }, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save activity log: {e}")
    
    def add_entry(self, entry: ActivityLogEntry):
        """Add a new entry to the log"""
        self.entries.insert(0, entry)  # Add to beginning (newest first)
        self.save()
    
    def get_entries(self, limit: int = 100) -> List[dict]:
        """Get log entries as dictionaries"""
        return [asdict(e) for e in self.entries[:limit]]
    
    def get_stats(self) -> dict:
        """Get statistics from the log"""
        total = len(self.entries)
        success = sum(1 for e in self.entries if e.status == "success")
        deleted = sum(1 for e in self.entries if e.status == "deleted")
        failed = total - success - deleted
        
        by_type = {"content": 0, "message": 0, "email": 0}
        for e in self.entries:
            if e.status == "success" and e.db_type in by_type:
                by_type[e.db_type] += 1
        
        return {
            "total": total,
            "success": success,
            "failed": failed,
            "deleted": deleted,
            "by_type": by_type
        }


# =============================================================================
# Dashboard Service
# =============================================================================

class DashboardService:
    """Main service that runs the auto-clone process"""
    
    def __init__(self):
        self.activity_log = ActivityLog()
        self.last_run: Optional[datetime] = None
        self.next_run: Optional[datetime] = None
        self.is_running = False
        self.current_status = "Idle"
        self.metabase_config = None
        self.auto_config = None
        self.base_url = ""
        
        # Load configs
        self._load_configs()
    
    def _load_configs(self):
        """Load configuration files"""
        try:
            with open(METABASE_CONFIG_FILE, 'r') as f:
                self.metabase_config = json.load(f)
                self.base_url = self.metabase_config['base_url'].rstrip('/')
        except Exception as e:
            logging.error(f"Failed to load metabase config: {e}")
        
        try:
            with open(CONFIG_FILE, 'r') as f:
                self.auto_config = json.load(f)
        except Exception as e:
            logging.error(f"Failed to load auto clone config: {e}")
    
    def reload_configs(self):
        """Reload configuration files"""
        self._load_configs()
    
    def _get_databases_by_type_cached(self, identifier) -> Dict[str, List]:
        """
        Get databases by type with caching.
        Cache is valid for 24 hours - database types don't change often.
        """
        cache_file = "db_type_cache.json"
        cache_max_age = 24 * 60 * 60  # 24 hours in seconds
        
        # Try to load from cache
        try:
            if os.path.exists(cache_file):
                with open(cache_file, 'r') as f:
                    cache_data = json.load(f)
                
                cache_time = datetime.fromisoformat(cache_data.get('timestamp', '2000-01-01'))
                age = (datetime.now() - cache_time).total_seconds()
                
                if age < cache_max_age:
                    logging.info(f"Using cached database types (age: {age/3600:.1f} hours)")
                    
                    # Convert back to DatabaseInfo objects
                    from db_identifier import DatabaseInfo
                    grouped = {"content": [], "message": [], "email": [], "unknown": []}
                    
                    for db_type, dbs in cache_data.get('databases', {}).items():
                        for db in dbs:
                            info = DatabaseInfo(
                                id=db['id'],
                                name=db['name'],
                                engine=db.get('engine', 'unknown'),
                                tables=[],  # Don't need tables for cloning
                                identified_type=db_type if db_type != 'unknown' else None,
                                match_confidence=db.get('confidence', 0),
                                matched_tables=db.get('matched_tables', [])
                            )
                            grouped[db_type].append(info)
                    
                    return grouped
                else:
                    logging.info(f"Cache expired (age: {age/3600:.1f} hours), rescanning...")
        except Exception as e:
            logging.warning(f"Could not load cache: {e}")
        
        # No valid cache - scan databases (slow)
        logging.info("Scanning databases (this may take a few minutes)...")
        grouped = identifier.get_databases_by_type()
        
        # Save to cache
        try:
            cache_data = {
                'timestamp': datetime.now().isoformat(),
                'databases': {}
            }
            for db_type, dbs in grouped.items():
                cache_data['databases'][db_type] = [
                    {
                        'id': db.id,
                        'name': db.name,
                        'engine': db.engine,
                        'confidence': db.match_confidence,
                        'matched_tables': db.matched_tables
                    }
                    for db in dbs
                ]
            
            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            
            logging.info(f"Saved database types to cache")
        except Exception as e:
            logging.warning(f"Could not save cache: {e}")
        
        return grouped
    
    def get_status(self) -> dict:
        """Get current service status"""
        now = datetime.now()
        
        # Calculate time until next run
        seconds_until_next = 0
        if self.next_run:
            delta = self.next_run - now
            seconds_until_next = max(0, int(delta.total_seconds()))
        
        return {
            "is_running": self.is_running,
            "current_status": self.current_status,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "next_run": self.next_run.isoformat() if self.next_run else None,
            "seconds_until_next": seconds_until_next,
            "config_loaded": bool(self.metabase_config and self.auto_config)
        }
    
    def extract_customer_name(self, db_name: str) -> str:
        """Extract customer name from database name.
        Preserves version numbers like abc2, abc3 etc."""
        name = db_name
        
        # Suffixes to remove (order matters - check longer ones first)
        suffixes_to_remove = [
            '-SDB', '-sdb', 'email', 'Email', 'msg', 'Msg', 
            'message', 'Message', '-common', '-json', 'hub', 'Hub',
        ]
        
        for suffix in suffixes_to_remove:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
                break  # Only remove one suffix
        
        # Clean up trailing dashes/underscores but KEEP version numbers
        name = name.rstrip('-_')
        
        # Capitalize first letter
        if name:
            name = name[0].upper() + name[1:]
        
        return name or db_name
    
    def run_check(self):
        """Run the dashboard check and clone process"""
        if self.is_running:
            logging.warning("Check already running, skipping...")
            return
        
        self.is_running = True
        self.current_status = "Running check..."
        self.last_run = datetime.now()
        
        try:
            logging.info("="*60)
            logging.info("STARTING DASHBOARD CHECK")
            logging.info("="*60)
            
            # Reload configs in case they changed
            self.reload_configs()
            
            if not self.metabase_config or not self.auto_config:
                self.current_status = "Error: Config not loaded"
                logging.error("Configuration not loaded")
                return
            
            source_dashboards = self.auto_config.get('source_dashboards', {})
            dashboards_collections = self.auto_config.get('dashboards_collections', {})
            
            # Check if config is complete
            missing = []
            for db_type in ["content", "message", "email"]:
                if not source_dashboards.get(db_type):
                    missing.append(f"{db_type} source_dashboard")
                if not dashboards_collections.get(db_type):
                    missing.append(f"{db_type} dashboards_collection")
            
            if missing:
                self.current_status = f"Config incomplete: {', '.join(missing)}"
                logging.warning(f"Missing config: {missing}")
                return
            
            # Initialize components
            self.current_status = "Authenticating..."
            identifier = DatabaseIdentifier(METABASE_CONFIG_FILE)
            if not identifier.authenticate():
                self.current_status = "Error: Authentication failed"
                logging.error("Failed to authenticate with Metabase")
                return
            
            cloner = DashboardCloner(self.metabase_config)
            if not cloner.authenticate():
                self.current_status = "Error: Cloner authentication failed"
                logging.error("Failed to authenticate cloner")
                return
            
            headers = identifier.headers
            
            # Get databases by type - use cached results if available
            self.current_status = "Loading database info..."
            grouped = self._get_databases_by_type_cached(identifier)
            
            # Find databases with existing dashboards - ONLY check the 3 _DASHBOARDS collections
            # Also find empty dashboards (decomposed DBs) for cleanup
            self.current_status = "Checking existing dashboards..."
            target_collection_ids = [
                cid for cid in dashboards_collections.values() if cid
            ]
            dbs_with_dashboards, empty_dashboards = self._find_databases_with_dashboards_in_collections(headers, target_collection_ids)
            
            # Map collection IDs to types for logging
            collection_to_type = {v: k for k, v in dashboards_collections.items() if v}
            
            # Clean up empty dashboards (decomposed databases)
            if empty_dashboards:
                self.current_status = f"Cleaning up {len(empty_dashboards)} empty dashboards..."
                logging.info(f"\n--- Cleaning up {len(empty_dashboards)} empty dashboards ---")
                
                for empty_dash in empty_dashboards:
                    try:
                        dash_id = empty_dash['id']
                        dash_name = empty_dash['name']
                        collection_id = empty_dash.get('collection_id')
                        db_type = collection_to_type.get(collection_id, 'unknown')
                        
                        logging.info(f"  Deleting empty dashboard: {dash_name} (ID: {dash_id})")
                        
                        # Delete the dashboard
                        delete_resp = requests.delete(
                            f"{self.base_url}/api/dashboard/{dash_id}",
                            headers=headers
                        )
                        delete_resp.raise_for_status()
                        
                        # Log the deletion
                        entry = ActivityLogEntry(
                            timestamp=datetime.now().isoformat(),
                            database_name="(decomposed)",
                            database_id=0,
                            db_type=db_type,
                            dashboard_name=dash_name,
                            dashboard_id=dash_id,
                            dashboard_url="",
                            status="deleted",
                            error_message="Empty dashboard - database decomposed"
                        )
                        self.activity_log.add_entry(entry)
                        logging.info(f"  ✓ Deleted: {dash_name}")
                        
                    except Exception as e:
                        logging.error(f"  ✗ Failed to delete dashboard {empty_dash.get('id')}: {e}")
            
            # Find databases needing dashboards
            tasks = []
            for db_type in ["content", "message", "email"]:
                source_id = source_dashboards.get(db_type)
                collection_id = dashboards_collections.get(db_type)
                
                if not source_id or not collection_id:
                    continue
                
                for db in grouped.get(db_type, []):
                    if db.id not in dbs_with_dashboards:
                        tasks.append({
                            "database": db,
                            "source_dashboard_id": source_id,
                            "dashboards_collection_id": collection_id,
                            "db_type": db_type,
                            "customer_name": self.extract_customer_name(db.name)
                        })
            
            if not tasks:
                self.current_status = "All databases have dashboards"
                logging.info("No databases need dashboards")
                return
            
            logging.info(f"Found {len(tasks)} databases needing dashboards")
            
            MAX_RETRIES = 3  # Retry failed clones up to 3 times
            
            # Clone dashboards
            for i, task in enumerate(tasks, 1):
                db = task["database"]
                self.current_status = f"Cloning {i}/{len(tasks)}: {db.name}"
                logging.info(f"\n[{i}/{len(tasks)}] Cloning for: {db.name}")
                
                dashboard_name = f"{task['customer_name']} Dashboard"
                new_dashboard = None
                last_error = None
                
                # Retry loop for intermittent failures
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        # Reset cloner mappings for fresh attempt
                        if attempt > 1:
                            cloner.question_mapping = {}
                            cloner.dashboard_mapping = {}
                            logging.info(f"  Retry attempt {attempt}/{MAX_RETRIES}...")
                            time.sleep(attempt * 3)  # Wait 3s, 6s, 9s between retries
                        
                        # Create customer collection
                        source_parent = cloner.get_dashboard_collection_id(task["source_dashboard_id"])
                        collection_name = f"{task['customer_name']} Collection"
                        col = cloner.get_or_create_collection(collection_name, source_parent)
                        customer_collection_id = col['id'] if col else None
                        
                        # Check for linked dashboards
                        all_linked = cloner.find_all_linked_dashboards(task["source_dashboard_id"])
                        
                        if all_linked:
                            new_dashboard = cloner.clone_with_all_linked(
                                source_dashboard_id=task["source_dashboard_id"],
                                new_name=dashboard_name,
                                new_database_id=db.id,
                                dashboard_collection_id=customer_collection_id,
                                questions_collection_id=customer_collection_id,
                                main_dashboard_collection_id=task["dashboards_collection_id"]
                            )
                        else:
                            new_dashboard = cloner.clone_dashboard(
                                source_dashboard_id=task["source_dashboard_id"],
                                new_name=dashboard_name,
                                new_database_id=db.id,
                                dashboard_collection_id=task["dashboards_collection_id"],
                                questions_collection_id=customer_collection_id
                            )
                        
                        if new_dashboard:
                            # Success! Break out of retry loop
                            break
                        else:
                            last_error = "Clone returned None"
                            if attempt < MAX_RETRIES:
                                logging.warning(f"  Attempt {attempt} failed, will retry...")
                    
                    except Exception as e:
                        last_error = str(e)
                        if attempt < MAX_RETRIES:
                            logging.warning(f"  Attempt {attempt} failed: {e}, will retry...")
                        else:
                            logging.error(f"  All {MAX_RETRIES} attempts failed: {e}")
                
                # Log result
                if new_dashboard:
                    entry = ActivityLogEntry(
                        timestamp=datetime.now().isoformat(),
                        database_name=db.name,
                        database_id=db.id,
                        db_type=task["db_type"],
                        dashboard_name=dashboard_name,
                        dashboard_id=new_dashboard['id'],
                        dashboard_url=f"{self.base_url}/dashboard/{new_dashboard['id']}",
                        status="success"
                    )
                    self.activity_log.add_entry(entry)
                    logging.info(f"SUCCESS: Created {dashboard_name} (ID: {new_dashboard['id']})")
                else:
                    entry = ActivityLogEntry(
                        timestamp=datetime.now().isoformat(),
                        database_name=db.name,
                        database_id=db.id,
                        db_type=task["db_type"],
                        dashboard_name=dashboard_name,
                        dashboard_id=0,
                        dashboard_url="",
                        status="failed",
                        error_message=f"Failed after {MAX_RETRIES} attempts: {last_error}"
                    )
                    self.activity_log.add_entry(entry)
                    logging.error(f"FAILED: Could not create dashboard for {db.name} after {MAX_RETRIES} attempts")
            
            self.current_status = f"Completed - processed {len(tasks)} databases"
            logging.info("="*60)
            logging.info("CHECK COMPLETE")
            logging.info("="*60)
            
        except Exception as e:
            self.current_status = f"Error: {str(e)}"
            logging.error(f"Check failed: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            self.is_running = False
    
    def _find_databases_with_dashboards_in_collections(self, headers: dict, collection_ids: List[int]) -> Set[int]:
        """
        Find database IDs that already have dashboards in the specified collections.
        Also identifies empty dashboards (no questions) for cleanup.
        
        OPTIMIZED: Only checks dashboards in the _DASHBOARDS collections,
        and only checks ONE question per dashboard (all questions use same DB).
        
        Returns:
            Tuple of (databases_with_dashboards: Set[int], empty_dashboards: List[dict])
        """
        databases_with_dashboards = set()
        empty_dashboards = []  # Dashboards with no questions (decomposed DBs)
        
        if not collection_ids:
            return databases_with_dashboards, empty_dashboards
        
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        try:
            # Get all dashboards (lightweight list)
            response = requests.get(f"{self.base_url}/api/dashboard", headers=headers)
            response.raise_for_status()
            all_dashboards = response.json()
            
            # Filter to only dashboards in our target collections
            target_dashboards = [
                d for d in all_dashboards 
                if d.get('collection_id') in collection_ids
            ]
            
            logging.info(f"Checking {len(target_dashboards)} dashboards in _DASHBOARDS collections (parallel)...")
            
            def get_dashboard_info(dash):
                """Get the database ID from a dashboard's first question, or mark as empty"""
                try:
                    resp = requests.get(
                        f"{self.base_url}/api/dashboard/{dash['id']}",
                        headers=headers
                    )
                    resp.raise_for_status()
                    full_dash = resp.json()
                    
                    dashcards = full_dash.get('dashcards', []) or full_dash.get('ordered_cards', [])
                    
                    # Count actual question cards (not text cards)
                    question_cards = [dc for dc in dashcards if dc.get('card', {}).get('id')]
                    
                    if len(question_cards) == 0:
                        # Empty dashboard - no questions
                        return {
                            'type': 'empty',
                            'dashboard': {
                                'id': dash['id'],
                                'name': full_dash.get('name', dash.get('name', 'Unknown')),
                                'collection_id': dash.get('collection_id')
                            }
                        }
                    
                    # Has questions - get database ID from first one
                    for dc in dashcards:
                        card = dc.get('card', {})
                        if card and card.get('id'):
                            db_id = card.get('database_id')
                            if db_id:
                                return {'type': 'valid', 'db_id': db_id}
                            
                            # Fetch question if needed
                            card_id = card.get('id')
                            if card_id:
                                try:
                                    q_resp = requests.get(
                                        f"{self.base_url}/api/card/{card_id}",
                                        headers=headers
                                    )
                                    q_resp.raise_for_status()
                                    db_id = q_resp.json().get('database_id')
                                    if db_id:
                                        return {'type': 'valid', 'db_id': db_id}
                                except:
                                    pass
                except Exception as e:
                    logging.debug(f"Error checking dashboard {dash.get('id')}: {e}")
                
                return None
            
            # Parallel fetch with 5 workers
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_dash = {executor.submit(get_dashboard_info, d): d for d in target_dashboards}
                
                for future in as_completed(future_to_dash):
                    result = future.result()
                    if result:
                        if result['type'] == 'valid' and result.get('db_id'):
                            databases_with_dashboards.add(result['db_id'])
                        elif result['type'] == 'empty':
                            empty_dashboards.append(result['dashboard'])
                    
        except Exception as e:
            logging.error(f"Error finding databases with dashboards: {e}")
        
        logging.info(f"Found {len(databases_with_dashboards)} databases with existing dashboards")
        if empty_dashboards:
            logging.info(f"Found {len(empty_dashboards)} empty dashboards to clean up")
        
        return databases_with_dashboards, empty_dashboards


# =============================================================================
# Flask App
# =============================================================================

app = Flask(__name__)
CORS(app)

# Global service instance
service = DashboardService()
scheduler = BackgroundScheduler()


def scheduled_job():
    """Job that runs on schedule"""
    logging.info("Scheduled job triggered")
    service.run_check()


def update_next_run():
    """Update the next run time - every 4 hours (00:00, 04:00, 08:00, 12:00, 16:00, 20:00)"""
    now = datetime.now()
    current_hour = now.hour
    
    # Find next 4-hour slot
    schedule_hours = [0, 4, 8, 12, 16, 20]
    next_scheduled_hour = None
    
    for h in schedule_hours:
        if h > current_hour:
            next_scheduled_hour = h
            break
    
    if next_scheduled_hour is None:
        # Next slot is tomorrow at 00:00
        next_run = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    else:
        next_run = now.replace(hour=next_scheduled_hour, minute=0, second=0, microsecond=0)
    
    service.next_run = next_run


@app.route('/')
def index():
    """Serve the main UI"""
    return render_template('index.html')


@app.route('/api/status')
def get_status():
    """Get current service status"""
    update_next_run()
    return jsonify(service.get_status())


@app.route('/api/logs')
def get_logs():
    """Get activity logs"""
    limit = request.args.get('limit', 100, type=int)
    return jsonify({
        "entries": service.activity_log.get_entries(limit),
        "stats": service.activity_log.get_stats()
    })


@app.route('/api/config')
def get_config():
    """Get current configuration"""
    service.reload_configs()
    return jsonify({
        "metabase_url": service.base_url,
        "source_dashboards": service.auto_config.get('source_dashboards', {}) if service.auto_config else {},
        "dashboards_collections": service.auto_config.get('dashboards_collections', {}) if service.auto_config else {}
    })


@app.route('/api/run', methods=['POST'])
def trigger_run():
    """Manually trigger a check"""
    if service.is_running:
        return jsonify({"error": "Check already running"}), 400
    
    # Run in background thread
    thread = threading.Thread(target=service.run_check)
    thread.start()
    
    return jsonify({"message": "Check started"})


@app.route('/api/refresh-cache', methods=['POST'])
def refresh_cache():
    """Force refresh the database type cache"""
    try:
        cache_file = "db_type_cache.json"
        if os.path.exists(cache_file):
            os.remove(cache_file)
            return jsonify({"message": "Cache cleared. Next run will rescan databases."})
        else:
            return jsonify({"message": "No cache to clear."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/databases')
def get_databases():
    """Get database identification results"""
    try:
        with open("db_identification_results.json", 'r') as f:
            data = json.load(f)
        
        summary = {
            "content": len(data.get("content", [])),
            "message": len(data.get("message", [])),
            "email": len(data.get("email", [])),
            "unknown": len(data.get("unknown", []))
        }
        
        return jsonify({
            "summary": summary,
            "data": data
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/settings', methods=['GET'])
def get_settings():
    """Get all settings (credentials and config)"""
    try:
        # Load metabase config
        metabase_config = {}
        if os.path.exists(METABASE_CONFIG_FILE):
            with open(METABASE_CONFIG_FILE, 'r') as f:
                metabase_config = json.load(f)
        
        # Load auto clone config
        auto_config = {}
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r') as f:
                auto_config = json.load(f)
        
        return jsonify({
            "metabase": {
                "base_url": metabase_config.get('base_url', ''),
                "username": metabase_config.get('username', ''),
                "password": "********" if metabase_config.get('password') else ''  # Don't expose password
            },
            "source_dashboards": auto_config.get('source_dashboards', {
                "content": None,
                "message": None,
                "email": None
            }),
            "dashboards_collections": auto_config.get('dashboards_collections', {
                "content": None,
                "message": None,
                "email": None
            })
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/settings', methods=['POST'])
def save_settings():
    """Save all settings"""
    try:
        data = request.json
        
        # Save metabase config
        if 'metabase' in data:
            metabase_data = data['metabase']
            
            # Load existing config to preserve password if not changed
            existing_config = {}
            if os.path.exists(METABASE_CONFIG_FILE):
                with open(METABASE_CONFIG_FILE, 'r') as f:
                    existing_config = json.load(f)
            
            new_config = {
                "base_url": metabase_data.get('base_url', existing_config.get('base_url', '')),
                "username": metabase_data.get('username', existing_config.get('username', '')),
                "password": metabase_data.get('password') if metabase_data.get('password') and metabase_data.get('password') != '********' else existing_config.get('password', '')
            }
            
            with open(METABASE_CONFIG_FILE, 'w') as f:
                json.dump(new_config, f, indent=4)
        
        # Save auto clone config
        auto_config = {}
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r') as f:
                auto_config = json.load(f)
        
        if 'source_dashboards' in data:
            auto_config['source_dashboards'] = {
                "content": data['source_dashboards'].get('content'),
                "message": data['source_dashboards'].get('message'),
                "email": data['source_dashboards'].get('email')
            }
        
        if 'dashboards_collections' in data:
            auto_config['dashboards_collections'] = {
                "content": data['dashboards_collections'].get('content'),
                "message": data['dashboards_collections'].get('message'),
                "email": data['dashboards_collections'].get('email')
            }
        
        with open(CONFIG_FILE, 'w') as f:
            json.dump(auto_config, f, indent=4)
        
        # Reload configs in service
        service.reload_configs()
        
        return jsonify({"message": "Settings saved successfully"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/test-connection', methods=['POST'])
def test_connection():
    """Test Metabase connection with provided credentials"""
    try:
        data = request.json
        base_url = data.get('base_url', '').rstrip('/')
        username = data.get('username', '')
        password = data.get('password', '')
        
        # If password is masked, use existing password
        if password == '********':
            if os.path.exists(METABASE_CONFIG_FILE):
                with open(METABASE_CONFIG_FILE, 'r') as f:
                    existing = json.load(f)
                    password = existing.get('password', '')
        
        if not base_url or not username or not password:
            return jsonify({"success": False, "error": "Missing credentials"}), 400
        
        # Try to authenticate
        response = requests.post(
            f"{base_url}/api/session",
            json={"username": username, "password": password},
            timeout=10
        )
        
        if response.status_code == 200:
            return jsonify({"success": True, "message": "Connection successful!"})
        else:
            return jsonify({"success": False, "error": f"Authentication failed: {response.status_code}"})
    except requests.exceptions.Timeout:
        return jsonify({"success": False, "error": "Connection timeout"})
    except requests.exceptions.ConnectionError:
        return jsonify({"success": False, "error": "Could not connect to server"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =============================================================================
# Main
# =============================================================================

def create_templates_folder():
    """Create templates folder and HTML file"""
    os.makedirs('templates', exist_ok=True)


def main():
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('dashboard_service.log')
        ]
    )
    
    # Create templates folder
    create_templates_folder()
    
    # Setup scheduler - run every 4 hours (at 00:00, 04:00, 08:00, 12:00, 16:00, 20:00)
    scheduler.add_job(
        scheduled_job,
        CronTrigger(hour='0,4,8,12,16,20', minute=0),  # Every 4 hours at :00
        id='dashboard_check',
        name='Dashboard Check',
        replace_existing=True
    )
    
    # Update next run time
    update_next_run()
    
    # Start scheduler
    scheduler.start()
    logging.info(f"Scheduler started. Next run at: {service.next_run}")
    
    # Run Flask app
    print("\n" + "="*60)
    print("DASHBOARD AUTO-CLONE SERVICE")
    print("="*60)
    print(f"Web UI: http://localhost:1206")
    print(f"Next scheduled run: {service.next_run}")
    print("="*60 + "\n")
    
    try:
        app.run(host='0.0.0.0', port=1206, debug=False, threaded=True)
    except KeyboardInterrupt:
        scheduler.shutdown()
        print("\nService stopped.")


if __name__ == "__main__":
    main()
