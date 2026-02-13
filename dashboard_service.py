"""
Dashboard Auto-Clone Service
Runs 24/7, checks every 4 hours for databases needing dashboards and creates them.
Provides a web UI with countdown timer and activity logs.

ALL DATA IS STORED IN MONGODB - No local file storage.
Set MONGODB_URI environment variable to connect.
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

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Fix Windows console encoding
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except:
        pass

from flask import Flask, render_template, jsonify, request, session
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from db_identifier import DatabaseIdentifier, DatabaseInfo
from simple_clone import DashboardCloner, StopRequested

# =============================================================================
# MongoDB Storage - All data stored in MongoDB
# =============================================================================

class MongoDBStorage:
    """Centralized MongoDB storage for all application data"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self.mongo_client = None
        self.db = None
        self.connected = False
        self._initialized = True
        self._connect()
    
    def _connect(self):
        """Connect to MongoDB"""
        # Default MongoDB connection string (can be overridden by environment variable)
        DEFAULT_MONGODB_URI = "mongodb+srv://sudityanimmala_db_user:1ckKshSh3rcJBjLj@metabase.crnrwej.mongodb.net/?appName=Metabase"
        
        mongodb_uri = os.environ.get('MONGODB_URI', DEFAULT_MONGODB_URI)
        
        try:
            from pymongo import MongoClient
            logging.info(f"Connecting to MongoDB...")
            self.mongo_client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000)
            # Test connection
            self.mongo_client.admin.command('ping')
            self.db = self.mongo_client['metabase_dashboard_service']
            self.connected = True
            logging.info("Connected to MongoDB successfully")
            
            # Create indexes for better performance
            self._create_indexes()
        except Exception as e:
            logging.error(f"Failed to connect to MongoDB: {e}")
            self.connected = False
    
    def _create_indexes(self):
        """Create indexes for better query performance"""
        try:
            # Activity log indexes
            self.db['activity_log'].create_index([('timestamp', -1)])
            self.db['activity_log'].create_index([('status', 1)])
            self.db['activity_log'].create_index([('db_type', 1)])
            
            # Config indexes
            self.db['config'].create_index([('key', 1)], unique=True)
            
            # User indexes
            self.db['users'].create_index([('id', 1)], unique=True)
            self.db['users'].create_index([('email', 1)], unique=True)
            
            # Session indexes
            self.db['sessions'].create_index([('session_id', 1)], unique=True)
            self.db['sessions'].create_index([('user_id', 1)])
            self.db['sessions'].create_index([('expires_at', 1)])
            
            # Merged dashboards indexes
            self.db['merged_dashboards'].create_index([('id', 1)], unique=True)
            self.db['merged_dashboards'].create_index([('created_by.id', 1)])
            self.db['merged_dashboards'].create_index([('type', 1)])
            self.db['merged_dashboards'].create_index([('created_at', -1)])
            
            logging.info("MongoDB indexes created")
        except Exception as e:
            logging.warning(f"Could not create indexes: {e}")
    
    def is_connected(self) -> bool:
        """Check if MongoDB is connected"""
        if not self.connected:
            # Try to reconnect
            self._connect()
        return self.connected
    
    def ensure_connected(self) -> bool:
        """Ensure MongoDB is connected, try to reconnect if not"""
        if not self.connected:
            self._connect()
            return self.connected
        
        # Verify connection is still alive with a ping
        try:
            self.mongo_client.admin.command('ping')
            return True
        except Exception as e:
            logging.warning(f"MongoDB connection lost, reconnecting... ({e})")
            self.connected = False
            self._connect()
            return self.connected
    
    # =========================================================================
    # Configuration Storage
    # =========================================================================
    
    def get_config(self, key: str, default: dict = None) -> dict:
        """Get a configuration value from MongoDB"""
        if not self.ensure_connected():
            logging.warning(f"Cannot get config '{key}': MongoDB not connected")
            return default or {}
        
        try:
            doc = self.db['config'].find_one({'key': key})
            logging.info(f"MongoDB get_config '{key}': found={doc is not None}")
            if doc:
                doc.pop('_id', None)
                doc.pop('key', None)
                value = doc.get('value', default or {})
                logging.info(f"MongoDB get_config '{key}' value: {value}")
                return value
            return default or {}
        except Exception as e:
            logging.error(f"Failed to get config '{key}': {e}")
            return default or {}
    
    def set_config(self, key: str, value: dict) -> bool:
        """Set a configuration value in MongoDB"""
        if not self.ensure_connected():
            logging.error(f"Cannot set config '{key}': MongoDB not connected")
            return False
        
        try:
            result = self.db['config'].update_one(
                {'key': key},
                {'$set': {'key': key, 'value': value, 'updated_at': datetime.now().isoformat()}},
                upsert=True
            )
            logging.info(f"MongoDB set_config '{key}': matched={result.matched_count}, modified={result.modified_count}, upserted={result.upserted_id}")
            return True
        except Exception as e:
            logging.error(f"Failed to set config '{key}': {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def get_metabase_config(self) -> dict:
        """Get Metabase connection configuration"""
        return self.get_config('metabase_config', {
            'base_url': '',
            'username': '',
            'password': ''
        })
    
    def set_metabase_config(self, config: dict) -> bool:
        """Set Metabase connection configuration"""
        return self.set_config('metabase_config', config)
    
    def get_auto_clone_config(self) -> dict:
        """Get auto clone configuration"""
        return self.get_config('auto_clone_config', {
            'source_dashboards': {'content': None, 'message': None, 'email': None},
            'dashboards_collections': {'content': None, 'message': None, 'email': None}
        })
    
    def set_auto_clone_config(self, config: dict) -> bool:
        """Set auto clone configuration"""
        return self.set_config('auto_clone_config', config)
    
    # =========================================================================
    # Activity Log Storage
    # =========================================================================
    
    def add_activity_log(self, entry: dict) -> bool:
        """Add an activity log entry"""
        if not self.ensure_connected():
            logging.error(f"Cannot add activity log - MongoDB not connected")
            return False
        
        try:
            result = self.db['activity_log'].insert_one(entry)
            logging.info(f"Activity log saved: {entry.get('dashboard_name', 'unknown')} - ID: {result.inserted_id}")
            return True
        except Exception as e:
            logging.error(f"Failed to add activity log: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def get_activity_logs(self, limit: int = 500) -> List[dict]:
        """Get activity log entries"""
        if not self.connected:
            return []
        
        try:
            # Fetch entries
            cursor = self.db['activity_log'].find().limit(limit * 2)
            entries = []
            for doc in cursor:
                doc.pop('_id', None)
                entries.append(doc)
            
            # Sort by timestamp properly (normalize Z suffix for consistent sorting)
            def normalize_timestamp(ts):
                # Remove Z suffix for consistent string sorting
                return ts.rstrip('Z') if ts else ''
            
            entries.sort(key=lambda x: normalize_timestamp(x.get('timestamp', '')), reverse=True)
            
            return entries[:limit]
        except Exception as e:
            logging.error(f"Failed to get activity logs: {e}")
            return []
    
    def get_activity_log_count(self) -> int:
        """Get total count of activity log entries"""
        if not self.connected:
            return 0
        
        try:
            return self.db['activity_log'].count_documents({})
        except Exception as e:
            logging.error(f"Failed to count activity logs: {e}")
            return 0
    
    def get_activity_stats(self) -> dict:
        """Get activity log statistics"""
        if not self.connected:
            return {"total": 0, "success": 0, "failed": 0, "deleted": 0, "by_type": {"content": 0, "message": 0, "email": 0}}
        
        try:
            pipeline = [
                {
                    '$group': {
                        '_id': None,
                        'total': {'$sum': 1},
                        'success': {'$sum': {'$cond': [{'$eq': ['$status', 'success']}, 1, 0]}},
                        'deleted': {'$sum': {'$cond': [{'$eq': ['$status', 'deleted']}, 1, 0]}},
                        'failed': {'$sum': {'$cond': [{'$eq': ['$status', 'failed']}, 1, 0]}},
                        'content': {'$sum': {'$cond': [{'$and': [{'$eq': ['$status', 'success']}, {'$eq': ['$db_type', 'content']}]}, 1, 0]}},
                        'message': {'$sum': {'$cond': [{'$and': [{'$eq': ['$status', 'success']}, {'$eq': ['$db_type', 'message']}]}, 1, 0]}},
                        'email': {'$sum': {'$cond': [{'$and': [{'$eq': ['$status', 'success']}, {'$eq': ['$db_type', 'email']}]}, 1, 0]}}
                    }
                }
            ]
            result = list(self.db['activity_log'].aggregate(pipeline))
            if result:
                r = result[0]
                return {
                    "total": r.get('total', 0),
                    "success": r.get('success', 0),
                    "failed": r.get('failed', 0),
                    "deleted": r.get('deleted', 0),
                    "by_type": {
                        "content": r.get('content', 0),
                        "message": r.get('message', 0),
                        "email": r.get('email', 0)
                    }
                }
            return {"total": 0, "success": 0, "failed": 0, "deleted": 0, "by_type": {"content": 0, "message": 0, "email": 0}}
        except Exception as e:
            logging.error(f"Failed to get activity stats: {e}")
            return {"total": 0, "success": 0, "failed": 0, "deleted": 0, "by_type": {"content": 0, "message": 0, "email": 0}}
    
    # =========================================================================
    # Database Identification Results Storage
    # =========================================================================
    
    def save_db_identification_results(self, results: dict) -> bool:
        """Save database identification results"""
        return self.set_config('db_identification_results', results)
    
    def get_db_identification_results(self) -> dict:
        """Get database identification results"""
        return self.get_config('db_identification_results', {
            'content': [], 'message': [], 'email': [], 'unknown': []
        })
    
    # =========================================================================
    # Dashboard Coverage Storage
    # =========================================================================
    
    def save_dashboard_coverage(self, coverage: dict) -> bool:
        """Save dashboard coverage data"""
        return self.set_config('dashboard_coverage', coverage)
    
    def get_dashboard_coverage(self) -> dict:
        """Get dashboard coverage data"""
        return self.get_config('dashboard_coverage', {
            'databases_with_dashboards': {},
            'databases_without_dashboards': {}
        })
    
    # =========================================================================
    # User Authentication Storage
    # =========================================================================
    
    def create_or_update_user(self, user_data: dict) -> bool:
        """Create or update a user on login"""
        if not self.ensure_connected():
            logging.error("Cannot create/update user - MongoDB not connected")
            return False
        
        try:
            user_id = user_data.get('id')
            now = datetime.utcnow().isoformat() + 'Z'
            
            # Check if user exists
            existing = self.db['users'].find_one({'id': user_id})
            
            if existing:
                # Update last login
                self.db['users'].update_one(
                    {'id': user_id},
                    {'$set': {
                        'name': user_data.get('name'),
                        'email': user_data.get('email'),
                        'last_login': now
                    }}
                )
            else:
                # Create new user
                user_data['first_login'] = now
                user_data['last_login'] = now
                self.db['users'].insert_one(user_data)
            
            logging.info(f"User logged in: {user_data.get('email')}")
            return True
        except Exception as e:
            logging.error(f"Failed to create/update user: {e}")
            return False
    
    def get_user(self, user_id: str) -> Optional[dict]:
        """Get user by ID"""
        if not self.ensure_connected():
            return None
        
        try:
            doc = self.db['users'].find_one({'id': user_id})
            if doc:
                doc.pop('_id', None)
            return doc
        except Exception as e:
            logging.error(f"Failed to get user: {e}")
            return None
    
    def create_session(self, user_id: str, user_email: str, user_name: str) -> Optional[str]:
        """Create a new session for a user"""
        if not self.ensure_connected():
            return None
        
        try:
            import uuid
            session_id = str(uuid.uuid4())
            now = datetime.utcnow()
            expires = now + timedelta(days=7)  # Session expires in 7 days
            
            session_doc = {
                'session_id': session_id,
                'user_id': user_id,
                'user_email': user_email,
                'user_name': user_name,
                'created_at': now.isoformat() + 'Z',
                'expires_at': expires.isoformat() + 'Z'
            }
            
            self.db['sessions'].insert_one(session_doc)
            logging.info(f"Session created for user: {user_email}")
            return session_id
        except Exception as e:
            logging.error(f"Failed to create session: {e}")
            return None
    
    def get_session(self, session_id: str) -> Optional[dict]:
        """Get and validate a session"""
        if not self.ensure_connected():
            return None
        
        try:
            doc = self.db['sessions'].find_one({'session_id': session_id})
            if not doc:
                return None
            
            # Check if expired
            expires_at = doc.get('expires_at', '')
            if expires_at:
                expires = datetime.fromisoformat(expires_at.rstrip('Z'))
                if datetime.utcnow() > expires:
                    # Session expired, delete it
                    self.db['sessions'].delete_one({'session_id': session_id})
                    return None
            
            doc.pop('_id', None)
            return doc
        except Exception as e:
            logging.error(f"Failed to get session: {e}")
            return None
    
    def delete_session(self, session_id: str) -> bool:
        """Delete a session (logout)"""
        if not self.ensure_connected():
            return False
        
        try:
            self.db['sessions'].delete_one({'session_id': session_id})
            return True
        except Exception as e:
            logging.error(f"Failed to delete session: {e}")
            return False
    
    def cleanup_expired_sessions(self):
        """Remove expired sessions"""
        if not self.ensure_connected():
            return
        
        try:
            now = datetime.utcnow().isoformat() + 'Z'
            result = self.db['sessions'].delete_many({
                'expires_at': {'$lt': now}
            })
            if result.deleted_count > 0:
                logging.info(f"Cleaned up {result.deleted_count} expired sessions")
        except Exception as e:
            logging.error(f"Failed to cleanup sessions: {e}")
    
    # =========================================================================
    # Merged Dashboards Storage
    # =========================================================================
    
    def save_merged_dashboard(self, data: dict) -> Optional[str]:
        """Save a new merged dashboard configuration"""
        if not self.ensure_connected():
            logging.error("Cannot save merged dashboard - MongoDB not connected")
            return None
        
        try:
            import uuid
            dashboard_id = str(uuid.uuid4())
            now = datetime.utcnow().isoformat() + 'Z'
            
            doc = {
                'id': dashboard_id,
                'name': data.get('name', 'Unnamed Merged Dashboard'),
                'type': data.get('type'),  # content, message, or email
                'source_dashboards': data.get('source_dashboards', []),
                'created_by': data.get('created_by', {}),
                'created_at': now,
                'updated_at': now
            }
            
            self.db['merged_dashboards'].insert_one(doc)
            logging.info(f"Merged dashboard created: {doc['name']} (ID: {dashboard_id})")
            return dashboard_id
        except Exception as e:
            logging.error(f"Failed to save merged dashboard: {e}")
            return None
    
    def get_merged_dashboards(self, user_id: str = None) -> List[dict]:
        """Get all merged dashboards, optionally filtered by user"""
        if not self.ensure_connected():
            return []
        
        try:
            query = {}
            if user_id:
                query['created_by.id'] = user_id
            
            cursor = self.db['merged_dashboards'].find(query).sort('created_at', -1)
            dashboards = []
            for doc in cursor:
                doc.pop('_id', None)
                dashboards.append(doc)
            return dashboards
        except Exception as e:
            logging.error(f"Failed to get merged dashboards: {e}")
            return []
    
    def get_merged_dashboard(self, dashboard_id: str) -> Optional[dict]:
        """Get a single merged dashboard by ID"""
        if not self.ensure_connected():
            return None
        
        try:
            doc = self.db['merged_dashboards'].find_one({'id': dashboard_id})
            if doc:
                doc.pop('_id', None)
            return doc
        except Exception as e:
            logging.error(f"Failed to get merged dashboard: {e}")
            return None
    
    def delete_merged_dashboard(self, dashboard_id: str) -> bool:
        """Delete a merged dashboard"""
        if not self.ensure_connected():
            return False
        
        try:
            result = self.db['merged_dashboards'].delete_one({'id': dashboard_id})
            if result.deleted_count > 0:
                logging.info(f"Merged dashboard deleted: {dashboard_id}")
                return True
            return False
        except Exception as e:
            logging.error(f"Failed to delete merged dashboard: {e}")
            return False


# Global MongoDB storage instance
mongo_storage = MongoDBStorage()


# =============================================================================
# Activity Log Entry
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
    status: str  # "success", "failed", "deleted", or "updated"
    error_message: Optional[str] = None
    performed_by: Optional[str] = "auto-clone"  # "auto-clone", "manual-run", or user email


class ActivityLog:
    """Manages the activity log for dashboard creation - uses MongoDB"""
    
    def __init__(self):
        self.storage = mongo_storage
    
    def add_entry(self, entry: ActivityLogEntry):
        """Add a new entry to the log"""
        success = self.storage.add_activity_log(asdict(entry))
        if not success:
            logging.error(f"FAILED to save activity log for: {entry.dashboard_name}")
        return success
    
    def get_entries(self, limit: int = 500) -> List[dict]:
        """Get log entries as dictionaries"""
        return self.storage.get_activity_logs(limit)
    
    def get_total_count(self) -> int:
        """Get total count of log entries"""
        return self.storage.get_activity_log_count()
    
    def get_stats(self) -> dict:
        """Get statistics from the log"""
        return self.storage.get_activity_stats()


# =============================================================================
# Dashboard Service
# =============================================================================

class DashboardService:
    """Main service that runs the auto-clone process"""
    
    def __init__(self):
        self.storage = mongo_storage
        self.activity_log = ActivityLog()
        self.last_run: Optional[datetime] = None
        self.next_run: Optional[datetime] = None
        self.is_running = False
        self.stop_requested = False  # Flag to stop the current run
        self.is_manual_run = False  # Flag to track if run was triggered manually
        self.current_status = "Idle"
        self.metabase_config = None
        self.auto_config = None
        self.base_url = ""
        
        # Load configs from MongoDB
        self._load_configs()
    
    def _load_configs(self):
        """Load configuration from MongoDB"""
        # Load Metabase config from MongoDB
        self.metabase_config = self.storage.get_metabase_config()
        
        if self.metabase_config.get('base_url'):
                self.base_url = self.metabase_config['base_url'].rstrip('/')
            logging.info("Loaded Metabase config from MongoDB")
        else:
            # Fall back to environment variables if MongoDB config is empty
            metabase_url = os.environ.get('METABASE_URL')
            metabase_username = os.environ.get('METABASE_USERNAME')
            metabase_password = os.environ.get('METABASE_PASSWORD')
            
            if metabase_url and metabase_username and metabase_password:
                self.metabase_config = {
                    'base_url': metabase_url,
                    'username': metabase_username,
                    'password': metabase_password
                }
                self.base_url = metabase_url.rstrip('/')
                # Save to MongoDB for future use
                self.storage.set_metabase_config(self.metabase_config)
                logging.info("Loaded Metabase config from environment variables and saved to MongoDB")
            else:
                logging.warning("Metabase config not found in MongoDB or environment variables")
        
        # Load auto clone config from MongoDB
        self.auto_config = self.storage.get_auto_clone_config()
        logging.info("Loaded auto clone config from MongoDB")
    
    def reload_configs(self):
        """Reload configuration from MongoDB"""
        self._load_configs()
    
    def _get_databases_by_type(self, identifier) -> Dict[str, List]:
        """
        Get databases by type - always scans fresh from Metabase.
        """
        logging.info("Scanning databases (this may take a few minutes)...")
        self.current_status = "Scanning databases..."
        grouped = identifier.get_databases_by_type()
        
        # Log summary
        total = sum(len(dbs) for dbs in grouped.values())
        logging.info(f"Found {total} databases: {len(grouped.get('content', []))} content, "
                    f"{len(grouped.get('message', []))} message, {len(grouped.get('email', []))} email, "
                    f"{len(grouped.get('unknown', []))} unknown")
        
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
            "stop_requested": self.stop_requested,
            "current_status": self.current_status,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "next_run": self.next_run.isoformat() if self.next_run else None,
            "seconds_until_next": seconds_until_next,
            "config_loaded": bool(self.metabase_config and self.auto_config)
        }
    
    def extract_customer_name(self, db_name: str) -> str:
        """Use the exact database name as the customer name.
        No modifications - keeps the name exactly as it is in Metabase."""
        return db_name
    
    def stop_run(self):
        """Request to stop the current run"""
        if self.is_running:
            self.stop_requested = True
            self.current_status = "Stopping..."
            logging.info("Stop requested - will stop after current task completes")
            return True
        return False
    
    def run_check(self):
        """Run the dashboard check and clone process"""
        if self.is_running:
            logging.warning("Check already running, skipping...")
            return
        
        self.is_running = True
        self.stop_requested = False  # Reset stop flag
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
            identifier = DatabaseIdentifier(config=self.metabase_config)
            if not identifier.authenticate():
                self.current_status = "Error: Authentication failed"
                logging.error("Failed to authenticate with Metabase")
                return
            
            # Pass stop check callback so cloner can abort mid-operation
            cloner = DashboardCloner(
                self.metabase_config,
                stop_check_callback=lambda: self.stop_requested
            )
            if not cloner.authenticate():
                self.current_status = "Error: Cloner authentication failed"
                logging.error("Failed to authenticate cloner")
                return
            
            headers = identifier.headers
            
            # Check if stop was requested
            if self.stop_requested:
                self.current_status = "Stopped by user"
                logging.info("Stopped by user before database scan")
                return
            
            # Get databases by type - use cached results if available
            self.current_status = "Loading database info..."
            grouped = self._get_databases_by_type(identifier)
            
            # Check if stop was requested
            if self.stop_requested:
                self.current_status = "Stopped by user"
                logging.info("Stopped by user after database scan")
                return
            
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
                            timestamp=datetime.utcnow().isoformat() + 'Z',
                            database_name="(decomposed)",
                            database_id=0,
                            db_type=db_type,
                            dashboard_name=dash_name,
                            dashboard_id=dash_id,
                            dashboard_url="",
                            status="deleted",
                            error_message="Empty dashboard - database decomposed",
                            performed_by="auto-clone"
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
                # Check if stop was requested
                if self.stop_requested:
                    self.current_status = "Stopped by user"
                    logging.info("="*60)
                    logging.info(f"STOPPED BY USER after {i-1}/{len(tasks)} databases")
                    logging.info("="*60)
                    return
                
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
                    
                    except StopRequested:
                        # User requested stop - exit immediately
                        self.current_status = "Stopped by user"
                        logging.info("="*60)
                        logging.info(f"STOPPED BY USER during clone of {db.name}")
                        logging.info("="*60)
                        return
                    
                    except Exception as e:
                        last_error = str(e)
                        if attempt < MAX_RETRIES:
                            logging.warning(f"  Attempt {attempt} failed: {e}, will retry...")
                        else:
                            logging.error(f"  All {MAX_RETRIES} attempts failed: {e}")
                
                # Log result
                if new_dashboard:
                    # Determine who performed this action
                    if self.is_manual_run:
                        current_user = get_current_user()
                        performed_by = current_user.get('email') if current_user else 'manual-run'
                    else:
                        performed_by = "auto-clone"
                    
                    entry = ActivityLogEntry(
                        timestamp=datetime.utcnow().isoformat() + 'Z',
                        database_name=db.name,
                        database_id=db.id,
                        db_type=task["db_type"],
                        dashboard_name=dashboard_name,
                        dashboard_id=new_dashboard['id'],
                        dashboard_url=f"{self.base_url}/dashboard/{new_dashboard['id']}",
                        status="success",
                        performed_by=performed_by
                    )
                    self.activity_log.add_entry(entry)
                    logging.info(f"SUCCESS: Created {dashboard_name} (ID: {new_dashboard['id']})")
                else:
                    if self.is_manual_run:
                        current_user = get_current_user()
                        performed_by = current_user.get('email') if current_user else 'manual-run'
                    else:
                        performed_by = "auto-clone"
                    
                    entry = ActivityLogEntry(
                        timestamp=datetime.utcnow().isoformat() + 'Z',
                        database_name=db.name,
                        database_id=db.id,
                        db_type=task["db_type"],
                        dashboard_name=dashboard_name,
                        dashboard_id=0,
                        dashboard_url="",
                        status="failed",
                        error_message=f"Failed after {MAX_RETRIES} attempts: {last_error}",
                        performed_by=performed_by
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
            self.is_manual_run = False  # Reset manual run flag
    
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
CORS(app, supports_credentials=True)

# Session secret key for Flask sessions
app.secret_key = os.environ.get('SESSION_SECRET_KEY', 'dev-secret-key-change-in-production')

# Global service instance
service = DashboardService()
scheduler = BackgroundScheduler()

# =============================================================================
# Microsoft OAuth Authentication
# =============================================================================

import msal
import uuid

# OAuth Configuration
MICROSOFT_CLIENT_ID = os.environ.get('MICROSOFT_CLIENT_ID', '')
MICROSOFT_CLIENT_SECRET = os.environ.get('MICROSOFT_CLIENT_SECRET', '')
MICROSOFT_TENANT_ID = os.environ.get('MICROSOFT_TENANT_ID', '')
ALLOWED_EMAIL_DOMAIN = os.environ.get('ALLOWED_EMAIL_DOMAIN', '')

# Microsoft OAuth endpoints
AUTHORITY = f"https://login.microsoftonline.com/{MICROSOFT_TENANT_ID}" if MICROSOFT_TENANT_ID else ""
REDIRECT_PATH = "/api/auth/callback"
SCOPE = ["User.Read"]

def get_msal_app():
    """Create MSAL confidential client application"""
    if not MICROSOFT_CLIENT_ID or not MICROSOFT_CLIENT_SECRET or not MICROSOFT_TENANT_ID:
        return None
    return msal.ConfidentialClientApplication(
        MICROSOFT_CLIENT_ID,
        authority=AUTHORITY,
        client_credential=MICROSOFT_CLIENT_SECRET
    )

def get_current_user():
    """Get current user from session cookie"""
    from flask import request
    session_id = request.cookies.get('session_id')
    if not session_id:
        return None
    session = mongo_storage.get_session(session_id)
    if not session:
        return None
    return {
        'id': session.get('user_id'),
        'email': session.get('user_email'),
        'name': session.get('user_name')
    }

def require_auth(f):
    """Decorator to require authentication for routes"""
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        user = get_current_user()
        if not user:
            return jsonify({"error": "Authentication required"}), 401
        return f(*args, **kwargs)
    return decorated

@app.route('/api/auth/status')
def auth_status():
    """Check if authentication is configured and user's login status"""
    auth_configured = bool(MICROSOFT_CLIENT_ID and MICROSOFT_CLIENT_SECRET and MICROSOFT_TENANT_ID)
    user = get_current_user()
    return jsonify({
        "auth_configured": auth_configured,
        "authenticated": user is not None,
        "user": user,
        "allowed_domain": ALLOWED_EMAIL_DOMAIN if auth_configured else None
    })

@app.route('/api/auth/login')
def auth_login():
    """Start Microsoft OAuth login flow"""
    msal_app = get_msal_app()
    if not msal_app:
        return jsonify({"error": "OAuth not configured. Set MICROSOFT_CLIENT_ID, MICROSOFT_CLIENT_SECRET, and MICROSOFT_TENANT_ID environment variables."}), 500
    
    # Get the redirect URI from the request
    # Force HTTPS in production (when behind a proxy like Render)
    url_root = request.url_root.rstrip('/')
    if url_root.startswith('http://') and not ('localhost' in url_root or '127.0.0.1' in url_root):
        url_root = url_root.replace('http://', 'https://', 1)
    redirect_uri = url_root + REDIRECT_PATH
    
    # Generate auth URL
    auth_url = msal_app.get_authorization_request_url(
        SCOPE,
        redirect_uri=redirect_uri,
        state=str(uuid.uuid4())
    )
    
    return jsonify({"auth_url": auth_url})

@app.route('/api/auth/callback')
def auth_callback():
    """Handle OAuth callback from Microsoft"""
    from flask import redirect, make_response
    
    msal_app = get_msal_app()
    if not msal_app:
        return redirect('/?error=oauth_not_configured')
    
    # Get authorization code from query params
    code = request.args.get('code')
    if not code:
        error = request.args.get('error', 'unknown_error')
        error_desc = request.args.get('error_description', 'No authorization code received')
        logging.error(f"OAuth error: {error} - {error_desc}")
        return redirect(f'/?error={error}')
    
    # Get redirect URI (must match the one used in login)
    # Force HTTPS in production (when behind a proxy like Render)
    url_root = request.url_root.rstrip('/')
    if url_root.startswith('http://') and not ('localhost' in url_root or '127.0.0.1' in url_root):
        url_root = url_root.replace('http://', 'https://', 1)
    redirect_uri = url_root + REDIRECT_PATH
    
    try:
        # Exchange code for token
        result = msal_app.acquire_token_by_authorization_code(
            code,
            scopes=SCOPE,
            redirect_uri=redirect_uri
        )
        
        if 'error' in result:
            logging.error(f"Token error: {result.get('error')} - {result.get('error_description')}")
            return redirect(f"/?error={result.get('error')}")
        
        # Get user info from token claims
        id_token_claims = result.get('id_token_claims', {})
        user_email = id_token_claims.get('preferred_username', '') or id_token_claims.get('email', '')
        user_name = id_token_claims.get('name', '')
        user_id = id_token_claims.get('oid', '') or id_token_claims.get('sub', '')
        
        if not user_email:
            logging.error("No email in token claims")
            return redirect('/?error=no_email')
        
        # Validate email domain
        if ALLOWED_EMAIL_DOMAIN:
            email_domain = user_email.split('@')[-1].lower()
            if email_domain != ALLOWED_EMAIL_DOMAIN.lower():
                logging.warning(f"Access denied for domain: {email_domain} (allowed: {ALLOWED_EMAIL_DOMAIN})")
                return redirect(f'/?error=domain_not_allowed&domain={email_domain}')
        
        # Create or update user in MongoDB
        user_data = {
            'id': user_id,
            'email': user_email,
            'name': user_name
        }
        mongo_storage.create_or_update_user(user_data)
        
        # Create session
        session_id = mongo_storage.create_session(user_id, user_email, user_name)
        if not session_id:
            return redirect('/?error=session_creation_failed')
        
        # Set session cookie and redirect to home
        response = make_response(redirect('/'))
        response.set_cookie(
            'session_id',
            session_id,
            httponly=True,
            secure=request.is_secure,
            samesite='Lax',
            max_age=7 * 24 * 60 * 60  # 7 days
        )
        
        logging.info(f"User logged in successfully: {user_email}")
        return response
        
    except Exception as e:
        logging.error(f"OAuth callback error: {e}")
        import traceback
        traceback.print_exc()
        return redirect(f'/?error=callback_failed')

@app.route('/api/auth/logout', methods=['POST'])
def auth_logout():
    """Logout user by deleting session"""
    from flask import make_response
    
    session_id = request.cookies.get('session_id')
    if session_id:
        mongo_storage.delete_session(session_id)
    
    response = make_response(jsonify({"success": True}))
    response.delete_cookie('session_id')
    return response

@app.route('/api/auth/me')
def auth_me():
    """Get current user info"""
    user = get_current_user()
    if not user:
        return jsonify({"error": "Not authenticated"}), 401
    return jsonify(user)


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


@app.route('/api/test-log')
def test_log():
    """Test endpoint to verify MongoDB logging works"""
    from datetime import datetime
    
    test_entry = ActivityLogEntry(
        timestamp=datetime.utcnow().isoformat() + 'Z',
        database_name="TEST_DATABASE",
        database_id=99999,
        db_type="content",
        dashboard_name="TEST Dashboard - Delete Me",
        dashboard_id=99999,
        dashboard_url="https://test.com",
        status="success"
    )
    
    success = service.activity_log.add_entry(test_entry)
    
    return jsonify({
        "success": success,
        "mongodb_connected": mongo_storage.is_connected(),
        "entry": asdict(test_entry)
    })


@app.route('/api/logs')
def get_logs():
    """Get activity logs"""
    limit = request.args.get('limit', 500, type=int)
    entries = service.activity_log.get_entries(limit)
    total_count = service.activity_log.get_total_count()
    return jsonify({
        "entries": entries,
        "stats": service.activity_log.get_stats(),
        "total_count": total_count,
        "showing": len(entries)
    })


@app.route('/api/dashboards/<db_type>')
def get_dashboards_list(db_type):
    """Get list of dashboards in a _DASHBOARDS collection"""
    if db_type not in ['content', 'message', 'email']:
        return jsonify({"error": "Invalid type. Use: content, message, or email"}), 400
    
    try:
        if not service.metabase_config:
            return jsonify({"error": "Metabase not configured", "dashboards": []})
        
        # Get collection ID from MongoDB config
        auto_config = mongo_storage.get_auto_clone_config()
        dashboards_collections = auto_config.get('dashboards_collections', {})
        col_id = dashboards_collections.get(db_type)
        
        if not col_id:
            return jsonify({"error": f"No collection configured for {db_type}", "dashboards": []})
        
        import requests
        base_url = service.metabase_config['base_url'].rstrip('/')
        
        # Authenticate
        auth_response = requests.post(
            f"{base_url}/api/session",
            json={
                "username": service.metabase_config['username'],
                "password": service.metabase_config['password']
            },
            timeout=10
        )
        if auth_response.status_code != 200:
            return jsonify({"error": "Authentication failed", "dashboards": []})
        
        headers = {"X-Metabase-Session": auth_response.json()["id"]}
        
        # Get dashboards in collection
        items_response = requests.get(
            f"{base_url}/api/collection/{col_id}/items",
            headers=headers,
            timeout=30
        )
        
        if items_response.status_code != 200:
            return jsonify({"error": "Failed to fetch dashboards", "dashboards": []})
        
        items = items_response.json()
        
        # Extract dashboard info
        dashboards = []
        data = items if isinstance(items, list) else items.get('data', [])
        
        for item in data:
            if item.get('model') == 'dashboard':
                dash_id = item.get('id')
                dash_info = {
                    'id': dash_id,
                    'name': item.get('name'),
                    'description': item.get('description', ''),
                    'url': f"{base_url}/dashboard/{dash_id}"
                }
                
                # Check if this dashboard has an active update task
                task_id = dashboard_to_task.get(dash_id)
                if task_id and task_id in update_tasks:
                    task = update_tasks[task_id]
                    if not task.get('completed', False):
                        dash_info['active_update'] = {
                            'task_id': task_id,
                            'progress': task.get('progress', 0),
                            'status': task.get('status', ''),
                            'cancel_requested': task.get('cancel_requested', False)
                        }
                
                dashboards.append(dash_info)
        
        # Sort by name
        dashboards.sort(key=lambda x: x.get('name', '').lower())
        
        return jsonify({
            "type": db_type,
            "collection_id": col_id,
            "count": len(dashboards),
            "dashboards": dashboards,
            "metabase_url": base_url
        })
        
    except Exception as e:
        logging.error(f"Failed to get dashboards list: {e}")
        return jsonify({"error": str(e), "dashboards": []})


# Store for update task progress
update_tasks = {}
# Map dashboard_id to active task_id for tracking updates across page reloads
dashboard_to_task = {}

@app.route('/api/dashboard/update', methods=['POST'])
@require_auth
def update_dashboard():
    """
    Update a dashboard by cloning from source first, then deleting old one only on success.
    This is safer - if cloning fails, the old dashboard remains intact.
    """
    import threading
    import uuid
    
    try:
        data = request.json
        dashboard_id = data.get('dashboard_id')
        dashboard_type = data.get('dashboard_type')
        dashboard_name = data.get('dashboard_name')
        
        if not dashboard_id or not dashboard_type:
            return jsonify({"success": False, "error": "Missing dashboard_id or dashboard_type"}), 400
        
        if dashboard_type not in ['content', 'message', 'email']:
            return jsonify({"success": False, "error": "Invalid dashboard type"}), 400
        
        # Get config
        if not service.metabase_config:
            return jsonify({"success": False, "error": "Metabase not configured"}), 400
        
        auto_config = mongo_storage.get_auto_clone_config()
        source_dashboards = auto_config.get('source_dashboards', {})
        dashboards_collections = auto_config.get('dashboards_collections', {})
        
        source_dashboard_id = source_dashboards.get(dashboard_type)
        dashboards_collection_id = dashboards_collections.get(dashboard_type)
        
        if not source_dashboard_id:
            return jsonify({"success": False, "error": f"No source dashboard configured for {dashboard_type}"}), 400
        
        if not dashboards_collection_id:
            return jsonify({"success": False, "error": f"No dashboards collection configured for {dashboard_type}"}), 400
        
        # Create task ID for progress tracking
        task_id = str(uuid.uuid4())
        
        # Map dashboard to task so we can track it across page reloads
        dashboard_to_task[dashboard_id] = task_id
        
        update_tasks[task_id] = {
            'progress': 0,
            'status': 'Starting update...',
            'completed': False,
            'success': False,
            'cancelled': False,
            'cancel_requested': False,
            'error': None,
            'new_dashboard_id': None,
            'new_url': None,
            'old_dashboard_id': dashboard_id,
            # Track newly created items for cleanup on cancel
            'created_items': {
                'dashboards': [],
                'cards': [],
                'collection_id': None
            }
        }
        
        # Run update in background thread
        def run_update():
            try:
                _execute_dashboard_update(
                    task_id, dashboard_id, dashboard_type, dashboard_name,
                    source_dashboard_id, dashboards_collection_id
                )
            except Exception as e:
                logging.error(f"Update task {task_id} failed: {e}")
                import traceback
                traceback.print_exc()
                update_tasks[task_id]['completed'] = True
                update_tasks[task_id]['success'] = False
                update_tasks[task_id]['error'] = str(e)
        
        thread = threading.Thread(target=run_update)
        thread.start()
        
        return jsonify({"success": True, "task_id": task_id})
        
    except Exception as e:
        logging.error(f"Failed to start dashboard update: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/dashboard/update/cancel/<task_id>', methods=['POST'])
@require_auth
def cancel_dashboard_update(task_id):
    """Cancel a running dashboard update and clean up any created items"""
    task = update_tasks.get(task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404
    
    if task['completed']:
        return jsonify({"error": "Task already completed", "success": task['success']}), 400
    
    # Set cancel flag
    task['cancel_requested'] = True
    task['status'] = 'Cancelling...'
    
    logging.info(f"Cancel requested for update task {task_id}")
    
    return jsonify({"success": True, "message": "Cancel requested"})


def _cleanup_created_items(task, headers, base_url):
    """Clean up any items created during a cancelled or failed update"""
    created = task.get('created_items', {})
    
    # Delete created dashboards
    for dash_id in created.get('dashboards', []):
        try:
            logging.info(f"Cleaning up: deleting dashboard {dash_id}")
            requests.delete(f"{base_url}/api/dashboard/{dash_id}", headers=headers, timeout=10)
        except Exception as e:
            logging.warning(f"Failed to delete dashboard {dash_id}: {e}")
    
    # Delete created cards/questions
    for card_id in created.get('cards', []):
        try:
            logging.info(f"Cleaning up: deleting card {card_id}")
            requests.delete(f"{base_url}/api/card/{card_id}", headers=headers, timeout=10)
        except Exception as e:
            logging.warning(f"Failed to delete card {card_id}: {e}")


def _execute_dashboard_update(task_id, dashboard_id, dashboard_type, dashboard_name,
                               source_dashboard_id, dashboards_collection_id):
    """Execute the dashboard update process - clone first, delete old only on success"""
    import requests
    
    task = update_tasks[task_id]
    headers = None
    base_url = None
    
    def check_cancelled():
        """Check if cancel was requested and handle cleanup"""
        if task.get('cancel_requested'):
            task['status'] = 'Cleaning up cancelled update...'
            if headers and base_url:
                _cleanup_created_items(task, headers, base_url)
            task['cancelled'] = True
            task['completed'] = True
            task['success'] = False
            task['status'] = 'Update cancelled'
            task['error'] = 'Cancelled by user'
            # Clean up mapping
            old_dash_id = task.get('old_dashboard_id')
            if old_dash_id and dashboard_to_task.get(old_dash_id) == task_id:
                del dashboard_to_task[old_dash_id]
            logging.info(f"Update task {task_id} cancelled by user")
            return True
        return False
    
    try:
        base_url = service.metabase_config['base_url'].rstrip('/')
        
        # Step 1: Authenticate
        task['progress'] = 5
        task['status'] = 'Authenticating...'
        
        if check_cancelled():
            return
        
        auth_response = requests.post(
            f"{base_url}/api/session",
            json={
                "username": service.metabase_config['username'],
                "password": service.metabase_config['password']
            },
            timeout=10
        )
        if auth_response.status_code != 200:
            raise Exception("Failed to authenticate with Metabase")
        
        headers = {"X-Metabase-Session": auth_response.json()["id"]}
        
        # Step 2: Get the OLD dashboard info (we need this to find the database)
        task['progress'] = 10
        task['status'] = 'Getting dashboard info...'
        
        if check_cancelled():
            return
        
        dash_response = requests.get(
            f"{base_url}/api/dashboard/{dashboard_id}",
            headers=headers,
            timeout=30
        )
        if dash_response.status_code != 200:
            raise Exception(f"Dashboard {dashboard_id} not found")
        
        old_dashboard_data = dash_response.json()
        old_collection_id = old_dashboard_data.get('collection_id')
        
        # Extract customer name from dashboard name (e.g., "Customer ABC Dashboard" -> "Customer ABC")
        customer_name = dashboard_name
        if dashboard_name.endswith(' Dashboard'):
            customer_name = dashboard_name[:-10]  # Remove " Dashboard"
        
        # Step 3: Find the target database from the OLD dashboard's cards
        task['progress'] = 15
        task['status'] = 'Finding target database...'
        
        if check_cancelled():
            return
        
        # Get database ID from old dashboard's cards
        target_database_id = None
        old_dashcards = old_dashboard_data.get('dashcards', []) or old_dashboard_data.get('ordered_cards', [])
        for dc in old_dashcards:
            card = dc.get('card', {})
            if card and card.get('database_id'):
                target_database_id = card.get('database_id')
                break
        
        # If not found in cards, search by customer name
        if not target_database_id:
            from db_identifier import DatabaseIdentifier
            identifier = DatabaseIdentifier(service.metabase_config)
            if identifier.authenticate():
                grouped = identifier.get_databases_by_type()
                for db in grouped.get(dashboard_type, []):
                    db_customer = service.extract_customer_name(db.name)
                    if db_customer and db_customer.lower() == customer_name.lower():
                        target_database_id = db.id
                        logging.info(f"Found target database by name: {db.name} (ID: {db.id})")
                        break
                    if customer_name.lower() in db.name.lower():
                        target_database_id = db.id
                        logging.info(f"Found target database (fuzzy): {db.name} (ID: {db.id})")
                        break
        
        if not target_database_id:
            raise Exception(f"Could not find database for customer: {customer_name}")
        
        logging.info(f"Target database ID: {target_database_id}")
        
        # Step 4: Find the OLD customer collection (where questions are stored)
        task['progress'] = 20
        task['status'] = 'Finding customer collection...'
        
        if check_cancelled():
            return
        
        old_questions_collection_id = None
        collection_name = f"{customer_name} Collection"
        collections_response = requests.get(
            f"{base_url}/api/collection",
            headers=headers,
            timeout=30
        )
        if collections_response.status_code == 200:
            all_collections = collections_response.json()
            for col in all_collections:
                if col.get('name', '').lower() == collection_name.lower():
                    old_questions_collection_id = col.get('id')
                    logging.info(f"Found old customer collection: {collection_name} (ID: {old_questions_collection_id})")
                    break
        
        # Step 5: Clone the NEW dashboard (this is the safe part - old dashboard still exists)
        task['progress'] = 30
        task['status'] = 'Cloning new dashboard...'
        
        if check_cancelled():
            return
        
        cloner = DashboardCloner(
            service.metabase_config,
            stop_check_callback=lambda: task.get('cancel_requested', False)
        )
        if not cloner.authenticate():
            raise Exception("Failed to authenticate cloner")
        
        # Get source dashboard's parent collection for the customer collection
        source_parent = cloner.get_dashboard_collection_id(source_dashboard_id)
        
        # Create a NEW customer collection with a temporary suffix to avoid conflicts
        temp_collection_name = f"{customer_name} Collection (updating)"
        new_customer_col = cloner.create_collection(temp_collection_name, source_parent)
        new_questions_collection_id = new_customer_col['id'] if new_customer_col else None
        
        if new_questions_collection_id:
            task['created_items']['collection_id'] = new_questions_collection_id
        
        task['progress'] = 40
        task['status'] = 'Analyzing linked dashboards...'
        
        if check_cancelled():
            return
        
        # Check for linked dashboards
        all_linked = cloner.find_all_linked_dashboards(source_dashboard_id)
        
        new_dashboard = None
        new_dashboard_name = f"{customer_name} Dashboard"
        
        task['progress'] = 50
        task['status'] = 'Creating new dashboard...'
        
        if check_cancelled():
            return
        
        try:
            if all_linked:
                task['status'] = f'Cloning dashboard with {len(all_linked)} linked dashboards...'
                new_dashboard = cloner.clone_with_all_linked(
                    source_dashboard_id=source_dashboard_id,
                    new_name=new_dashboard_name,
                    new_database_id=target_database_id,
                    dashboard_collection_id=new_questions_collection_id,
                    questions_collection_id=new_questions_collection_id,
                    main_dashboard_collection_id=dashboards_collection_id
                )
            else:
                new_dashboard = cloner.clone_dashboard(
                    source_dashboard_id=source_dashboard_id,
                    new_name=new_dashboard_name,
                    new_database_id=target_database_id,
                    dashboard_collection_id=dashboards_collection_id,
                    questions_collection_id=new_questions_collection_id
                )
        except StopRequested:
            # Clone was cancelled
            task['status'] = 'Cleaning up cancelled update...'
            _cleanup_created_items(task, headers, base_url)
            # Also delete the temp collection
            if new_questions_collection_id:
                try:
                    requests.delete(f"{base_url}/api/collection/{new_questions_collection_id}", headers=headers, timeout=10)
                except:
                    pass
            task['cancelled'] = True
            task['completed'] = True
            task['success'] = False
            task['status'] = 'Update cancelled'
            task['error'] = 'Cancelled by user'
            return
        
        if check_cancelled():
            return
        
        if not new_dashboard:
            raise Exception("Failed to clone dashboard")
        
        new_dashboard_id = new_dashboard.get('id')
        task['created_items']['dashboards'].append(new_dashboard_id)
        
        # Track all created questions from the cloner
        for old_id, new_id in cloner.question_mapping.items():
            task['created_items']['cards'].append(new_id)
        
        # Track all created linked dashboards
        for old_id, new_id in cloner.dashboard_mapping.items():
            if new_id != new_dashboard_id:
                task['created_items']['dashboards'].append(new_id)
        
        task['progress'] = 70
        task['status'] = 'New dashboard created successfully!'
        
        if check_cancelled():
            return
        
        # ============================================================
        # SUCCESS! Now safe to delete the OLD dashboard and questions
        # ============================================================
        
        task['progress'] = 75
        task['status'] = 'Cleaning up old dashboard...'
        
        # Delete old dashboard
        try:
            delete_response = requests.delete(
                f"{base_url}/api/dashboard/{dashboard_id}",
                headers=headers,
                timeout=30
            )
            if delete_response.status_code in [200, 204]:
                logging.info(f"Deleted old dashboard {dashboard_id}")
            else:
                logging.warning(f"Failed to delete old dashboard {dashboard_id}: {delete_response.status_code}")
        except Exception as e:
            logging.warning(f"Error deleting old dashboard: {e}")
        
        task['progress'] = 80
        task['status'] = 'Cleaning up old questions...'
        
        # Delete old questions in the old customer collection
        if old_questions_collection_id:
            try:
                items_response = requests.get(
                    f"{base_url}/api/collection/{old_questions_collection_id}/items",
                    headers=headers,
                    timeout=30
                )
                if items_response.status_code == 200:
                    items = items_response.json()
                    items_data = items if isinstance(items, list) else items.get('data', [])
                    
                    for item in items_data:
                        if item.get('model') == 'card':
                            card_id = item.get('id')
                            try:
                                requests.delete(f"{base_url}/api/card/{card_id}", headers=headers, timeout=10)
                            except:
                                pass
                        elif item.get('model') == 'dashboard':
                            linked_dash_id = item.get('id')
                            try:
                                requests.delete(f"{base_url}/api/dashboard/{linked_dash_id}", headers=headers, timeout=10)
                            except:
                                pass
                
                # Delete the old collection itself
                try:
                    requests.delete(f"{base_url}/api/collection/{old_questions_collection_id}", headers=headers, timeout=10)
                    logging.info(f"Deleted old collection {old_questions_collection_id}")
                except:
                    pass
                    
            except Exception as e:
                logging.warning(f"Error cleaning up old customer collection: {e}")
        
        task['progress'] = 85
        task['status'] = 'Renaming new collection...'
        
        # Rename the temp collection to the proper name
        if new_questions_collection_id:
            try:
                requests.put(
                    f"{base_url}/api/collection/{new_questions_collection_id}",
                    headers=headers,
                    json={"name": collection_name},
                    timeout=10
                )
                logging.info(f"Renamed collection to: {collection_name}")
            except Exception as e:
                logging.warning(f"Failed to rename collection: {e}")
        
        task['progress'] = 90
        task['status'] = 'Finalizing...'
        
        new_url = f"{base_url}/dashboard/{new_dashboard_id}"
        
        # Get database info for logging
        db_name = customer_name
        try:
            db_response = requests.get(f"{base_url}/api/database/{target_database_id}", headers=headers, timeout=10)
            if db_response.status_code == 200:
                db_name = db_response.json().get('name', customer_name)
        except:
            pass
        
        # Log the activity
        current_user = get_current_user()
        performed_by = current_user.get('email') if current_user else 'manual-update'
        
        entry = ActivityLogEntry(
            timestamp=datetime.utcnow().isoformat() + 'Z',
            database_name=db_name,
            database_id=target_database_id,
            db_type=dashboard_type,
            dashboard_name=new_dashboard_name,
            dashboard_id=new_dashboard_id,
            dashboard_url=new_url,
            status="updated",
            error_message=None,
            performed_by=performed_by
        )
        service.activity_log.add_entry(entry)
        
        # Clear created items since update was successful
        task['created_items'] = {'dashboards': [], 'cards': [], 'collection_id': None}
        
        task['progress'] = 100
        task['status'] = 'Update complete!'
        task['completed'] = True
        task['success'] = True
        task['new_dashboard_id'] = new_dashboard_id
        task['new_url'] = new_url
        
        # Clean up mapping when complete (keep it for a bit so user can see final status)
        # We'll clean it up after they fetch the status
        
        logging.info(f"Dashboard update complete: {dashboard_name} -> ID {new_dashboard_id}")
        
    except Exception as e:
        logging.error(f"Dashboard update failed: {e}")
        import traceback
        traceback.print_exc()
        
        # Clean up any created items on failure
        if headers and base_url:
            task['status'] = 'Cleaning up after error...'
            _cleanup_created_items(task, headers, base_url)
            # Also delete the temp collection if created
            temp_col_id = task.get('created_items', {}).get('collection_id')
            if temp_col_id:
                try:
                    requests.delete(f"{base_url}/api/collection/{temp_col_id}", headers=headers, timeout=10)
                except:
                    pass
        
        task['completed'] = True
        task['success'] = False
        task['error'] = str(e)
        task['status'] = f'Error: {str(e)}'
        
        # Clean up mapping on error
        old_dash_id = task.get('old_dashboard_id')
        if old_dash_id and dashboard_to_task.get(old_dash_id) == task_id:
            del dashboard_to_task[old_dash_id]


@app.route('/api/dashboard/update/status/<task_id>')
@require_auth
def get_update_status(task_id):
    """Get the status of a dashboard update task"""
    task = update_tasks.get(task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404
    
    response = {
        "progress": task['progress'],
        "status": task['status'],
        "completed": task['completed'],
        "success": task['success'],
        "cancelled": task.get('cancelled', False),
        "error": task['error'],
        "new_dashboard_id": task.get('new_dashboard_id'),
        "new_url": task.get('new_url'),
        "old_dashboard_id": task.get('old_dashboard_id')
    }
    
    # Clean up mapping after successful completion (user has fetched final status)
    if task['completed'] and task['success']:
        old_dash_id = task.get('old_dashboard_id')
        if old_dash_id and dashboard_to_task.get(old_dash_id) == task_id:
            del dashboard_to_task[old_dash_id]
    
    return jsonify(response)


@app.route('/api/dashboard/delete/<int:dashboard_id>', methods=['POST'])
@require_auth
def delete_dashboard_endpoint(dashboard_id):
    """Delete a dashboard and its associated questions/collection"""
    import requests
    
    try:
        data = request.json or {}
        dashboard_type = data.get('dashboard_type')
        dashboard_name = data.get('dashboard_name', f'Dashboard {dashboard_id}')
        
        # Get current user
        current_user = get_current_user()
        user_email = current_user.get('email') if current_user else 'unknown'
        
        if not service.metabase_config:
            return jsonify({"success": False, "error": "Metabase not configured"}), 400
        
        base_url = service.metabase_config['base_url'].rstrip('/')
        
        # Authenticate
        auth_response = requests.post(
            f"{base_url}/api/session",
            json={
                "username": service.metabase_config['username'],
                "password": service.metabase_config['password']
            },
            timeout=10
        )
        if auth_response.status_code != 200:
            return jsonify({"success": False, "error": "Failed to authenticate"}), 500
        
        headers = {"X-Metabase-Session": auth_response.json()["id"]}
        
        # Get dashboard info before deleting
        dash_response = requests.get(
            f"{base_url}/api/dashboard/{dashboard_id}",
            headers=headers,
            timeout=30
        )
        
        if dash_response.status_code != 200:
            return jsonify({"success": False, "error": f"Dashboard {dashboard_id} not found"}), 404
        
        dashboard_data = dash_response.json()
        collection_id = dashboard_data.get('collection_id')
        
        # Extract customer name from dashboard name
        customer_name = dashboard_name
        if dashboard_name.endswith(' Dashboard'):
            customer_name = dashboard_name[:-10]
        
        # Find customer collection
        customer_collection_id = None
        collection_name = f"{customer_name} Collection"
        collections_response = requests.get(
            f"{base_url}/api/collection",
            headers=headers,
            timeout=30
        )
        if collections_response.status_code == 200:
            all_collections = collections_response.json()
            for col in all_collections:
                if col.get('name', '').lower() == collection_name.lower():
                    customer_collection_id = col.get('id')
                    logging.info(f"Found customer collection: {collection_name} (ID: {customer_collection_id})")
                    break
        
        # Delete dashboard
        delete_response = requests.delete(
            f"{base_url}/api/dashboard/{dashboard_id}",
            headers=headers,
            timeout=30
        )
        if delete_response.status_code not in [200, 204]:
            return jsonify({"success": False, "error": f"Failed to delete dashboard: {delete_response.status_code}"}), 500
        
        logging.info(f"Deleted dashboard {dashboard_id}: {dashboard_name}")
        
        # Delete questions in customer collection
        if customer_collection_id:
            try:
                items_response = requests.get(
                    f"{base_url}/api/collection/{customer_collection_id}/items",
                    headers=headers,
                    timeout=30
                )
                if items_response.status_code == 200:
                    items = items_response.json()
                    items_data = items if isinstance(items, list) else items.get('data', [])
                    
                    for item in items_data:
                        if item.get('model') == 'card':
                            card_id = item.get('id')
                            try:
                                requests.delete(f"{base_url}/api/card/{card_id}", headers=headers, timeout=10)
                            except:
                                pass
                        elif item.get('model') == 'dashboard':
                            linked_dash_id = item.get('id')
                            try:
                                requests.delete(f"{base_url}/api/dashboard/{linked_dash_id}", headers=headers, timeout=10)
                            except:
                                pass
                
                # Delete the collection itself
                try:
                    requests.delete(f"{base_url}/api/collection/{customer_collection_id}", headers=headers, timeout=10)
                    logging.info(f"Deleted customer collection {customer_collection_id}")
                except:
                    pass
                    
            except Exception as e:
                logging.warning(f"Error cleaning up customer collection: {e}")
        
        # Log the deletion
        entry = ActivityLogEntry(
            timestamp=datetime.utcnow().isoformat() + 'Z',
            database_name=customer_name,
            database_id=0,  # We don't have the database ID anymore
            db_type=dashboard_type or 'unknown',
            dashboard_name=dashboard_name,
            dashboard_id=dashboard_id,
            dashboard_url="",
            status="deleted",
            error_message=None,
            performed_by=user_email
        )
        service.activity_log.add_entry(entry)
        
        return jsonify({"success": True, "message": "Dashboard deleted successfully"})
        
    except Exception as e:
        logging.error(f"Failed to delete dashboard: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/dashboard/rename/<int:dashboard_id>', methods=['POST'])
@require_auth
def rename_dashboard_endpoint(dashboard_id):
    """Rename a dashboard in Metabase"""
    import requests
    
    try:
        data = request.json or {}
        new_name = data.get('new_name', '').strip()
        dashboard_type = data.get('dashboard_type')
        
        if not new_name:
            return jsonify({"success": False, "error": "New name is required"}), 400

        # Get current user for logging
        current_user = get_current_user()
        user_email = current_user.get('email') if current_user else 'unknown'

        if not service.metabase_config:
            return jsonify({"success": False, "error": "Metabase not configured"}), 400

        base_url = service.metabase_config['base_url'].rstrip('/')

        # Authenticate with Metabase
        auth_response = requests.post(
            f"{base_url}/api/session",
            json={
                "username": service.metabase_config['username'],
                "password": service.metabase_config['password']
            },
            timeout=10
        )
        if auth_response.status_code != 200:
            return jsonify({"success": False, "error": "Failed to authenticate"}), 500

        headers = {"X-Metabase-Session": auth_response.json()["id"]}

        # Get current dashboard details
        dash_response = requests.get(f"{base_url}/api/dashboard/{dashboard_id}", headers=headers, timeout=10)
        if dash_response.status_code != 200:
            return jsonify({"success": False, "error": "Dashboard not found"}), 404

        dashboard = dash_response.json()
        old_name = dashboard.get('name', 'Unknown')
        
        # Update dashboard name
        update_response = requests.put(
            f"{base_url}/api/dashboard/{dashboard_id}",
            headers=headers,
            json={"name": new_name},
            timeout=10
        )
        
        if update_response.status_code != 200:
            return jsonify({"success": False, "error": f"Failed to update dashboard: {update_response.status_code}"}), 500

        # Log the rename action
        collection_name = "Unknown"
        if dashboard.get('collection_id'):
            coll_response = requests.get(
                f"{base_url}/api/collection/{dashboard['collection_id']}", 
                headers=headers, 
                timeout=10
            )
            if coll_response.status_code == 200:
                collection_name = coll_response.json().get('name', 'Unknown')
        
        entry = ActivityLogEntry(
            timestamp=datetime.utcnow().isoformat() + 'Z',
            database_name=collection_name,
            database_id=0,
            db_type=dashboard_type or 'unknown',
            dashboard_name=f"{old_name} → {new_name}",
            dashboard_id=dashboard_id,
            dashboard_url=f"{base_url}/dashboard/{dashboard_id}",
            status="renamed",
            error_message=None,
            performed_by=user_email
        )
        service.activity_log.add_entry(entry)

        return jsonify({"success": True, "message": "Dashboard renamed successfully", "new_name": new_name})

    except Exception as e:
        logging.error(f"Failed to rename dashboard: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/dashboard-counts')
def get_dashboard_counts():
    """Get dashboard counts from _DASHBOARDS collections using IDs from MongoDB config"""
    try:
        if not service.metabase_config:
            return jsonify({"content": 0, "message": 0, "email": 0, "total": 0})
        
        # Get collection IDs from MongoDB config
        auto_config = mongo_storage.get_auto_clone_config()
        dashboards_collections = auto_config.get('dashboards_collections', {})
        
        if not any(dashboards_collections.values()):
            return jsonify({"content": 0, "message": 0, "email": 0, "total": 0})
        
        import requests
        base_url = service.metabase_config['base_url'].rstrip('/')
        
        # Authenticate
        auth_response = requests.post(
            f"{base_url}/api/session",
            json={
                "username": service.metabase_config['username'],
                "password": service.metabase_config['password']
            },
            timeout=10
        )
        if auth_response.status_code != 200:
            return jsonify({"content": 0, "message": 0, "email": 0, "total": 0})
        
        headers = {"X-Metabase-Session": auth_response.json()["id"]}
        
        counts = {'content': 0, 'message': 0, 'email': 0, 'total': 0}
        
        # Count dashboards in each collection using the stored IDs
        for db_type in ['content', 'message', 'email']:
            col_id = dashboards_collections.get(db_type)
            if col_id:
                try:
                    items_response = requests.get(
                        f"{base_url}/api/collection/{col_id}/items",
                        headers=headers,
                        timeout=10
                    )
                    if items_response.status_code == 200:
                        items = items_response.json()
                        # Handle both list response and dict with 'data' key
                        if isinstance(items, list):
                            counts[db_type] = len(items)
                        elif isinstance(items, dict) and 'data' in items:
                            counts[db_type] = len(items['data'])
                        elif isinstance(items, dict) and 'total' in items:
                            counts[db_type] = items['total']
                except Exception as e:
                    logging.error(f"Failed to count {db_type}: {e}")
        
        counts['total'] = counts['content'] + counts['message'] + counts['email']
        return jsonify(counts)
        
    except Exception as e:
        logging.error(f"Failed to get dashboard counts: {e}")
        return jsonify({"content": 0, "message": 0, "email": 0, "total": 0})


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
@require_auth
def trigger_run():
    """Manually trigger a check"""
    if service.is_running:
        return jsonify({"error": "Check already running"}), 400
    
    # Set manual run flag
    service.is_manual_run = True
    
    # Run in background thread
    thread = threading.Thread(target=service.run_check)
    thread.start()
    
    return jsonify({"message": "Check started"})


@app.route('/api/stop', methods=['POST'])
def stop_run():
    """Stop the current running check"""
    if not service.is_running:
        return jsonify({"error": "No check is currently running"}), 400
    
    if service.stop_run():
        return jsonify({"message": "Stop requested - will stop after current task"})
    else:
        return jsonify({"error": "Could not stop"}), 500


@app.route('/api/refresh-cache', methods=['POST'])
def refresh_cache():
    """Force refresh the database identification cache in MongoDB"""
    try:
        # Clear the cached results in MongoDB
        mongo_storage.save_db_identification_results({
            'content': [], 'message': [], 'email': [], 'unknown': []
        })
            return jsonify({"message": "Cache cleared. Next run will rescan databases."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/databases')
def get_databases():
    """Get database identification results from MongoDB"""
    try:
        data = mongo_storage.get_db_identification_results()
        
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
    """Get all settings from MongoDB"""
    try:
        # Check MongoDB connection
        if not mongo_storage.is_connected():
            logging.warning("MongoDB not connected when getting settings")
            return jsonify({
                "error": "MongoDB not connected",
                "metabase": {"base_url": "", "username": "", "password": ""},
                "source_dashboards": {"content": None, "message": None, "email": None},
                "dashboards_collections": {"content": None, "message": None, "email": None}
            })
        
        # Load metabase config from MongoDB
        metabase_config = mongo_storage.get_metabase_config()
        logging.info(f"Loaded metabase config: {metabase_config.get('base_url', 'N/A')}, {metabase_config.get('username', 'N/A')}")
        
        # Load auto clone config from MongoDB
        auto_config = mongo_storage.get_auto_clone_config()
        logging.info(f"Loaded auto config: {auto_config}")
        
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
        logging.error(f"Error getting settings: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/settings', methods=['POST'])
def save_settings():
    """Save all settings to MongoDB"""
    try:
        # Check MongoDB connection first
        if not mongo_storage.is_connected():
            return jsonify({"error": "MongoDB is not connected. Please check MONGODB_URI environment variable."}), 500
        
        data = request.json
        logging.info(f"Saving settings: {data}")
        
        # Save metabase config to MongoDB
        if 'metabase' in data:
            metabase_data = data['metabase']
            
            # Load existing config to preserve password if not changed
            existing_config = mongo_storage.get_metabase_config()
            
            new_config = {
                "base_url": metabase_data.get('base_url', existing_config.get('base_url', '')),
                "username": metabase_data.get('username', existing_config.get('username', '')),
                "password": metabase_data.get('password') if metabase_data.get('password') and metabase_data.get('password') != '********' else existing_config.get('password', '')
            }
            
            if not mongo_storage.set_metabase_config(new_config):
                return jsonify({"error": "Failed to save Metabase config to MongoDB"}), 500
            logging.info(f"Saved metabase config: {new_config['base_url']}, {new_config['username']}")
        
        # Save auto clone config to MongoDB
        existing_auto_config = mongo_storage.get_auto_clone_config()
        
        if 'source_dashboards' in data:
            existing_auto_config['source_dashboards'] = {
                "content": data['source_dashboards'].get('content'),
                "message": data['source_dashboards'].get('message'),
                "email": data['source_dashboards'].get('email')
            }
        
        if 'dashboards_collections' in data:
            existing_auto_config['dashboards_collections'] = {
                "content": data['dashboards_collections'].get('content'),
                "message": data['dashboards_collections'].get('message'),
                "email": data['dashboards_collections'].get('email')
            }
        
        if not mongo_storage.set_auto_clone_config(existing_auto_config):
            return jsonify({"error": "Failed to save auto clone config to MongoDB"}), 500
        logging.info(f"Saved auto clone config: {existing_auto_config}")
        
        # Reload configs in service
        service.reload_configs()
        
        return jsonify({"message": "Settings saved successfully"})
    except Exception as e:
        logging.error(f"Error saving settings: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/test-connection', methods=['POST'])
def test_connection():
    """Test Metabase connection with provided credentials"""
    try:
        data = request.json
        base_url = data.get('base_url', '').rstrip('/')
        username = data.get('username', '')
        password = data.get('password', '')
        
        # If password is masked, use existing password from MongoDB
        if password == '********':
            existing = mongo_storage.get_metabase_config()
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


@app.route('/api/mongodb-status')
def mongodb_status():
    """Check MongoDB connection status"""
    return jsonify({
        "connected": mongo_storage.is_connected(),
        "message": "Connected to MongoDB" if mongo_storage.is_connected() else "Not connected to MongoDB. Set MONGODB_URI environment variable."
    })


# =============================================================================
# Merged Dashboards API
# =============================================================================

@app.route('/api/merged-dashboards', methods=['GET'])
@require_auth
def get_merged_dashboards():
    """Get all merged dashboards"""
    try:
        dashboards = mongo_storage.get_merged_dashboards()
        return jsonify({
            "success": True,
            "dashboards": dashboards,
            "count": len(dashboards)
        })
    except Exception as e:
        logging.error(f"Failed to get merged dashboards: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/merged-dashboards', methods=['POST'])
@require_auth
def create_merged_dashboard():
    """Create a new merged dashboard"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"success": False, "error": "No data provided"}), 400
        
        if not data.get('name'):
            return jsonify({"success": False, "error": "Name is required"}), 400
        
        if not data.get('type') or data['type'] not in ['content', 'message', 'email']:
            return jsonify({"success": False, "error": "Valid type (content, message, email) is required"}), 400
        
        if not data.get('source_dashboards') or len(data['source_dashboards']) < 2:
            return jsonify({"success": False, "error": "At least 2 source dashboards are required"}), 400
        
        # Get current user
        user = get_current_user()
        if user:
            data['created_by'] = {
                'id': user.get('id'),
                'email': user.get('email'),
                'name': user.get('name')
            }
        
        dashboard_id = mongo_storage.save_merged_dashboard(data)
        
        if dashboard_id:
            return jsonify({
                "success": True,
                "id": dashboard_id,
                "message": "Merged dashboard created successfully"
            })
        else:
            return jsonify({"success": False, "error": "Failed to save merged dashboard"}), 500
            
    except Exception as e:
        logging.error(f"Failed to create merged dashboard: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/merged-dashboards/<dashboard_id>', methods=['GET'])
@require_auth
def get_merged_dashboard(dashboard_id):
    """Get a single merged dashboard by ID"""
    try:
        dashboard = mongo_storage.get_merged_dashboard(dashboard_id)
        
        if not dashboard:
            return jsonify({"success": False, "error": "Dashboard not found"}), 404
        
        return jsonify({
            "success": True,
            "dashboard": dashboard
        })
    except Exception as e:
        logging.error(f"Failed to get merged dashboard: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/merged-dashboards/<dashboard_id>', methods=['DELETE'])
@require_auth
def delete_merged_dashboard(dashboard_id):
    """Delete a merged dashboard"""
    try:
        success = mongo_storage.delete_merged_dashboard(dashboard_id)
        
        if success:
            return jsonify({
                "success": True,
                "message": "Merged dashboard deleted successfully"
            })
        else:
            return jsonify({"success": False, "error": "Dashboard not found or could not be deleted"}), 404
            
    except Exception as e:
        logging.error(f"Failed to delete merged dashboard: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/analyze-dashboard/<int:dashboard_id>')
def analyze_dashboard(dashboard_id):
    """Analyze complete dashboard structure - tabs, cards, filters, click behaviors, etc."""
    try:
        if not service.metabase_config:
            return jsonify({"error": "Metabase not configured"}), 500
        
        base_url = service.metabase_config['base_url'].rstrip('/')
        
        # Authenticate
        auth_response = requests.post(
            f"{base_url}/api/session",
            json={
                "username": service.metabase_config['username'],
                "password": service.metabase_config['password']
            },
            timeout=10
        )
        if auth_response.status_code != 200:
            return jsonify({"error": "Auth failed"}), 500
        
        headers = {"X-Metabase-Session": auth_response.json()["id"]}
        
        # Get full dashboard
        dash_response = requests.get(
            f"{base_url}/api/dashboard/{dashboard_id}",
            headers=headers,
            timeout=30
        )
        if dash_response.status_code != 200:
            return jsonify({"error": "Dashboard fetch failed"}), 500
        
        dash = dash_response.json()
        
        # Extract complete structure
        analysis = {
            "dashboard": {
                "id": dash.get('id'),
                "name": dash.get('name'),
                "description": dash.get('description'),
                "collection_id": dash.get('collection_id'),
            },
            "tabs": [],
            "parameters": [],  # Dashboard filters
            "cards": [],
            "click_behaviors": [],
        }
        
        # Tabs
        for tab in dash.get('tabs', []):
            analysis["tabs"].append({
                "id": tab.get('id'),
                "name": tab.get('name'),
                "position": tab.get('position')
            })
        
        # Parameters (filters)
        for param in dash.get('parameters', []):
            analysis["parameters"].append({
                "id": param.get('id'),
                "name": param.get('name'),
                "slug": param.get('slug'),
                "type": param.get('type'),
                "default": param.get('default'),
            })
        
        # Cards with full detail
        for dc in dash.get('dashcards', []):
            card = dc.get('card', {})
            if not card:
                continue
                
            card_info = {
                "dashcard_id": dc.get('id'),
                "card_id": card.get('id'),
                "name": card.get('name'),
                "display": card.get('display'),
                "description": card.get('description'),
                "position": {
                    "row": dc.get('row'),
                    "col": dc.get('col'),
                    "size_x": dc.get('size_x'),
                    "size_y": dc.get('size_y'),
                },
                "dashboard_tab_id": dc.get('dashboard_tab_id'),
                "visualization_settings": dc.get('visualization_settings', {}),
                "parameter_mappings": dc.get('parameter_mappings', []),
            }
            
            # Click behavior
            viz_settings = dc.get('visualization_settings', {})
            click_behavior = viz_settings.get('click_behavior', {})
            if click_behavior:
                card_info["click_behavior"] = {
                    "type": click_behavior.get('type'),
                    "linkType": click_behavior.get('linkType'),
                    "targetId": click_behavior.get('targetId'),
                    "parameterMapping": click_behavior.get('parameterMapping', {}),
                }
                analysis["click_behaviors"].append({
                    "card_name": card.get('name'),
                    "behavior": card_info["click_behavior"]
                })
            
            # Query details
            if card.get('dataset_query'):
                dq = card.get('dataset_query', {})
                card_info["query"] = {
                    "type": dq.get('type'),
                    "database": dq.get('database'),
                }
                if dq.get('native'):
                    card_info["query"]["native"] = True
                if dq.get('query'):
                    card_info["query"]["source_table"] = dq.get('query', {}).get('source-table')
            
            # Result metadata (column info)
            if card.get('result_metadata'):
                card_info["columns"] = [
                    {"name": col.get('name'), "display_name": col.get('display_name'), "base_type": col.get('base_type')}
                    for col in card.get('result_metadata', [])
                ]
            
            analysis["cards"].append(card_info)
        
        # Summary
        analysis["summary"] = {
            "total_tabs": len(analysis["tabs"]),
            "total_cards": len(analysis["cards"]),
            "total_parameters": len(analysis["parameters"]),
            "cards_with_click_behavior": len(analysis["click_behaviors"]),
            "display_types": list(set(c.get('display') for c in analysis["cards"] if c.get('display'))),
        }
        
        return jsonify(analysis)
        
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@app.route('/api/merged-dashboard-data/<dashboard_id>')
@require_auth
def get_merged_dashboard_data(dashboard_id):
    """Fetch and aggregate real-time data from source dashboards using dashboard card query API"""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time
    
    try:
        # Get filter parameters from query string
        filter_params = {}
        for key, value in request.args.items():
            if value and value.strip():  # Only include non-empty values
                filter_params[key] = value.strip()
        
        logging.info(f"Merged dashboard request with filters: {filter_params}")
        
        # Get the merged dashboard config
        merged_dashboard = mongo_storage.get_merged_dashboard(dashboard_id)
        if not merged_dashboard:
            return jsonify({"success": False, "error": "Merged dashboard not found"}), 404
        
        source_dashboards = merged_dashboard.get('source_dashboards', [])
        if not source_dashboards:
            return jsonify({"success": False, "error": "No source dashboards configured"}), 400
        
        # Check Metabase config
        if not service.metabase_config:
            return jsonify({"success": False, "error": "Metabase not configured"}), 500
        
        base_url = service.metabase_config['base_url'].rstrip('/')
        
        # Authenticate with Metabase
        auth_response = requests.post(
            f"{base_url}/api/session",
            json={
                "username": service.metabase_config['username'],
                "password": service.metabase_config['password']
            },
            timeout=10
        )
        if auth_response.status_code != 200:
            return jsonify({"success": False, "error": "Metabase authentication failed"}), 500
        
        headers = {"X-Metabase-Session": auth_response.json()["id"]}
        
        # Fetch all dashboard data
        all_dashboard_data = []
        card_structure = None
        tabs_structure = None  # Store tabs from first dashboard
        
        for source in source_dashboards:
            source_id = source.get('id')
            try:
                # Get dashboard details first
                dash_response = requests.get(
                    f"{base_url}/api/dashboard/{source_id}",
                    headers=headers,
                    timeout=30
                )
                if dash_response.status_code != 200:
                    logging.warning(f"Failed to fetch dashboard {source_id}: status {dash_response.status_code}")
                    continue
                
                dash_data = dash_response.json()
                dashcards = dash_data.get('dashcards', []) or dash_data.get('ordered_cards', [])
                
                # Store tabs structure from first dashboard
                if tabs_structure is None:
                    tabs_structure = dash_data.get('tabs', [])
                
                # Store parameters structure from first dashboard
                if 'parameters_structure' not in locals():
                    parameters_structure = []
                    for param in dash_data.get('parameters', []):
                        parameters_structure.append({
                            'id': param.get('id'),
                            'name': param.get('name'),
                            'slug': param.get('slug'),
                            'type': param.get('type'),
                            'default': param.get('default')
                        })
                
                # Filter to only cards with actual questions
                question_cards = []
                for dc in dashcards:
                    card = dc.get('card', {})
                    if card and card.get('id'):
                        question_cards.append(dc)
                
                logging.info(f"Dashboard {source_id} ({source.get('name')}) has {len(question_cards)} question cards")
                
                # Store structure from first dashboard
                if card_structure is None:
                    card_structure = []
                    for idx, dc in enumerate(question_cards):
                        card = dc.get('card', {})
                        viz_settings = dc.get('visualization_settings', {})
                        card_structure.append({
                            'index': idx,
                            'dashcard_id': dc.get('id'),
                            'card_id': card.get('id'),
                            'name': card.get('name', 'Unnamed'),
                            'display': card.get('display', 'table'),
                            'visualization_settings': viz_settings,
                            'dashboard_tab_id': dc.get('dashboard_tab_id'),
                            'row': dc.get('row', 0),
                            'col': dc.get('col', 0),
                            'size_x': dc.get('size_x', 4),
                            'size_y': dc.get('size_y', 4),
                            'click_behavior': viz_settings.get('click_behavior'),
                            'parameter_mappings': dc.get('parameter_mappings', []),
                            'columns': card.get('result_metadata', [])
                        })
                
                # Build parameter values for the query based on filter_params
                # Map filter_params to actual dashboard parameter IDs
                # Store the parameter mapping for use with each dashcard
                dash_params = dash_data.get('parameters', [])
                param_values_by_id = {}  # param_id -> filter_value
                
                for param in dash_params:
                    param_slug = param.get('slug', '')
                    param_name = param.get('name', '')
                    param_id = param.get('id')
                    
                    # Check if we have a filter value for this parameter
                    for filter_key, filter_value in filter_params.items():
                        filter_key_lower = filter_key.lower().replace('_', '').replace('-', '').replace(' ', '')
                        slug_lower = param_slug.lower().replace('_', '').replace('-', '').replace(' ', '')
                        name_lower = param_name.lower().replace('_', '').replace('-', '').replace(' ', '')
                        
                        if (filter_key_lower == slug_lower or 
                            filter_key_lower == name_lower or 
                            filter_key_lower in slug_lower or 
                            filter_key_lower in name_lower or
                            slug_lower in filter_key_lower or
                            name_lower in filter_key_lower):
                            param_values_by_id[param_id] = filter_value
                            logging.info(f"  Mapped filter '{filter_key}' to param '{param_slug}' (id: {param_id})")
                            break
                
                # Fetch card data using dashcard query endpoint with proper async handling
                dashboard_cards_data = {}
                for idx, dc in enumerate(question_cards):
                    dashcard_id = dc.get('id')
                    card = dc.get('card', {})
                    card_id = card.get('id')
                    
                    try:
                        # Use the dashcard query endpoint
                        query_url = f"{base_url}/api/dashboard/{source_id}/dashcard/{dashcard_id}/card/{card_id}/query"
                        
                        # Build query params for THIS specific dashcard using its parameter_mappings
                        # Each dashcard has its own parameter_mappings that specify how dashboard params map to card fields
                        query_params = []
                        param_mappings = dc.get('parameter_mappings', [])
                        
                        for mapping in param_mappings:
                            param_id = mapping.get('parameter_id')
                            target = mapping.get('target')
                            
                            if param_id in param_values_by_id and target:
                                query_params.append({
                                    "id": param_id,
                                    "target": target,
                                    "value": param_values_by_id[param_id]
                                })
                                logging.info(f"    Card {idx}: Adding param {param_id} with target {target}")
                        
                        # Build query body with parameters if any
                        query_body = {"parameters": query_params} if query_params else {}
                        
                        # Query the card - accept both 200 and 202 (202 still contains data!)
                        card_response = requests.post(
                            query_url,
                            headers=headers,
                            json=query_body,
                            timeout=60
                        )
                        
                        # Metabase returns 202 for async queries BUT still includes the data!
                        if card_response.status_code in [200, 202]:
                            result = card_response.json()
                            data = result.get('data', {})
                            rows = data.get('rows', [])
                            
                            if data:
                                dashboard_cards_data[idx] = {
                                    'data': data,
                                    'display': card.get('display', 'table'),
                                    'name': card.get('name', 'Unnamed')
                                }
                                logging.info(f"  Got data for card {idx}: {card.get('name')} - {len(rows)} rows (status {card_response.status_code})")
                            else:
                                logging.warning(f"  Card {idx} ({card.get('name')}) returned empty data")
                        else:
                            logging.warning(f"  Card {idx} query failed: status {card_response.status_code}")
                    except Exception as e:
                        logging.warning(f"  Error fetching card {card_id}: {e}")
                
                all_dashboard_data.append({
                    'source_id': source_id,
                    'source_name': source.get('name', f'Dashboard {source_id}'),
                    'cards_data': dashboard_cards_data
                })
                logging.info(f"  Collected {len(dashboard_cards_data)} cards from dashboard {source_id}")
                
            except Exception as e:
                logging.warning(f"Failed to process dashboard {source_id}: {e}")
                import traceback
                traceback.print_exc()
        
        if not all_dashboard_data:
            return jsonify({"success": False, "error": "Could not fetch data from any source dashboard"}), 500
        
        logging.info(f"Card structure has {len(card_structure) if card_structure else 0} cards")
        logging.info(f"Tabs structure: {tabs_structure}")
        
        # Aggregate the data by index
        aggregated_cards = aggregate_dashboard_data_by_index(card_structure, all_dashboard_data)
        
        return jsonify({
            "success": True,
            "merged_dashboard": {
                "id": dashboard_id,
                "name": merged_dashboard.get('name'),
                "type": merged_dashboard.get('type'),
                "source_count": len(source_dashboards),
                "sources": [{"id": s.get('id'), "name": s.get('name')} for s in source_dashboards],
                "tabs": tabs_structure or [],
                "parameters": parameters_structure if 'parameters_structure' in locals() else [],
                "cards": aggregated_cards
            }
        })
        
    except Exception as e:
        logging.error(f"Failed to get merged dashboard data: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


def aggregate_dashboard_data_by_index(card_structure, all_dashboard_data):
    """
    Aggregate data from multiple dashboards based on visualization type.
    Cards are matched by their INDEX position (since cloned dashboards have same structure).
    - Scalar/Number: Sum values
    - Table: Union rows
    - Charts: Merge data series
    """
    if not card_structure:
        logging.warning("No card structure to aggregate")
        return []
    
    aggregated_cards = []
    
    for card_info in card_structure:
        card_index = card_info['index']
        display_type = card_info.get('display', 'table')
        
        # Collect data for this card from all sources BY INDEX
        card_data_list = []
        for dash_data in all_dashboard_data:
            cards_data = dash_data.get('cards_data', {})
            if card_index in cards_data:
                card_data_list.append({
                    'source': dash_data.get('source_name'),
                    'data': cards_data[card_index].get('data', {})
                })
        
        if not card_data_list:
            logging.warning(f"No data found for card index {card_index}")
            continue
        
        # Aggregate based on display type
        aggregated_data = None
        
        if display_type in ['scalar', 'number', 'progress']:
            # Sum scalar values
            aggregated_data = aggregate_scalar(card_data_list)
        elif display_type == 'table':
            # Union table rows
            aggregated_data = aggregate_table(card_data_list)
        elif display_type in ['bar', 'line', 'area', 'pie', 'row', 'combo']:
            # Merge chart data
            aggregated_data = aggregate_chart(card_data_list)
        else:
            # Default: use first source's data
            if card_data_list:
                aggregated_data = card_data_list[0].get('data')
        
        aggregated_cards.append({
            'index': card_index,
            'name': card_info.get('name'),
            'display': display_type,
            'visualization_settings': card_info.get('visualization_settings', {}),
            'dashboard_tab_id': card_info.get('dashboard_tab_id'),
            'row': card_info.get('row', 0),
            'col': card_info.get('col', 0),
            'size_x': card_info.get('size_x', 4),
            'size_y': card_info.get('size_y', 4),
            'click_behavior': card_info.get('click_behavior'),
            'columns': card_info.get('columns', []),
            'data': aggregated_data,
            'source_count': len(card_data_list)
        })
    
    return aggregated_cards


def aggregate_scalar(card_data_list):
    """Sum scalar values from multiple sources"""
    total = 0
    cols = None
    
    for item in card_data_list:
        data = item.get('data', {})
        rows = data.get('rows', [])
        
        if cols is None:
            cols = data.get('cols', [])
        
        logging.info(f"  Scalar aggregation - source: {item.get('source')}, rows: {rows}")
        
        if rows and len(rows) > 0:
            try:
                row = rows[0]
                # Find the numeric value - it might be in any column
                # Usually it's the last column (index -1) or the second column (index 1)
                value = None
                
                # Try last column first (most common for scalar queries)
                if len(row) > 0:
                    last_val = row[-1]
                    if isinstance(last_val, (int, float)) and last_val is not None:
                        value = last_val
                    # Also check first column if it's numeric
                    elif isinstance(row[0], (int, float)) and row[0] is not None:
                        value = row[0]
                
                logging.info(f"    Extracted value: {value}")
                
                if value is not None:
                    total += value
            except (IndexError, TypeError) as e:
                logging.warning(f"    Error extracting value: {e}")
    
    logging.info(f"  Scalar total: {total}")
    
    return {
        'cols': cols or [{'name': 'value', 'display_name': 'Value', 'base_type': 'type/Integer'}],
        'rows': [[total]],
        'native_form': {'query': 'Aggregated from multiple sources'}
    }


def aggregate_table(card_data_list):
    """Union table rows from multiple sources"""
    all_rows = []
    cols = None
    
    for item in card_data_list:
        data = item.get('data', {})
        rows = data.get('rows', [])
        
        if cols is None:
            cols = data.get('cols', [])
        
        # Add source column to identify where data came from
        for row in rows:
            all_rows.append(row)
    
    return {
        'cols': cols or [],
        'rows': all_rows,
        'native_form': {'query': 'Aggregated from multiple sources'}
    }


def aggregate_chart(card_data_list):
    """
    Merge chart data series - sum values for the same category/label.
    For pie/bar charts: rows are typically [label, value] - we sum values with same label.
    For line charts: rows are typically [date, value] - we sum values for same date.
    """
    cols = None
    
    # Dictionary to aggregate values by label/category (first column)
    aggregated = {}
    
    for item in card_data_list:
        data = item.get('data', {})
        rows = data.get('rows', [])
        
        if cols is None:
            cols = data.get('cols', [])
        
        for row in rows:
            if not row or len(row) < 2:
                continue
            
            # First column is the label/category, rest are values
            label = row[0]
            if label is None:
                label = 'Unknown'
            
            # Convert label to string for consistent keys
            label_key = str(label)
            
            if label_key not in aggregated:
                # Initialize with zeros for all value columns
                aggregated[label_key] = {
                    'label': label,
                    'values': [0] * (len(row) - 1)
                }
            
            # Sum all value columns (everything after the first column)
            for i in range(1, len(row)):
                val = row[i]
                if isinstance(val, (int, float)) and val is not None:
                    aggregated[label_key]['values'][i-1] += val
    
    # Convert back to rows format
    merged_rows = []
    for label_key, data in aggregated.items():
        merged_row = [data['label']] + data['values']
        merged_rows.append(merged_row)
    
    # Sort by first column (label) if possible
    try:
        merged_rows.sort(key=lambda x: str(x[0]) if x else '')
    except:
        pass
    
    logging.info(f"  Chart aggregation: {len(card_data_list)} sources -> {len(merged_rows)} unique categories")
    
    return {
        'cols': cols or [],
        'rows': merged_rows,
        'native_form': {'query': 'Aggregated from multiple sources'}
    }


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


@app.route('/api/drill-through', methods=['POST'])
@require_auth
def drill_through():
    """
    Fetch drill-through data from a target dashboard or question.
    This replicates Metabase's click behavior by fetching data from the target
    with the appropriate filter parameters applied.
    """
    try:
        data = request.json
        target_type = data.get('targetType', 'dashboard')  # 'dashboard' or 'question'
        target_id = data.get('targetId')
        filter_params = data.get('filterParams', {})  # {paramName: value}
        
        if not target_id:
            return jsonify({"success": False, "error": "targetId is required"}), 400
        
        # Check Metabase config
        if not service.metabase_config:
            return jsonify({"success": False, "error": "Metabase not configured"}), 500
        
        base_url = service.metabase_config['base_url'].rstrip('/')
        
        # Authenticate with Metabase
        auth_response = requests.post(
            f"{base_url}/api/session",
            json={
                "username": service.metabase_config['username'],
                "password": service.metabase_config['password']
            },
            timeout=10
        )
        if auth_response.status_code != 200:
            return jsonify({"success": False, "error": "Metabase authentication failed"}), 500
        
        headers = {"X-Metabase-Session": auth_response.json()["id"]}
        
        if target_type == 'dashboard':
            # Fetch dashboard data with filters
            return fetch_dashboard_drill_through(base_url, headers, target_id, filter_params)
        else:
            # Fetch question/card data with filters
            return fetch_question_drill_through(base_url, headers, target_id, filter_params)
            
    except Exception as e:
        logging.error(f"Drill-through error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


def fetch_dashboard_drill_through(base_url, headers, dashboard_id, filter_params):
    """Fetch data from a target dashboard with filter parameters applied"""
    try:
        # Get dashboard details
        dash_response = requests.get(
            f"{base_url}/api/dashboard/{dashboard_id}",
            headers=headers,
            timeout=30
        )
        if dash_response.status_code != 200:
            return jsonify({"success": False, "error": f"Failed to fetch dashboard {dashboard_id}"}), 404
        
        dash_data = dash_response.json()
        dashboard_name = dash_data.get('name', f'Dashboard {dashboard_id}')
        dashcards = dash_data.get('dashcards', []) or dash_data.get('ordered_cards', [])
        parameters = dash_data.get('parameters', [])
        tabs = dash_data.get('tabs', [])
        
        # Build parameter values for the query
        # Map filter_params to actual parameter IDs
        param_values = {}
        for param in parameters:
            param_slug = param.get('slug', '')
            param_name = param.get('name', '')
            param_id = param.get('id')
            
            # Check if we have a filter value for this parameter
            for filter_key, filter_value in filter_params.items():
                filter_key_lower = filter_key.lower().replace('_', '').replace('-', '')
                slug_lower = param_slug.lower().replace('_', '').replace('-', '')
                name_lower = param_name.lower().replace('_', '').replace('-', '')
                
                if filter_key_lower == slug_lower or filter_key_lower == name_lower or filter_key_lower in slug_lower or filter_key_lower in name_lower:
                    param_values[param_id] = filter_value
                    break
        
        logging.info(f"Drill-through to dashboard {dashboard_id} with params: {param_values}")
        
        # Fetch data for each card
        cards_data = []
        for dc in dashcards:
            card = dc.get('card', {})
            if not card or not card.get('id'):
                continue
            
            dashcard_id = dc.get('id')
            card_id = card.get('id')
            
            try:
                # Build the query with parameters
                query_url = f"{base_url}/api/dashboard/{dashboard_id}/dashcard/{dashcard_id}/card/{card_id}/query"
                
                # Include parameter values in the query
                query_body = {"parameters": [{"id": k, "value": v} for k, v in param_values.items()]} if param_values else {}
                
                card_response = requests.post(
                    query_url,
                    headers=headers,
                    json=query_body,
                    timeout=60
                )
                
                if card_response.status_code in [200, 202]:
                    result = card_response.json()
                    data = result.get('data', {})
                    
                    if data:
                        viz_settings = dc.get('visualization_settings', {})
                        cards_data.append({
                            'name': card.get('name', 'Unnamed'),
                            'display': card.get('display', 'table'),
                            'data': data,
                            'dashboard_tab_id': dc.get('dashboard_tab_id'),
                            'click_behavior': viz_settings.get('click_behavior'),
                            'columns': card.get('result_metadata', [])
                        })
            except Exception as e:
                logging.warning(f"Error fetching card {card_id}: {e}")
        
        return jsonify({
            "success": True,
            "drill_through": {
                "type": "dashboard",
                "id": dashboard_id,
                "name": dashboard_name,
                "tabs": tabs,
                "parameters": parameters,
                "applied_filters": filter_params,
                "cards": cards_data
            }
        })
        
    except Exception as e:
        logging.error(f"Dashboard drill-through error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


def fetch_question_drill_through(base_url, headers, question_id, filter_params):
    """Fetch data from a target question/card with filter parameters applied"""
    try:
        # Get question details
        question_response = requests.get(
            f"{base_url}/api/card/{question_id}",
            headers=headers,
            timeout=30
        )
        if question_response.status_code != 200:
            return jsonify({"success": False, "error": f"Failed to fetch question {question_id}"}), 404
        
        question_data = question_response.json()
        question_name = question_data.get('name', f'Question {question_id}')
        
        # Build query with filters
        # For native queries, we'd need to handle parameters differently
        # For MBQL queries, we can add filters
        
        query_body = {}
        if filter_params:
            # Add filters to the query
            query_body["parameters"] = [
                {"type": "category", "target": ["variable", ["template-tag", k]], "value": v}
                for k, v in filter_params.items()
            ]
        
        # Execute the query
        query_response = requests.post(
            f"{base_url}/api/card/{question_id}/query",
            headers=headers,
            json=query_body,
            timeout=60
        )
        
        if query_response.status_code not in [200, 202]:
            return jsonify({"success": False, "error": f"Query failed: {query_response.status_code}"}), 500
        
        result = query_response.json()
        data = result.get('data', {})
        
        return jsonify({
            "success": True,
            "drill_through": {
                "type": "question",
                "id": question_id,
                "name": question_name,
                "display": question_data.get('display', 'table'),
                "applied_filters": filter_params,
                "data": data,
                "columns": question_data.get('result_metadata', [])
            }
        })
        
    except Exception as e:
        logging.error(f"Question drill-through error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


if __name__ == "__main__":
    main()
