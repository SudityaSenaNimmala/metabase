"""
Auto Clone - Automated Dashboard Cloning
Automatically identifies database types and clones dashboards for databases that don't have one.

Workflow:
1. Load config (credentials, source dashboard IDs, _DASHBOARDS collection IDs)
2. Scan all databases and identify their type (content/message/email)
3. Check which databases already have dashboards
4. Clone dashboards only for databases that don't have one
5. Store main dashboard in the _DASHBOARDS collection for that type

Usage:
    python auto_clone.py                    # Interactive mode - shows what will be cloned
    python auto_clone.py --run              # Actually run the cloning
    python auto_clone.py --type content     # Clone only for content databases
    python auto_clone.py --customer "name"  # Clone for specific customer only
"""

import sys
import json
import logging
import argparse
import requests
from typing import Dict, List, Optional, Set
from dataclasses import dataclass

# Fix Windows console encoding
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except:
        pass

from db_identifier import DatabaseIdentifier, DatabaseInfo, DB_TYPE_SIGNATURES
from simple_clone import DashboardCloner, load_config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

def load_auto_clone_config() -> dict:
    """Load auto clone configuration"""
    try:
        with open("auto_clone_config.json", 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error("auto_clone_config.json not found!")
        return {}
    except Exception as e:
        logger.error(f"Failed to load auto_clone_config.json: {e}")
        return {}


@dataclass
class CloneTask:
    """Represents a dashboard clone task"""
    database: DatabaseInfo
    source_dashboard_id: int
    dashboards_collection_id: int
    customer_name: str
    db_type: str


class AutoCloner:
    """Automatically clones dashboards based on database type"""
    
    def __init__(self, config_file: str = "metabase_config.json"):
        self.metabase_config = self._load_config(config_file)
        self.auto_config = load_auto_clone_config()
        self.identifier = DatabaseIdentifier(config_file)
        self.cloner = None
        self.base_url = self.metabase_config['base_url'].rstrip('/')
        self.headers = {}
        
    def _load_config(self, config_file: str) -> dict:
        """Load Metabase configuration"""
        try:
            with open(config_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            raise
    
    def authenticate(self) -> bool:
        """Authenticate with Metabase"""
        if not self.identifier.authenticate():
            return False
        
        self.headers = self.identifier.headers
        self.cloner = DashboardCloner(self.metabase_config)
        return self.cloner.authenticate()
    
    def get_source_dashboards(self) -> Dict[str, int]:
        """Get source dashboard IDs from config"""
        return self.auto_config.get('source_dashboards', {})
    
    def get_dashboards_collections(self) -> Dict[str, int]:
        """Get _DASHBOARDS collection IDs from config"""
        return self.auto_config.get('dashboards_collections', {})
    
    def extract_customer_name(self, db_name: str) -> str:
        """
        Extract customer name from database name.
        Removes common suffixes like -SDB, email, etc.
        PRESERVES version numbers like abc2, abc3.
        """
        name = db_name
        
        # Remove common suffixes (order matters - check longer ones first)
        suffixes_to_remove = [
            '-SDB', '-sdb',
            'email', 'Email',
            'msg', 'Msg', 'message', 'Message',
            '-common', '-json',
            'hub', 'Hub',
        ]
        
        for suffix in suffixes_to_remove:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
                break  # Only remove one suffix
        
        # Clean up any trailing dashes or underscores but KEEP version numbers
        name = name.rstrip('-_')
        
        # Capitalize first letter
        if name:
            name = name[0].upper() + name[1:]
        
        return name or db_name
    
    def get_all_dashboards(self) -> List[dict]:
        """Get all dashboards from Metabase"""
        try:
            response = requests.get(
                f"{self.base_url}/api/dashboard",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get dashboards: {e}")
            return []
    
    def get_all_questions(self) -> List[dict]:
        """Get all questions/cards from Metabase"""
        try:
            response = requests.get(
                f"{self.base_url}/api/card",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get questions: {e}")
            return []
    
    def find_databases_with_dashboards(self) -> Set[int]:
        """
        Find all database IDs that already have dashboards.
        A database "has a dashboard" if any question in any dashboard uses that database.
        """
        databases_with_dashboards = set()
        
        # Get all questions and check their database_id
        questions = self.get_all_questions()
        
        # Build a map of question_id -> database_id
        question_db_map = {}
        for q in questions:
            if q.get('database_id'):
                question_db_map[q['id']] = q['database_id']
        
        # Get all dashboards and check which databases they use
        dashboards = self.get_all_dashboards()
        
        for dash in dashboards:
            # Get full dashboard details to see the cards
            try:
                response = requests.get(
                    f"{self.base_url}/api/dashboard/{dash['id']}",
                    headers=self.headers
                )
                response.raise_for_status()
                full_dash = response.json()
                
                # Check each card in the dashboard
                dashcards = full_dash.get('dashcards', []) or full_dash.get('ordered_cards', [])
                for dc in dashcards:
                    card = dc.get('card', {})
                    if card:
                        db_id = card.get('database_id')
                        if db_id:
                            databases_with_dashboards.add(db_id)
                        
                        # Also check via question_id
                        card_id = card.get('id')
                        if card_id and card_id in question_db_map:
                            databases_with_dashboards.add(question_db_map[card_id])
            except:
                pass
        
        return databases_with_dashboards
    
    def get_databases_needing_dashboards(self, db_type_filter: Optional[str] = None) -> List[CloneTask]:
        """
        Get list of databases that need dashboards cloned.
        Only returns databases that don't already have a dashboard.
        """
        tasks = []
        
        source_dashboards = self.get_source_dashboards()
        dashboards_collections = self.get_dashboards_collections()
        
        # Check config
        for db_type in ["content", "message", "email"]:
            if not source_dashboards.get(db_type):
                logger.warning(f"No source dashboard configured for {db_type}")
            if not dashboards_collections.get(db_type):
                logger.warning(f"No _DASHBOARDS collection configured for {db_type}")
        
        # Get databases that already have dashboards
        logger.info("Checking which databases already have dashboards...")
        dbs_with_dashboards = self.find_databases_with_dashboards()
        logger.info(f"Found {len(dbs_with_dashboards)} databases with existing dashboards")
        
        # Get all databases grouped by type
        grouped = self.identifier.get_databases_by_type()
        
        for db_type in ["content", "message", "email"]:
            if db_type_filter and db_type != db_type_filter:
                continue
            
            source_id = source_dashboards.get(db_type)
            collection_id = dashboards_collections.get(db_type)
            
            if not source_id:
                continue
            if not collection_id:
                logger.warning(f"No _DASHBOARDS collection for {db_type}, skipping")
                continue
            
            for db in grouped.get(db_type, []):
                # Skip if database already has a dashboard
                if db.id in dbs_with_dashboards:
                    logger.debug(f"Skipping {db.name} - already has dashboard")
                    continue
                
                customer_name = self.extract_customer_name(db.name)
                
                task = CloneTask(
                    database=db,
                    source_dashboard_id=source_id,
                    dashboards_collection_id=collection_id,
                    customer_name=customer_name,
                    db_type=db_type
                )
                tasks.append(task)
        
        return tasks
    
    def clone_for_database(self, task: CloneTask) -> bool:
        """Clone dashboard for a specific database"""
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"Cloning {task.db_type.upper()} dashboard for: {task.database.name}")
            logger.info(f"Customer: {task.customer_name}")
            logger.info(f"Source Dashboard: {task.source_dashboard_id}")
            logger.info(f"Target Database: {task.database.name} (ID: {task.database.id})")
            logger.info(f"_DASHBOARDS Collection: {task.dashboards_collection_id}")
            logger.info(f"{'='*60}")
            
            # Create customer collection for linked dashboards and questions
            # This will be in the same parent as the source dashboard
            source_parent = self.cloner.get_dashboard_collection_id(task.source_dashboard_id)
            collection_name = f"{task.customer_name} Collection"
            col = self.cloner.get_or_create_collection(collection_name, source_parent)
            customer_collection_id = col['id'] if col else None
            
            if customer_collection_id:
                logger.info(f"Customer collection: {collection_name} (ID: {customer_collection_id})")
            
            # Generate dashboard name
            dashboard_name = f"{task.customer_name} Dashboard"
            
            # Check for linked dashboards
            all_linked = self.cloner.find_all_linked_dashboards(task.source_dashboard_id)
            
            if all_linked:
                logger.info(f"Source has {len(all_linked)} linked dashboards - will clone all")
                # Clone with all linked dashboards
                # Main dashboard -> _DASHBOARDS collection
                # Linked dashboards & questions -> customer collection
                new_dashboard = self.cloner.clone_with_all_linked(
                    source_dashboard_id=task.source_dashboard_id,
                    new_name=dashboard_name,
                    new_database_id=task.database.id,
                    dashboard_collection_id=customer_collection_id,  # For linked dashboards
                    questions_collection_id=customer_collection_id,  # For questions
                    main_dashboard_collection_id=task.dashboards_collection_id  # Main dashboard goes here
                )
            else:
                # Clone single dashboard -> _DASHBOARDS collection
                new_dashboard = self.cloner.clone_dashboard(
                    source_dashboard_id=task.source_dashboard_id,
                    new_name=dashboard_name,
                    new_database_id=task.database.id,
                    dashboard_collection_id=task.dashboards_collection_id,
                    questions_collection_id=customer_collection_id
                )
            
            if new_dashboard:
                logger.info(f"[SUCCESS] Created dashboard: {dashboard_name} (ID: {new_dashboard['id']})")
                logger.info(f"URL: {self.metabase_config['base_url']}/dashboard/{new_dashboard['id']}")
                return True
            else:
                logger.error(f"[FAILED] Could not create dashboard for {task.database.name}")
                return False
                
        except Exception as e:
            logger.error(f"[ERROR] Failed to clone for {task.database.name}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def show_status(self, db_type_filter: Optional[str] = None):
        """Show current status - what needs to be cloned"""
        print("\n" + "="*70)
        print("AUTO CLONE STATUS")
        print("="*70)
        
        # Check config
        source_dashboards = self.get_source_dashboards()
        dashboards_collections = self.get_dashboards_collections()
        
        print("\nConfiguration:")
        print("-" * 40)
        for db_type in ["content", "message", "email"]:
            src = source_dashboards.get(db_type)
            col = dashboards_collections.get(db_type)
            status = "[OK]" if src and col else "[MISSING]"
            print(f"  {db_type.upper():10} Source: {src or 'NOT SET':5}  Collection: {col or 'NOT SET':5}  {status}")
        
        # Get tasks
        tasks = self.get_databases_needing_dashboards(db_type_filter)
        
        if not tasks:
            print("\n[OK] All databases already have dashboards!")
            return []
        
        # Group by type
        by_type = {"content": [], "message": [], "email": []}
        for task in tasks:
            by_type[task.db_type].append(task)
        
        print(f"\nDatabases NEEDING dashboards ({len(tasks)} total):")
        print("-" * 40)
        
        for db_type in ["content", "message", "email"]:
            type_tasks = by_type[db_type]
            if type_tasks:
                print(f"\n{db_type.upper()} ({len(type_tasks)}):")
                for task in type_tasks:
                    print(f"  - {task.database.name} -> {task.customer_name} Dashboard")
        
        print("\n" + "="*70)
        return tasks
    
    def run(self, db_type_filter: Optional[str] = None, customer_filter: Optional[str] = None, 
            dry_run: bool = True):
        """
        Run the auto clone process.
        
        Args:
            db_type_filter: Only clone for specific type (content/message/email)
            customer_filter: Only clone for specific customer (database name pattern)
            dry_run: If True, only show what would be done. If False, actually clone.
        """
        # Show status first
        tasks = self.show_status(db_type_filter)
        
        if not tasks:
            return
        
        # Filter by customer if specified
        if customer_filter:
            tasks = [t for t in tasks if customer_filter.lower() in t.database.name.lower()]
            if not tasks:
                print(f"\nNo databases found matching '{customer_filter}'")
                return
            print(f"\nFiltered to {len(tasks)} database(s) matching '{customer_filter}'")
        
        if dry_run:
            print("\n[DRY RUN] To actually clone, run with --run flag")
            print(f"Command: python auto_clone.py --run")
            return
        
        # Confirm
        print(f"\nAbout to clone dashboards for {len(tasks)} database(s).")
        confirm = input("Proceed? (yes/no): ").strip().lower()
        
        if confirm not in ['yes', 'y']:
            print("Cancelled.")
            return
        
        # Clone
        success = 0
        failed = 0
        
        for i, task in enumerate(tasks, 1):
            print(f"\n[{i}/{len(tasks)}] Processing {task.database.name}...")
            if self.clone_for_database(task):
                success += 1
            else:
                failed += 1
        
        # Summary
        print("\n" + "="*70)
        print("AUTO CLONE COMPLETE")
        print("="*70)
        print(f"  Success: {success}")
        print(f"  Failed: {failed}")
        print("="*70)


def main():
    parser = argparse.ArgumentParser(description='Auto-clone dashboards for databases without one')
    parser.add_argument('--run', action='store_true', help='Actually run the cloning (default is dry run)')
    parser.add_argument('--type', choices=['content', 'message', 'email'], help='Clone only specific type')
    parser.add_argument('--customer', type=str, help='Clone for specific customer (database name pattern)')
    
    args = parser.parse_args()
    
    auto_cloner = AutoCloner()
    
    print("Connecting to Metabase...")
    if not auto_cloner.authenticate():
        print("ERROR: Failed to authenticate!")
        return
    print("Connected!\n")
    
    # Check config
    source_dashboards = auto_cloner.get_source_dashboards()
    dashboards_collections = auto_cloner.get_dashboards_collections()
    
    missing_config = []
    for db_type in ["content", "message", "email"]:
        if not source_dashboards.get(db_type):
            missing_config.append(f"{db_type} source_dashboard")
        if not dashboards_collections.get(db_type):
            missing_config.append(f"{db_type} dashboards_collection")
    
    if missing_config:
        print("[WARNING] Missing configuration:")
        for m in missing_config:
            print(f"  - {m}")
        print("\nPlease edit auto_clone_config.json with the required IDs.")
        print("\nExample:")
        print('''{
    "source_dashboards": {
        "content": 3,
        "message": 5,
        "email": 7
    },
    "dashboards_collections": {
        "content": 10,
        "message": 11,
        "email": 12
    }
}''')
        
        # Still show status even without full config
        if not args.run:
            print("\nShowing database identification anyway...\n")
            grouped = auto_cloner.identifier.get_databases_by_type()
            auto_cloner.identifier.print_summary(grouped)
        return
    
    # Run
    auto_cloner.run(
        db_type_filter=args.type,
        customer_filter=args.customer,
        dry_run=not args.run
    )


if __name__ == "__main__":
    main()
