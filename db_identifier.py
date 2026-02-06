"""
Database Type Identifier
Scans all Metabase databases and identifies their type (content/message/email)
based on the tables/collections that exist in each database.
"""

import sys
import json
import logging
import requests
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

# Fix Windows console encoding
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except:
        pass

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION: Tables that identify each database type
# =============================================================================
# These tables are UNIQUE to each database type and are used to identify them
# The script checks if these tables exist to determine the database type

DB_TYPE_SIGNATURES = {
    "content": [
        # Tables unique to CONTENT databases (file migration)
        "UsersContentMigInfo",
        "UsersContentDataSize", 
        "MoveWorkSpaces",
        "MoveWorkSpaceStatus",
        "MoveFileSize",
        "MoveJobDetails",
        "FileFolderInfo",
        "FilePermissionDetail",
        "DeltaMoveInfo",
        "DeltaScheduler",
    ],
    "message": [
        # Tables unique to MESSAGE databases (Slack/Teams migration)
        "MessageWorkSpace",
        "MessageWorkSpaceTransferStatus",
        "MessageJob",
        "MessageMoveQueue",
        "MessageReport",
        "MessageReportWS",
        "UsersMessageInfo",
        "MessageDmsChannelsInfo",
        "MessageTransferConfiguration",
        "SlackDms",
        "ChannelsFileDetail",
        "ConversationsFetchingInfo",
    ],
    "email": [
        # Tables unique to EMAIL databases (email migration)
        "emailInfo",
        "emailWorkSpace",
        "emailMoveQueue",
        "emailFolderInfo",
        "emailJobDetails",
        "emailBatches",
        "EmailPikingQueue",
        "CalendarDetails",
        "calendarEvent",
        "contactsInfo",
        "ContactMoveQueue",
    ]
}

# Minimum number of signature tables that must match to identify a type
MIN_MATCH_THRESHOLD = 2  # At least 2 tables must match for confident identification


@dataclass
class DatabaseInfo:
    """Information about a database and its identified type"""
    id: int
    name: str
    engine: str
    tables: List[str]
    identified_type: Optional[str]
    match_confidence: float  # 0.0 to 1.0
    matched_tables: List[str]


class DatabaseIdentifier:
    """Identifies database types by scanning their table structures"""
    
    def __init__(self, config_file: str = "metabase_config.json"):
        self.config = self._load_config(config_file)
        self.base_url = self.config['base_url'].rstrip('/')
        self.headers = {}
        
    def _load_config(self, config_file: str) -> dict:
        """Load configuration from file or environment variables"""
        try:
            with open(config_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            # Fall back to environment variables
            metabase_url = os.environ.get('METABASE_URL')
            metabase_username = os.environ.get('METABASE_USERNAME')
            metabase_password = os.environ.get('METABASE_PASSWORD')
            
            if metabase_url and metabase_username and metabase_password:
                logger.info("Loaded config from environment variables")
                return {
                    'base_url': metabase_url,
                    'username': metabase_username,
                    'password': metabase_password
                }
            else:
                logger.error("Config file not found and environment variables not set")
                raise
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            raise
    
    def authenticate(self) -> bool:
        """Authenticate with Metabase"""
        try:
            response = requests.post(
                f"{self.base_url}/api/session",
                json={
                    "username": self.config['username'],
                    "password": self.config['password']
                }
            )
            response.raise_for_status()
            session_token = response.json()["id"]
            self.headers = {"X-Metabase-Session": session_token}
            logger.info("[OK] Authenticated with Metabase")
            return True
        except Exception as e:
            logger.error(f"[ERROR] Authentication failed: {e}")
            return False
    
    def get_all_databases(self) -> List[dict]:
        """Get all databases from Metabase"""
        try:
            response = requests.get(
                f"{self.base_url}/api/database",
                headers=self.headers
            )
            response.raise_for_status()
            databases = response.json().get("data", [])
            logger.info(f"[OK] Found {len(databases)} databases")
            return databases
        except Exception as e:
            logger.error(f"[ERROR] Failed to get databases: {e}")
            return []
    
    def get_database_tables(self, database_id: int) -> List[str]:
        """Get all table names from a database"""
        try:
            response = requests.get(
                f"{self.base_url}/api/database/{database_id}/metadata",
                headers=self.headers
            )
            response.raise_for_status()
            metadata = response.json()
            
            tables = []
            for table in metadata.get('tables', []):
                table_name = table.get('name', '').lower()
                # Also get display name and schema
                display_name = table.get('display_name', '').lower()
                schema = table.get('schema', '').lower()
                
                tables.append(table_name)
                if display_name and display_name != table_name:
                    tables.append(display_name)
                    
            return list(set(tables))  # Remove duplicates
        except Exception as e:
            logger.debug(f"Could not get tables for database {database_id}: {e}")
            return []
    
    def identify_database_type(self, tables: List[str]) -> Tuple[Optional[str], float, List[str]]:
        """
        Identify database type based on its tables.
        
        Returns:
            Tuple of (type_name, confidence, matched_tables)
        """
        tables_lower = [t.lower() for t in tables]
        
        best_match = None
        best_confidence = 0.0
        best_matched = []
        
        for db_type, signature_tables in DB_TYPE_SIGNATURES.items():
            if not signature_tables:
                continue
                
            # Find matching tables
            matched = []
            for sig_table in signature_tables:
                sig_lower = sig_table.lower()
                # Check for exact match or partial match
                for table in tables_lower:
                    if sig_lower == table or sig_lower in table or table in sig_lower:
                        matched.append(sig_table)
                        break
            
            # Calculate confidence
            if matched:
                confidence = len(matched) / len(signature_tables)
                
                if len(matched) >= MIN_MATCH_THRESHOLD and confidence > best_confidence:
                    best_match = db_type
                    best_confidence = confidence
                    best_matched = matched
        
        return best_match, best_confidence, best_matched
    
    def scan_all_databases(self) -> List[DatabaseInfo]:
        """Scan all databases and identify their types - with parallel fetching for speed"""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        results = []
        databases = self.get_all_databases()
        
        logger.info(f"Scanning {len(databases)} databases in parallel...")
        
        def process_database(db):
            """Process a single database"""
            db_id = db['id']
            db_name = db.get('name', f'Database {db_id}')
            db_engine = db.get('engine', 'unknown')
            
            # Get tables
            tables = self.get_database_tables(db_id)
            
            # Identify type
            db_type, confidence, matched = self.identify_database_type(tables)
            
            return DatabaseInfo(
                id=db_id,
                name=db_name,
                engine=db_engine,
                tables=tables,
                identified_type=db_type,
                match_confidence=confidence,
                matched_tables=matched
            )
        
        # Use ThreadPoolExecutor for parallel fetching (5 concurrent requests)
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_db = {executor.submit(process_database, db): db for db in databases}
            
            for future in as_completed(future_to_db):
                try:
                    info = future.result()
                    results.append(info)
                    
                    if info.identified_type:
                        logger.info(f"  ✓ {info.name}: {info.identified_type.upper()}")
                    else:
                        logger.debug(f"  - {info.name}: UNKNOWN")
                except Exception as e:
                    db = future_to_db[future]
                    logger.warning(f"  ✗ Failed to scan {db.get('name')}: {e}")
        
        # Log summary
        type_counts = {"content": 0, "message": 0, "email": 0, "unknown": 0}
        for info in results:
            if info.identified_type:
                type_counts[info.identified_type] += 1
            else:
                type_counts["unknown"] += 1
        
        logger.info(f"Scan complete: {type_counts['content']} content, {type_counts['message']} message, {type_counts['email']} email, {type_counts['unknown']} unknown")
        
        return results
    
    def get_databases_by_type(self) -> Dict[str, List[DatabaseInfo]]:
        """Get all databases grouped by their identified type"""
        all_dbs = self.scan_all_databases()
        
        grouped = {
            "content": [],
            "message": [],
            "email": [],
            "unknown": []
        }
        
        for db in all_dbs:
            if db.identified_type:
                grouped[db.identified_type].append(db)
            else:
                grouped["unknown"].append(db)
        
        return grouped
    
    def print_summary(self, grouped: Dict[str, List[DatabaseInfo]]):
        """Print a summary of identified databases"""
        print("\n" + "="*70)
        print("DATABASE IDENTIFICATION SUMMARY")
        print("="*70)
        
        for db_type in ["content", "message", "email", "unknown"]:
            dbs = grouped.get(db_type, [])
            print(f"\n{db_type.upper()} DATABASES ({len(dbs)}):")
            print("-" * 40)
            
            if not dbs:
                print("  (none found)")
            else:
                for db in dbs:
                    conf_str = f" [{db.match_confidence:.0%}]" if db.identified_type else ""
                    print(f"  • {db.name} (ID: {db.id}){conf_str}")
                    if db.matched_tables:
                        print(f"    Matched: {', '.join(db.matched_tables)}")
        
        print("\n" + "="*70)
    
    def export_results(self, output_file: str = "db_identification_results.json"):
        """Export identification results to JSON"""
        grouped = self.get_databases_by_type()
        
        # Convert to serializable format
        export_data = {}
        for db_type, dbs in grouped.items():
            export_data[db_type] = [
                {
                    "id": db.id,
                    "name": db.name,
                    "engine": db.engine,
                    "tables_count": len(db.tables),
                    "confidence": db.match_confidence,
                    "matched_tables": db.matched_tables
                }
                for db in dbs
            ]
        
        with open(output_file, 'w') as f:
            json.dump(export_data, f, indent=2)
        
        logger.info(f"[OK] Exported results to {output_file}")
        return export_data


def main():
    """Main function to identify all databases"""
    print("\n" + "="*70)
    print("DATABASE TYPE IDENTIFIER")
    print("="*70)
    print("\nThis tool scans all Metabase databases and identifies their type")
    print("based on the tables that exist in each database.\n")
    
    # Check if signatures are configured
    total_signatures = sum(len(tables) for tables in DB_TYPE_SIGNATURES.values())
    if total_signatures == 0:
        print("⚠️  WARNING: No signature tables configured!")
        print("\nPlease edit DB_TYPE_SIGNATURES in this file to add table names")
        print("that identify each database type.\n")
        print("Example:")
        print('  DB_TYPE_SIGNATURES = {')
        print('      "content": ["articles", "posts", "content_items"],')
        print('      "message": ["messages", "conversations"],')
        print('      "email": ["emails", "inbox", "mail_queue"]')
        print('  }')
        print("\nContinuing to scan databases (will show all as UNKNOWN)...\n")
    
    identifier = DatabaseIdentifier()
    
    if not identifier.authenticate():
        print("ERROR: Failed to authenticate with Metabase")
        return
    
    # Scan and identify
    grouped = identifier.get_databases_by_type()
    
    # Print summary
    identifier.print_summary(grouped)
    
    # Export results
    identifier.export_results()
    
    print("\nResults exported to: db_identification_results.json")


if __name__ == "__main__":
    main()
