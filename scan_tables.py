"""
Quick script to scan and list all tables in each database.
This helps identify which tables are unique to each database type.
"""

import sys
import json
import requests

# Fix Windows console encoding
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except:
        pass

def load_config():
    with open("metabase_config.json", 'r') as f:
        return json.load(f)

def main():
    config = load_config()
    base_url = config['base_url'].rstrip('/')
    
    # Authenticate
    print("Connecting to Metabase...")
    response = requests.post(
        f"{base_url}/api/session",
        json={"username": config['username'], "password": config['password']}
    )
    response.raise_for_status()
    headers = {"X-Metabase-Session": response.json()["id"]}
    print("Connected!\n")
    
    # Get all databases
    response = requests.get(f"{base_url}/api/database", headers=headers)
    response.raise_for_status()
    databases = response.json().get("data", [])
    
    print(f"Found {len(databases)} databases\n")
    print("="*80)
    
    all_db_tables = {}
    
    for db in databases:
        db_id = db['id']
        db_name = db.get('name', f'Database {db_id}')
        db_engine = db.get('engine', 'unknown')
        
        print(f"\n[DB] {db_name} (ID: {db_id}, Engine: {db_engine})")
        print("-" * 60)
        
        # Get tables
        try:
            response = requests.get(
                f"{base_url}/api/database/{db_id}/metadata",
                headers=headers
            )
            response.raise_for_status()
            metadata = response.json()
            
            tables = []
            for table in metadata.get('tables', []):
                table_name = table.get('name', '')
                schema = table.get('schema', '')
                full_name = f"{schema}.{table_name}" if schema else table_name
                tables.append(table_name)
                print(f"  • {full_name}")
            
            all_db_tables[db_name] = {
                "id": db_id,
                "engine": db_engine,
                "tables": sorted(tables)
            }
            
            if not tables:
                print("  (no tables found)")
                
        except Exception as e:
            print(f"  [!] Could not fetch tables: {e}")
            all_db_tables[db_name] = {
                "id": db_id,
                "engine": db_engine,
                "tables": [],
                "error": str(e)
            }
    
    print("\n" + "="*80)
    
    # Save to file for reference
    with open("all_database_tables.json", 'w') as f:
        json.dump(all_db_tables, f, indent=2)
    
    print(f"\n[OK] Saved all tables to: all_database_tables.json")
    print("\nReview this file to identify which tables are unique to each database type.")
    print("Then update DB_TYPE_SIGNATURES in db_identifier.py")


if __name__ == "__main__":
    main()
