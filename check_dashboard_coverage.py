"""
Check which databases have dashboards and which don't.
Shows exactly how the tool determines dashboard coverage.
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
    print("Getting all databases...")
    response = requests.get(f"{base_url}/api/database", headers=headers)
    response.raise_for_status()
    databases = {db['id']: db['name'] for db in response.json().get("data", [])}
    print(f"Found {len(databases)} databases\n")
    
    # Get all questions and their database_id
    print("Getting all questions...")
    response = requests.get(f"{base_url}/api/card", headers=headers)
    response.raise_for_status()
    questions = response.json()
    print(f"Found {len(questions)} questions\n")
    
    # Map question_id -> database_id
    question_db_map = {}
    for q in questions:
        if q.get('database_id'):
            question_db_map[q['id']] = q['database_id']
    
    # Get all dashboards
    print("Getting all dashboards...")
    response = requests.get(f"{base_url}/api/dashboard", headers=headers)
    response.raise_for_status()
    dashboards = response.json()
    print(f"Found {len(dashboards)} dashboards\n")
    
    # Track which databases have dashboards
    databases_with_dashboards = {}  # db_id -> list of dashboard names
    
    print("Analyzing dashboard coverage...")
    for dash in dashboards:
        dash_id = dash['id']
        dash_name = dash.get('name', f'Dashboard {dash_id}')
        
        try:
            # Get full dashboard details
            response = requests.get(
                f"{base_url}/api/dashboard/{dash_id}",
                headers=headers
            )
            response.raise_for_status()
            full_dash = response.json()
            
            # Check each card
            dashcards = full_dash.get('dashcards', []) or full_dash.get('ordered_cards', [])
            for dc in dashcards:
                card = dc.get('card', {})
                if card:
                    db_id = card.get('database_id')
                    if db_id:
                        if db_id not in databases_with_dashboards:
                            databases_with_dashboards[db_id] = []
                        if dash_name not in databases_with_dashboards[db_id]:
                            databases_with_dashboards[db_id].append(dash_name)
        except Exception as e:
            print(f"  Warning: Could not analyze dashboard {dash_id}: {e}")
    
    # Load identification results
    try:
        with open("db_identification_results.json", 'r') as f:
            identification = json.load(f)
    except:
        identification = {}
    
    # Create reverse lookup: db_id -> type
    db_types = {}
    for db_type, dbs in identification.items():
        for db in dbs:
            db_types[db['id']] = db_type
    
    # Print results
    print("\n" + "="*80)
    print("DATABASE DASHBOARD COVERAGE")
    print("="*80)
    
    # Databases WITH dashboards
    print(f"\nDATABASES WITH DASHBOARDS ({len(databases_with_dashboards)}):")
    print("-" * 60)
    for db_id in sorted(databases_with_dashboards.keys()):
        db_name = databases.get(db_id, f"Unknown DB {db_id}")
        db_type = db_types.get(db_id, "unknown")
        dash_list = databases_with_dashboards[db_id]
        print(f"  [{db_type:8}] {db_name} (ID: {db_id})")
        for d in dash_list[:3]:  # Show first 3 dashboards
            print(f"            -> {d}")
        if len(dash_list) > 3:
            print(f"            ... and {len(dash_list) - 3} more")
    
    # Databases WITHOUT dashboards
    dbs_without = set(databases.keys()) - set(databases_with_dashboards.keys())
    
    print(f"\nDATABASES WITHOUT DASHBOARDS ({len(dbs_without)}):")
    print("-" * 60)
    
    # Group by type
    by_type = {"content": [], "message": [], "email": [], "unknown": []}
    for db_id in dbs_without:
        db_name = databases.get(db_id, f"Unknown DB {db_id}")
        db_type = db_types.get(db_id, "unknown")
        by_type[db_type].append((db_id, db_name))
    
    for db_type in ["content", "message", "email", "unknown"]:
        dbs = by_type[db_type]
        if dbs:
            print(f"\n  {db_type.upper()} ({len(dbs)}):")
            for db_id, db_name in sorted(dbs, key=lambda x: x[1]):
                print(f"    - {db_name} (ID: {db_id})")
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"  Total databases: {len(databases)}")
    print(f"  With dashboards: {len(databases_with_dashboards)}")
    print(f"  Without dashboards: {len(dbs_without)}")
    print()
    print("  By type needing dashboards:")
    for db_type in ["content", "message", "email", "unknown"]:
        print(f"    {db_type:10}: {len(by_type[db_type])}")
    print("="*80)
    
    # Save results
    results = {
        "databases_with_dashboards": {
            databases.get(db_id, str(db_id)): {
                "id": db_id,
                "type": db_types.get(db_id, "unknown"),
                "dashboards": dash_list
            }
            for db_id, dash_list in databases_with_dashboards.items()
        },
        "databases_without_dashboards": {
            db_name: {
                "id": db_id,
                "type": db_types.get(db_id, "unknown")
            }
            for db_id, db_name in [(did, databases.get(did, str(did))) for did in dbs_without]
        }
    }
    
    with open("dashboard_coverage.json", 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nResults saved to: dashboard_coverage.json")


if __name__ == "__main__":
    main()
