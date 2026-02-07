# Metabase Dashboard Auto-Clone Service

A 24/7 service that automatically creates dashboards for new databases. Runs every 4 hours and provides a beautiful web UI with countdown timer and activity logs.

## Features

- **24/7 Service** - Runs continuously, checking every 4 hours (00:00, 04:00, 08:00, 12:00, 16:00, 20:00)
- **Auto-detect database types** (content/message/email) by scanning tables
- **Auto-detect missing dashboards** - only clones for databases without dashboards
- **Full drillthrough support** - clones linked dashboards with working click behaviors
- **Beautiful Web UI** - Real-time countdown timer and activity logs
- **Persistent logs** - All dashboard creations are logged with timestamps

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Metabase Connection

Edit `metabase_config.json`:

```json
{
    "base_url": "https://your-metabase.com",
    "username": "your-email@example.com",
    "password": "your-password"
}
```

### 3. Configure Auto Clone

Edit `auto_clone_config.json`:

```json
{
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
}
```

- `source_dashboards`: Template dashboard IDs to clone for each type
- `dashboards_collections`: `_DASHBOARDS` collection IDs where main dashboards will be stored

### 4. Start the Service

**Windows:**
```bash
start_service.bat
```

**Or manually:**
```bash
python dashboard_service.py
```

### 5. Open Web UI

Navigate to: **http://localhost:5000**

## Web UI Features

### Countdown Timer
- Shows time until next automatic check
- Displays current status (Idle, Running, etc.)
- "Run Now" button to trigger immediate check

### Statistics
- Total dashboards created
- Breakdown by type (Content, Message, Email)

### Activity Log
- Full history of all dashboard creations
- Shows database name, dashboard name, type, timestamp
- Direct links to open created dashboards
- Error messages for failed creations

## How It Works

### Every Hour (at :00)

1. **Scan databases** → Identify type (content/message/email)
2. **Check existing dashboards** → Find which DBs already have dashboards
3. **Find missing** → List DBs that need dashboards
4. **Clone** → For each missing DB:
   - Create `{Customer} Collection` for questions & linked dashboards
   - Clone all questions with database remapping
   - Clone linked dashboards (for drillthrough)
   - Store main dashboard in `_DASHBOARDS` collection
   - Log the activity

### Storage Structure

```
_DASHBOARDS (collection)
├── Acme Dashboard          <- Main dashboard
├── Beta Dashboard
└── ...

Acme Collection
├── Acme Dashboard - Detail View    <- Linked dashboards
├── Question 1                       <- Questions
├── Question 2
└── ...
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Web UI |
| `/api/status` | GET | Service status and countdown |
| `/api/logs` | GET | Activity logs and stats |
| `/api/config` | GET | Current configuration |
| `/api/run` | POST | Trigger manual check |
| `/api/databases` | GET | Database identification results |

## Project Structure

```
metabase/
├── dashboard_service.py    # Main service with scheduler and API
├── templates/
│   └── index.html         # Web UI
├── auto_clone.py          # CLI auto-clone script
├── auto_clone_config.json # Source dashboards & collection IDs
├── simple_clone.py        # Manual dashboard clone
├── db_identifier.py       # Database type identification
├── metabase_manager.py    # Core Metabase API library
├── metabase_config.json   # Metabase connection config
├── requirements.txt       # Python dependencies
├── start_service.bat      # Windows start script
├── dashboard_activity.json # Activity log (auto-generated)
└── README.md             # This file
```

## Output Files

| File | Description |
|------|-------------|
| `dashboard_activity.json` | Persistent activity log |
| `dashboard_service.log` | Service log file |
| `db_identification_results.json` | Databases grouped by type |

## Troubleshooting

### "Source dashboard not configured"
Edit `auto_clone_config.json` with your template dashboard IDs.

### "No _DASHBOARDS collection configured"
Edit `auto_clone_config.json` with the collection IDs where dashboards should be stored.

### Service not starting
- Check if port 5000 is available
- Verify Python dependencies are installed
- Check `dashboard_service.log` for errors

### Dashboards not being created
- Verify Metabase credentials in `metabase_config.json`
- Check if source dashboards exist
- Review activity log for error messages

## Requirements

- Python 3.8+
- Flask, Flask-CORS, APScheduler
- requests library
- Access to Metabase API
