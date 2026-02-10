# Metabase Dashboard Auto-Clone Service

A 24/7 service that automatically creates dashboards for new databases. Runs every 4 hours and provides a beautiful web UI with countdown timer and activity logs.

**All data is stored in MongoDB** - no local file storage required.

## Features

- **24/7 Service** - Runs continuously, checking every 4 hours (00:00, 04:00, 08:00, 12:00, 16:00, 20:00)
- **Auto-detect database types** (content/message/email) by scanning tables
- **Auto-detect missing dashboards** - only clones for databases without dashboards
- **Full drillthrough support** - clones linked dashboards with working click behaviors
- **Beautiful Web UI** - Real-time countdown timer and activity logs
- **MongoDB Storage** - All configuration and logs stored in MongoDB (cloud-ready)
- **Stop Button** - Stop running jobs gracefully via the web UI

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Set MongoDB Connection (REQUIRED)

Set the `MONGODB_URI` environment variable:

**Windows (PowerShell):**
```powershell
$env:MONGODB_URI = "mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority"
```

**Windows (CMD):**
```cmd
set MONGODB_URI=mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority
```

**Linux/Mac:**
```bash
export MONGODB_URI="mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority"
```

### 3. Start the Service

**Windows:**
```bash
start_service.bat
```

**Or manually:**
```bash
python dashboard_service.py
```

### 4. Configure via Web UI

Navigate to: **http://localhost:1206**

Click **Settings** to configure:
- Metabase URL, username, and password
- Source dashboard IDs (templates to clone)
- Target collection IDs (where dashboards are stored)

All settings are saved to MongoDB automatically.

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
| `/api/settings` | GET/POST | Get/save all settings |
| `/api/run` | POST | Trigger manual check |
| `/api/stop` | POST | Stop current running check |
| `/api/databases` | GET | Database identification results |
| `/api/mongodb-status` | GET | Check MongoDB connection |
| `/api/test-connection` | POST | Test Metabase connection |

## MongoDB Collections

All data is stored in MongoDB under the `metabase_dashboard_service` database:

| Collection | Description |
|------------|-------------|
| `config` | All configuration (metabase credentials, source dashboards, collection IDs) |
| `activity_log` | Dashboard creation history with timestamps and status |

## Project Structure

```
metabase/
├── dashboard_service.py    # Main service with scheduler, API, and MongoDB storage
├── templates/
│   └── index.html         # Web UI
├── auto_clone.py          # CLI auto-clone script
├── simple_clone.py        # Manual dashboard clone
├── db_identifier.py       # Database type identification
├── metabase_manager.py    # Core Metabase API library
├── requirements.txt       # Python dependencies
├── start_service.bat      # Windows start script
├── env.example            # Example environment variables
└── README.md             # This file
```

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `MONGODB_URI` | **Yes** | MongoDB connection string |
| `METABASE_URL` | No | Initial Metabase URL (can be set via UI) |
| `METABASE_USERNAME` | No | Initial Metabase username (can be set via UI) |
| `METABASE_PASSWORD` | No | Initial Metabase password (can be set via UI) |

## Troubleshooting

### "MongoDB Disconnected" in UI
- Check that `MONGODB_URI` environment variable is set
- Verify MongoDB connection string is correct
- Ensure MongoDB server is accessible

### "Source dashboard not configured"
Click Settings in the UI and configure the source dashboard IDs.

### "No _DASHBOARDS collection configured"
Click Settings in the UI and configure the collection IDs.

### Service not starting
- Check if port 1206 is available
- Verify Python dependencies are installed
- Check console output for errors

### Dashboards not being created
- Verify Metabase credentials in Settings
- Test connection using the "Test Connection" button
- Check if source dashboards exist
- Review activity log for error messages

## Requirements

- Python 3.8+
- MongoDB (local or cloud like MongoDB Atlas)
- Flask, Flask-CORS, APScheduler
- pymongo, dnspython
- requests library
- Access to Metabase API
