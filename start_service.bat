@echo off
echo ============================================
echo Dashboard Auto-Clone Service
echo ============================================
echo.

REM Check if MONGODB_URI is set
if "%MONGODB_URI%"=="" (
    echo [WARNING] MONGODB_URI environment variable is not set!
    echo.
    echo Please set MONGODB_URI before starting the service:
    echo   set MONGODB_URI=mongodb+srv://user:pass@cluster.mongodb.net/
    echo.
    echo Or create a .env file with MONGODB_URI=your-connection-string
    echo.
    pause
    exit /b 1
)

echo [OK] MongoDB URI is configured
echo.
echo Installing dependencies...
pip install -r requirements.txt
echo.
echo Starting service...
echo Web UI will be available at: http://localhost:1206
echo.
python dashboard_service.py
pause
