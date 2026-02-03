@echo off
echo ============================================
echo Dashboard Auto-Clone Service
echo ============================================
echo.
echo Installing dependencies...
pip install -r requirements.txt
echo.
echo Starting service...
echo Web UI will be available at: http://localhost:1206
echo.
python dashboard_service.py
pause
