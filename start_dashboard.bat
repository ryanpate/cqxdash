@echo off
REM start_dashboard.bat - Windows startup script

echo ========================================
echo Starting CQI Dashboard...
echo ========================================

REM Check if Python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3 from https://www.python.org/
    pause
    exit /b 1
)

REM Check and install dependencies
echo Checking dependencies...
python -c "import flask" 2>nul
if %errorlevel% neq 0 (
    echo Installing required packages...
    pip install flask flask-cors snowflake-connector-python pandas
)

REM Start Flask API in new window
echo Starting Flask API server...
start "CQI Dashboard API" cmd /k python app.py

REM Wait for API to start
timeout /t 3 /nobreak >nul

REM Check if API is running
curl -s http://localhost:5000/api/health >nul 2>&1
if %errorlevel% equ 0 (
    echo API is running successfully!
) else (
    echo WARNING: API might not be running. Check the API window for errors.
)

REM Start web server in new window
echo Starting web server for dashboard...
start "CQI Dashboard Web Server" cmd /k python -m http.server 8080

REM Wait for web server to start
timeout /t 2 /nobreak >nul

REM Open browser
echo Opening dashboard in browser...
start http://localhost:8080/index.html

echo ========================================
echo CQI Dashboard is running!
echo ========================================
echo Dashboard URL: http://localhost:8080/index.html
echo API URL: http://localhost:5000/api
echo.
echo Close this window to keep services running
echo Or press any key to stop all services
echo ========================================

pause

REM Stop services
echo Stopping services...
taskkill /FI "WindowTitle eq CQI Dashboard API*" /T /F >nul 2>&1
taskkill /FI "WindowTitle eq CQI Dashboard Web Server*" /T /F >nul 2>&1
echo Services stopped. Goodbye!
pause