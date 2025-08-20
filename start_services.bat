@echo off
title CQI Dashboard Manager
color 0A
cls

echo ================================================
echo     CQI DASHBOARD SERVICE MANAGER
echo     Web Port: 8000  ^|  API Port: 5000
echo ================================================
echo.

:MENU
echo What would you like to do?
echo.
echo [1] Start API Service
echo [2] Stop API Service  
echo [3] Restart API Service
echo [4] Check Status
echo [5] Open Dashboard in Browser
echo [6] View Logs
echo [7] Exit
echo.
set /p choice="Enter your choice (1-7): "

if "%choice%"=="1" goto START
if "%choice%"=="2" goto STOP
if "%choice%"=="3" goto RESTART
if "%choice%"=="4" goto STATUS
if "%choice%"=="5" goto BROWSER
if "%choice%"=="6" goto LOGS
if "%choice%"=="7" exit
goto MENU

:START
echo.
echo Starting API Service...
schtasks /run /tn "CQI_Dashboard_API"
timeout /t 3 >nul
goto STATUS

:STOP
echo.
echo Stopping API Service...
schtasks /end /tn "CQI_Dashboard_API"
taskkill /F /IM python.exe /FI "WINDOWTITLE eq CQI Dashboard API*" >nul 2>&1
echo Service stopped.
echo.
pause
goto MENU

:RESTART
echo.
echo Restarting API Service...
schtasks /end /tn "CQI_Dashboard_API" >nul 2>&1
taskkill /F /IM python.exe /FI "WINDOWTITLE eq CQI Dashboard API*" >nul 2>&1
timeout /t 2 >nul
schtasks /run /tn "CQI_Dashboard_API"
echo Service restarted.
timeout /t 3 >nul
goto STATUS

:STATUS
echo.
echo ================================================
echo SERVICE STATUS CHECK
echo ================================================
echo.
echo Checking Flask API on port 5000...
netstat -an | findstr :5000 >nul
if %errorlevel%==0 (
    echo [RUNNING] API is active on port 5000
    curl -s http://127.0.0.1:5000/api/health
) else (
    echo [STOPPED] API is not running on port 5000
)

echo.
echo Checking IIS on port 8000...
netstat -an | findstr :8000 >nul
if %errorlevel%==0 (
    echo [RUNNING] IIS is active on port 8000
) else (
    echo [WARNING] IIS may not be configured on port 8000
)

echo.
echo ================================================
echo.
pause
goto MENU

:BROWSER
echo.
echo Opening dashboard in browser...
start http://cqxdashboard.web.att.com:8000
goto MENU

:LOGS
echo.
echo Opening logs folder...
start E:\inetpub\wwwroot\cqxdashboard\logs
goto MENU