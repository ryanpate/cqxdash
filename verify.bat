@echo off
echo ================================================
echo CQI Dashboard Configuration Check
echo ================================================
echo.

echo Configuration:
echo - Drive: E:\
echo - Web Port: 8000
echo - API Port: 5000
echo - URL: http://cqxdashboard.web.att.com:8000
echo.

echo Checking components...
echo.

REM Check directories
if exist "E:\inetpub\wwwroot\cqxdashboard\index.html" (
    echo [OK] HTML files present
) else (
    echo [ERROR] HTML files missing
)

if exist "E:\inetpub\wwwroot\cqxdashboard\api\app.py" (
    echo [OK] API files present  
) else (
    echo [ERROR] API files missing
)

if exist "E:\inetpub\wwwroot\cqxdashboard\api\private_key.txt" (
    echo [OK] Private key present
) else (
    echo [WARNING] Private key missing - will use sample data
)

REM Check ports
echo.
netstat -an | findstr :5000 >nul
if %errorlevel%==0 (
    echo [OK] Port 5000 (API) is listening
) else (
    echo [ERROR] Port 5000 (API) is not active
)

netstat -an | findstr :8000 >nul
if %errorlevel%==0 (
    echo [OK] Port 8000 (Web) is listening
) else (
    echo [WARNING] Port 8000 (Web) may not be configured
)

echo.
echo Testing endpoints...
curl -f -s http://127.0.0.1:5000/api/health >nul 2>&1
if %errorlevel%==0 (
    echo [OK] API endpoint responding
) else (
    echo [ERROR] API endpoint not responding
)

echo.
echo ================================================
echo Dashboard should be available at:
echo http://cqxdashboard.web.att.com:8000
echo ================================================
pause