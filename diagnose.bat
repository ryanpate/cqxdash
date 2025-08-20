@echo off
echo ================================================
echo CQI Dashboard Diagnostics
echo ================================================
echo.

echo 1. Checking IIS Service...
sc query W3SVC | findstr STATE
echo.

echo 2. Checking ports...
echo Port 8000 (IIS):
netstat -an | findstr :8000
echo.
echo Port 5000 (API):
netstat -an | findstr :5000
echo.

echo 3. Checking IIS sites...
%windir%\system32\inetsrv\appcmd list sites
echo.

echo 4. Testing localhost access...
curl -I http://localhost:8000
echo.

echo 5. Checking Windows Firewall...
netsh advfirewall firewall show rule name=all | findstr 8000
echo.

pause