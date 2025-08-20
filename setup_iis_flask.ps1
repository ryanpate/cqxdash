# Run as Administrator

Write-Host "Setting up CQI Dashboard for IIS..." -ForegroundColor Cyan

# 1. Create directories
$baseDir = "E:\inetpub\wwwroot\cqxdashboard"
$apiDir = "$baseDir\api"
$logsDir = "$baseDir\logs"

New-Item -ItemType Directory -Path $baseDir -Force
New-Item -ItemType Directory -Path $apiDir -Force
New-Item -ItemType Directory -Path $logsDir -Force

# 2. Create run script
$runScript = @'
from app import app
import sys
import logging

# Setup logging
logging.basicConfig(
    filename='E:/inetpub/wwwroot/cqxdashboard/logs/flask_api.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

# Add console output
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

if __name__ == '__main__':
    print("Starting CQI Dashboard API on port 5000...")
    try:
        app.run(host='127.0.0.1', port=5000, debug=False, threaded=True)
    except Exception as e:
        logging.error(f"Failed to start: {e}")
        sys.exit(1)
'@

Set-Content -Path "$apiDir\run_flask.py" -Value $runScript

# 3. Create batch file
$batchFile = @'
@echo off
cd /d C:\inetpub\wwwroot\cqxdashboard\api
echo Starting CQI Dashboard API...

:LOOP
python run_flask.py
echo API stopped. Restarting in 5 seconds...
timeout /t 5
goto LOOP
'@

Set-Content -Path "$apiDir\start_api.bat" -Value $batchFile

# 4. Set permissions
icacls $baseDir /grant "IIS_IUSRS:(OI)(CI)F" /T
icacls $baseDir /grant "IIS AppPool\DefaultAppPool:(OI)(CI)F" /T

# 5. Create scheduled task
$action = New-ScheduledTaskAction -Execute "$apiDir\start_api.bat" -WorkingDirectory $apiDir
$trigger = New-ScheduledTaskTrigger -AtStartup
$settings = New-ScheduledTaskSettingsSet -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
$principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest

Register-ScheduledTask -TaskName "CQI_Dashboard_API" -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force

# 6. Start the task
Start-ScheduledTask -TaskName "CQI_Dashboard_API"

Write-Host "Setup complete! Checking status..." -ForegroundColor Green

# Wait for startup
Start-Sleep -Seconds 5

# Check if running
$port = Get-NetTCPConnection -LocalPort 5000 -ErrorAction SilentlyContinue
if ($port) {
    Write-Host "✓ API is running on port 5000" -ForegroundColor Green
} else {
    Write-Host "✗ API is not running. Check logs at $logsDir" -ForegroundColor Red
}