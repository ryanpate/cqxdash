# Run as Administrator
Write-Host "===============================================" -ForegroundColor Cyan
Write-Host "  CQI Dashboard Setup for IIS" -ForegroundColor Cyan
Write-Host "  Drive: E:" -ForegroundColor Cyan
Write-Host "  Web Port: 8000" -ForegroundColor Cyan
Write-Host "  API Port: 5000" -ForegroundColor Cyan
Write-Host "===============================================" -ForegroundColor Cyan

# 1. Create directories
Write-Host "" -ForegroundColor Yellow
Write-Host "Creating directories on E: drive..." -ForegroundColor Yellow

$baseDir = "E:\inetpub\wwwroot\cqxdashboard"
$apiDir = "$baseDir\api"
$logsDir = "$baseDir\logs"

New-Item -ItemType Directory -Path $baseDir -Force | Out-Null
New-Item -ItemType Directory -Path $apiDir -Force | Out-Null
New-Item -ItemType Directory -Path $logsDir -Force | Out-Null

Write-Host "[OK] Directories created" -ForegroundColor Green

# 2. Create Flask runner script
Write-Host "Creating Flask runner script..." -ForegroundColor Yellow

$runScript = @'
from app import app
import sys
import logging
import os

# Change to API directory
os.chdir('E:/inetpub/wwwroot/cqxdashboard/api')

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
    print("="*50)
    print("CQI Dashboard API Starting")
    print("Port: 5000")
    print("="*50)
    try:
        app.run(host='127.0.0.1', port=5000, debug=False, threaded=True)
    except Exception as e:
        logging.error(f"Failed to start: {e}")
        sys.exit(1)
'@

Set-Content -Path "$apiDir\run_flask.py" -Value $runScript
Write-Host "[OK] Python runner script created" -ForegroundColor Green

# 3. Create batch file for API
Write-Host "Creating batch file..." -ForegroundColor Yellow

$batchFile = @'
@echo off
title CQI Dashboard API - Port 5000
E:
cd E:\inetpub\wwwroot\cqxdashboard\api
echo ========================================
echo CQI Dashboard API
echo Starting on port 5000...
echo ========================================
echo.
echo %date% %time% Starting API >> E:\inetpub\wwwroot\cqxdashboard\logs\startup.log

:LOOP
python run_flask.py
echo.
echo %date% %time% API stopped, restarting in 5 seconds... >> E:\inetpub\wwwroot\cqxdashboard\logs\startup.log
echo API crashed! Restarting in 5 seconds...
timeout /t 5
goto LOOP
'@

Set-Content -Path "$apiDir\start_api.bat" -Value $batchFile
Write-Host "[OK] Batch file created" -ForegroundColor Green

# 4. Set permissions
Write-Host "Setting permissions..." -ForegroundColor Yellow
icacls $baseDir /grant "IIS_IUSRS:(OI)(CI)F" /T /Q
icacls $baseDir /grant "IIS AppPool\DefaultAppPool:(OI)(CI)F" /T /Q
icacls $baseDir /grant "NETWORK SERVICE:(OI)(CI)F" /T /Q
icacls $baseDir /grant "Everyone:(OI)(CI)RX" /T /Q
Write-Host "[OK] Permissions set" -ForegroundColor Green

# 5. Configure IIS site
Write-Host "Configuring IIS..." -ForegroundColor Yellow

Import-Module WebAdministration -ErrorAction SilentlyContinue

# Check if site exists
$siteName = "cqxdashboard.web.att.com"
$sites = Get-Website
$site = $null

foreach ($s in $sites) {
    if ($s.Name -like "*cqx*" -or $s.Name -eq $siteName) {
        $site = $s
        break
    }
}

if ($site) {
    # Update physical path
    Set-ItemProperty "IIS:\Sites\$($site.Name)" -Name physicalPath -Value $baseDir
    Write-Host "[OK] IIS site configured" -ForegroundColor Green
} else {
    Write-Host "[WARNING] IIS site not found. Please create it manually." -ForegroundColor Red
}

# 6. Create scheduled task
Write-Host "Creating scheduled task for API..." -ForegroundColor Yellow

# Remove existing task if it exists
$existingTask = Get-ScheduledTask -TaskName "CQI_Dashboard_API" -ErrorAction SilentlyContinue
if ($existingTask) {
    Unregister-ScheduledTask -TaskName "CQI_Dashboard_API" -Confirm:$false
}

$action = New-ScheduledTaskAction -Execute "$apiDir\start_api.bat" -WorkingDirectory $apiDir
$trigger = New-ScheduledTaskTrigger -AtStartup
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1)
$principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest

Register-ScheduledTask -TaskName "CQI_Dashboard_API" -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null

Write-Host "[OK] Scheduled task created" -ForegroundColor Green

# 7. Start the API
Write-Host "Starting the API service..." -ForegroundColor Yellow
Start-ScheduledTask -TaskName "CQI_Dashboard_API"

Write-Host "Waiting for API to start..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

# 8. Verify everything is working
Write-Host "" -ForegroundColor Cyan
Write-Host "Verifying setup..." -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan

# Check port 5000
$port5000 = Get-NetTCPConnection -LocalPort 5000 -ErrorAction SilentlyContinue
if ($port5000) {
    Write-Host "[OK] API is running on port 5000" -ForegroundColor Green
    
    # Test API health
    try {
        $response = Invoke-WebRequest -Uri "http://127.0.0.1:5000/api/health" -UseBasicParsing -TimeoutSec 5
        Write-Host "[OK] API health check successful" -ForegroundColor Green
    } catch {
        Write-Host "[ERROR] API health check failed" -ForegroundColor Red
    }
} else {
    Write-Host "[ERROR] API is not running on port 5000" -ForegroundColor Red
    Write-Host "Try running manually: $apiDir\start_api.bat" -ForegroundColor Yellow
}

# Check IIS
$iisStatus = Get-Website
$iisRunning = $false
foreach ($site in $iisStatus) {
    if ($site.Name -like "*cqx*") {
        if ($site.State -eq "Started") {
            $iisRunning = $true
        }
        break
    }
}

if ($iisRunning) {
    Write-Host "[OK] IIS site is running" -ForegroundColor Green
} else {
    Write-Host "[ERROR] IIS site is not running" -ForegroundColor Red
}

Write-Host ""
Write-Host "================================================" -ForegroundColor Cyan
Write-Host "Setup Complete!" -ForegroundColor Green
Write-Host "================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Dashboard URL: http://cqxdashboard.web.att.com:8000" -ForegroundColor Cyan
Write-Host "Direct API URL: http://localhost:5000/api/health" -ForegroundColor Cyan
Write-Host ""
Write-Host "Logs location:" -ForegroundColor Yellow
Write-Host $logsDir -ForegroundColor Yellow