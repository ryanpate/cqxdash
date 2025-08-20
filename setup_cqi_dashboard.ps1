# Run as Administrator
Write-Host "===============================================" -ForegroundColor Cyan
Write-Host "  CQI Dashboard Setup for IIS" -ForegroundColor Cyan
Write-Host "  Drive: E:\" -ForegroundColor Cyan
Write-Host "  Web Port: 8000" -ForegroundColor Cyan
Write-Host "  API Port: 5000" -ForegroundColor Cyan
Write-Host "===============================================" -ForegroundColor Cyan

# 1. Create directories
Write-Host "`nCreating directories on E: drive..." -ForegroundColor Yellow

$baseDir = "E:\inetpub\wwwroot\cqxdashboard"
$apiDir = "$baseDir\api"
$logsDir = "$baseDir\logs"

New-Item -ItemType Directory -Path $baseDir -Force | Out-Null
New-Item -ItemType Directory -Path $apiDir -Force | Out-Null
New-Item -ItemType Directory -Path $logsDir -Force | Out-Null

Write-Host "✓ Directories created" -ForegroundColor Green

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
Write-Host "✓ Python runner script created" -ForegroundColor Green

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
Write-Host "✓ Batch file created" -ForegroundColor Green

# 4. Set permissions
Write-Host "Setting permissions..." -ForegroundColor Yellow
icacls $baseDir /grant "IIS_IUSRS:(OI)(CI)F" /T /Q
icacls $baseDir /grant "IIS AppPool\DefaultAppPool:(OI)(CI)F" /T /Q
icacls $baseDir /grant "NETWORK SERVICE:(OI)(CI)F" /T /Q
icacls $baseDir /grant "Everyone:(OI)(CI)RX" /T /Q
Write-Host "✓ Permissions set" -ForegroundColor Green

# 5. Configure IIS site
Write-Host "Configuring IIS..." -ForegroundColor Yellow

Import-Module WebAdministration -ErrorAction SilentlyContinue

# Check if site exists
$siteName = "cqxdashboard.web.att.com"
$site = Get-Website | Where-Object { $_.Name -like "*cqx*" -or $_.Name -eq $siteName }

if ($site) {
    # Update physical path
    Set-ItemProperty "IIS:\Sites\$($site.Name)" -Name physicalPath -Value $baseDir
    
    # Update binding to port 8000
    $binding = Get-WebBinding -Name $site.Name
    if ($binding) {
        Set-WebBinding -Name $site.Name -BindingInformation "*:8000:" -PropertyName Port -Value 8000
    }
    
    Write-Host "✓ IIS site configured" -ForegroundColor Green
} else {
    Write-Host "✗ IIS site not found. Please create it manually." -ForegroundColor Red
}

# 6. Create scheduled task
Write-Host "Creating scheduled task for API..." -ForegroundColor Yellow

# Remove existing task if it exists
Unregister-ScheduledTask -TaskName "CQI_Dashboard_API" -Confirm:$false -ErrorAction SilentlyContinue

$action = New-ScheduledTaskAction -Execute "$apiDir\start_api.bat" -WorkingDirectory $apiDir
$trigger = New-ScheduledTaskTrigger -AtStartup
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RestartCount 999 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit (New-TimeSpan -Days 365)

$principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest

Register-ScheduledTask -TaskName "CQI_Dashboard_API" -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null

Write-Host "✓ Scheduled task created" -ForegroundColor Green

# 7. Start the API
Write-Host "Starting the API service..." -ForegroundColor Yellow
Start-ScheduledTask -TaskName "CQI_Dashboard_API"

Write-Host "Waiting for API to start..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

# 8. Verify everything is working
Write-Host "`nVerifying setup..." -ForegroundColor Cyan
Write-Host ("=" * 50)

# Check port 5000
$port5000 = Get-NetTCPConnection -LocalPort 5000 -ErrorAction SilentlyContinue
if ($port5000) {
    Write-Host "✓ API is running on port 5000" -ForegroundColor Green
    
    # Test API health
    try {
        $response = Invoke-WebRequest -Uri "http://127.0.0.1:5000/api/health" -UseBasicParsing -TimeoutSec 5
        Write-Host "✓ API health check successful" -ForegroundColor Green
    } catch {
        Write-Host "✗ API health check failed" -ForegroundColor Red
    }
} else {
    Write-Host "✗ API is not running on port 5000" -ForegroundColor Red
    Write-Host "  Try running manually: $apiDir\start_api.bat" -ForegroundColor Yellow
}

# Check IIS
$iisStatus = Get-Website | Where-Object { $_.Name -like "*cqx*" }
if ($iisStatus -and $iisStatus.State -eq "Started") {
    Write-Host "✓ IIS site is running" -ForegroundColor Green
} else {
    Write-Host "✗ IIS site is not running" -ForegroundColor Red
}

Write-Host ""
Write-Host ("=" * 50) -ForegroundColor Cyan
Write-Host "Setup Complete!" -ForegroundColor Green
Write-Host ("=" * 50) -ForegroundColor Cyan
Write-Host ""
Write-Host "Dashboard URL: http://cqxdashboard.web.att.com:8000" -ForegroundColor Cyan
Write-Host "Direct API URL: http://localhost:5000/api/health" -ForegroundColor Cyan
Write-Host ""
Write-Host "Logs location: $logsDir" -ForegroundColor Yellow