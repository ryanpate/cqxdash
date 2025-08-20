@echo off
cd /d C:\inetpub\wwwroot\cqxdashboard\api
echo Starting CQI Dashboard API...
echo %date% %time% Starting API >> C:\inetpub\wwwroot\cqxdashboard\logs\startup.log

:RESTART
python app.py
echo %date% %time% API crashed, restarting... >> C:\inetpub\wwwroot\cqxdashboard\logs\startup.log
timeout /t 5 /nobreak
goto RESTART