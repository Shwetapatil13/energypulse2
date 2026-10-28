# PowerShell script to launch all EnergyPulse pipeline components in separate PowerShell windows
# DASSS Big Data Case Study - PICT Pune

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " Launching Complete EnergyPulse Real-Time Pipeline" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Start Dashboard
Write-Host "1. Launching Dashboard Server in a new window..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-File", (Join-Path $ScriptDir "run_dashboard.ps1")

Start-Sleep -Seconds 2

# 2. Start Kafka
Write-Host "2. Checking / Starting Kafka via Docker Compose..." -ForegroundColor Yellow
& (Join-Path $ScriptDir "start_kafka.ps1")

# 3. Start Spark Streaming Job
Write-Host "3. Launching Spark Streaming Job in a new window..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-File", (Join-Path $ScriptDir "run_spark.ps1")

Start-Sleep -Seconds 5

# 4. Start Producer Replay
Write-Host "4. Launching Historical Replay Producer in a new window..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-File", (Join-Path $ScriptDir "run_producer.ps1")

Write-Host ""
Write-Host "[SUCCESS] All pipeline services have been launched!" -ForegroundColor Green
Write-Host "Open the dashboard in your browser: http://127.0.0.1:8000/" -ForegroundColor Cyan
