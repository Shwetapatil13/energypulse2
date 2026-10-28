# PowerShell script to gracefully stop all running pipeline components
# DASSS Big Data Case Study - PICT Pune

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " Stopping EnergyPulse Pipeline Services" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# Stop Uvicorn & Python streaming processes
Write-Host "Terminating Python and Spark processes..." -ForegroundColor Yellow
Get-Process python -ErrorAction SilentlyContinue | Where-Object {
    $_.Path -like "*MDM*" -or $_.CommandLine -like "*energypulse*" -or $_.CommandLine -like "*uvicorn*"
} | Stop-Process -Force -ErrorAction SilentlyContinue

# Stop Docker Compose
Write-Host "Stopping Docker containers..." -ForegroundColor Yellow
Set-Location $ProjectRoot
if (Get-Command docker -ErrorAction SilentlyContinue) {
    docker compose down --remove-orphans -ErrorAction SilentlyContinue
}

Write-Host "[OK] All EnergyPulse services stopped cleanly." -ForegroundColor Green
