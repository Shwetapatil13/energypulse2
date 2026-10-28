# PowerShell script to launch EnergyPulse Dashboard
# DASSS Big Data Case Study - PICT Pune

param (
    [string]$HostIp = "127.0.0.1",
    [int]$Port = 8000
)

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$PythonExe = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $PythonExe)) { $PythonExe = "python" }

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " Starting EnergyPulse Web Dashboard & WebSocket Server" -ForegroundColor Cyan
Write-Host " URL: http://${HostIp}:${Port}/" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

Set-Location $ProjectRoot
& $PythonExe -m uvicorn dashboard.server:app --host $HostIp --port $Port --reload
