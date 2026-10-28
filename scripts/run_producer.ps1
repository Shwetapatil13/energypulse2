# PowerShell script to run the Historical Replay Producer
# Usage: .\scripts\run_producer.ps1 [-Limit 2000] [-Delay 0.05]

param (
    [int]$Limit = 0,
    [double]$Delay = 0.05,
    [string]$BootstrapServers = "localhost:9092",
    [string]$Topic = "energy-readings",
    [string]$DataFile = "data/household_power_consumption.txt"
)

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$PythonExe = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $PythonExe)) { $PythonExe = "python" }

$ProducerScript = Join-Path $ProjectRoot "producer\producer.py"

$ArgsList = @(
    $ProducerScript,
    "--file", (Join-Path $ProjectRoot $DataFile),
    "--delay", $Delay,
    "--bootstrap-servers", $BootstrapServers,
    "--topic", $Topic
)

if ($Limit -gt 0) {
    $ArgsList += @("--limit", $Limit)
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " Starting EnergyPulse Historical Replay Producer" -ForegroundColor Cyan
Write-Host " Topic: $Topic | Delay: ${Delay}s | Limit: $(if ($Limit -gt 0) { $Limit } else { 'All' })" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

& $PythonExe @ArgsList
