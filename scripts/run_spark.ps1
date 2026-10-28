# PowerShell script to launch PySpark Structured Streaming Job
# DASSS Big Data Case Study - PICT Pune

param (
    [string]$BootstrapServers = "localhost:9092",
    [string]$Topic = "energy-readings",
    [double]$Threshold = 5.0
)

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$PythonExe = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $PythonExe)) { $PythonExe = "python" }

# Set Windows winutils & HADOOP_HOME
$WinutilsDir = Join-Path $ProjectRoot "tools\winutils"
if (Test-Path $WinutilsDir) {
    $env:HADOOP_HOME = $WinutilsDir
    $env:PATH = "$(Join-Path $WinutilsDir 'bin');$env:PATH"
    Write-Host "[OK] Configured HADOOP_HOME to: $WinutilsDir" -ForegroundColor Green
}

$SparkScript = Join-Path $ProjectRoot "spark\streaming_job.py"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " Launching PySpark Structured Streaming Job" -ForegroundColor Cyan
Write-Host " Kafka: $BootstrapServers | Topic: $Topic | Threshold: ${Threshold} kW" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

& $PythonExe $SparkScript --bootstrap-servers $BootstrapServers --topic $Topic --threshold $Threshold
