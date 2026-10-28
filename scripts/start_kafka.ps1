# PowerShell script to start Kafka and Zookeeper via Docker Compose
# DASSS Big Data Case Study - PICT Pune

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
Set-Location $ProjectRoot

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " Starting Apache Kafka & Zookeeper" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# Check if Docker CLI exists
$dockerCmd = Get-Command docker -ErrorAction SilentlyContinue
if (-not $dockerCmd) {
    Write-Error "Docker is not installed or not in PATH."
    exit 1
}

# Check if Docker daemon is responsive
try {
    $null = docker info 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Docker engine not responding"
    }
} catch {
    Write-Warning "Docker Desktop engine is not running."
    Write-Host "Please start Docker Desktop from the Start menu and wait for the engine to initialize." -ForegroundColor Yellow
    exit 1
}

Write-Host "Bringing up containers with Docker Compose..." -ForegroundColor Yellow
docker compose up -d

Write-Host "Waiting for Kafka broker to initialize on localhost:9092..." -ForegroundColor Yellow
Start-Sleep -Seconds 8

docker compose ps

Write-Host "[SUCCESS] Kafka and Zookeeper are active. Broker available at localhost:9092" -ForegroundColor Green
