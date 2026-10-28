# PowerShell Setup Script for EnergyPulse
# DASSS Big Data Case Study - PICT Pune

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " EnergyPulse Project Environment Setup" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Check Python
$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
    Write-Error "Python 3.10+ not found in PATH. Please install Python and try again."
    exit 1
}
Write-Host "[OK] Detected Python: $(& python --version)" -ForegroundColor Green

# 2. Check Java
$javaCmd = Get-Command java -ErrorAction SilentlyContinue
if (-not $javaCmd) {
    Write-Warning "[WARN] Java not found in PATH. Apache Spark requires Java 11, 17, or 21."
} else {
    Write-Host "[OK] Detected Java: $(& java -version 2>&1 | Select-Object -First 1)" -ForegroundColor Green
}

# 3. Create Virtual Environment
$venvPath = Join-Path $ProjectRoot ".venv"
if (-not (Test-Path $venvPath)) {
    Write-Host "Creating Python virtual environment at .venv..." -ForegroundColor Yellow
    & python -m venv $venvPath
}
$pythonExe = Join-Path $venvPath "Scripts\python.exe"

# 4. Install Dependencies
Write-Host "Installing requirements from requirements.txt..." -ForegroundColor Yellow
& $pythonExe -m pip install --upgrade pip
& $pythonExe -m pip install -r (Join-Path $ProjectRoot "requirements.txt")

# 5. Create Required Output & Checkpoint Directories
$dirs = @("data", "output\windowed", "output\alerts", "output\csv", "checkpoints\windowed", "checkpoints\alerts", "checkpoints\csv")
foreach ($d in $dirs) {
    $fullPath = Join-Path $ProjectRoot $d
    if (-not (Test-Path $fullPath)) {
        New-Item -ItemType Directory -Path $fullPath -Force | Out-Null
    }
}

Write-Host "[SUCCESS] EnergyPulse environment setup completed!" -ForegroundColor Green
Write-Host "Next step: Run '.\scripts\download_data.ps1' to fetch or generate the dataset." -ForegroundColor Cyan
