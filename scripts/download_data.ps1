# PowerShell script to download or generate the UCI Household Power Consumption dataset
# DASSS Big Data Case Study - PICT Pune

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$DataDir = Join-Path $ProjectRoot "data"
$DataFile = Join-Path $DataDir "household_power_consumption.txt"
$ZipFile = Join-Path $DataDir "household_power_consumption.zip"
$Url = "https://archive.ics.uci.edu/static/public/235/individual+household+electric+power+consumption.zip"

if (-not (Test-Path $DataDir)) {
    New-Item -ItemType Directory -Path $DataDir -Force | Out-Null
}

if ((Test-Path $DataFile) -and ((Get-Item $DataFile).Length -gt 1048576)) {
    Write-Host "[OK] Dataset file already exists: $DataFile ($((Get-Item $DataFile).Length) bytes)" -ForegroundColor Green
    exit 0
}

Write-Host "Attempting to download UCI dataset via PowerShell from: $Url..." -ForegroundColor Cyan

try {
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    Invoke-WebRequest -Uri $Url -OutFile $ZipFile -TimeoutSec 30 -ErrorAction Stop
    Write-Host "Extracting ZIP archive..." -ForegroundColor Yellow
    Expand-Archive -Path $ZipFile -DestinationPath $DataDir -Force
    Remove-Item -Path $ZipFile -Force -ErrorAction SilentlyContinue
    if (Test-Path $DataFile) {
        Write-Host "[SUCCESS] Dataset ready at: $DataFile" -ForegroundColor Green
        exit 0
    }
} catch {
    Write-Warning "PowerShell download failed: $_. Invoking Python fallback generator..."
}

# Python Fallback
$PythonExe = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $PythonExe)) {
    $PythonExe = "python"
}

$PyScript = Join-Path $ScriptDir "download_data.py"
& $PythonExe $PyScript

if (Test-Path $DataFile) {
    Write-Host "[SUCCESS] Dataset verified at: $DataFile" -ForegroundColor Green
} else {
    Write-Error "Failed to obtain or generate dataset."
    exit 1
}
