$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$venvPython = Join-Path $projectRoot ".venv\Scripts\python.exe"
$venvActivate = Join-Path $projectRoot ".venv\Scripts\Activate.ps1"
$frontendNodeModules = Join-Path $projectRoot "frontend\node_modules"

if (-not (Test-Path -LiteralPath $venvPython)) {
    Write-Host "Missing .venv\Scripts\python.exe. Create .venv and install Python dependencies first." -ForegroundColor Red
    exit 1
}
if (-not (Test-Path -LiteralPath $frontendNodeModules)) {
    Write-Host "Missing frontend\node_modules. Run: cd frontend; npm install" -ForegroundColor Red
    exit 1
}
if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
    Write-Host "npm was not found. Install Node.js 22 LTS or 24 LTS, then reopen PowerShell." -ForegroundColor Red
    exit 1
}

function Quote-PowerShell([string]$value) { return "'" + $value.Replace("'", "''") + "'" }
function Wait-ForUrl([string]$url, [string]$name) {
    $deadline = (Get-Date).AddSeconds(60)
    while ((Get-Date) -lt $deadline) {
        try {
            Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 2 | Out-Null
            Write-Host "$name is ready: $url" -ForegroundColor Green
            return $true
        } catch { Start-Sleep -Milliseconds 500 }
    }
    Write-Host "$name was not ready after 60 seconds. Check the two PowerShell windows for errors." -ForegroundColor Yellow
    return $false
}

$rootLiteral = Quote-PowerShell $projectRoot
$activateLiteral = Quote-PowerShell $venvActivate
$backendCommand = "Set-Location -LiteralPath $rootLiteral; & $activateLiteral; python -m uvicorn backend.app.main:app --reload"
$frontendCommand = "Set-Location -LiteralPath $rootLiteral; Set-Location frontend; npm run dev"

Start-Process powershell.exe -ArgumentList "-NoExit", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", $backendCommand
Start-Process powershell.exe -ArgumentList "-NoExit", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", $frontendCommand

$backendReady = Wait-ForUrl "http://127.0.0.1:8000/health" "Backend"
$frontendReady = Wait-ForUrl "http://127.0.0.1:5173" "Frontend"
if ($backendReady -and $frontendReady) { Start-Process "http://127.0.0.1:5173" } else { exit 1 }
