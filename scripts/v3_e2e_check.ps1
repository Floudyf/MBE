param(
  [int]$BackendPort = 8000,
  [int]$FrontendPort = 5173
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$frontendRoot = Join-Path $root "frontend"
$pythonExe = Join-Path $env:LOCALAPPDATA "Programs\Python\Python312\python.exe"
if (-not (Test-Path -LiteralPath $pythonExe)) {
  $pythonExe = "py"
  $pythonArgs = @("-3.12")
} else {
  $pythonArgs = @()
}

function Wait-Http {
  param(
    [string]$Url,
    [string]$Name,
    [int]$TimeoutSeconds = 90
  )
  $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
  while ((Get-Date) -lt $deadline) {
    try {
      $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 3
      if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500) {
        Write-Host "$Name is ready at $Url"
        return
      }
    } catch {
      Start-Sleep -Milliseconds 750
    }
  }
  throw "$Name did not become ready at $Url within $TimeoutSeconds seconds."
}

function Stop-ProcessTree {
  param([int]$ProcessId)
  if (-not $ProcessId) { return }
  $children = Get-CimInstance Win32_Process -Filter "ParentProcessId = $ProcessId" -ErrorAction SilentlyContinue
  foreach ($child in $children) {
    Stop-ProcessTree -ProcessId $child.ProcessId
  }
  Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
}

$backend = $null
$frontend = $null
$exitCode = 0

try {
  $env:PYTHONPATH = $root
  $backendArgs = @($pythonArgs + @("-m", "uvicorn", "backend.app.main:app", "--host", "127.0.0.1", "--port", [string]$BackendPort))
  $backend = Start-Process -FilePath $pythonExe -ArgumentList $backendArgs -WorkingDirectory $root -PassThru -WindowStyle Hidden
  $frontend = Start-Process -FilePath "npm.cmd" -ArgumentList @("run", "dev", "--", "--host", "127.0.0.1", "--port", [string]$FrontendPort) -WorkingDirectory $frontendRoot -PassThru -WindowStyle Hidden

  Wait-Http -Url "http://127.0.0.1:$BackendPort/health" -Name "backend"
  Wait-Http -Url "http://127.0.0.1:$FrontendPort" -Name "frontend"

  Push-Location $frontendRoot
  try {
    & npx.cmd playwright test
    $exitCode = $LASTEXITCODE
  } finally {
    Pop-Location
  }
} catch {
  Write-Error $_
  $exitCode = 1
} finally {
  if ($frontend) { Stop-ProcessTree -ProcessId $frontend.Id }
  if ($backend) { Stop-ProcessTree -ProcessId $backend.Id }
}

exit $exitCode
