[CmdletBinding()]
param(
    [switch]$SkipToolInstall,
    [switch]$SkipDependencyInstall
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2.0
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$projectRoot = Split-Path -Parent $PSScriptRoot
$venvDir = Join-Path $projectRoot ".venv"
$venvPython = Join-Path $venvDir "Scripts\python.exe"
$requirementsFile = Join-Path $projectRoot "backend\requirements.txt"
$frontendDir = Join-Path $projectRoot "frontend"
$frontendPackage = Join-Path $frontendDir "package.json"
$frontendLock = Join-Path $frontendDir "package-lock.json"
$toolsDir = Join-Path $projectRoot ".tools"

function Write-Step([string]$message) {
    Write-Host "`n==> $message" -ForegroundColor Cyan
}

function Write-Ok([string]$message) {
    Write-Host "[OK] $message" -ForegroundColor Green
}

function Write-Warn([string]$message) {
    Write-Host "[WARN] $message" -ForegroundColor Yellow
}

function Refresh-ProcessPath {
    $machinePath = [Environment]::GetEnvironmentVariable("Path", "Machine")
    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    $parts = @($machinePath, $userPath, $env:Path) | Where-Object { $_ }
    $env:Path = ($parts -join ";")
}

function Quote-PowerShell([string]$value) {
    return "'" + $value.Replace("'", "''") + "'"
}

function Invoke-NativeVisible {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [string[]]$Arguments = @()
    )

    # Native stdout is normally part of PowerShell's success stream.  When a
    # caller assigns a function result to a variable, that stdout would be
    # captured together with the intended return value.  Route it to the host
    # so functions such as Ensure-FrontendDependencies return only their
    # structured result/path.
    & $FilePath @Arguments 2>&1 | ForEach-Object { Write-Host $_ }
    return [int]$LASTEXITCODE
}

function Test-Url([string]$url) {
    try {
        Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 2 | Out-Null
        return $true
    } catch {
        return $false
    }
}

function Wait-ForUrl([string]$url, [string]$name) {
    $deadline = (Get-Date).AddSeconds(90)
    while ((Get-Date) -lt $deadline) {
        if (Test-Url $url) {
            Write-Ok "$name is ready: $url"
            return $true
        }
        Start-Sleep -Milliseconds 500
    }
    Write-Warn "$name was not ready after 90 seconds. Check its PowerShell window."
    return $false
}

function Get-ListeningProcessId([int]$port) {
    try {
        $connection = Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($connection) { return [int]$connection.OwningProcess }
    } catch {}
    return 0
}

function Test-Python312Candidate([string]$file, [string[]]$prefixArgs) {
    if (-not $file) { return $null }
    try {
        $versionText = (& $file @prefixArgs -c "import sys; print('.'.join(map(str, sys.version_info[:3])))" 2>$null | Select-Object -Last 1)
        if (-not $versionText) { return $null }
        $version = [Version]$versionText.Trim()
        if ($version.Major -eq 3 -and $version.Minor -eq 12) {
            return [pscustomobject]@{
                File = $file
                PrefixArgs = @($prefixArgs)
                Version = $version.ToString()
            }
        }
    } catch {}
    return $null
}

function Find-Python312 {
    Refresh-ProcessPath
    $candidates = New-Object System.Collections.Generic.List[object]

    $py = Get-Command py.exe -ErrorAction SilentlyContinue
    if ($py) { $candidates.Add([pscustomobject]@{ File = $py.Source; Args = @("-3.12") }) }

    foreach ($name in @("python.exe", "python3.exe")) {
        $command = Get-Command $name -ErrorAction SilentlyContinue
        if ($command) { $candidates.Add([pscustomobject]@{ File = $command.Source; Args = @() }) }
    }

    $patterns = @(
        (Join-Path $env:LocalAppData "Programs\Python\Python312*\python.exe"),
        (Join-Path $env:ProgramFiles "Python312*\python.exe")
    )
    if (${env:ProgramFiles(x86)}) {
        $patterns += (Join-Path ${env:ProgramFiles(x86)} "Python312*\python.exe")
    }

    foreach ($pattern in $patterns) {
        Get-ChildItem -Path $pattern -File -ErrorAction SilentlyContinue |
            Sort-Object FullName -Descending |
            ForEach-Object { $candidates.Add([pscustomobject]@{ File = $_.FullName; Args = @() }) }
    }

    $seen = @{}
    foreach ($candidate in $candidates) {
        $key = "$($candidate.File)|$($candidate.Args -join ' ')"
        if ($seen.ContainsKey($key)) { continue }
        $seen[$key] = $true
        $result = Test-Python312Candidate $candidate.File $candidate.Args
        if ($result) { return $result }
    }
    return $null
}

function Install-Python312 {
    if ($SkipToolInstall) { throw "Python 3.12 was not found and automatic tool installation is disabled." }

    Write-Step "Python 3.12 not found; installing it"
    $winget = Get-Command winget.exe -ErrorAction SilentlyContinue
    if ($winget) {
        Write-Host "Trying winget package Python.Python.3.12..."
        $wingetExit = Invoke-NativeVisible -FilePath $winget.Source -Arguments @(
            "install",
            "--id", "Python.Python.3.12",
            "--exact",
            "--silent",
            "--accept-package-agreements",
            "--accept-source-agreements"
        )
        if ($wingetExit -eq 0) {
            Refresh-ProcessPath
            $python = Find-Python312
            if ($python) { return $python }
        } else {
            Write-Warn "winget could not install Python 3.12; trying the official installer."
        }
    }

    $installerUrl = "https://www.python.org/ftp/python/3.12.10/python-3.12.10-amd64.exe"
    $installerPath = Join-Path $env:TEMP "python-3.12.10-amd64.exe"
    Write-Host "Downloading official Python installer..."
    Invoke-WebRequest -Uri $installerUrl -OutFile $installerPath -UseBasicParsing
    $process = Start-Process -FilePath $installerPath -ArgumentList @(
        "/quiet",
        "InstallAllUsers=0",
        "PrependPath=1",
        "Include_launcher=1",
        "Include_pip=1",
        "Include_test=0"
    ) -Wait -PassThru
    Remove-Item -LiteralPath $installerPath -Force -ErrorAction SilentlyContinue
    if ($process.ExitCode -ne 0) { throw "Python installer failed with exit code $($process.ExitCode)." }

    Refresh-ProcessPath
    $python = Find-Python312
    if (-not $python) { throw "Python 3.12 installation completed, but it still could not be detected. Reopen the terminal and retry." }
    return $python
}

function Test-VenvPython {
    if (-not (Test-Path -LiteralPath $venvPython)) { return $false }
    try {
        $versionText = (& $venvPython -c "import sys; print('.'.join(map(str, sys.version_info[:3])))" 2>$null | Select-Object -Last 1)
        if (-not $versionText) { return $false }
        $version = [Version]$versionText.Trim()
        return $version.Major -eq 3 -and $version.Minor -eq 12
    } catch {
        return $false
    }
}

function Ensure-VirtualEnvironment($basePython) {
    Write-Step "Checking Python virtual environment"
    if (Test-VenvPython) {
        Write-Ok "Reusable .venv detected: $venvPython"
    } else {
        if (Test-Path -LiteralPath $venvDir) {
            $backupName = ".venv.broken-{0}" -f (Get-Date -Format "yyyyMMdd-HHmmss")
            $backupPath = Join-Path $projectRoot $backupName
            Write-Warn "The copied .venv is not portable or uses the wrong Python. Moving it to $backupName"
            Move-Item -LiteralPath $venvDir -Destination $backupPath
        }

        Write-Host "Creating a clean .venv with Python $($basePython.Version)..."
        $baseArgs = @($basePython.PrefixArgs)
        & $basePython.File @baseArgs -m venv $venvDir
        if ($LASTEXITCODE -ne 0 -or -not (Test-VenvPython)) {
            throw "Failed to create a working Python 3.12 virtual environment."
        }
        Write-Ok "Created .venv"
    }

    if (-not (Test-Path -LiteralPath $requirementsFile)) {
        throw "Missing backend requirements file: $requirementsFile"
    }

    $requirementsHash = (Get-FileHash -LiteralPath $requirementsFile -Algorithm SHA256).Hash
    $stampFile = Join-Path $venvDir ".mbe_requirements.sha256"
    $stampHash = if (Test-Path -LiteralPath $stampFile) { (Get-Content -LiteralPath $stampFile -Raw).Trim() } else { "" }
    $importsOk = $false
    try {
        & $venvPython -c "import fastapi, uvicorn, pydantic, yaml" 2>$null
        $importsOk = $LASTEXITCODE -eq 0
    } catch {}

    if (-not $importsOk -or $stampHash -ne $requirementsHash) {
        if ($SkipDependencyInstall) { throw "Backend Python dependencies are missing or outdated." }
        Write-Step "Installing backend Python dependencies"
        & $venvPython -m pip install --upgrade pip
        if ($LASTEXITCODE -ne 0) { throw "pip upgrade failed." }
        & $venvPython -m pip install -r $requirementsFile
        if ($LASTEXITCODE -ne 0) { throw "Installing backend requirements failed." }
        Set-Content -LiteralPath $stampFile -Value $requirementsHash -Encoding ASCII
    }
    Write-Ok "Backend Python environment is ready"
}

function Test-NodeCandidate([string]$file) {
    if (-not $file) { return $null }
    try {
        $text = (& $file --version 2>$null | Select-Object -Last 1)
        if (-not $text) { return $null }
        $version = [Version]$text.Trim().TrimStart("v")
        if ($version.Major -ge 22 -and $version.Major -lt 25) {
            return [pscustomobject]@{ File = $file; Version = $version.ToString() }
        }
    } catch {}
    return $null
}

function Find-Node {
    Refresh-ProcessPath
    $files = @((Join-Path $toolsDir "node\node.exe"))
    $systemNode = Get-Command node.exe -ErrorAction SilentlyContinue
    if ($systemNode) { $files += $systemNode.Source }
    foreach ($file in $files) {
        $result = Test-NodeCandidate $file
        if ($result) { return $result }
    }
    return $null
}

function Install-PortableNode {
    if ($SkipToolInstall) { throw "Node.js 22-24 was not found and automatic tool installation is disabled." }
    Write-Step "Node.js 22-24 not found; installing a portable Node.js 22 runtime"

    New-Item -ItemType Directory -Path $toolsDir -Force | Out-Null
    $index = Invoke-WebRequest -Uri "https://nodejs.org/dist/latest-v22.x/" -UseBasicParsing
    $match = [regex]::Match($index.Content, 'href="(node-v22[^"/]+-win-x64\.zip)"')
    if (-not $match.Success) { throw "Could not resolve the latest Node.js 22 Windows archive." }

    $fileName = $match.Groups[1].Value
    $archivePath = Join-Path $env:TEMP $fileName
    $extractRoot = Join-Path $env:TEMP ("mbe-node-" + [guid]::NewGuid().ToString("N"))
    Invoke-WebRequest -Uri ("https://nodejs.org/dist/latest-v22.x/" + $fileName) -OutFile $archivePath -UseBasicParsing
    Expand-Archive -LiteralPath $archivePath -DestinationPath $extractRoot -Force
    Remove-Item -LiteralPath $archivePath -Force -ErrorAction SilentlyContinue

    $expanded = Get-ChildItem -LiteralPath $extractRoot -Directory | Select-Object -First 1
    if (-not $expanded) { throw "Downloaded Node.js archive had an unexpected layout." }
    $target = Join-Path $toolsDir "node"
    Remove-Item -LiteralPath $target -Recurse -Force -ErrorAction SilentlyContinue
    Move-Item -LiteralPath $expanded.FullName -Destination $target
    Remove-Item -LiteralPath $extractRoot -Recurse -Force -ErrorAction SilentlyContinue
    $env:Path = "$target;$env:Path"

    $node = Find-Node
    if (-not $node) { throw "Portable Node.js installation failed." }
    return $node
}

function Ensure-FrontendDependencies($node) {
    Write-Step "Checking frontend environment"
    $nodeDir = Split-Path -Parent $node.File
    if ($nodeDir -and ($env:Path -notlike "$nodeDir;*")) { $env:Path = "$nodeDir;$env:Path" }

    $npm = Get-Command npm.cmd -ErrorAction SilentlyContinue
    if (-not $npm) { $npm = Get-Command npm -ErrorAction SilentlyContinue }
    if (-not $npm) { throw "npm was not found next to the selected Node.js runtime." }
    if (-not (Test-Path -LiteralPath $frontendPackage)) { throw "Missing frontend package.json: $frontendPackage" }
    $npmPath = [string]$npm.Source

    $lockSource = if (Test-Path -LiteralPath $frontendLock) { $frontendLock } else { $frontendPackage }
    $lockHash = (Get-FileHash -LiteralPath $lockSource -Algorithm SHA256).Hash
    $nodeModules = Join-Path $frontendDir "node_modules"
    $stampFile = Join-Path $nodeModules ".mbe_lock.sha256"
    $stampHash = if (Test-Path -LiteralPath $stampFile) { (Get-Content -LiteralPath $stampFile -Raw).Trim() } else { "" }
    $viteCmd = Join-Path $nodeModules ".bin\vite.cmd"

    if (-not (Test-Path -LiteralPath $viteCmd) -or $stampHash -ne $lockHash) {
        if ($SkipDependencyInstall) { throw "Frontend dependencies are missing or were copied from another machine." }
        Write-Step "Installing clean frontend dependencies"
        Push-Location $frontendDir
        try {
            $npmArguments = if (Test-Path -LiteralPath $frontendLock) { @("ci") } else { @("install") }
            $npmExit = Invoke-NativeVisible -FilePath $npmPath -Arguments $npmArguments
            if ($npmExit -ne 0) { throw "npm dependency installation failed with exit code $npmExit." }
        } finally {
            Pop-Location
        }
        New-Item -ItemType Directory -Path $nodeModules -Force | Out-Null
        Set-Content -LiteralPath $stampFile -Value $lockHash -Encoding ASCII
    }
    Write-Ok "Frontend environment is ready (Node $($node.Version))"

    # Return exactly one scalar value.  Do not let npm's console output become
    # part of this return value, because it is later quoted as an executable.
    return $npmPath
}

function Get-RequiredGoVersion {
    $goMod = Join-Path $projectRoot "executor\go.mod"
    if (-not (Test-Path -LiteralPath $goMod)) { return [Version]"1.26.1" }
    $line = Get-Content -LiteralPath $goMod | Where-Object { $_ -match '^go\s+([0-9]+\.[0-9]+(?:\.[0-9]+)?)' } | Select-Object -First 1
    if ($line -match '^go\s+([0-9]+\.[0-9]+(?:\.[0-9]+)?)') { return [Version]$Matches[1] }
    return [Version]"1.26.1"
}

function Find-Go([Version]$minimum) {
    Refresh-ProcessPath
    $files = @((Join-Path $toolsDir "go\bin\go.exe"))
    $systemGo = Get-Command go.exe -ErrorAction SilentlyContinue
    if ($systemGo) { $files += $systemGo.Source }
    foreach ($file in $files) {
        if (-not $file) { continue }
        try {
            $text = (& $file version 2>$null | Select-Object -Last 1)
            if ($text -match 'go([0-9]+\.[0-9]+(?:\.[0-9]+)?)') {
                $version = [Version]$Matches[1]
                if ($version -ge $minimum) { return [pscustomobject]@{ File = $file; Version = $version.ToString() } }
            }
        } catch {}
    }
    return $null
}

function Install-PortableGo([Version]$minimum) {
    if ($SkipToolInstall) { throw "Go $minimum or newer was not found and automatic tool installation is disabled." }
    Write-Step "Go $minimum or newer not found; installing a portable Go runtime"

    $releases = Invoke-RestMethod -Uri "https://go.dev/dl/?mode=json"
    $release = $releases | Where-Object {
        $versionText = $_.version -replace '^go', ''
        try { ([Version]$versionText) -ge $minimum } catch { $false }
    } | Select-Object -First 1
    if (-not $release) { throw "No suitable Go release was returned by go.dev." }
    $file = $release.files | Where-Object { $_.os -eq "windows" -and $_.arch -eq "amd64" -and $_.kind -eq "archive" } | Select-Object -First 1
    if (-not $file) { throw "No suitable Go Windows archive was returned by go.dev." }

    New-Item -ItemType Directory -Path $toolsDir -Force | Out-Null
    $archivePath = Join-Path $env:TEMP $file.filename
    $extractRoot = Join-Path $env:TEMP ("mbe-go-" + [guid]::NewGuid().ToString("N"))
    Invoke-WebRequest -Uri ("https://go.dev/dl/" + $file.filename) -OutFile $archivePath -UseBasicParsing
    Expand-Archive -LiteralPath $archivePath -DestinationPath $extractRoot -Force
    Remove-Item -LiteralPath $archivePath -Force -ErrorAction SilentlyContinue

    $expanded = Join-Path $extractRoot "go"
    if (-not (Test-Path -LiteralPath $expanded)) { throw "Downloaded Go archive had an unexpected layout." }
    $target = Join-Path $toolsDir "go"
    Remove-Item -LiteralPath $target -Recurse -Force -ErrorAction SilentlyContinue
    Move-Item -LiteralPath $expanded -Destination $target
    Remove-Item -LiteralPath $extractRoot -Recurse -Force -ErrorAction SilentlyContinue
    $env:Path = "$(Join-Path $target 'bin');$env:Path"

    $go = Find-Go $minimum
    if (-not $go) { throw "Portable Go installation failed." }
    return $go
}

try {
    Set-Location -LiteralPath $projectRoot
    Write-Host "========================================================" -ForegroundColor DarkCyan
    Write-Host " MBE Smart Launcher" -ForegroundColor Cyan
    Write-Host " Project: $projectRoot"
    Write-Host "========================================================" -ForegroundColor DarkCyan

    Write-Step "Scanning Python 3.12"
    $python = Find-Python312
    if (-not $python) { $python = Install-Python312 }
    Write-Ok "Using Python $($python.Version): $($python.File) $($python.PrefixArgs -join ' ')"
    Ensure-VirtualEnvironment $python

    Write-Step "Scanning Node.js 22-24"
    $node = Find-Node
    if (-not $node) { $node = Install-PortableNode }
    $npmPath = [string](Ensure-FrontendDependencies $node)
    if (-not (Test-Path -LiteralPath $npmPath)) {
        throw "Selected npm executable does not exist: $npmPath"
    }

    $requiredGo = Get-RequiredGoVersion
    Write-Step "Scanning Go $requiredGo or newer"
    $go = Find-Go $requiredGo
    if (-not $go) { $go = Install-PortableGo $requiredGo }
    $goBin = Split-Path -Parent $go.File
    if ($goBin) { $env:Path = "$goBin;$env:Path" }
    Write-Ok "Using Go $($go.Version): $($go.File)"

    $backendUrl = "http://127.0.0.1:8000/health"
    $frontendUrl = "http://127.0.0.1:5173"
    $rootLiteral = Quote-PowerShell $projectRoot
    $venvPythonLiteral = Quote-PowerShell $venvPython
    $npmLiteral = Quote-PowerShell $npmPath
    $pathLiteral = Quote-PowerShell $env:Path

    $backendAlreadyReady = Test-Url $backendUrl
    if ($backendAlreadyReady) {
        Write-Ok "Backend is already running; reusing port 8000"
    } else {
        $backendPid = Get-ListeningProcessId 8000
        if ($backendPid) { throw "Port 8000 is occupied by PID $backendPid, but the MBE health endpoint is not responding." }
        $backendCommand = "`$env:Path=$pathLiteral; Set-Location -LiteralPath $rootLiteral; & $venvPythonLiteral -m uvicorn backend.app.main:app --reload --host 127.0.0.1 --port 8000"
        Start-Process powershell.exe -ArgumentList "-NoExit", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", $backendCommand
    }

    $frontendAlreadyReady = Test-Url $frontendUrl
    if ($frontendAlreadyReady) {
        Write-Ok "Frontend is already running; reusing port 5173"
    } else {
        $frontendPid = Get-ListeningProcessId 5173
        if ($frontendPid) { throw "Port 5173 is occupied by PID $frontendPid, but the MBE frontend is not responding." }
        $frontendCommand = "`$env:Path=$pathLiteral; Set-Location -LiteralPath $rootLiteral; Set-Location frontend; & $npmLiteral run dev -- --host 127.0.0.1 --port 5173"
        Start-Process powershell.exe -ArgumentList "-NoExit", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", $frontendCommand
    }

    $backendReady = Wait-ForUrl $backendUrl "Backend"
    $frontendReady = Wait-ForUrl $frontendUrl "Frontend"
    if (-not ($backendReady -and $frontendReady)) { throw "MBE startup did not complete." }

    Start-Process $frontendUrl
    Write-Host "`nMBE started successfully. You may close this launcher window." -ForegroundColor Green
    Write-Warn "If a .venv.broken-* directory was created, delete it after confirming MBE works."
} catch {
    Write-Host "`n[ERROR] $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "Project root: $projectRoot" -ForegroundColor DarkGray
    exit 1
}
