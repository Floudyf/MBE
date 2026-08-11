param(
    [string]$RepoRoot = "C:\Projects\Metaverse_Blockchain_Env",
    [int]$MinAgeHours = 1,
    [switch]$Apply
)

$ErrorActionPreference = "Stop"

function Get-DirectorySizeBytes {
    param([string]$Path)
    [int64]$total = 0
    if (-not (Test-Path -LiteralPath $Path -PathType Container)) { return $total }
    foreach ($file in @(Get-ChildItem -LiteralPath $Path -File -Recurse -Force -ErrorAction SilentlyContinue)) {
        $total += [int64]$file.Length
    }
    return $total
}

function Read-JsonObject {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { return $null }
    try { return Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json }
    catch { return $null }
}

function Test-ValidBundle {
    param([string]$GroupDir)
    $manifest = Join-Path $GroupDir "artifact_manifest.json"
    $bundle = Join-Path $GroupDir "artifacts.zip"
    if (-not (Test-Path -LiteralPath $manifest -PathType Leaf)) { return $false }
    if (-not (Test-Path -LiteralPath $bundle -PathType Leaf)) { return $false }
    $info = Get-Item -LiteralPath $bundle
    if ($info.Length -le 0) { return $false }
    try {
        Add-Type -AssemblyName System.IO.Compression.FileSystem -ErrorAction SilentlyContinue
        $archive = [System.IO.Compression.ZipFile]::OpenRead($bundle)
        try { return $archive.Entries.Count -gt 0 }
        finally { $archive.Dispose() }
    } catch { return $false }
}

function Test-WithinRuntimeRoot {
    param([string]$Path, [string]$RuntimeRoot)
    $rootFull = [System.IO.Path]::GetFullPath($RuntimeRoot).TrimEnd('\')
    $pathFull = [System.IO.Path]::GetFullPath($Path).TrimEnd('\')
    if (-not $pathFull.StartsWith($rootFull + '\', [System.StringComparison]::OrdinalIgnoreCase)) { return $false }
    if (-not ([System.IO.Path]::GetDirectoryName($pathFull)).Equals($rootFull, [System.StringComparison]::OrdinalIgnoreCase)) { return $false }
    return ([System.IO.Path]::GetFileName($pathFull)).StartsWith("v5_", [System.StringComparison]::OrdinalIgnoreCase)
}

$RepoRoot = [System.IO.Path]::GetFullPath($RepoRoot)
if (-not (Test-Path -LiteralPath (Join-Path $RepoRoot ".git") -PathType Container)) { throw "Not an MBE git worktree: $RepoRoot" }
if ($MinAgeHours -lt 1) { throw "MinAgeHours must be >= 1" }

$runtimeBase = if ($env:MBE_RUNTIME_ROOT) { $env:MBE_RUNTIME_ROOT } else { Join-Path $RepoRoot ".cache" }
$runtimeRoot = Join-Path ([System.IO.Path]::GetFullPath($runtimeBase)) "v5_real_cluster_runs"
$formalRoot = if ($env:MBE_FORMAL_RUN_ROOT) { [System.IO.Path]::GetFullPath($env:MBE_FORMAL_RUN_ROOT) } else { Join-Path $RepoRoot ".cache\v5_formal_runs" }

if (-not (Test-Path -LiteralPath $formalRoot -PathType Container)) { throw "Formal RunGroup root not found: $formalRoot" }
if (-not (Test-Path -LiteralPath $runtimeRoot -PathType Container)) {
    Write-Host "No V5 real-cluster runtime cache exists: $runtimeRoot"
    exit 0
}

$terminalGroups = @("completed", "completed_with_failures", "failed", "blocked", "cancelled")
$references = @{}
$groups = @()

foreach ($groupDir in @(Get-ChildItem -LiteralPath $formalRoot -Directory -Filter "v5grp_*" -ErrorAction SilentlyContinue)) {
    $group = Read-JsonObject (Join-Path $groupDir.FullName "run_group.json")
    if ($null -eq $group) { continue }
    $childDir = Join-Path $groupDir.FullName "children"
    $children = @()
    if (Test-Path -LiteralPath $childDir -PathType Container) {
        foreach ($childFile in @(Get-ChildItem -LiteralPath $childDir -File -Filter "v5child_*.json" -ErrorAction SilentlyContinue)) {
            $child = Read-JsonObject $childFile.FullName
            if ($null -ne $child) { $children += $child }
        }
    }
    $groups += [pscustomobject]@{ Dir = $groupDir.FullName; Group = $group; Children = $children }
    foreach ($child in $children) {
        $runId = [string]$child.result.run_id
        if (-not $runId.StartsWith("v5_")) { continue }
        if (-not $references.ContainsKey($runId)) { $references[$runId] = @() }
        $references[$runId] += [string]$group.run_group_id
    }
}

$now = [DateTime]::UtcNow
$candidates = @()
$preserved = @()

foreach ($entry in $groups) {
    $group = $entry.Group
    $groupId = [string]$group.run_group_id
    $groupStatus = [string]$group.status
    $bundleReady = Test-ValidBundle $entry.Dir

    foreach ($child in $entry.Children) {
        $runId = [string]$child.result.run_id
        if (-not $runId.StartsWith("v5_")) { continue }
        $runtimeDir = Join-Path $runtimeRoot $runId
        if (-not (Test-Path -LiteralPath $runtimeDir -PathType Container)) { continue }

        $reason = ""
        if ($terminalGroups -notcontains $groupStatus) { $reason = "group_not_terminal" }
        elseif ([string]$child.status -ne "completed") { $reason = "child_not_completed_keep_diagnostics" }
        elseif (-not $bundleReady) { $reason = "formal_bundle_not_ready" }
        elseif (@($references[$runId]).Count -ne 1) { $reason = "runtime_shared_by_multiple_groups" }
        elseif ((Test-Path -LiteralPath (Join-Path $runtimeDir "RUNNING")) -or (Test-Path -LiteralPath (Join-Path $runtimeDir ".running")) -or (Test-Path -LiteralPath (Join-Path $runtimeDir "supervisor.pid"))) { $reason = "active_marker_present" }
        elseif (-not (Test-WithinRuntimeRoot $runtimeDir $runtimeRoot)) { $reason = "path_safety_guard" }

        $dirInfo = Get-Item -LiteralPath $runtimeDir
        $ageHours = ($now - $dirInfo.LastWriteTimeUtc).TotalHours
        if (-not $reason -and $ageHours -lt $MinAgeHours) { $reason = "younger_than_min_age" }
        $size = Get-DirectorySizeBytes $runtimeDir
        $item = [pscustomobject]@{
            run_group_id = $groupId
            child_run_id = [string]$child.child_run_id
            run_id = $runId
            status = [string]$child.status
            runtime_dir = $runtimeDir
            size_bytes = $size
            age_hours = [math]::Round($ageHours, 3)
            reason = $reason
        }
        if ($reason) { $preserved += $item } else { $candidates += $item }
    }
}

[int64]$candidateBytes = 0
foreach ($item in $candidates) { $candidateBytes += [int64]$item.size_bytes }

Write-Host "============================================================"
Write-Host "MBE formal runtime cache cleanup"
Write-Host "Mode: $(if ($Apply) { 'APPLY' } else { 'DRY-RUN' })"
Write-Host "Formal root: $formalRoot"
Write-Host "Runtime root: $runtimeRoot"
Write-Host "Minimum age: $MinAgeHours hour(s)"
Write-Host "Candidate dirs: $($candidates.Count)"
Write-Host ("Candidate size: {0:N2} GiB" -f ($candidateBytes / 1GB))
Write-Host "============================================================"

foreach ($item in $candidates | Sort-Object size_bytes -Descending) {
    Write-Host ("{0,8:N2} GiB  {1}  {2}" -f ($item.size_bytes / 1GB), $item.run_group_id, $item.run_id)
}

$reportItems = @()
foreach ($item in $candidates) {
    $deleted = $false
    $errorText = ""
    if ($Apply) {
        try {
            if (-not (Test-WithinRuntimeRoot $item.runtime_dir $runtimeRoot)) { throw "path safety guard rejected runtime dir" }
            if ((Test-Path -LiteralPath (Join-Path $item.runtime_dir "RUNNING")) -or (Test-Path -LiteralPath (Join-Path $item.runtime_dir ".running")) -or (Test-Path -LiteralPath (Join-Path $item.runtime_dir "supervisor.pid"))) { throw "active marker appeared before delete" }
            Remove-Item -LiteralPath $item.runtime_dir -Recurse -Force
            $deleted = $true
        } catch {
            $errorText = $_.Exception.Message
        }
    }
    $reportItems += [pscustomobject]@{
        run_group_id = $item.run_group_id
        child_run_id = $item.child_run_id
        run_id = $item.run_id
        size_bytes = $item.size_bytes
        age_hours = $item.age_hours
        deleted = $deleted
        error = $errorText
    }
}

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$reportDir = Join-Path $formalRoot ("cleanup_reports\formal_runtime_cache\" + $stamp)
New-Item -ItemType Directory -Path $reportDir -Force | Out-Null
$report = [ordered]@{
    schema_version = "mbe_v5_formal_runtime_cache_cleanup_v1"
    dry_run = -not [bool]$Apply
    min_age_hours = $MinAgeHours
    candidate_count = $candidates.Count
    candidate_bytes = $candidateBytes
    deleted_count = @($reportItems | Where-Object { $_.deleted }).Count
    preserved_count = $preserved.Count
    note = "RunGroup metadata and formal artifacts.zip are preserved; completed child raw runtime directories are cache and may be removed only after formal bundle validation. Failed/cancelled child diagnostics are preserved."
    items = $reportItems
    preserved = $preserved
}
$report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $reportDir "cleanup_report.json") -Encoding UTF8
$reportItems | Export-Csv -LiteralPath (Join-Path $reportDir "cleanup_report.csv") -NoTypeInformation -Encoding UTF8

Write-Host ""
Write-Host "Report: $(Join-Path $reportDir 'cleanup_report.json')"
if (-not $Apply) {
    Write-Host "DRY-RUN only. No runtime cache was deleted."
    Write-Host "To apply after reviewing the report, run:"
    Write-Host "  powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`" -RepoRoot `"$RepoRoot`" -MinAgeHours $MinAgeHours -Apply"
} else {
    $deletedBytes = 0L
    foreach ($item in $reportItems) { if ($item.deleted) { $deletedBytes += [int64]$item.size_bytes } }
    Write-Host ("Released approximately {0:N2} GiB from completed formal runtime cache." -f ($deletedBytes / 1GB))
}
