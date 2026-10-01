param(
    [Parameter(Mandatory = $true)]
    [string]$RepoRoot,
    [double]$MinimumFreeGB = 12,
    [double]$MinimumCacheGB = 2,
    [switch]$Quiet
)

$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path -LiteralPath $RepoRoot).Path
$driveRoot = [IO.Path]::GetPathRoot($repo)
$driveName = $driveRoot.TrimEnd([char]92).TrimEnd([char]58)

function Get-SnapshotFloorGB {
    $dataDir = Join-Path $repo 'kalshi-suite\data'
    if (-not (Test-Path -LiteralPath $dataDir -PathType Container)) {
        $dataDir = Join-Path $repo 'data'
    }
    [double]$bytes = 1GB
    foreach ($name in @('kalshi.db', 'kalshi.db-wal', 'execution_shadow.db', 'execution_shadow.db-wal')) {
        $path = Join-Path $dataDir $name
        $file = Get-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue
        if ($null -ne $file -and -not $file.PSIsContainer) {
            $bytes += [double]$file.Length
        }
    }
    return $bytes / 1GB
}

function Get-FreeGB {
    # Some managed shells expose the filesystem drive but redact Get-PSDrive's Free property as
    # either null or zero. DriveInfo reads the local volume statistic directly without WMI/CIM.
    try {
        $driveInfo = [IO.DriveInfo]::new($driveRoot)
        if ($driveInfo.IsReady) {
            return [double]$driveInfo.AvailableFreeSpace / 1GB
        }
    } catch {
        # Fall through to the provider value for unusual mounted filesystems.
    }
    $drive = Get-PSDrive -PSProvider FileSystem -Name $driveName -ErrorAction Stop
    if ($null -eq $drive.Free) {
        throw "Free-space truth is unavailable for $driveRoot"
    }
    return [double]$drive.Free / 1GB
}

function Write-GuardStatus([string]$Message) {
    if (-not $Quiet) { Write-Host $Message }
}

$snapshotFloorGB = Get-SnapshotFloorGB
$effectiveMinimumFreeGB = [Math]::Max($MinimumFreeGB, $snapshotFloorGB)
$before = Get-FreeGB
if ($before -ge $effectiveMinimumFreeGB) {
    Write-GuardStatus ('[OK] Disk headroom: {0:N1} GB free; safety floor {1:N3} GB.' -f $before, $effectiveMinimumFreeGB)
    exit 0
}

# Go's build cache is reproducible and is the only thing this guard may delete. Never touch Temp,
# databases, snapshots, archives, logs, downloads, or user files automatically. Reclaim only when
# the drive is already below the safety floor and the cache is large enough to matter.
$go = Get-Command go -ErrorAction SilentlyContinue
if ($go) {
    $cacheText = (& $go.Source env GOCACHE 2>$null | Select-Object -First 1)
    $cachePath = if ($null -ne $cacheText) { ([string]$cacheText).Trim() } else { '' }
    if ($cachePath -and (Test-Path -LiteralPath $cachePath -PathType Container)) {
        $cacheResolved = (Resolve-Path -LiteralPath $cachePath).Path
        $measure = Get-ChildItem -LiteralPath $cacheResolved -File -Recurse -Force -ErrorAction SilentlyContinue |
            Measure-Object Length -Sum
        $cacheGB = [double]$measure.Sum / 1GB
        if ($cacheGB -ge $MinimumCacheGB) {
            Write-GuardStatus ('[!] Only {0:N1} GB free; reclaiming {1:N1} GB of regenerable Go build cache.' -f $before, $cacheGB)
            & $go.Source clean -cache
            if ($LASTEXITCODE -ne 0) { throw "go clean -cache failed with exit code $LASTEXITCODE" }
        }
    }
}

$after = Get-FreeGB
if ($after -lt $effectiveMinimumFreeGB) {
    throw ('Disk safety stop: only {0:N1} GB free on {1}. Need at least {2:N3} GB before build/start (the larger of the operator floor and both databases/WALs plus 1 GiB). No project or personal data was deleted.' -f $after, $driveRoot, $effectiveMinimumFreeGB)
}
Write-GuardStatus ('[OK] Disk headroom restored: {0:N1} GB free; safety floor {1:N3} GB.' -f $after, $effectiveMinimumFreeGB)
