# launch-suite.ps1 starts the service in a visible, supervised console.
#
# Why this exists: the operator's PC crash killed the terminal session that held
# KALSHI_SUITE_PASSPHRASE, and a plain relaunch booted the suite public-only (auth locked).
# This script makes an unattended start authenticated and operator-visible:
#
#   1. PASSPHRASE: if KALSHI_SUITE_PASSPHRASE is not already in this shell, it is read from the
#      User-scope environment (the HKCU\Environment registry, i.e. what `setx` writes) and placed
#      into the child process environment ONLY. The value is NEVER printed, echoed, or logged.
#   2. HEADLESS + ONE VISIBLE CONSOLE: kalshi-suite.exe starts with `-headless serve` in this
#      launcher's console, with KALSHI_APP_SPAWNED=1. It opens no second terminal, app window, or
#      browser. The console remains available as a manual shutdown control.
#   3. SUPERVISED CONSOLE: this launcher remains alive while the suite is alive. R135 found that
#      returning after the 30s receipt left the visible suite process orphaned; an abrupt exit then
#      lost its native stderr and could not be distinguished from an external close. The launcher
#      now retains the last native stderr in data\kalshi-suite-stderr.log and records the exit code.
#   4. RECEIPT: after 30s it prints the build name from /api/status so the caller knows what
#      came up without opening anything.
#
# Usage:  powershell -NoProfile -ExecutionPolicy Bypass -File .\launch-suite.ps1
# `-Hidden` suppresses this script's child/status output only when the script already runs in a
# console-less host. Do not WMI-launch powershell.exe for a silent boot: Windows Terminal can host
# that parent before this script executes. The R142 silent run launches kalshi-suite.exe directly.

param([switch]$Hidden)

$ErrorActionPreference = 'Stop'

$root = $PSScriptRoot
if (-not $root) { $root = Split-Path -Parent $MyInvocation.MyCommand.Path }
Set-Location $root

# Hidden means silent as well as invisible. Keep the ordinary visible launcher receipts intact,
# but do not leak them into a transient host/terminal when the operator explicitly requests a
# background launch. Native suite stderr still goes to data\kalshi-suite-stderr.log.
function Write-LauncherStatus([string]$Message) {
    if (-not $Hidden) { Write-Host $Message }
}

$exe = Join-Path $root 'kalshi-suite.exe'
if (-not (Test-Path $exe)) {
    Write-LauncherStatus '[X] kalshi-suite.exe not found next to this script - run build-suite.bat nolaunch first.'
    exit 1
}

# Dashboard address: config.json server_addr when readable, else the built-in default.
$addr = '127.0.0.1:8787'
$cfgPath = Join-Path $root 'config.json'
if (Test-Path $cfgPath) {
    try {
        $cfg = Get-Content $cfgPath -Raw | ConvertFrom-Json
        if ($cfg.server_addr) { $addr = [string]$cfg.server_addr }
    } catch {
        Write-LauncherStatus '[!] Could not parse config.json here - assuming 127.0.0.1:8787 (the suite itself reports config errors).'
    }
}
$statusUrl = 'http://' + $addr + '/api/status'

# Refuse a launch unless the complete old runtime is gone. An HTTP-only check is not sufficient:
# a wedged suite can still own the process or listener while /api/status is unavailable, and a
# crashed suite can leave live_ml.py behind. Checking the launcher too prevents two simultaneous
# launchers from each creating a child. Run this both now and immediately before Process.Start so
# the disk/credential setup below cannot turn a clean initial check into a launch race.
function Get-LaunchBlockers {
    $processes = @(Get-CimInstance Win32_Process -ErrorAction Stop)
    $suiteProcesses = @($processes | Where-Object { $_.Name -ieq 'kalshi-suite.exe' })
    $mlProcesses = @($processes | Where-Object {
        $_.Name -imatch '^python(?:w)?\.exe$' -and
        [string]$_.CommandLine -imatch '(?:^|[\\/\s\"])(?:live_ml\.py)(?:[\s\"]|$)'
    })
    $launcherProcesses = @($processes | Where-Object {
        $_.ProcessId -ne $PID -and
        $_.Name -imatch '^(?:powershell|pwsh)\.exe$' -and
        [string]$_.CommandLine -imatch '(?:^|\s)-File\s+(?:\"[^\"]*launch-suite\.ps1\"|\S*launch-suite\.ps1)(?:\s|$)'
    })

    $ports = [Collections.Generic.HashSet[int]]::new()
    [void]$ports.Add(8787)
    if ($addr -match ':(\d+)$') { [void]$ports.Add([int]$Matches[1]) }
    $listeners = @(Get-NetTCPConnection -State Listen -ErrorAction Stop |
        Where-Object { $ports.Contains([int]$_.LocalPort) })

    $blockers = [Collections.Generic.List[string]]::new()
    foreach ($p in $suiteProcesses) {
        $blockers.Add(('suite process PID {0} ({1})' -f $p.ProcessId, $p.ExecutablePath))
    }
    foreach ($p in $mlProcesses) {
        $blockers.Add(('ML sidecar PID {0} ({1})' -f $p.ProcessId, $p.Name))
    }
    foreach ($p in $launcherProcesses) {
        $blockers.Add(('launcher PID {0}' -f $p.ProcessId))
    }
    foreach ($l in $listeners) {
        $blockers.Add(('listener {0}:{1} owned by PID {2}' -f $l.LocalAddress, $l.LocalPort, $l.OwningProcess))
    }
    return @($blockers)
}

function Assert-CleanLaunchState {
    $blockers = @(Get-LaunchBlockers)
    if ($blockers.Count -eq 0) { return }
    Write-LauncherStatus '[X] Refusing to start: the previous suite runtime is not completely stopped.'
    foreach ($blocker in $blockers) { Write-LauncherStatus ('    - ' + $blocker) }
    Write-LauncherStatus '    Stop the old suite tree and wait for every process and listener above to disappear.'
    exit 23
}

Assert-CleanLaunchState

# R144 disk headroom guard: SQLite reached SQLITE_FULL after repeated test/build variants grew the
# reproducible Go cache to 40 GB. The guard may clear only that cache, only when C: is already below
# 12 GB free. It never touches Temp, databases, snapshots, logs, downloads, or other user data.
$diskGuard = Join-Path $root 'scripts\ensure-disk-headroom.ps1'
if (Test-Path -LiteralPath $diskGuard) {
    & $diskGuard -RepoRoot (Join-Path $root '..') -Quiet:$Hidden
}

# Credential env family: current shell first, then the User-scope registry (HKCU\Environment,
# i.e. what `setx` writes). R88: generalized beyond the passphrase — polyUSLoad/demo/prod signer
# read the PROCESS env, and a parent that predates the setx (MCP server, old service) carries a
# STALE env: on 2026-07-05 the suite booted polyus-RED although POLY_US_KEY_ID was correctly set
# at User scope. Values are injected into THIS process only (children inherit), scrubbed after
# the spawn, and NEVER printed - only the names of what was injected.
$credVars = @('KALSHI_SUITE_PASSPHRASE','POLY_US_KEY_ID','POLY_US_SECRET_FILE',
              'KALSHI_PROD_KEY_ID','KALSHI_PROD_KEY_FILE','KALSHI_DEMO_KEY_ID','KALSHI_DEMO_KEY_FILE')
$injected = @()
foreach ($n in $credVars) {
    if (-not (Get-Item ("Env:" + $n) -ErrorAction SilentlyContinue)) {
        $v = [Environment]::GetEnvironmentVariable($n, 'User')
        if ($v) { Set-Item ("Env:" + $n) $v; $injected += $n; $v = $null }
    }
}
if ($injected.Count) {
    Write-LauncherStatus ('[OK] Injected from User environment (values not shown): ' + ($injected -join ', '))
}
if (-not $env:KALSHI_SUITE_PASSPHRASE) {
    Write-LauncherStatus '[!] KALSHI_SUITE_PASSPHRASE not found in this shell or the User environment.'
    Write-LauncherStatus '    The encrypted credential store (if used) stays LOCKED. Harmless when'
    Write-LauncherStatus '    kalshi_key_file + kalshi_key_id are set (R87 file source wins). Fix otherwise:'
    Write-LauncherStatus '    setx KALSHI_SUITE_PASSPHRASE "<passphrase>" once (User scope), then re-run.'
}

# KALSHI_APP_SPAWNED=1 = the suite must not open its app window or a browser tab (no focus steal).
$env:KALSHI_APP_SPAWNED = '1'
$stderrPath = Join-Path $root 'data\kalshi-suite-stderr.log'
$hiddenStderrTask = $null
try {
    Assert-CleanLaunchState
    # R134 operator order: this console must stay VISIBLE. The visible window is an intentional
    # exception to the usual background-helper rule and is the operator's manual stop control.
    # Redirect ONLY native stderr: structured operating logs remain visible on stdout and in
    # data\kalshi-suite.log, while a Go panic/fatal runtime message survives a closing child console.
    if ($Hidden) {
        # Start-Process -WindowStyle Hidden can still allocate a Windows Terminal host before the
        # window-style request takes effect. A no-shell ProcessStartInfo with CreateNoWindow=true
        # gives the suite no console to display at all. stderr is drained asynchronously so a fatal
        # runtime message cannot block the child; it is persisted after exit.
        $startInfo = New-Object System.Diagnostics.ProcessStartInfo
        $startInfo.FileName = $exe
        $startInfo.Arguments = '-headless serve'
        $startInfo.WorkingDirectory = $root
        $startInfo.UseShellExecute = $false
        $startInfo.CreateNoWindow = $true
        $startInfo.WindowStyle = [System.Diagnostics.ProcessWindowStyle]::Hidden
        $startInfo.RedirectStandardError = $true
        $proc = New-Object System.Diagnostics.Process
        $proc.StartInfo = $startInfo
        if (-not $proc.Start()) { throw 'kalshi-suite hidden start returned false' }
        $hiddenStderrTask = $proc.StandardError.ReadToEndAsync()
    } else {
        # Share the launcher's visible console. Creating a second normal window here used to leave
        # one terminal showing only the 30-second receipt and another showing suite logs. With one
        # console Ctrl+C/window-close still reaches the supervised child, and WaitForExit below
        # retains the exit code and bounded shutdown fallback.
        $proc = Start-Process -FilePath $exe -ArgumentList @('-headless','serve') -WorkingDirectory $root -NoNewWindow -RedirectStandardError $stderrPath -PassThru
    }
} finally {
    Remove-Item Env:KALSHI_APP_SPAWNED -ErrorAction SilentlyContinue
    foreach ($n in $injected) { Remove-Item ("Env:" + $n) -ErrorAction SilentlyContinue }
}
if ($Hidden) {
    Write-LauncherStatus ('[OK] kalshi-suite.exe -headless serve started HIDDEN by explicit operator request (PID ' + $proc.Id + ').')
} else {
    Write-LauncherStatus ('[OK] kalshi-suite.exe -headless serve started in a VISIBLE console (PID ' + $proc.Id + ').')
}
Write-LauncherStatus '    Waiting 30s for boot...'
Start-Sleep -Seconds 30

# The receipt: build name from /api/status.
try {
    $st = Invoke-RestMethod -Uri $statusUrl -TimeoutSec 10
    Write-LauncherStatus ('[OK] Suite is UP - build ' + $st.build_name + ' (' + $st.version + ') - env ' + $st.environment + ' - exec mode ' + $st.exec_mode + '.')
    Write-LauncherStatus ('     Dashboard: http://' + $addr + '/')
} catch {
    Write-LauncherStatus ('[X] No /api/status answer at http://' + $addr + '/ after 30s - read the visible kalshi-suite console (fatal boot errors hold it open for 60s).')
}

# Keep this WMI-detached parent alive for the full suite lifetime. Besides avoiding an orphaned
# detached launch, this lets us retain the native exit code even when the HTTP/log stream ends
# abruptly. It is intentionally NOT an auto-restart loop: operator state and live-order recovery
# must be reconciled before any restart.
try {
    $proc.WaitForExit()
    if ($Hidden -and $hiddenStderrTask) {
        [IO.File]::WriteAllText($stderrPath, $hiddenStderrTask.GetAwaiter().GetResult())
    }
    $exitCode = $proc.ExitCode
    if ($exitCode -eq 0) {
        Write-LauncherStatus '[OK] kalshi-suite exited with code 0.'
    } else {
        Write-LauncherStatus ('[X] kalshi-suite exited unexpectedly with code ' + $exitCode + '.')
        Write-LauncherStatus ('    Native stderr retained at ' + $stderrPath)
        Write-LauncherStatus '    This launcher will close in 60s; read the visible message or the retained file.'
        Start-Sleep -Seconds 60
    }
    exit $exitCode
} finally {
    if (-not $proc.HasExited) {
        # Ctrl+C is delivered to every process sharing this console. Give the suite's signal
        # handler time to cancel venue work, stop ML and finish its bounded final snapshot before
        # falling back to a hard kill. A window-X/session teardown may bypass this block entirely,
        # which the suite's incomplete-exit breadcrumb detects on the next boot.
        Write-LauncherStatus '[!] Launcher is stopping; waiting up to 250s for the suite to exit cleanly...'
        [void]$proc.WaitForExit(250000)
        if (-not $proc.HasExited) {
            Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
        }
    }
}
