@echo off
setlocal
cd /d "%~dp0"

echo ==================================================
echo   Kalshi Suite (backend) - build
echo ==================================================
echo.

where go >nul 2>nul
if errorlevel 1 (
  echo [X] Go is not installed ^(or not on your PATH^).
  echo     Install it once, then re-run this file:
  echo.
  echo       winget install --id GoLang.Go -e
  echo.
  echo     ...or download from https://go.dev/dl/
  echo.
  pause
  exit /b 1
)
for /f "tokens=*" %%v in ('go version') do echo [OK] %%v

rem R144: repeated full test/build variants grew Go's disposable build cache to 40 GB and filled
rem C:, which made SQLite and the ML sidecar fail writes. The guard touches only that regenerable
rem cache, only below 12 GB free; it never removes Temp, databases, backups, logs, or user files.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\ensure-disk-headroom.ps1" -RepoRoot "%~dp0.."
if errorlevel 1 (
  echo [X] Not enough safe disk headroom to build.
  exit /b 1
)

echo.
echo Fetching dependencies ^(go mod tidy^) - needs internet the first time...
go mod tidy
if errorlevel 1 (
  echo [X] go mod tidy failed. Review the error above.
  pause
  exit /b 1
)

echo.
echo Building kalshi-suite.exe ...
rem R18 latency: Go 1.25+ jsonv2 engine - measurably faster JSON decode on the WS hot path.
rem Full test suite verified green under this experiment (2026-07-02). Harmless if unset works better: delete this line to revert.
set GOEXPERIMENT=jsonv2
rem R20/R144: stamp the full source identity + time, plus the stable animal carried by the clean
rem commit subject. Dirty source gets a content fingerprint and never inherits the committed animal.
set GITSHA=unknown
set BUILDNAME=
for /f "tokens=1,2 delims=|" %%i in ('powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\build-identity.ps1" -RepoRoot "%~dp0.."') do (
  set GITSHA=%%i
  set BUILDNAME=%%j
)
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyMMdd-HHmm"') do set BUILDTIME=%%i
go build -ldflags "-X \"main.buildVersion=%GITSHA%@%BUILDTIME%\" -X \"main.buildReleaseName=%BUILDNAME%\"" -o kalshi-suite.exe ./cmd/kalshi-suite
if errorlevel 1 (
  echo [X] Build failed. Review the error above.
  pause
  exit /b 1
)
echo [OK] Built kalshi-suite.exe

echo.
echo Building kalshi-app.exe ^(standalone desktop app window - R51^)...
rem -H windowsgui = a real app window, no console. Closing the window NEVER stops trading.
go build -ldflags "-H windowsgui" -o kalshi-app.exe ./cmd/kalshi-app
if errorlevel 1 (
  echo [!] kalshi-app build failed - the suite still works in the browser. Review the error above.
) else (
  echo [OK] Built kalshi-app.exe - double-click it any time for the app window ^(attaches if the suite is running, starts it if not^)
)

echo.
REM ============================================================================================
REM R82 TELEGRAM-ONLY (operator): the ntfy forwarder is RETIRED from the boot path. Telegram's
REM built-in 2-minute briefing loop is the sole messenger (config messenger_mode="telegram",
REM the default) and the suite no longer writes the briefing .md drops the forwarder watched.
REM The block below is intentionally COMMENTED OUT, not deleted - to bring ntfy back, set
REM messenger_mode to "ntfy" or "both" in config.json/Settings and un-comment these lines.
REM ============================================================================================
REM echo Making sure the forwarder is running in the background...
REM tasklist /fi "imagename eq kalshi-forwarder.exe" 2>nul | find /i "kalshi-forwarder.exe" >nul
REM if not errorlevel 1 goto :fwd_done
REM if not exist "%~dp0..\forwarder\forwarder-config.json" (
REM   echo [!] Forwarder not set up yet - run forwarder\setup-forwarder.bat once. Skipping.
REM   goto :fwd_done
REM )
REM pushd "%~dp0..\forwarder"
REM REM -H windowsgui = no console window, so it stays off your taskbar.
REM go build -ldflags="-H windowsgui" -o kalshi-forwarder.exe . 2>nul
REM if exist kalshi-forwarder.exe (
REM   start "" kalshi-forwarder.exe -config forwarder-config.json
REM   echo [OK] Forwarder started hidden ^(no taskbar window^).
REM ) else (
REM   echo [!] Forwarder build failed - run forwarder\setup-forwarder.bat. Skipping.
REM )
REM popd
REM :fwd_done
echo [OK] Messenger: Telegram-only ^(R82^) - ntfy forwarder not built/started ^(block kept above, commented^).

echo.
echo Checking the ML edge-scorer deps ^(optional - needs Python^)...
where python >nul 2>nul
if errorlevel 1 (
  echo [!] Python not found - the suite runs without the ML scorer. Install Python 3 to enable it.
) else (
  REM ensure deps once; only installs if missing, stays quiet otherwise
  python -c "import sklearn, numpy" >nul 2>nul || python -m pip install --quiet scikit-learn numpy
  REM R52 CUDA: xgboost trains the model on the NVIDIA GPU when one exists (RTX 5070 Ti here);
  REM the sidecar probes at startup and auto-falls back to the sklearn CPU model if not.
  python -c "import xgboost" >nul 2>nul || python -m pip install --quiet xgboost
  echo [OK] ML deps ready ^(incl. xgboost GPU^) - the scorer is a no-console child and stops with the suite.
)

echo.
rem R86: optional flag - "build-suite.bat nolaunch" builds everything but does NOT start the suite
rem (detached starts use launch-suite.ps1: visible HEADLESS console, no app/browser window).
if /i "%~1"=="nolaunch" (
  echo Build complete - "nolaunch" flag set, NOT starting the suite ^(R86^).
  echo Start it later with launch-suite.ps1 ^(visible headless console, passphrase from User env^)
  echo or yourself with:  start "Kalshi Suite (headless)" kalshi-suite.exe -headless serve
  echo ==================================================
  exit /b 0
)
echo Starting the suite in its OWN console - the DESKTOP APP window opens to the dashboard ^(R53^).
echo Closing the app window never stops trading. To stop trading: this console, Ctrl+C ^(no Y/N prompt^).
echo ==================================================
rem R86 (operator: "ctrl c isnt working"): the exe is launched DIRECTLY again. The R79 wrapper
rem   start "Kalshi Suite" cmd /c "kalshi-suite.exe serve & if errorlevel 1 (... pause)"
rem kept crash text visible, but it put cmd's batch layer back in charge of the console - Ctrl+C
rem hit the wrapper (the "Terminate batch job (Y/N)?" prompt / swallowed interrupt) instead of the
rem suite. Crash visibility moved INTO the exe (fatalBootHold in cmd/kalshi-suite/main.go): on a
rem fatal boot error - config parse, data dir, storage open, port bind - kalshi-suite prints the
rem error and HOLDS ITS OWN WINDOW open for 60s before exiting, so nothing vanishes unread and no
rem batch chain sits between Ctrl+C and the server. disableQuickEdit in the exe already keeps the
rem stop prompt-free.
start "Kalshi Suite" kalshi-suite.exe serve
