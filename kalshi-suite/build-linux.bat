@echo off
REM ── Cross-compile the suite for a Linux server (Oracle Always Free) from Windows ──────────────
REM Oracle's free tier is ARM Ampere A1 (arm64) by default; the small AMD micro VMs are amd64.
REM Builds BOTH so you can use whichever instance you created. Pure-Go SQLite (modernc) = no CGO,
REM so this cross-compiles cleanly with zero extra toolchain. Output lands in dist\.
setlocal
cd /d %~dp0
if not exist dist mkdir dist

echo Building linux/arm64 (Oracle Ampere A1)...
set GOOS=linux
set GOARCH=arm64
set CGO_ENABLED=0
go build -o dist\kalshi-suite-linux-arm64 .\cmd\kalshi-suite
if errorlevel 1 goto :err

echo Building linux/amd64 (AMD micro VMs)...
set GOARCH=amd64
go build -o dist\kalshi-suite-linux-amd64 .\cmd\kalshi-suite
if errorlevel 1 goto :err

set GOOS=
set GOARCH=
set CGO_ENABLED=
echo.
echo Done. Binaries in dist\:
dir /b dist
echo.
echo Next: see SERVER_SETUP.md to copy these (plus config.json + ml\) to the server.
goto :eof

:err
echo BUILD FAILED.
exit /b 1
