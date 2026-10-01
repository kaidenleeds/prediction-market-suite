@echo off
setlocal
cd /d "%~dp0"

REM Build automatically if the exe is missing.
if not exist "kalshi-forwarder.exe" (
  where go >nul 2>nul || (echo Go not installed - run setup-forwarder.bat first. & pause & exit /b 1)
  echo Building kalshi-forwarder.exe ...
  go build -o kalshi-forwarder.exe . || (echo Build failed - run setup-forwarder.bat. & pause & exit /b 1)
)

if not exist "forwarder-config.json" (
  echo No forwarder-config.json found - run setup-forwarder.bat first.
  pause
  exit /b 1
)

echo Starting forwarder. Leave this window open. Press Ctrl+C to stop.
echo --------------------------------------------------------------
kalshi-forwarder.exe -config forwarder-config.json
echo --------------------------------------------------------------
echo Forwarder stopped.
pause
