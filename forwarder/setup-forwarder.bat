@echo off
setlocal
cd /d "%~dp0"

echo ==================================================
echo   Kalshi Forwarder - one-time setup
echo ==================================================
echo.

REM --- 1) Is Go installed? ---
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

REM --- 2) Build the program ---
echo.
echo Building kalshi-forwarder.exe ...
go build -o kalshi-forwarder.exe .
if errorlevel 1 (
  echo.
  echo [X] Build failed. Review the error above.
  pause
  exit /b 1
)
echo [OK] Built kalshi-forwarder.exe

REM --- 3) Create the config if it does not exist, then open it ---
if not exist "forwarder-config.json" (
  copy /y "config.example.json" "forwarder-config.json" >nul
  echo [OK] Created forwarder-config.json
  echo.
  echo Opening it now. Set your ntfy "topic" to a long secret string,
  echo then SAVE and CLOSE Notepad to continue.
  echo ^(Install the free "ntfy" app on your phone and subscribe to that same topic.^)
  echo.
  pause
  notepad "forwarder-config.json"
) else (
  echo [OK] forwarder-config.json already exists - leaving it alone.
)

echo.
echo Done. To start forwarding, double-click:  run-forwarder.bat
echo.
pause
