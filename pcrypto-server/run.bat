@echo off
REM Run the standalone pcrypto-server. Paper by default; pass -live for live data
REM (order placement stays a logged placeholder). Any flags you add are passed through.
REM   run.bat                 paper
REM   run.bat -live           live data, placeholder orders
REM   run.bat -min-conf 0.5   confidence gate
cd /d "%~dp0"
echo pcrypto-server starting -- dashboard: http://127.0.0.1:8799   (Ctrl+C to stop)
go run . %*
