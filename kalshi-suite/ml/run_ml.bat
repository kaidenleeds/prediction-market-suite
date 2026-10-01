@echo off
REM Live ML edge-scorer for kalshi-suite. Reads ..\data\kalshi.db (read-only),
REM retrains every cycle, writes ..\data\ml_predictions.json, prints top +EV markets.
REM First time only:  pip install scikit-learn numpy
cd /d "%~dp0"
python live_ml.py --interval 10 %*
pause
