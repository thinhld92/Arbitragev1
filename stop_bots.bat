@echo off
cd /d "%~dp0"
call venv\Scripts\activate
python stop_bots.py
echo.
pause
