@echo off
setlocal

cd /d "%~dp0"

echo [1/6] Checking Python...
where py >nul 2>&1
if %errorlevel%==0 (
    set "PY_CMD=py -3"
) else (
    where python >nul 2>&1
    if %errorlevel%==0 (
        set "PY_CMD=python"
    ) else (
        echo [ERROR] Python was not found in PATH.
        echo Install Python 3.9+ and enable "Add Python to PATH", then run again.
        exit /b 1
    )
)

echo [2/6] Creating venv (if needed)...
if not exist "venv\Scripts\python.exe" (
    %PY_CMD% -m venv venv
    if errorlevel 1 goto :error
) else (
    echo venv already exists. Reusing current venv.
)

echo [3/6] Activating venv...
call "venv\Scripts\activate.bat"
if errorlevel 1 goto :error

echo [4/6] Upgrading pip/setuptools/wheel...
python -m pip install --upgrade pip setuptools wheel
if errorlevel 1 goto :error

echo [5/6] Installing Python dependencies...
python -m pip install MetaTrader5 redis requests ujson hiredis
if errorlevel 1 goto :error

echo [6/6] Verifying imports...
python -c "import MetaTrader5, redis, requests, ujson, hiredis; print('Dependencies are ready.')"
if errorlevel 1 goto :error

echo.
echo Setup complete.
echo To start bot: run start_bots.bat
exit /b 0

:error
echo.
echo [ERROR] Setup failed. Read the message above to fix the issue.
exit /b 1
