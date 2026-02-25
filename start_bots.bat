@echo off
:: Dòng lệnh ma thuật: Ép Windows quay về thư mục gốc của file .bat này
cd /d "%~dp0"

echo Dang khoi dong moi truong ao...
call venv\Scripts\activate

echo Dang goi Quan doc Launcher...
python src\launcher.py

echo.
::pause