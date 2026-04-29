@echo off
echo ============================================
echo  b24endclickhouse — установка зависимостей
echo ============================================
cd /d "%~dp0"
pip install -r requirements.txt
echo.
echo Готово! Теперь запустите start.bat
pause
