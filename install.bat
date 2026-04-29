@echo off
echo ============================================
echo  Установка зависимостей BI-коннектора
echo ============================================
cd /d "%~dp0"
pip install -r requirements.txt
echo.
echo Готово! Теперь запустите start.bat
pause
