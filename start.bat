@echo off
echo ============================================
echo  BI-коннектор Bitrix24 -> ClickHouse
echo ============================================
cd /d "%~dp0"
echo Запуск на http://127.0.0.1:8000
echo Для остановки нажмите Ctrl+C
echo.
start "" http://127.0.0.1:8000
python -m uvicorn main:app --host 127.0.0.1 --port 8000
pause
