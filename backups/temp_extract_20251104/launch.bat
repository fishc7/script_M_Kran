@echo off
chcp 65001 >nul
title M_Kran System Launcher

echo ========================================
echo    M_Kran System Launcher
echo    Glavnyy zapusk sistemy
echo ========================================
echo.

REM Proveryaem nalichie Python
python --version >nul 2>&1
if errorlevel 1 (
    echo OSHIBKA: Python ne nayden v sisteme!
    echo Ustanovite Python s sayta https://python.org
    pause
    exit /b 1
)

echo Python nayden. Vyberite komponent dlya zapuska:
echo.
echo 1. Web Application (Veb-interfeys Flask)
echo 2. Vue.js Application (Sovremennyy veb-interfeys)
echo 3. Script Launcher (Zapusk skriptov)
echo 4. Database Check (Proverka bazy dannykh)
echo 5. System Status (Status sistemy)
echo 6. Web App Diagnostics (Diagnostika veb-prilozheniya)
echo 7. Stop All Servers (Ostanovka vsekh serverov)
echo 8. Exit (Vykhod)
echo.

set /p choice="Vvedite nomer (1-8): "

if "%choice%"=="1" goto web
if "%choice%"=="2" goto vue_app
if "%choice%"=="3" goto script_launcher
if "%choice%"=="4" goto db_check
if "%choice%"=="5" goto system_status
if "%choice%"=="6" goto web_diagnostics
if "%choice%"=="7" goto stop_servers
if "%choice%"=="8" goto exit
goto invalid_choice

:web
echo.
echo Zapusk Web Application...
echo.

REM Proveryaem nalichie web_launcher.py
if not exist "web_launcher.py" (
    echo OSHIBKA: Fayl web_launcher.py ne nayden
    pause
    goto end
)

REM Proveryaem nalichie web/app/app.py
if not exist "web\app\app.py" (
    echo OSHIBKA: Fayl web\app\app.py ne nayden
    pause
    goto end
)

REM Proveryaem zavisimosti
echo Proverka zavisimostey dlya veb-prilozheniya...
python -c "import flask" 2>nul
if errorlevel 1 (
    echo Ustanavlivaem Flask...
    pip install flask
)

python -c "import pandas" 2>nul
if errorlevel 1 (
    echo Ustanavlivaem pandas...
    pip install pandas
)

python -c "import openpyxl" 2>nul
if errorlevel 1 (
    echo Ustanavlivaem openpyxl...
    pip install openpyxl
)

echo.
echo Zapusk veb-prilozheniya...
echo Ustanavlivaem peremennye okruzheniya dlya UTF-8...
set PYTHONIOENCODING=utf-8
set PYTHONLEGACYWINDOWSSTDIO=1

echo Запуск сервера...
echo Для остановки сервера нажмите Ctrl+C
echo Серверы будут автоматически остановлены при закрытии
echo.
echo Веб-приложение доступно по адресу: http://127.0.0.1:5000
echo.

python web_launcher.py
goto end

:vue_app
echo.
echo Zapusk Vue.js Application...
echo.

if exist "launch_vue_simple.ps1" (
    echo Запуск Vue.js приложения...
    echo Для остановки сервера нажмите Ctrl+C
    echo Серверы будут автоматически остановлены при закрытии
    echo.
    echo Vue.js приложение доступно по адресу: http://localhost:5000/vue
    echo.
    
    powershell -ExecutionPolicy Bypass -File "launch_vue_simple.ps1"
) else (
    echo OSHIBKA: Fayl launch_vue_simple.ps1 ne nayden
    pause
)
goto end

:script_launcher
echo.
echo Zapusk Script Launcher...
echo OSHIBKA: Script Launcher nedostupen (papka desktop udalena)
echo Ispolzuyte veb-interfeys dlya zapuska skriptov
pause
goto end

:db_check
echo.
echo Proverka bazy dannykh...
if exist "scripts\diagnostics\check_db.py" (
    python scripts\diagnostics\check_db.py
) else (
    echo OSHIBKA: Fayl scripts\diagnostics\check_db.py ne nayden
    pause
)
goto end

:system_status
echo.
echo Proverka statusa sistemy...
echo.
echo Proverka portov...
if exist "scripts\diagnostics\check_ports.py" (
    python scripts\diagnostics\check_ports.py
) else (
    echo Fayl scripts\diagnostics\check_ports.py ne nayden
)
echo.
echo Proverka servera...
echo Fayl check_server_status.py ne nayden - funktsiya nedostupna
pause
goto end

:web_diagnostics
echo.
echo Diagnostika veb-prilozheniya...
if exist "scripts\diagnostics\check_web_app.py" (
    python scripts\diagnostics\check_web_app.py
) else (
    echo OSHIBKA: Fayl scripts\diagnostics\check_web_app.py ne nayden
    pause
)
goto end

:stop_servers
echo.
echo Остановка всех серверов M_Kran...
echo.

if exist "stop_servers.py" (
    python stop_servers.py
) else (
    echo ОШИБКА: Файл stop_servers.py не найден
    echo Попробуйте остановить серверы вручную:
    echo   - Нажмите Ctrl+C в окнах с запущенными серверами
    echo   - Или используйте Диспетчер задач Windows
    pause
)
goto end

:invalid_choice
echo.
echo Nevernyy vybor. Pozhaluysta, vvedite chislo ot 1 do 8.
pause
goto end

:exit
echo.
echo Vykhod iz sistemy...
exit /b 0

:end
echo.
echo Nazhmite lyubuyu klavishu dlya vozvrata v glavnoe menyu...
pause >nul
cls
goto :eof
