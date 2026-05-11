@echo off
setlocal
cd /d "%~dp0"
chcp 65001 >nul
title M_Kran Launcher

:menu
cls
echo ========================================
echo           M_Kran Launcher
echo ========================================
echo.
echo 1. Start Web Application
echo 2. Start Vue Application
echo 3. Database Check
echo 4. Web Diagnostics
echo 5. Stop All Servers
echo 6. Exit
echo.
set /p choice=Select option ^(1-6^): 

if "%choice%"=="1" goto web
if "%choice%"=="2" goto vue
if "%choice%"=="3" goto db
if "%choice%"=="4" goto webdiag
if "%choice%"=="5" goto stop
if "%choice%"=="6" goto done
goto bad

:web
echo.
echo Starting Web Application...
if not exist "web_launcher.py" (
  echo ERROR: web_launcher.py not found
  goto pauseback
)
set PYTHONIOENCODING=utf-8
set PYTHONLEGACYWINDOWSSTDIO=1
set "PY_EXE=%LocalAppData%\Programs\Python\Python314\python.exe"
if exist "%PY_EXE%" (
  powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -FilePath '%PY_EXE%' -ArgumentList 'web_launcher.py' -WorkingDirectory '%~dp0' -WindowStyle Hidden"
) else (
  powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -FilePath 'python' -ArgumentList 'web_launcher.py' -WorkingDirectory '%~dp0' -WindowStyle Hidden"
)
goto done

:vue
echo.
echo Starting Vue Application...
if exist "launch_vue_simple.ps1" (
  powershell -ExecutionPolicy Bypass -File "launch_vue_simple.ps1"
) else (
  echo ERROR: launch_vue_simple.ps1 not found
)
goto pauseback

:db
echo.
echo Running DB check...
if exist "scripts\diagnostics\check_db.py" (
  python "scripts\diagnostics\check_db.py"
) else (
  echo WARNING: scripts\diagnostics\check_db.py not found
)
goto pauseback

:webdiag
echo.
echo Running web diagnostics...
if exist "scripts\diagnostics\check_web_app.py" (
  python "scripts\diagnostics\check_web_app.py"
) else (
  echo WARNING: scripts\diagnostics\check_web_app.py not found
)
goto pauseback

:stop
echo.
echo Stopping all servers...
if exist "stop_servers.py" (
  python "stop_servers.py"
) else (
  echo ERROR: stop_servers.py not found
)
goto pauseback

:bad
echo.
echo Invalid choice. Use 1..6
goto pauseback

:pauseback
echo.
pause
goto menu

:done
endlocal
exit /b 0
