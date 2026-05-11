@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..\..") do set "PROJECT_ROOT=%%~fI"
cd /d "%PROJECT_ROOT%"
chcp 65001 >nul
title M_Kran Visible Launcher

echo ========================================
echo       M_Kran Visible Launcher
echo ========================================
echo Starting web application in current window...
echo.

set PYTHONIOENCODING=utf-8
set PYTHONLEGACYWINDOWSSTDIO=1
set "PY_EXE=%LocalAppData%\Programs\Python\Python314\python.exe"

if exist "%PY_EXE%" (
  "%PY_EXE%" "%PROJECT_ROOT%\web_launcher.py"
) else (
  python "%PROJECT_ROOT%\web_launcher.py"
)

echo.
echo Web launcher finished.
pause
endlocal
exit /b 0
