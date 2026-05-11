@echo off
setlocal
cd /d "%~dp0"
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
  "%PY_EXE%" "web_launcher.py"
) else (
  python "web_launcher.py"
)

echo.
echo Web launcher finished.
pause
endlocal
exit /b 0
