@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..\..") do set "PROJECT_ROOT=%%~fI"
cd /d "%PROJECT_ROOT%"
chcp 65001 >nul
title Интерактивное восстановление из бэкапа M_Kran

echo.
echo ========================================
echo    Интерактивное восстановление
echo           проекта M_Kran
echo ========================================
echo.

python "%PROJECT_ROOT%\scripts\utils\restore_interactive.py"

echo.
echo Нажмите любую клавишу для выхода...
pause >nul
endlocal
exit /b 0
