@echo off
chcp 65001 >nul
title M_Kran Server Stopper

echo ========================================
echo    M_Kran Server Stopper
echo    Остановка всех серверов M_Kran
echo ========================================
echo.

REM Проверяем наличие Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ОШИБКА: Python не найден в системе!
    echo Установите Python с сайта https://python.org
    pause
    exit /b 1
)

echo Python найден. Останавливаем серверы...
echo.

REM Запускаем скрипт остановки
python stop_servers.py

echo.
echo Нажмите любую клавишу для выхода...
pause >nul
