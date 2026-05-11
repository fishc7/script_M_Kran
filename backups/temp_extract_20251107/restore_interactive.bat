@echo off
chcp 65001 >nul
title Интерактивное восстановление из бэкапа M_Kran

echo.
echo ========================================
echo    Интерактивное восстановление
echo           проекта M_Kran
echo ========================================
echo.

python restore_interactive.py

echo.
echo Нажмите любую клавишу для выхода...
pause >nul
