@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..\..") do set "PROJECT_ROOT=%%~fI"
cd /d "%PROJECT_ROOT%"
chcp 65001 >nul
title Менеджер резервного копирования M_Kran

:menu
cls
echo ========================================
echo    Менеджер резервного копирования
echo           проекта M_Kran
echo ========================================
echo.
echo Выберите действие:
echo.
echo 1. Создать полный бэкап проекта
echo 2. Создать критический бэкап (только важные файлы)
echo 3. Создать бэкап только базы данных
echo 4. Показать список всех бэкапов
echo 5. Проверить целостность бэкапа
echo 6. Восстановить из бэкапа
echo 7. Очистить старые бэкапы
echo 8. Выход
echo.
echo ========================================
echo.

set /p choice="Введите номер (1-8): "

if "%choice%"=="1" goto full_backup
if "%choice%"=="2" goto critical_backup
if "%choice%"=="3" goto database_backup
if "%choice%"=="4" goto list_backups
if "%choice%"=="5" goto verify_backup
if "%choice%"=="6" goto restore_backup
if "%choice%"=="7" goto cleanup_backups
if "%choice%"=="8" goto exit
goto menu

:full_backup
cls
echo Создание полного бэкапа...
python "%PROJECT_ROOT%\backup_system.py" --type full
echo.
pause
goto menu

:critical_backup
cls
echo Создание критического бэкапа...
python "%PROJECT_ROOT%\backup_system.py" --type critical
echo.
pause
goto menu

:database_backup
cls
echo Создание бэкапа базы данных...
python "%PROJECT_ROOT%\backup_system.py" --type database
echo.
pause
goto menu

:list_backups
cls
echo Список всех бэкапов:
echo.
python "%PROJECT_ROOT%\backup_system.py" --list
echo.
pause
goto menu

:verify_backup
cls
echo Проверка целостности бэкапа
echo.
set /p backup_path="Введите путь к бэкапу: "
python "%PROJECT_ROOT%\backup_system.py" --verify "%backup_path%"
echo.
pause
goto menu

:restore_backup
cls
echo Восстановление из бэкапа
echo.
set /p backup_path="Введите путь к бэкапу: "
set /p restore_dir="Введите папку для восстановления (Enter для авто): "
if "%restore_dir%"=="" (
    python "%PROJECT_ROOT%\backup_system.py" --restore "%backup_path%"
) else (
    python "%PROJECT_ROOT%\backup_system.py" --restore "%backup_path%" --restore-dir "%restore_dir%"
)
echo.
pause
goto menu

:cleanup_backups
cls
echo Очистка старых бэкапов
echo.
set /p keep_count="Сколько последних бэкапов оставить (по умолчанию 10): "
if "%keep_count%"=="" set keep_count=10
python "%PROJECT_ROOT%\backup_system.py" --cleanup %keep_count%
echo.
pause
goto menu

:exit
echo До свидания!
endlocal
exit /b 0


















