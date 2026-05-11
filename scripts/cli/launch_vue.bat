@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..\..") do set "PROJECT_ROOT=%%~fI"
cd /d "%PROJECT_ROOT%"
chcp 65001 >nul
title Vue.js Application Launcher

echo ========================================
echo    Vue.js Application Launcher
echo    Запуск современного веб-интерфейса
echo ========================================
echo.

echo Проверка Node.js...
node --version >nul 2>&1
if errorlevel 1 (
    echo ОШИБКА: Node.js не найден в системе!
    echo Установите Node.js с сайта https://nodejs.org
    pause
    exit /b 1
)
echo Node.js найден.

echo Проверка npm...
npm --version >nul 2>&1
if errorlevel 1 (
    echo ОШИБКА: npm не найден в системе!
    pause
    exit /b 1
)
echo npm найден.

echo Node.js и npm найдены.

REM Проверяем наличие web/app/app.py
if not exist "%PROJECT_ROOT%\web\app\app.py" (
    echo ОШИБКА: Файл web\app\app.py не найден
    pause
    exit /b 1
)

REM Проверяем наличие package.json
if not exist "%PROJECT_ROOT%\web\package.json" (
    echo ОШИБКА: Файл web\package.json не найден
    pause
    exit /b 1
)

REM Проверяем зависимости Vue.js
echo Проверка зависимостей Vue.js...
cd /d "%PROJECT_ROOT%\web"
if not exist "node_modules" (
    echo Устанавливаем зависимости Vue.js...
    npm install
    if errorlevel 1 (
        echo ОШИБКА: Не удалось установить зависимости Vue.js
        pause
        exit /b 1
    )
)

REM Собираем Vue.js приложение
echo Сборка Vue.js приложения...
npm run build
if errorlevel 1 (
    echo ОШИБКА: Не удалось собрать Vue.js приложение
    echo Проверьте ошибки выше и попробуйте снова
    pause
    exit /b 1
)

REM Проверяем, что файлы сборки созданы
if not exist "static\vue-dist\assets\main-*.js" (
    echo ОШИБКА: Файлы сборки Vue.js не найдены
    echo Попробуйте запустить сборку вручную: npm run build
    pause
    exit /b 1
)

echo Vue.js приложение успешно собрано!

REM Проверяем зависимости Flask
echo Проверка зависимостей Flask...
cd /d "%PROJECT_ROOT%"
python -c "import flask" 2>nul
if errorlevel 1 (
    echo Устанавливаем Flask...
    pip install flask
)

python -c "import pandas" 2>nul
if errorlevel 1 (
    echo Устанавливаем pandas...
    pip install pandas
)

python -c "import openpyxl" 2>nul
if errorlevel 1 (
    echo Устанавливаем openpyxl...
    pip install openpyxl
)

echo.
echo Запуск Vue.js приложения через Flask...
echo Приложение будет доступно по адресу: http://localhost:5000/vue
echo.

REM Проверяем, не занят ли порт 5000
netstat -an | findstr ":5000" | findstr "LISTENING" >nul
if not errorlevel 1 (
    echo ПРЕДУПРЕЖДЕНИЕ: Порт 5000 уже занят!
    echo Возможно, уже запущен другой сервер Flask
    echo.
    set /p continue="Продолжить запуск? (y/n): "
    if /i not "%continue%"=="y" exit /b 0
)

echo Нажмите Ctrl+C для остановки сервера
echo.

REM Запускаем Flask сервер в фоновом режиме
cd /d "%PROJECT_ROOT%\web\app"
start /b python app.py

REM Ждем немного, чтобы сервер успел запуститься
echo Ожидание запуска сервера...
timeout /t 3 /nobreak >nul

REM Проверяем, что сервер запустился
netstat -an | findstr ":5000" | findstr "LISTENING" >nul
if not errorlevel 1 (
    echo Сервер запущен! Открываем браузер...
    
    REM Открываем браузер с главной страницей Vue.js
    start http://localhost:5000/vue
    
    echo Браузер открыт! Приложение доступно по адресу: http://localhost:5000/vue
    echo.
    echo Для остановки сервера нажмите любую клавишу...
    pause >nul
    
    REM Останавливаем сервер
    taskkill /f /im python.exe >nul 2>&1
    echo Сервер остановлен.
) else (
    echo ОШИБКА: Сервер не запустился!
)

echo.
echo Нажмите любую клавишу для выхода...
pause >nul
endlocal
exit /b 0
