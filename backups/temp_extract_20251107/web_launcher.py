#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
M_Kran Web Application Launcher
Улучшенная версия с лучшей обработкой ошибок
"""

import os
import sys
import time
import threading
import subprocess
import socket
import webbrowser
import signal
import atexit
from pathlib import Path

# Исправление кодировки для Windows консоли
if sys.platform.startswith('win'):
    import codecs
    # Устанавливаем UTF-8 для stdout и stderr
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.detach())
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.detach())
    # Устанавливаем переменную окружения для subprocess
    os.environ['PYTHONIOENCODING'] = 'utf-8'

# Try to import requests at module level
try:
    import requests
except ImportError:
    requests = None

# Глобальные переменные
browser_opened = False
server_process = None
html_server = None
cleanup_done = False

def cleanup_processes():
    """Корректно завершает все запущенные процессы"""
    global cleanup_done, server_process, html_server
    
    if cleanup_done:
        return
    
    cleanup_done = True
    print("\n[STOP] Завершение всех процессов...")
    
    # Останавливаем HTML сервер
    if html_server:
        try:
            print("  Останавливаем HTML сервер...")
            html_server.terminate()
            html_server.wait(timeout=3)
            print("  ✓ HTML сервер остановлен")
        except Exception as e:
            print(f"  ⚠ Ошибка остановки HTML сервера: {e}")
    
    # Останавливаем основной сервер
    if server_process:
        try:
            print("  Останавливаем основной сервер...")
            server_process.terminate()
            try:
                server_process.wait(timeout=5)
                print("  ✓ Основной сервер остановлен")
            except subprocess.TimeoutExpired:
                print("  ⚠ Принудительное завершение сервера...")
                server_process.kill()
                server_process.wait(timeout=2)
                print("  ✓ Сервер принудительно завершен")
        except Exception as e:
            print(f"  ⚠ Ошибка остановки сервера: {e}")
    
    print("[OK] Все процессы завершены")

def signal_handler(signum, frame):
    """Обработчик сигналов завершения"""
    print(f"\n[STOP] Получен сигнал {signum}")
    cleanup_processes()
    sys.exit(0)

def register_cleanup():
    """Регистрирует обработчики завершения"""
    # Регистрируем функцию очистки при выходе
    atexit.register(cleanup_processes)
    
    # Регистрируем обработчики сигналов
    signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # Завершение процесса
    
    # Для Windows также обрабатываем SIGBREAK
    if sys.platform.startswith('win'):
        try:
            signal.signal(signal.SIGBREAK, signal_handler)
        except AttributeError:
            pass  # SIGBREAK может быть недоступен

def start_html_server():
    """Запускает HTML сервер для отчетов"""
    global html_server
    
    try:
        # Импортируем HTML сервер
        sys.path.append(os.path.join(os.path.dirname(__file__), 'web', 'app'))
        from html_server import start_html_server as start_server
        
        html_server = start_server()
        return True
        
    except Exception as e:
        print(f"⚠️  Не удалось запустить HTML сервер: {e}")
        return False

def print_header():
    """Выводит заголовок приложения"""
    print("=" * 50)
    print("    M_Kran Web Application Launcher")
    print("         Улучшенная версия")
    print("=" * 50)

def check_dependencies():
    """Проверяет наличие необходимых зависимостей"""
    print("Проверка зависимостей...")
    
    dependencies = {
        'flask': 'Flask web framework',
        'sqlite3': 'SQLite database',
        'pandas': 'Data processing',
        'openpyxl': 'Excel file handling',
        'werkzeug': 'WSGI utilities'
    }
    
    missing = []
    for module, description in dependencies.items():
        try:
            if module == 'sqlite3':
                # sqlite3 встроен в Python, проверяем по-другому
                import sqlite3
                print(f"  [OK] {module}")
            else:
                __import__(module)
                print(f"  [OK] {module}")
        except ImportError:
            print(f"  [ERROR] {module} - {description}")
            missing.append(module)
    
    if missing:
        print(f"\n[WARNING] Отсутствуют модули: {', '.join(missing)}")
        print("   Приложение может работать некорректно!")
        return False
    else:
        print("[OK] Все зависимости найдены")
        return True

def check_files(base_dir=None):
    """Проверяет наличие необходимых файлов и папок"""
    print("Проверка файлов и папок...")
    
    if base_dir is None:
        base_dir = os.getcwd()
    
    required_items = [
        ('web/app/app.py', 'file'),
        ('database/BD_Kingisepp', 'directory'),
        ('scripts/data_loaders', 'directory'),
        ('scripts/data_cleaners', 'directory'),
        ('desktop/qt_app', 'directory')
    ]
    
    missing_items = []
    for item_path, item_type in required_items:
        full_path = os.path.join(base_dir, item_path)
        path_obj = Path(full_path)
        if item_type == 'file' and path_obj.is_file():
            print(f"  [OK] {item_path} (файл)")
        elif item_type == 'directory' and path_obj.is_dir():
            print(f"  [OK] {item_path} (папка)")
        else:
            print(f"  [ERROR] {item_path} ({item_type})")
            missing_items.append(item_path)
    
    if missing_items:
        print(f"\n[WARNING] Отсутствуют элементы: {', '.join(missing_items)}")
        return False
    
    return True

def safe_import(module_name, fallback_function=None):
    """Безопасный импорт модуля с fallback функцией"""
    try:
        return __import__(module_name)
    except ImportError:
        if fallback_function:
            return fallback_function()
        return None

def wait_for_server_simple(url, timeout=30):
    """Упрощенная проверка сервера через socket"""
    print(f"Ожидание запуска сервера (socket)...")
    start_time = time.time()
    
    # Извлекаем хост и порт из URL
    if url.startswith('http://'):
        url = url[7:]
    elif url.startswith('https://'):
        url = url[8:]
    
    if ':' in url:
        host, port_str = url.split(':', 1)
        port = int(port_str.split('/')[0])
    else:
        host = url.split('/')[0]
        port = 80
    
    print(f"   Проверяем подключение к {host}:{port}")
    
    while time.time() - start_time < timeout:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result == 0:
                print(f"[OK] Сервер готов: {url}")
                return True
            else:
                print(f"   Ожидание... ({int(time.time() - start_time)}с)")
        except Exception as e:
            print(f"   Ошибка подключения: {e} ({int(time.time() - start_time)}с)")
        
        time.sleep(1)
    
    print(f"[ERROR] Сервер не запустился в течение {timeout} секунд")
    return False

def wait_for_server_advanced(url, timeout=30):
    """Продвинутая проверка сервера с requests"""
    if requests is None:
        print("[WARNING] requests не установлен, используется упрощенная проверка")
        return wait_for_server_simple(url, timeout)
    
    print(f"Ожидание запуска сервера (requests)...")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            response = requests.get(url, timeout=2)
            if response.status_code == 200:
                # Дополнительная проверка - убеждаемся, что это действительно наша страница
                if len(response.content) > 1000 and 'M_Kran' in response.text:
                    print(f"[OK] Сервер готов: {url}")
                    print(f"   Статус ответа: {response.status_code}")
                    print(f"   Размер ответа: {len(response.content)} байт")
                    print(f"   Проверка контента: OK")
                    return True
                else:
                    print(f"   Сервер отвечает, но контент неверный... ({int(time.time() - start_time)}с)")
                    time.sleep(1)
        except requests.exceptions.ConnectionError:
            print(f"   Ожидание подключения... ({int(time.time() - start_time)}с)")
            time.sleep(1)
        except requests.exceptions.Timeout:
            print(f"   Таймаут запроса... ({int(time.time() - start_time)}с)")
            time.sleep(1)
        except Exception as e:
            print(f"   Ошибка запроса: {e} ({int(time.time() - start_time)}с)")
            time.sleep(1)
    
    print(f"[ERROR] Сервер не запустился в течение {timeout} секунд")
    return False

def open_browser_safe(url):
    """Безопасное открытие браузера с проверками"""
    global browser_opened
    
    print(f"Открываем браузер: {url}")
    
    # Проверяем, не открыт ли уже браузер с этим URL
    psutil_available = safe_import('psutil')
    if psutil_available:
        try:
            browser_found = False
            for proc in psutil_available.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if proc.info['name'] and any(browser in proc.info['name'].lower() for browser in ['chrome', 'firefox', 'edge', 'iexplore']):
                        cmdline = proc.info['cmdline']
                        if cmdline and any(url in arg for arg in cmdline):
                            print(f"[INFO] Браузер уже открыт с URL: {url}")
                            print("   Проверяем, загружается ли страница...")
                            browser_found = True
                            # Не устанавливаем browser_opened = True, чтобы все равно попробовать открыть
                except (psutil_available.NoSuchProcess, psutil_available.AccessDenied):
                    continue
            
            # Если браузер не найден с нашим URL, сбрасываем флаг
            if not browser_found:
                browser_opened = False
                
        except Exception as e:
            print(f"[WARNING] Ошибка проверки процессов: {e}")
            browser_opened = False
    
    # Проверяем, не открыт ли уже браузер (только если мы точно знаем, что он открыт)
    if browser_opened:
        print(f"[INFO] Браузер уже открыт, пропускаем повторное открытие")
        return
    
    # Сначала пробуем принудительное открытие через команду start (Windows)
    print("   Пробуем принудительное открытие через команду start...")
    try:
        result = subprocess.run(['start', url], shell=True, capture_output=True, text=True, timeout=10, encoding='utf-8')
        if result.returncode == 0:
            print(f"[OK] Браузер открыт через команду start")
            browser_opened = True
            time.sleep(2)
            return
        else:
            print(f"   Команда start вернула код: {result.returncode}")
    except Exception as e:
        print(f"   Команда start не сработала: {e}")
    
    # Пробуем альтернативный способ через PowerShell
    print("   Пробуем открытие через PowerShell...")
    try:
        result = subprocess.run(['powershell', '-Command', f'Start-Process "{url}"'], 
                              capture_output=True, text=True, timeout=10, encoding='utf-8')
        if result.returncode == 0:
            print(f"[OK] Браузер открыт через PowerShell")
            browser_opened = True
            time.sleep(2)
            return
        else:
            print(f"   PowerShell вернул код: {result.returncode}")
    except Exception as e:
        print(f"   PowerShell не сработал: {e}")
    
    # Пробуем разные способы открытия браузера
    browsers_to_try = [
        'chrome',
        'firefox', 
        'edge',
        'safari',
        'default'
    ]
    
    opened_successfully = False
    
    for browser_name in browsers_to_try:
        try:
            if browser_name == 'default':
                # Используем системный браузер по умолчанию
                webbrowser.open(url, new=1, autoraise=True)
            else:
                # Пробуем конкретный браузер
                browser = webbrowser.get(browser_name)
                browser.open(url, new=1, autoraise=True)
            
            browser_opened = True
            opened_successfully = True
            print(f"[OK] Браузер открыт ({browser_name}): {url}")
            
            # Даем браузеру время на открытие
            time.sleep(2)
            
            # Проверяем, действительно ли страница загрузилась
            if requests is not None:
                try:
                    response = requests.get(url, timeout=5)
                    if response.status_code == 200:
                        print(f"[OK] Страница успешно загружена (статус: {response.status_code})")
                    else:
                        print(f"[WARNING] Страница загружена, но статус: {response.status_code}")
                except Exception as e:
                    print(f"[WARNING] Не удалось проверить загрузку страницы: {e}")
            
            return
            
        except Exception as e:
            print(f"   Не удалось открыть {browser_name}: {e}")
            continue
    
    if not opened_successfully:
        print(f"[ERROR] Не удалось открыть ни один браузер")
        print(f"   Откройте браузер вручную и перейдите по адресу: {url}")
        print(f"   Или скопируйте и вставьте этот адрес в адресную строку браузера")
        
        # Пробуем принудительно открыть через командную строку
        print("   Пробуем принудительное открытие...")
        try:
            # Пробуем разные команды для открытия браузера
            commands_to_try = [
                ['start', url],  # Windows
                ['cmd', '/c', 'start', url],  # Windows через cmd
                ['powershell', '-Command', f'Start-Process "{url}"'],  # PowerShell
            ]
            
            for cmd in commands_to_try:
                try:
                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=10, encoding='utf-8')
                    if result.returncode == 0:
                        print(f"[OK] Браузер открыт через командную строку")
                        browser_opened = True
                        break
                except Exception as e:
                    continue
        except Exception as e:
            print(f"[ERROR] Принудительное открытие не удалось: {e}")

def start_browser_after_server(url):
    """Запускает браузер после того, как сервер будет готов"""
    print(f"Ожидаем запуска сервера для открытия браузера...")
    
    if wait_for_server_advanced(url, timeout=30):
        print("Сервер готов! Открываем браузер...")
        
        # Дополнительная проверка - убеждаемся, что сервер отвечает на главную страницу
        if requests is not None:
            try:
                response = requests.get(url, timeout=5)
                if response.status_code == 200 and len(response.content) > 100:
                    print(f"[OK] Главная страница доступна (размер: {len(response.content)} байт)")
                    # Принудительно открываем браузер
                    global browser_opened
                    browser_opened = False  # Сбрасываем флаг
                    open_browser_safe(url)
                else:
                    print(f"[WARNING] Сервер отвечает, но страница может быть пустой (статус: {response.status_code})")
                    browser_opened = False  # Сбрасываем флаг
                    open_browser_safe(url)
            except Exception as e:
                print(f"[WARNING] Не удалось проверить главную страницу: {e}")
                browser_opened = False  # Сбрасываем флаг
                open_browser_safe(url)
    else:
        print("[WARNING] Не удалось дождаться запуска сервера")
        print("   Попробуйте открыть браузер вручную: " + url)
        print("   Или подождите еще немного и обновите страницу")

def main():
    """Главная функция запуска"""
    global server_process
    
    # Проверяем и останавливаем старые процессы перед запуском
    print("Проверка запущенных процессов на порту 5000...")
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('127.0.0.1', 5000))
        sock.close()
        
        if result == 0:
            print("[WARNING] Порт 5000 занят. Останавливаем старые процессы...")
            try:
                subprocess.run(['python', 'stop_servers.py'], timeout=10, check=False)
                print("[OK] Старые процессы остановлены")
                time.sleep(2)  # Даем время на освобождение порта
            except Exception as e:
                print(f"[WARNING] Не удалось остановить процессы автоматически: {e}")
                print("Попробуйте вручную: python stop_servers.py")
    except Exception as e:
        print(f"[INFO] Проверка порта не выполнена: {e}")
    
    # Регистрируем обработчики завершения
    register_cleanup()
    
    # Проверяем, не запускается ли это при создании exe файла
    if getattr(sys, 'frozen', False):
        if len(sys.argv) > 1 and sys.argv[1] == '--build':
            print("Режим сборки - сервер не запускается")
            return
    
    # Проверяем переменную окружения для предотвращения запуска при сборке
    if os.environ.get('PYINSTALLER_BUILD') == '1':
        print("Режим сборки PyInstaller - сервер не запускается")
        return
    
    # Определяем базовую директорию
    if getattr(sys, 'frozen', False):
        # Если запущено из exe, используем директорию exe-файла
        base_dir = os.path.dirname(sys.executable)
        print(f"[INFO] Запуск из exe-файла: {base_dir}")
    else:
        # Если запущено из Python, используем текущую директорию
        base_dir = os.getcwd()
        print(f"[INFO] Запуск из Python: {base_dir}")
    
    print_header()
    
    # Проверяем зависимости и файлы
    if not check_dependencies():
        print("\n[WARNING] Некоторые зависимости отсутствуют!")
    
    if not check_files(base_dir):
        print("\n[WARNING] Некоторые файлы отсутствуют!")
    
    # Проверяем доступность модулей для app.py
    print("\nПроверка модулей для Flask приложения...")
    try:
        import flask
        print("  [OK] flask")
    except ImportError as e:
        print(f"  [ERROR] flask: {e}")
    
    try:
        import sqlite3
        print("  [OK] sqlite3")
    except ImportError as e:
        print(f"  [ERROR] sqlite3: {e}")
    
    try:
        import pandas
        print("  [OK] pandas")
    except ImportError as e:
        print(f"  [ERROR] pandas: {e}")
    
    try:
        import openpyxl
        print("  [OK] openpyxl")
    except ImportError as e:
        print(f"  [ERROR] openpyxl: {e}")
    
    try:
        import werkzeug
        print("  [OK] werkzeug")
    except ImportError as e:
        print(f"  [ERROR] werkzeug: {e}")
    
    print("\nВозможности:")
    print("   • Просмотр и управление базой данных")
    print("   • Запуск скрипт обработки")
    print("")
    print("   • Загрузка файлов (с сохранением оригинальных имен)")
    
    # Определяем URL
    url = "http://127.0.0.1:5000"
    
    print(f"\nВеб-приложение будет доступно по адресу: {url}")
    print("Браузер откроется автоматически после запуска сервера")
    print("\nДля остановки нажмите Ctrl+C")
    print("=" * 50)
    
    # Устанавливаем переменные окружения для Flask
    env = os.environ.copy()
    env['FLASK_ENV'] = 'production'
    env['FLASK_DEBUG'] = '0'
    # Отключаем предупреждение о development server
    env['WERKZEUG_RUN_MAIN'] = 'true'
    # Устанавливаем переменную для предотвращения ошибки WERKZEUG_SERVER_FD
    # Удаляем эту переменную, если она есть, чтобы Flask не пытался её использовать
    if 'WERKZEUG_SERVER_FD' in env:
        del env['WERKZEUG_SERVER_FD']
    
    # Добавляем базовую директорию в PYTHONPATH
    if 'PYTHONPATH' in env:
        env['PYTHONPATH'] = base_dir + os.pathsep + env['PYTHONPATH']
    else:
        env['PYTHONPATH'] = base_dir
    
    print(f"   Базовая директория: {base_dir}")
    print(f"   PYTHONPATH: {env['PYTHONPATH']}")
    
    # Запускаем Flask приложение
    print("Запуск Flask приложения...")
    try:
        # Проверяем, что файл app.py существует
        app_path = os.path.join(base_dir, 'web', 'app', 'app.py')
        if not os.path.exists(app_path):
            print(f"[ERROR] Файл {app_path} не найден!")
            return 1
        
        # Используем Anaconda Python для веб-приложения
        anaconda_python_path = r"C:\anaconda3\python.exe"
        if os.path.exists(anaconda_python_path):
            python_executable = anaconda_python_path
            print(f"   Используем Anaconda Python: {python_executable}")
        else:
            # Если Anaconda не найден, используем текущий Python
            python_executable = sys.executable
            print(f"   Используем текущий Python: {python_executable}")
        
        print(f"   Запускаем: {python_executable} {app_path}")
        
        # Сначала проверяем синтаксис файла
        try:
            result = subprocess.run([python_executable, '-m', 'py_compile', app_path], 
                                  capture_output=True, text=True, timeout=10, encoding='utf-8')
            if result.returncode != 0:
                print(f"[ERROR] Ошибка синтаксиса в {app_path}:")
                print(result.stderr)
                return 1
            print("  [OK] Синтаксис файла корректен")
        except Exception as e:
            print(f"[WARNING] Не удалось проверить синтаксис: {e}")
        
        # Запускаем Flask приложение через subprocess с правильными переменными окружения
        print("   Запускаем Flask приложение...")
        
        # Создаем чистую среду без проблемных переменных
        clean_env = env.copy()
        for key in ['WERKZEUG_SERVER_FD', 'WERKZEUG_RUN_MAIN', 'FLASK_ENV', 'FLASK_DEBUG']:
            if key in clean_env:
                del clean_env[key]
        
        # Устанавливаем переменные для корректной работы Flask
        clean_env['FLASK_APP'] = app_path
        clean_env['FLASK_ENV'] = 'production'
        
        server_process = subprocess.Popen(
            [python_executable, app_path],
            env=clean_env,
            stdout=None,  # Не перехватываем stdout
            stderr=None,  # Не перехватываем stderr
            text=True,
            encoding='utf-8'
        )
        
        print("[OK] Веб-приложение запущено!")
        
        # Запускаем HTML сервер для отчетов
        print("Запуск HTML сервера для отчетов...")
        if start_html_server():
            print("[OK] HTML сервер запущен на порту 8080")
        else:
            print("[WARNING] HTML сервер не запущен")
        
        print("Логи сервера:")
        print("-" * 50)
        
        # Запускаем браузер в отдельном потоке
        browser_thread = threading.Thread(target=start_browser_after_server, args=(url,))
        browser_thread.daemon = True
        browser_thread.start()
        
        # Также запускаем резервное открытие браузера через 8 секунд (увеличили время)
        def backup_browser_open():
            time.sleep(8)
            if not browser_opened:
                print("Резервное открытие браузера...")
                open_browser_safe(url)
            else:
                print("Браузер уже открыт, резервное открытие не требуется")
        
        backup_thread = threading.Thread(target=backup_browser_open)
        backup_thread.daemon = True
        backup_thread.start()
        
        # Добавляем информацию о том, как открыть браузер вручную
        print(f"\n[INFO] Если браузер не открылся автоматически:")
        print(f"   1. Откройте любой браузер (Chrome, Firefox, Edge)")
        print(f"   2. Введите в адресной строке: {url}")
        print(f"   3. Или нажмите Ctrl+Click на эту ссылку: {url}")
        
        # Ожидаем завершения процесса
        try:
            print("\n[INFO] Сервер запущен. Нажмите Ctrl+C для остановки...")
            server_process.wait()  # Ждем завершения процесса
                        
        except KeyboardInterrupt:
            print("\n[STOP] Получен сигнал остановки...")
        finally:
            if server_process:
                server_process.terminate()
                try:
                    server_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    server_process.kill()
                print("[OK] Сервер остановлен")
    
    except Exception as e:
        print(f"[ERROR] Ошибка запуска сервера: {e}")
        return 1
    
    print("\nДо свидания!")
    return 0

if __name__ == "__main__":
    # Дополнительная проверка для предотвращения запуска при сборке
    try:
        if 'PyInstaller' in sys.modules or 'pyinstaller' in sys.modules:
            print("Обнаружен PyInstaller - сервер не запускается")
            sys.exit(0)
    except:
        pass
    
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nПриложение остановлено пользователем")
        sys.exit(0)
    except Exception as e:
        print(f"\n[ERROR] Критическая ошибка: {e}")
        sys.exit(1)
