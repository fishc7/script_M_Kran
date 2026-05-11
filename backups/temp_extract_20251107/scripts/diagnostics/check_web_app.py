#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Диагностика веб-приложения M_Kran
"""

import os
import sys
import subprocess
import importlib

def print_header():
    print("=" * 60)
    print("    ДИАГНОСТИКА ВЕБ-ПРИЛОЖЕНИЯ M_Kran")
    print("=" * 60)

def check_python():
    """Проверка версии Python"""
    print("\n1. ПРОВЕРКА PYTHON")
    print(f"   Версия Python: {sys.version}")
    print(f"   Исполняемый файл: {sys.executable}")
    print(f"   Платформа: {sys.platform}")
    return True

def check_dependencies():
    """Проверка зависимостей"""
    print("\n2. ПРОВЕРКА ЗАВИСИМОСТЕЙ")
    
    dependencies = {
        'flask': 'Flask web framework',
        'pandas': 'Data processing',
        'openpyxl': 'Excel file handling',
        'werkzeug': 'WSGI utilities',
        'jinja2': 'Template engine',
        'sqlite3': 'SQLite database (встроенный)',
        'requests': 'HTTP library (опционально)'
    }
    
    missing = []
    for module, description in dependencies.items():
        try:
            if module == 'sqlite3':
                import sqlite3
                version = sqlite3.sqlite_version
            else:
                mod = importlib.import_module(module)
                version = getattr(mod, '__version__', 'unknown')
            print(f"   [OK] {module} v{version} - {description}")
        except ImportError:
            print(f"   [ERROR] {module} - {description}")
            missing.append(module)
    
    if missing:
        print(f"\n   Отсутствуют модули: {', '.join(missing)}")
        print("   Установите их командой: pip install " + " ".join(missing))
        return False
    else:
        print("   Все зависимости найдены!")
        return True

def check_files():
    """Проверка файлов"""
    print("\n3. ПРОВЕРКА ФАЙЛОВ")
    
    base_dir = os.getcwd()
    required_files = [
        ('web_launcher.py', 'file'),
        ('web/app/app.py', 'file'),
        ('web/app/requirements.txt', 'file'),
        ('web/templates', 'directory'),
        ('web/static', 'directory'),
        ('database/BD_Kingisepp', 'directory'),
        ('scripts', 'directory')
    ]
    
    missing = []
    for file_path, file_type in required_files:
        full_path = os.path.join(base_dir, file_path)
        if file_type == 'file' and os.path.isfile(full_path):
            size = os.path.getsize(full_path)
            print(f"   [OK] {file_path} ({size} байт)")
        elif file_type == 'directory' and os.path.isdir(full_path):
            print(f"   [OK] {file_path} (папка)")
        else:
            print(f"   [ERROR] {file_path} ({file_type})")
            missing.append(file_path)
    
    if missing:
        print(f"\n   Отсутствуют: {', '.join(missing)}")
        return False
    else:
        print("   Все файлы найдены!")
        return True

def check_database():
    """Проверка базы данных"""
    print("\n4. ПРОВЕРКА БАЗЫ ДАННЫХ")
    
    db_path = os.path.join('database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db')
    if os.path.exists(db_path):
        size = os.path.getsize(db_path)
        print(f"   [OK] База данных найдена: {db_path} ({size} байт)")
        
        # Проверяем подключение к БД
        try:
            import sqlite3
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            conn.close()
            print(f"   [OK] Подключение к БД успешно, таблиц: {len(tables)}")
            return True
        except Exception as e:
            print(f"   [ERROR] Ошибка подключения к БД: {e}")
            return False
    else:
        print(f"   [ERROR] База данных не найдена: {db_path}")
        return False

def check_ports():
    """Проверка занятости портов"""
    print("\n5. ПРОВЕРКА ПОРТОВ")
    
    import socket
    
    ports_to_check = [5000, 8080, 8000]
    for port in ports_to_check:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(('localhost', port))
            sock.close()
            
            if result == 0:
                print(f"   [WARNING] Порт {port} занят")
            else:
                print(f"   [OK] Порт {port} свободен")
        except Exception as e:
            print(f"   [ERROR] Ошибка проверки порта {port}: {e}")

def test_web_launcher():
    """Тестирование web_launcher.py"""
    print("\n6. ТЕСТИРОВАНИЕ WEB_LAUNCHER.PY")
    
    try:
        # Проверяем синтаксис
        result = subprocess.run([sys.executable, '-m', 'py_compile', 'web_launcher.py'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print("   [OK] Синтаксис web_launcher.py корректен")
        else:
            print(f"   [ERROR] Ошибка синтаксиса в web_launcher.py:")
            print(f"   {result.stderr}")
            return False
    except Exception as e:
        print(f"   [ERROR] Ошибка проверки синтаксиса: {e}")
        return False
    
    return True

def main():
    print_header()
    
    # Выполняем все проверки
    checks = [
        check_python(),
        check_dependencies(),
        check_files(),
        check_database(),
        check_ports(),
        test_web_launcher()
    ]
    
    print("\n" + "=" * 60)
    print("РЕЗУЛЬТАТ ДИАГНОСТИКИ")
    print("=" * 60)
    
    if all(checks):
        print("[OK] ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ УСПЕШНО!")
        print("   Веб-приложение готово к запуску.")
        print("\n   Для запуска используйте: python web_launcher.py")
    else:
        print("[ERROR] ОБНАРУЖЕНЫ ПРОБЛЕМЫ!")
        print("   Исправьте ошибки перед запуском веб-приложения.")
    
    print("\nНажмите Enter для выхода...")
    input()

if __name__ == "__main__":
    main()
