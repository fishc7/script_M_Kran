#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для поиска мертвого кода в проекте M_Kran

Использование:
    python scripts/tools/find_dead_code.py

Требования:
    pip install vulture
"""

import os
import sys
import subprocess
from pathlib import Path

def find_dead_code():
    """Ищет мертвый код в проекте используя vulture"""
    
    project_root = Path(__file__).parent.parent.parent
    
    print("=" * 60)
    print("Поиск мертвого кода в проекте M_Kran")
    print("=" * 60)
    print()
    
    # Проверяем наличие vulture
    try:
        import vulture
        print("[OK] Vulture установлен")
    except ImportError:
        print("[ERROR] Vulture не установлен")
        print("Установите: pip install vulture")
        return 1
    
    # Директории для исключения
    exclude_dirs = [
        'backups',
        'archive',
        '__pycache__',
        '.pytest_cache',
        '.venv',
        'venv',
        'node_modules',
        '.git',
    ]
    
    # Строим команду vulture
    cmd = ['vulture', '.']
    
    # Добавляем исключения
    for exclude in exclude_dirs:
        cmd.extend(['--exclude', exclude])
    
    # Минимальный confidence (процент уверенности)
    cmd.extend(['--min-confidence', '80'])
    
    # Сортировка по размеру
    cmd.append('--sort-by-size')
    
    print("Запуск vulture...")
    print(f"Команда: {' '.join(cmd)}")
    print()
    
    try:
        # Запускаем vulture
        result = subprocess.run(
            cmd,
            cwd=project_root,
            capture_output=True,
            text=True,
            encoding='utf-8'
        )
        
        if result.stdout:
            print("Найденный мертвый код:")
            print("-" * 60)
            print(result.stdout)
        
        if result.stderr:
            print("Предупреждения:")
            print("-" * 60)
            print(result.stderr)
        
        # Сохраняем результаты в файл
        output_file = project_root / 'results' / 'dead_code_report.txt'
        output_file.parent.mkdir(exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("Отчет о мертвом коде\n")
            f.write("=" * 60 + "\n\n")
            if result.stdout:
                f.write(result.stdout)
            if result.stderr:
                f.write("\n\nПредупреждения:\n")
                f.write(result.stderr)
        
        print()
        print(f"[OK] Отчет сохранен в: {output_file}")
        
        return 0
        
    except Exception as e:
        print(f"[ERROR] Ошибка при запуске vulture: {e}")
        return 1


def find_unused_imports():
    """Ищет неиспользуемые импорты"""
    
    print()
    print("=" * 60)
    print("Поиск неиспользуемых импортов")
    print("=" * 60)
    print()
    
    try:
        import autoflake
        print("[OK] Autoflake доступен")
        print("Запустите: autoflake --remove-all-unused-imports --in-place --recursive .")
    except ImportError:
        print("[INFO] Autoflake не установлен")
        print("Установите: pip install autoflake")
        print("Затем запустите:")
        print("  autoflake --remove-all-unused-imports --in-place --recursive .")


if __name__ == "__main__":
    try:
        exit_code = find_dead_code()
        find_unused_imports()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nПрервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\nКритическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

















