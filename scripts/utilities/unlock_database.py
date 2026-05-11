#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Утилита для разблокировки базы данных SQLite
Удаляет файлы блокировки и принудительно закрывает соединения
"""

import os
import sqlite3
import time
from pathlib import Path

def unlock_database():
    """Принудительно разблокирует базу данных SQLite"""
    
    # Получаем путь к базе данных
    project_root = Path(__file__).parent.parent.parent
    db_path = project_root / 'database' / 'BD_Kingisepp' / 'M_Kran_Kingesepp.db'
    db_dir = db_path.parent
    
    print("🔓 РАЗБЛОКИРОВКА БАЗЫ ДАННЫХ SQLITE")
    print("=" * 50)
    print(f"Путь к БД: {db_path}")
    
    # 1. Проверяем существование файлов блокировки
    shm_file = db_path.with_suffix('.db-shm')
    wal_file = db_path.with_suffix('.db-wal')
    
    print(f"\n📁 Проверка файлов блокировки:")
    print(f"SHM файл: {shm_file} - {'существует' if shm_file.exists() else 'не найден'}")
    print(f"WAL файл: {wal_file} - {'существует' if wal_file.exists() else 'не найден'}")
    
    # 2. Пытаемся подключиться к базе данных
    print(f"\n🔌 Попытка подключения к базе данных...")
    try:
        conn = sqlite3.connect(str(db_path), timeout=5.0)
        conn.execute("PRAGMA wal_checkpoint(FULL)")
        conn.close()
        print("✅ База данных доступна")
        return True
    except sqlite3.OperationalError as e:
        if "database is locked" in str(e):
            print("❌ База данных заблокирована")
        else:
            print(f"❌ Ошибка подключения: {e}")
            return False
    
    # 3. Удаляем файлы блокировки
    print(f"\n🗑️ Удаление файлов блокировки...")
    
    try:
        if shm_file.exists():
            shm_file.unlink()
            print(f"✅ Удален файл: {shm_file.name}")
        
        if wal_file.exists():
            wal_file.unlink()
            print(f"✅ Удален файл: {wal_file.name}")
            
    except Exception as e:
        print(f"❌ Ошибка при удалении файлов: {e}")
        return False
    
    # 4. Проверяем результат
    print(f"\n🔍 Проверка результата...")
    time.sleep(1)  # Небольшая пауза
    
    try:
        conn = sqlite3.connect(str(db_path), timeout=5.0)
        conn.execute("PRAGMA wal_checkpoint(FULL)")
        conn.close()
        print("✅ База данных успешно разблокирована!")
        return True
    except sqlite3.OperationalError as e:
        print(f"❌ База данных все еще заблокирована: {e}")
        return False

def run_script():
    """Функция для запуска скрипта через веб-интерфейс"""
    print("DEBUG: Запуск утилиты разблокировки базы данных")
    success = unlock_database()
    if success:
        print("DEBUG: База данных успешно разблокирована")
    else:
        print("DEBUG: Не удалось разблокировать базу данных")
    return success

if __name__ == "__main__":
    unlock_database()
