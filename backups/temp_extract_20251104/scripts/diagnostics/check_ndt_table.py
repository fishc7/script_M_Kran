#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для проверки таблицы folder_NDT_Report
"""

import sqlite3
import os
import sys

# Добавляем путь к модулям проекта
sys.path.append(os.path.dirname(__file__))
from scripts.utilities.db_utils import get_database_path

def check_ndt_table():
    """Проверяет таблицу folder_NDT_Report"""
    
    # Получаем путь к базе данных
    db_path = get_database_path()
    if not db_path:
        print("[ERROR] Не удалось найти базу данных")
        return
    
    print(f"[DB] База данных: {db_path}")
    
    try:
        # Подключаемся к базе данных
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Проверяем, существует ли таблица
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='folder_NDT_Report'")
        table_exists = cursor.fetchone()
        
        if not table_exists:
            print("[ERROR] Таблица folder_NDT_Report не найдена")
            print("Запустите скрипт create_ndt_reports_table.py для создания таблицы")
            return
        
        print("[OK] Таблица folder_NDT_Report найдена")
        
        # Получаем количество записей
        cursor.execute("SELECT COUNT(*) FROM folder_NDT_Report")
        count = cursor.fetchone()[0]
        print(f"[STATS] Количество записей: {count}")
        
        if count > 0:
            # Получаем несколько примеров записей
            cursor.execute("SELECT file_name, full_path FROM folder_NDT_Report LIMIT 5")
            records = cursor.fetchall()
            
            print("\n[EXAMPLES] Примеры записей:")
            for i, (file_name, full_path) in enumerate(records, 1):
                print(f"  {i}. {file_name}")
                print(f"     Путь: {full_path}")
                print()
        
        # Проверяем структуру таблицы
        cursor.execute("PRAGMA table_info(folder_NDT_Report)")
        columns = cursor.fetchall()
        
        print("[STRUCTURE] Структура таблицы:")
        for col in columns:
            print(f"  - {col[1]} ({col[2]})")
        
        conn.close()
        
        if count > 0:
            print(f"\n[OK] Таблица содержит {count} записей с файловыми путями")
            print("Теперь вы можете открыть веб-интерфейс и просмотреть таблицу folder_NDT_Report")
        else:
            print("\n[WARNING] Таблица пуста")
            print("Запустите скрипт create_ndt_reports_table.py для заполнения таблицы")
        
    except Exception as e:
        print(f"[ERROR] Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_ndt_table()
