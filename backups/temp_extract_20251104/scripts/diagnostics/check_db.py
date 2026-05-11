#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import os

def check_database():
    """Проверяет содержимое базы данных"""
    
    # Путь к базе данных
    db_path = os.path.join('database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db')
    
    print(f"Проверка базы данных: {db_path}")
    print(f"Файл существует: {os.path.exists(db_path)}")
    
    if os.path.exists(db_path):
        print(f"Размер файла: {os.path.getsize(db_path)} байт")
    
    try:
        # Подключаемся к базе данных
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Получаем список всех таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row['name'] for row in cursor.fetchall()]
        
        print(f"\nНайдено таблиц: {len(tables)}")
        print("Список таблиц:")
        for table in tables:
            print(f"  - {table}")
        
        # Проверяем содержимое каждой таблицы
        print("\nСодержимое таблиц:")
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) as count FROM `{table}`")
                count = cursor.fetchone()['count']
                print(f"  {table}: {count} записей")
                
                # Показываем первые 3 записи для таблиц с данными
                if count > 0:
                    cursor.execute(f"SELECT * FROM `{table}` LIMIT 3")
                    rows = cursor.fetchall()
                    print(f"    Первые записи:")
                    for i, row in enumerate(rows, 1):
                        print(f"      {i}. {dict(row)}")
                print()
                
            except Exception as e:
                print(f"  {table}: Ошибка - {e}")
        
        conn.close()
        
    except Exception as e:
        print(f"Ошибка подключения к базе данных: {e}")

if __name__ == "__main__":
    check_database()
