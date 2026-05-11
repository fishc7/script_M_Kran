#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для копирования таблицы слов_клейм_факт в правильную базу данных
"""

import sqlite3
import os
import shutil

def copy_table_to_correct_database():
    """Копирует таблицу слов_клейм_факт в правильную базу данных"""
    
    # Пути к базам данных
    source_db = r"D:\МК_Кран\script_M_Kran\database\M_Kran_Kingesepp.db"
    target_db = r"D:\МК_Кран\script_M_Kran\database\BD_Kingisepp\M_Kran_Kingesepp.db"
    
    print(f"Источник: {source_db}")
    print(f"Цель: {target_db}")
    
    if not os.path.exists(source_db):
        print(f"Исходная база данных не найдена: {source_db}")
        return False
    
    if not os.path.exists(target_db):
        print(f"Целевая база данных не найдена: {target_db}")
        return False
    
    try:
        # Подключаемся к исходной базе
        source_conn = sqlite3.connect(source_db)
        source_cursor = source_conn.cursor()
        
        # Подключаемся к целевой базе
        target_conn = sqlite3.connect(target_db)
        target_cursor = target_conn.cursor()
        
        # Проверяем, существует ли таблица в исходной базе
        source_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='слов_клейм_факт'")
        if not source_cursor.fetchone():
            print("Таблица слов_клейм_факт не найдена в исходной базе")
            return False
        
        # Получаем структуру таблицы
        source_cursor.execute("PRAGMA table_info(слов_клейм_факт)")
        columns = source_cursor.fetchall()
        print(f"Структура таблицы: {len(columns)} столбцов")
        
        # Получаем данные
        source_cursor.execute("SELECT * FROM слов_клейм_факт")
        records = source_cursor.fetchall()
        print(f"Найдено записей: {len(records)}")
        
        # Проверяем, существует ли таблица в целевой базе
        target_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='слов_клейм_факт'")
        if target_cursor.fetchone():
            print("Таблица уже существует в целевой базе, удаляем...")
            target_cursor.execute("DROP TABLE слов_клейм_факт")
        
        # Создаем таблицу в целевой базе
        create_sql = """
        CREATE TABLE слов_клейм_факт (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Фактическое_Клеймо TEXT UNIQUE,
            ФИО TEXT,
            Примечание TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        target_cursor.execute(create_sql)
        print("Таблица создана в целевой базе")
        
        # Копируем данные
        insert_sql = "INSERT INTO слов_клейм_факт (id, Фактическое_Клеймо, ФИО, Примечание, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)"
        for record in records:
            target_cursor.execute(insert_sql, record)
        
        target_conn.commit()
        print(f"Скопировано {len(records)} записей")
        
        # Проверяем результат
        target_cursor.execute("SELECT COUNT(*) FROM слов_клейм_факт")
        count = target_cursor.fetchone()[0]
        print(f"Всего записей в целевой базе: {count}")
        
        # Показываем записи
        target_cursor.execute("SELECT * FROM слов_клейм_факт ORDER BY id")
        records = target_cursor.fetchall()
        print("\nЗаписи в целевой базе:")
        for record in records:
            print(f"  ID: {record[0]}, Клеймо: {record[1]}, ФИО: {record[2]}, Примечание: {record[3]}")
        
        source_conn.close()
        target_conn.close()
        
        print("\n✅ Копирование завершено успешно!")
        return True
        
    except Exception as e:
        print(f"Ошибка при копировании: {e}")
        return False

if __name__ == "__main__":
    copy_table_to_correct_database()
