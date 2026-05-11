#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import locale
import sqlite3
import os

def test_encoding():
    """Тестирует кодировку и подключение к базе данных"""
    print("=== Тест кодировки и базы данных ===")
    
    # Информация о системе
    print(f"Python версия: {sys.version}")
    print(f"Платформа: {sys.platform}")
    print(f"Кодировка по умолчанию: {sys.getdefaultencoding()}")
    print(f"Локаль: {locale.getlocale()}")
    
    # Тест вывода русских символов
    print("\n=== Тест вывода русских символов ===")
    try:
        print("Тест: Привет, мир!")
        print("Тест: Извлечение чисел из обозначений")
        print("Тест: Удаление префиксов S/F")
    except UnicodeEncodeError as e:
        print(f"Ошибка кодировки: {e}")
    
    # Тест подключения к базе данных
    print("\n=== Тест подключения к базе данных ===")
    
    # Путь к базе данных
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(project_root, 'BD_Kingisepp', 'M_Kran_Kingesepp.db')
    
    print(f"Путь к БД: {db_path}")
    print(f"Файл существует: {os.path.exists(db_path)}")
    
    if os.path.exists(db_path):
        try:
            # Подключение к базе данных
            conn = sqlite3.connect(db_path, timeout=30.0, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            
            # Устанавливаем кодировку UTF-8
            conn.execute("PRAGMA encoding='UTF-8'")
            
            cursor = conn.cursor()
            
            # Получаем список таблиц
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = cursor.fetchall()
            
            print(f"Найдено таблиц: {len(tables)}")
            
            for i, row in enumerate(tables[:5]):  # Показываем только первые 5 таблиц
                table_name = row['name']
                try:
                    print(f"  {i+1}. {table_name}")
                except UnicodeEncodeError:
                    print(f"  {i+1}. [table_name]")
                
                # Получаем количество записей
                try:
                    cursor.execute(f"SELECT COUNT(*) as count FROM `{table_name}`")
                    count = cursor.fetchone()['count']
                    print(f"     Записей: {count}")
                except Exception as e:
                    print(f"     Ошибка подсчета: {e}")
            
            if len(tables) > 5:
                print(f"  ... и еще {len(tables) - 5} таблиц")
            
            conn.close()
            print("✅ Подключение к базе данных успешно")
            
        except Exception as e:
            print(f"❌ Ошибка подключения к БД: {e}")
    else:
        print("❌ Файл базы данных не найден")

if __name__ == "__main__":
    test_encoding()

