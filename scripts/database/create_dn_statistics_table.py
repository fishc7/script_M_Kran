#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для создания таблиц статистики DN в базе данных
Вариант 1: Отдельные таблицы для каждого типа статистики
"""

import sqlite3
import os
import sys

def create_dn_statistics_tables():
    """Создает таблицы для статистики DN"""
    # Формируем путь к БД относительно корня проекта
    base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    db_path = os.path.join(base_path, 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db')
    
    if not os.path.exists(db_path):
        print(f"База данных не найдена: {db_path}")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 1. Таблица dn_statistics_daily - Статистика по дням
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dn_statistics_daily (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                дата DATE NOT NULL UNIQUE,
                день_недели TEXT,
                количество_записей INTEGER NOT NULL,
                среднее_dn REAL,
                минимальное_dn REAL,
                максимальное_dn REAL,
                сумма_dn REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_dn_statistics_daily_дата 
            ON dn_statistics_daily(дата)
        ''')
        
        # 2. Таблица dn_statistics_weekly - Статистика по неделям
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dn_statistics_weekly (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                год INTEGER NOT NULL,
                неделя TEXT NOT NULL,
                начало_недели DATE NOT NULL,
                конец_недели DATE NOT NULL,
                количество_записей INTEGER NOT NULL,
                среднее_dn REAL,
                минимальное_dn REAL,
                максимальное_dn REAL,
                сумма_dn REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(год, неделя)
            )
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_dn_statistics_weekly_период 
            ON dn_statistics_weekly(год, неделя)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_dn_statistics_weekly_даты 
            ON dn_statistics_weekly(начало_недели, конец_недели)
        ''')
        
        # 3. Таблица dn_statistics_monthly - Статистика по месяцам
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dn_statistics_monthly (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                год INTEGER NOT NULL,
                месяц INTEGER NOT NULL,
                месяц_название TEXT,
                год_месяц TEXT NOT NULL,
                количество_записей INTEGER NOT NULL,
                количество_дней INTEGER,
                среднее_dn REAL,
                минимальное_dn REAL,
                максимальное_dn REAL,
                сумма_dn REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(год, месяц)
            )
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_dn_statistics_monthly_период 
            ON dn_statistics_monthly(год, месяц)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_dn_statistics_monthly_год_месяц 
            ON dn_statistics_monthly(год_месяц)
        ''')
        
        # 4. Таблица dn_statistics_yearly - Статистика по годам
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dn_statistics_yearly (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                год INTEGER NOT NULL UNIQUE,
                количество_записей INTEGER NOT NULL,
                количество_дней INTEGER,
                количество_месяцев INTEGER,
                среднее_dn REAL,
                минимальное_dn REAL,
                максимальное_dn REAL,
                сумма_dn REAL,
                первая_дата DATE,
                последняя_дата DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_dn_statistics_yearly_год 
            ON dn_statistics_yearly(год)
        ''')
        
        # 5. Таблица dn_statistics_period - Общая статистика за весь период
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dn_statistics_period (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                общее_количество_записей INTEGER NOT NULL,
                количество_дней INTEGER,
                среднее_dn_за_период REAL,
                минимальное_dn REAL,
                максимальное_dn REAL,
                сумма_dn REAL,
                первая_дата DATE,
                последняя_дата DATE,
                calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        
        print("✅ Таблицы статистики DN успешно созданы!")
        print("   - dn_statistics_daily")
        print("   - dn_statistics_weekly")
        print("   - dn_statistics_monthly")
        print("   - dn_statistics_yearly")
        print("   - dn_statistics_period")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при создании таблиц: {e}")
        return False

if __name__ == "__main__":
    create_dn_statistics_tables()
