#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для анализа результатов нормализации столбцов Статус_ВИК и ВИК

РЕФАКТОРИНГ: Использует единый модуль scripts.core.database для подключения к БД
"""

import logging
from datetime import datetime
import os
import sys

# Добавляем путь к core модулю
core_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'core')
if core_dir not in sys.path:
    sys.path.insert(0, core_dir)

# Импортируем единый модуль подключения к БД
from core.database import get_database_connection, DatabaseConnection

def analyze_vik_status():
    """
    Анализ результатов нормализации столбцов Статус_ВИК и ВИК
    """
    print("=" * 80)
    print("АНАЛИЗ РЕЗУЛЬТАТОВ НОРМАЛИЗАЦИИ СТОЛБЦОВ СТАТУС_ВИК И ВИК")
    print("=" * 80)
    
    conn = get_database_connection()
    if not conn:
        print("❌ Не удалось подключиться к базе данных")
        return False
    
    try:
        cursor = conn.cursor()
        
        # 1. Общая статистика
        print("\n📊 ОБЩАЯ СТАТИСТИКА:")
        print("-" * 40)
        
        cursor.execute("SELECT COUNT(*) FROM logs_lnk")
        total_records = cursor.fetchone()[0]
        print(f"Всего записей в таблице logs_lnk: {total_records:,}")
        
        # 2. Статистика по Статус_ВИК
        print("\n📊 СТАТИСТИКА ПО СТАТУС_ВИК:")
        print("-" * 40)
        
        cursor.execute("""
            SELECT "Статус_ВИК", COUNT(*) as count
            FROM logs_lnk 
            WHERE "Статус_ВИК" IS NOT NULL AND "Статус_ВИК" != '' AND "Статус_ВИК" != 'NULL'
            GROUP BY "Статус_ВИК"
            ORDER BY count DESC
        """)
        status_stats = cursor.fetchall()
        
        for status, count in status_stats:
            percentage = (count / total_records) * 100
            print(f"'{status}': {count:,} записей ({percentage:.1f}%)")
        
        # 3. Записи с пустым Статус_ВИК
        print("\n📊 ЗАПИСИ С ПУСТЫМ СТАТУС_ВИК:")
        print("-" * 40)
        
        cursor.execute("""
            SELECT COUNT(*) FROM logs_lnk 
            WHERE "Статус_ВИК" IS NULL OR "Статус_ВИК" = '' OR "Статус_ВИК" = 'NULL'
        """)
        empty_status_count = cursor.fetchone()[0]
        print(f"Записей с пустым Статус_ВИК: {empty_status_count:,}")
        
        if empty_status_count > 0:
            # Показываем примеры записей с пустым Статус_ВИК
            cursor.execute("""
                SELECT "Чертеж", "Номер_стыка", "ВИК", "Статус_ВИК"
                FROM logs_lnk 
                WHERE "Статус_ВИК" IS NULL OR "Статус_ВИК" = '' OR "Статус_ВИК" = 'NULL'
                LIMIT 5
            """)
            examples = cursor.fetchall()
            print("\nПримеры записей с пустым Статус_ВИК:")
            for example in examples:
                print(f"  Чертеж: {example[0]}, Стык: {example[1]}, ВИК: {example[2]}, Статус_ВИК: {example[3]}")
        
        # 4. Анализ столбца ВИК
        print("\n📊 АНАЛИЗ СТОЛБЦА ВИК:")
        print("-" * 40)
        
        cursor.execute("""
            SELECT "ВИК", COUNT(*) as count
            FROM logs_lnk 
            WHERE "ВИК" IS NOT NULL AND "ВИК" != '' AND "ВИК" != 'NULL'
            GROUP BY "ВИК"
            ORDER BY count DESC
            LIMIT 10
        """)
        vik_stats = cursor.fetchall()
        
        print("Топ-10 значений в столбце ВИК:")
        for vik_value, count in vik_stats:
            print(f"'{vik_value}': {count:,} записей")
        
        # 5. Сравнение Статус_ВИК и ВИК
        print("\n📊 СРАВНЕНИЕ СТАТУС_ВИК И ВИК:")
        print("-" * 40)
        
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN "Статус_ВИК" = "ВИК" THEN 'Одинаковые'
                    WHEN ("Статус_ВИК" IS NULL OR "Статус_ВИК" = '' OR "Статус_ВИК" = 'NULL') 
                         AND ("ВИК" IS NOT NULL AND "ВИК" != '' AND "ВИК" != 'NULL') THEN 'Статус_ВИК пустой, ВИК заполнен'
                    WHEN ("ВИК" IS NULL OR "ВИК" = '' OR "ВИК" = 'NULL') 
                         AND ("Статус_ВИК" IS NOT NULL AND "Статус_ВИК" != '' AND "Статус_ВИК" != 'NULL') THEN 'ВИК пустой, Статус_ВИК заполнен'
                    WHEN ("Статус_ВИК" IS NULL OR "Статус_ВИК" = '' OR "Статус_ВИК" = 'NULL') 
                         AND ("ВИК" IS NULL OR "ВИК" = '' OR "ВИК" = 'NULL') THEN 'Оба пустые'
                    ELSE 'Разные значения'
                END as comparison,
                COUNT(*) as count
            FROM logs_lnk
            GROUP BY comparison
            ORDER BY count DESC
        """)
        comparison_stats = cursor.fetchall()
        
        for comparison, count in comparison_stats:
            percentage = (count / total_records) * 100
            print(f"{comparison}: {count:,} записей ({percentage:.1f}%)")
        
        # 6. Проверка на нестандартные значения
        print("\n📊 ПРОВЕРКА НА НЕСТАНДАРТНЫЕ ЗНАЧЕНИЯ:")
        print("-" * 40)
        
        # Ищем значения, которые не входят в стандартный маппинг
        standard_values = ['Годен', 'Н/П', 'Не годен', 'Пересвет', 'Не соответствует', 'Заказ отправлен']
        
        cursor.execute("""
            SELECT DISTINCT "Статус_ВИК"
            FROM logs_lnk 
            WHERE "Статус_ВИК" IS NOT NULL AND "Статус_ВИК" != '' AND "Статус_ВИК" != 'NULL'
        """)
        all_statuses = [row[0] for row in cursor.fetchall()]
        
        non_standard = [status for status in all_statuses if status not in standard_values]
        
        if non_standard:
            print("Найдены нестандартные значения в Статус_ВИК:")
            for status in non_standard:
                cursor.execute('SELECT COUNT(*) FROM logs_lnk WHERE "Статус_ВИК" = ?', (status,))
                count = cursor.fetchone()[0]
                print(f"  '{status}': {count:,} записей")
        else:
            print("✅ Все значения в Статус_ВИК соответствуют стандартному маппингу")
        
        print("\n" + "=" * 80)
        print("АНАЛИЗ ЗАВЕРШЕН")
        print("=" * 80)
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при анализе: {str(e)}")
        return False
    
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    """Основная функция"""
    analyze_vik_status()

if __name__ == "__main__":
    main()

# Для запуска через GUI
def run_script():
    """Функция для запуска скрипта через GUI"""
    main()
