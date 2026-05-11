#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для анализа результатов нормализации столбцов Статус_РК и РК
"""

import sqlite3
import logging
from datetime import datetime
import os
import sys

def get_database_connection():
    """
    Создает подключение к базе данных с автоматическим определением пути
    """
    import os
    
    # Получаем текущую директорию
    current_dir = os.getcwd()
    
    # Пробуем разные варианты путей для новой структуры проекта
    possible_paths = [
        # Если запускаем из корневой папки проекта (новая структура)
        os.path.join(current_dir, 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Если запускаем из папки scripts
        os.path.join(current_dir, '..', 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Если запускаем из папки scripts/data_loaders
        os.path.join(current_dir, '..', '..', 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Если запускаем из папки web/app
        os.path.join(current_dir, '..', '..', 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Если запускаем из папки desktop/qt_app
        os.path.join(current_dir, '..', '..', 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Старые пути для совместимости
        os.path.join(current_dir, 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        os.path.join(current_dir, '..', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
    ]
    
    for path in possible_paths:
        abs_path = os.path.abspath(path)
        if os.path.exists(abs_path):
            return sqlite3.connect(abs_path)
    
    # Если не нашли, возвращаем None
    return None

def analyze_rk_status():
    """
    Анализ результатов нормализации столбцов Статус_РК и РК
    """
    print("=" * 80)
    print("АНАЛИЗ РЕЗУЛЬТАТОВ НОРМАЛИЗАЦИИ СТОЛБЦОВ СТАТУС_РК И РК")
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
        
        # 2. Статистика по Статус_РК
        print("\n📊 СТАТИСТИКА ПО СТАТУС_РК:")
        print("-" * 40)
        
        cursor.execute("""
            SELECT "Статус_РК", COUNT(*) as count
            FROM logs_lnk 
            WHERE "Статус_РК" IS NOT NULL AND "Статус_РК" != '' AND "Статус_РК" != 'NULL'
            GROUP BY "Статус_РК"
            ORDER BY count DESC
        """)
        status_stats = cursor.fetchall()
        
        for status, count in status_stats:
            percentage = (count / total_records) * 100
            print(f"'{status}': {count:,} записей ({percentage:.1f}%)")
        
        # 3. Записи с пустым Статус_РК
        print("\n📊 ЗАПИСИ С ПУСТЫМ СТАТУС_РК:")
        print("-" * 40)
        
        cursor.execute("""
            SELECT COUNT(*) FROM logs_lnk 
            WHERE "Статус_РК" IS NULL OR "Статус_РК" = '' OR "Статус_РК" = 'NULL'
        """)
        empty_status_count = cursor.fetchone()[0]
        print(f"Записей с пустым Статус_РК: {empty_status_count:,}")
        
        if empty_status_count > 0:
            # Показываем примеры записей с пустым Статус_РК
            cursor.execute("""
                SELECT "Чертеж", "Номер_стыка", "РК", "Статус_РК"
                FROM logs_lnk 
                WHERE "Статус_РК" IS NULL OR "Статус_РК" = '' OR "Статус_РК" = 'NULL'
                LIMIT 5
            """)
            examples = cursor.fetchall()
            print("\nПримеры записей с пустым Статус_РК:")
            for example in examples:
                print(f"  Чертеж: {example[0]}, Стык: {example[1]}, РК: {example[2]}, Статус_РК: {example[3]}")
        
        # 4. Анализ столбца РК
        print("\n📊 АНАЛИЗ СТОЛБЦА РК:")
        print("-" * 40)
        
        cursor.execute("""
            SELECT "РК", COUNT(*) as count
            FROM logs_lnk 
            WHERE "РК" IS NOT NULL AND "РК" != '' AND "РК" != 'NULL'
            GROUP BY "РК"
            ORDER BY count DESC
            LIMIT 10
        """)
        rk_stats = cursor.fetchall()
        
        print("Топ-10 значений в столбце РК:")
        for rk_value, count in rk_stats:
            print(f"'{rk_value}': {count:,} записей")
        
        # 5. Сравнение Статус_РК и РК
        print("\n📊 СРАВНЕНИЕ СТАТУС_РК И РК:")
        print("-" * 40)
        
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN "Статус_РК" = "РК" THEN 'Одинаковые'
                    WHEN ("Статус_РК" IS NULL OR "Статус_РК" = '' OR "Статус_РК" = 'NULL') 
                         AND ("РК" IS NOT NULL AND "РК" != '' AND "РК" != 'NULL') THEN 'Статус_РК пустой, РК заполнен'
                    WHEN ("РК" IS NULL OR "РК" = '' OR "РК" = 'NULL') 
                         AND ("Статус_РК" IS NOT NULL AND "Статус_РК" != '' AND "Статус_РК" != 'NULL') THEN 'РК пустой, Статус_РК заполнен'
                    WHEN ("Статус_РК" IS NULL OR "Статус_РК" = '' OR "Статус_РК" = 'NULL') 
                         AND ("РК" IS NULL OR "РК" = '' OR "РК" = 'NULL') THEN 'Оба пустые'
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
            SELECT DISTINCT "Статус_РК"
            FROM logs_lnk 
            WHERE "Статус_РК" IS NOT NULL AND "Статус_РК" != '' AND "Статус_РК" != 'NULL'
        """)
        all_statuses = [row[0] for row in cursor.fetchall()]
        
        non_standard = [status for status in all_statuses if status not in standard_values]
        
        if non_standard:
            print("Найдены нестандартные значения в Статус_РК:")
            for status in non_standard:
                cursor.execute('SELECT COUNT(*) FROM logs_lnk WHERE "Статус_РК" = ?', (status,))
                count = cursor.fetchone()[0]
                print(f"  '{status}': {count:,} записей")
        else:
            print("✅ Все значения в Статус_РК соответствуют стандартному маппингу")
        
        # 7. Сравнение с ВИК статусами
        print("\n📊 СРАВНЕНИЕ С ВИК СТАТУСАМИ:")
        print("-" * 40)
        
        cursor.execute("""
            SELECT 
                "Статус_ВИК", "Статус_РК", COUNT(*) as count
            FROM logs_lnk 
            WHERE "Статус_ВИК" IS NOT NULL AND "Статус_ВИК" != '' AND "Статус_ВИК" != 'NULL'
              AND "Статус_РК" IS NOT NULL AND "Статус_РК" != '' AND "Статус_РК" != 'NULL'
            GROUP BY "Статус_ВИК", "Статус_РК"
            ORDER BY count DESC
            LIMIT 10
        """)
        comparison_vik_rk = cursor.fetchall()
        
        print("Топ-10 комбинаций Статус_ВИК и Статус_РК:")
        for vik_status, rk_status, count in comparison_vik_rk:
            print(f"  ВИК: '{vik_status}' + РК: '{rk_status}': {count:,} записей")
        
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
    analyze_rk_status()

if __name__ == "__main__":
    main()

# Для запуска через GUI
def run_script():
    """Функция для запуска скрипта через GUI"""
    main()
