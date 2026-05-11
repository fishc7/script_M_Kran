#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Диагностический скрипт для проверки значений Результаты_АКТ_ВИК в таблице wl_china
"""

import sqlite3
import os
import sys

# Настройка путей
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, project_root)

from scripts.utilities.db_utils import get_database_connection

def check_vik_values():
    """Проверка уникальных значений в Результаты_АКТ_ВИК"""
    conn = get_database_connection()
    if not conn:
        print("❌ Не удалось подключиться к базе данных")
        return
    
    c = conn.cursor()
    
    print("=" * 60)
    print("ПРОВЕРКА: Уникальные значения Результаты_АКТ_ВИК в wl_china")
    print("=" * 60)
    
    # Проверка существования таблицы
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='wl_china'")
    if not c.fetchone():
        print("❌ Таблица wl_china не существует")
        conn.close()
        return
    
    # Проверка существования столбца
    c.execute("PRAGMA table_info(wl_china)")
    columns = [col[1] for col in c.fetchall()]
    if 'Результаты_АКТ_ВИК' not in columns:
        print("❌ Столбец Результаты_АКТ_ВИК не найден в таблице wl_china")
        conn.close()
        return
    
    # 1. Все уникальные значения
    print("\n1. Все уникальные значения (с учетом регистра и пробелов):")
    print("-" * 60)
    c.execute("""
        SELECT DISTINCT Результаты_АКТ_ВИК, 
               COUNT(*) as count,
               LENGTH(Результаты_АКТ_ВИК) as length
        FROM wl_china 
        WHERE Результаты_АКТ_ВИК IS NOT NULL 
          AND Результаты_АКТ_ВИК != ''
          AND Результаты_АКТ_ВИК != 'None'
        GROUP BY Результаты_АКТ_ВИК
        ORDER BY count DESC
    """)
    
    values = c.fetchall()
    if not values:
        print("  Нет непустых значений")
    else:
        for val, count, length in values:
            print(f"  '{val}' (длина: {length}) - {count} записей")
    
    # 2. Проверка вариантов "Заказ отправлен" с учетом пробелов и регистра
    print("\n2. Проверка вариантов 'Заказ отправлен' (с учетом пробелов и регистра):")
    print("-" * 60)
    variants = [
        'Заказ отправлен',
        'заказ отправлен',
        'ЗАКАЗ ОТПРАВЛЕН',
        'Заказ Отправлен',
        'Заказ  отправлен',  # двойной пробел
        ' Заказ отправлен',  # пробел в начале
        'Заказ отправлен ',  # пробел в конце
        ' Заказ отправлен ', # пробелы с обеих сторон
    ]
    
    for variant in variants:
        c.execute("""
            SELECT COUNT(*) 
            FROM wl_china 
            WHERE Результаты_АКТ_ВИК = ?
        """, (variant,))
        count = c.fetchone()[0]
        if count > 0:
            print(f"  Найдено записей с точным совпадением '{variant}': {count}")
    
    # 3. Проверка вариантов "Заявлен"
    print("\n3. Проверка вариантов 'Заявлен':")
    print("-" * 60)
    variants = [
        'Заявлен',
        'заявлен',
        'ЗАЯВЛЕН',
        'Заявлен ',
        ' Заявлен',
        ' Заявлен ',
    ]
    
    for variant in variants:
        c.execute("""
            SELECT COUNT(*) 
            FROM wl_china 
            WHERE Результаты_АКТ_ВИК = ?
        """, (variant,))
        count = c.fetchone()[0]
        if count > 0:
            print(f"  Найдено записей с точным совпадением '{variant}': {count}")
    
    # 4. Проверка с TRIM (игнорируя пробелы)
    print("\n4. Уникальные значения после TRIM (игнорируя пробелы):")
    print("-" * 60)
    c.execute("""
        SELECT DISTINCT TRIM(Результаты_АКТ_ВИК) as trimmed_value, 
               COUNT(*) as count
        FROM wl_china 
        WHERE Результаты_АКТ_ВИК IS NOT NULL 
          AND TRIM(Результаты_АКТ_ВИК) != ''
          AND TRIM(Результаты_АКТ_ВИК) != 'None'
        GROUP BY TRIM(Результаты_АКТ_ВИК)
        ORDER BY count DESC
    """)
    
    trimmed_values = c.fetchall()
    for val, count in trimmed_values:
        print(f"  '{val}' - {count} записей")
    
    # 5. Проверка текущего условия в коде
    print("\n5. Проверка текущего условия в коде (wc.Результаты_АКТ_ВИК = 'Заказ отправлен'):")
    print("-" * 60)
    c.execute("""
        SELECT COUNT(*) 
        FROM wl_china 
        WHERE Результаты_АКТ_ВИК = 'Заказ отправлен'
    """)
    exact_match = c.fetchone()[0]
    print(f"  Точное совпадение: {exact_match} записей")
    
    # 6. Проверка с TRIM
    c.execute("""
        SELECT COUNT(*) 
        FROM wl_china 
        WHERE TRIM(COALESCE(Результаты_АКТ_ВИК, '')) = 'Заказ отправлен'
    """)
    trim_match = c.fetchone()[0]
    print(f"  С TRIM: {trim_match} записей")
    
    # 7. Проверка вариантов "Заявлен" (которые могут означать "Заказ отправлен")
    c.execute("""
        SELECT COUNT(*) 
        FROM wl_china 
        WHERE TRIM(COALESCE(Результаты_АКТ_ВИК, '')) = 'Заявлен'
    """)
    zayavlen_match = c.fetchone()[0]
    print(f"  Со значением 'Заявлен' (после TRIM): {zayavlen_match} записей")
    
    # 8. Примеры записей с разными значениями
    print("\n6. Примеры записей (первые 10):")
    print("-" * 60)
    c.execute("""
        SELECT 
            Номер_чертежа,
            Номер_сварного_шва,
            Результаты_АКТ_ВИК,
            LENGTH(Результаты_АКТ_ВИК) as length,
            TRIM(Результаты_АКТ_ВИК) as trimmed
        FROM wl_china 
        WHERE Результаты_АКТ_ВИК IS NOT NULL 
          AND Результаты_АКТ_ВИК != ''
          AND Результаты_АКТ_ВИК != 'None'
        LIMIT 10
    """)
    
    examples = c.fetchall()
    for i, (drawing, weld, result, length, trimmed) in enumerate(examples, 1):
        print(f"  {i}. Чертеж: {drawing}, Шов: {weld}")
        print(f"     Значение: '{result}' (длина: {length}, после TRIM: '{trimmed}')")
    
    conn.close()
    print("\n" + "=" * 60)
    print("Проверка завершена")
    print("=" * 60)

if __name__ == "__main__":
    check_vik_values()

