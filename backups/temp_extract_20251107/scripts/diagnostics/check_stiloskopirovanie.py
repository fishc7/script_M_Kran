#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Диагностический скрипт для проверки расчета стилоскопирования
"""

import sqlite3
import sys
import os

# Добавляем путь к утилитам
current_dir = os.path.dirname(os.path.abspath(__file__))
utilities_dir = os.path.join(os.path.dirname(current_dir), 'utilities')
sys.path.insert(0, utilities_dir)

from db_utils import get_database_connection

def check_stiloskopirovanie(drawing_number, weld_mark):
    """Проверяет данные стилоскопирования для конкретного чертежа и клейма"""
    
    conn = get_database_connection()
    c = conn.cursor()
    
    print(f"=" * 80)
    print(f"Проверка стилоскопирования для:")
    print(f"  Номер_чертежа: {drawing_number}")
    print(f"  клейма_сварщиков: {weld_mark}")
    print(f"=" * 80)
    
    # 1. Проверка в wl_china
    print("\n1. Данные в wl_china:")
    print("-" * 80)
    
    c.execute("""
        SELECT 
            Номер_чертежа,
            Клеймо_сварщика_корневой_слой,
            Клеймо_сварщика_заполнение_облицовка,
            Результаты_Заключения_Стилоскопирование,
            LENGTH(Результаты_Заключения_Стилоскопирование) as len,
            TRIM(COALESCE(Результаты_Заключения_Стилоскопирование, '')) as trimmed
        FROM wl_china 
        WHERE Номер_чертежа = ?
          AND (Клеймо_сварщика_корневой_слой = ? OR Клеймо_сварщика_заполнение_облицовка = ?)
          AND Дата_сварки IS NOT NULL 
          AND TRIM(Дата_сварки) <> ''
        ORDER BY Номер_сварного_шва
        LIMIT 10
    """, (drawing_number, weld_mark, weld_mark))
    
    wl_china_rows = c.fetchall()
    print(f"  Найдено записей в wl_china: {len(wl_china_rows)}")
    
    if wl_china_rows:
        print("\n  Примеры записей:")
        for row in wl_china_rows:
            print(f"    Клеймо корневой: {row[1]}, заполнение: {row[2]}")
            print(f"    Результаты_Заключения_Стилоскопирование: [{row[3]}] (длина: {row[4]}, trimmed: [{row[5]}])")
    
    # Статистика по Результаты_Заключения_Стилоскопирование
    c.execute("""
        SELECT 
            TRIM(COALESCE(Результаты_Заключения_Стилоскопирование, '')) as result,
            COUNT(*) as count
        FROM wl_china 
        WHERE Номер_чертежа = ?
          AND (Клеймо_сварщика_корневой_слой = ? OR Клеймо_сварщика_заполнение_облицовка = ?)
          AND Дата_сварки IS NOT NULL 
          AND TRIM(Дата_сварки) <> ''
        GROUP BY TRIM(COALESCE(Результаты_Заключения_Стилоскопирование, ''))
    """, (drawing_number, weld_mark, weld_mark))
    
    stats = c.fetchall()
    print("\n  Статистика по Результаты_Заключения_Стилоскопирование:")
    for stat in stats:
        print(f"    '{stat[0]}': {stat[1]} записей")
    
    # Подсчет с условием material_check
    print("\n  Подсчет с учетом material_check (исключение по материалу):")
    c.execute("""
        SELECT 
            SUM(CASE WHEN TRIM(COALESCE(Результаты_Заключения_Стилоскопирование, '')) = 'Заказ отправлен' 
                     AND (COALESCE(Базовый_материал_1, '') NOT LIKE '%09Г2C%' 
                          AND COALESCE(Базовый_материал_1, '') NOT LIKE '%09Г2С%' 
                          AND TRIM(COALESCE(Результаты_Заключения_РК, '')) != 'Не годен') 
                THEN 1 ELSE 0 END) as zayavlen,
            SUM(CASE WHEN TRIM(COALESCE(Результаты_Заключения_Стилоскопирование, '')) = 'годен' 
                     AND (COALESCE(Базовый_материал_1, '') NOT LIKE '%09Г2C%' 
                          AND COALESCE(Базовый_материал_1, '') NOT LIKE '%09Г2С%' 
                          AND TRIM(COALESCE(Результаты_Заключения_РК, '')) != 'Не годен') 
                THEN 1 ELSE 0 END) as goden
        FROM wl_china 
        WHERE Номер_чертежа = ?
          AND (Клеймо_сварщика_корневой_слой = ? OR Клеймо_сварщика_заполнение_облицовка = ?)
          AND Дата_сварки IS NOT NULL 
          AND TRIM(Дата_сварки) <> ''
    """, (drawing_number, weld_mark, weld_mark))
    
    calc = c.fetchone()
    print(f"    заявлен_стилоскопирование: {calc[0]}")
    print(f"    годен_стилоскопирование: {calc[1]}")
    
    # Детальная проверка каждой записи с "годен"
    print("\n  Детальная проверка записей с 'годен':")
    c.execute("""
        SELECT 
            Номер_сварного_шва,
            Клеймо_сварщика_корневой_слой,
            Клеймо_сварщика_заполнение_облицовка,
            Базовый_материал_1,
            Результаты_Заключения_РК,
            Результаты_Заключения_Стилоскопирование,
            CASE 
                WHEN (COALESCE(Базовый_материал_1, '') NOT LIKE '%09Г2C%' 
                      AND COALESCE(Базовый_материал_1, '') NOT LIKE '%09Г2С%' 
                      AND TRIM(COALESCE(Результаты_Заключения_РК, '')) != 'Не годен') 
                THEN 'ПРОШЕЛ' 
                ELSE 'ИСКЛЮЧЕН' 
            END as material_check_result
        FROM wl_china 
        WHERE Номер_чертежа = ?
          AND (Клеймо_сварщика_корневой_слой = ? OR Клеймо_сварщика_заполнение_облицовка = ?)
          AND Дата_сварки IS NOT NULL 
          AND TRIM(Дата_сварки) <> ''
          AND TRIM(COALESCE(Результаты_Заключения_Стилоскопирование, '')) = 'годен'
        ORDER BY Номер_сварного_шва
    """, (drawing_number, weld_mark, weld_mark))
    
    detailed = c.fetchall()
    for row in detailed:
        print(f"    Шов {row[0]}: клейма {row[1]}/{row[2]}, материал={row[3]}, РК={row[4]}, material_check={row[6]}")
    
    # 2. Проверка в сварено_сварщиком
    print("\n2. Данные в сварено_сварщиком:")
    print("-" * 80)
    c.execute("""
        SELECT 
            N_Линии,
            _Линия,
            Номер_чертежа,
            клейма_сварщиков,
            "Стило(PMI)",
            заявлен_стилоскопирование,
            годен_стилоскопирование,
            не_годен_стилоскопирование,
            не_подан_стилоскопирование,
            Всего_сваренно_сварщиком
        FROM сварено_сварщиком 
        WHERE Номер_чертежа = ?
          AND клейма_сварщиков LIKE ?
        ORDER BY N_Линии
    """, (drawing_number, f'%{weld_mark}%'))
    
    svarenno_rows = c.fetchall()
    if not svarenno_rows:
        print(f"  Записей не найдено в сварено_сварщиком")
    else:
        print(f"  Найдено записей в сварено_сварщиком: {len(svarenno_rows)}")
        for row in svarenno_rows:
            print(f"\n  Линия: {row[0]} ({row[1]})")
            print(f"    Клейма: {row[3]}")
            print(f"    Всего сварено: {row[9]}")
            print(f"    Стило(PMI): {row[4]}")
            print(f"    заявлен_стилоскопирование: {row[5]}")
            print(f"    годен_стилоскопирование: {row[6]}")
            print(f"    не_годен_стилоскопирование: {row[7]}")
            print(f"    не_подан_стилоскопирование: {row[8]}")
            print(f"    Проверка: Стило(PMI) - заявлен - годен = {row[4]} - {row[5]} - {row[6]} = {row[4] - row[5] - row[6]}")
    
    conn.close()

if __name__ == "__main__":
    drawing_number = "GCC-NAG-DDD-12460-12-1500-TK-ISO-00042"
    weld_mark = "2Z08"
    check_stiloskopirovanie(drawing_number, weld_mark)

