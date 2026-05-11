#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Проверка данных для конкретного чертежа и клейма сварщика
"""

import sqlite3
import os
import sys

# Настройка путей
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, project_root)

from scripts.utilities.db_utils import get_database_connection

def check_drawing_and_weld(drawing_number, weld_mark):
    """Проверка данных для конкретного чертежа и клейма"""
    conn = get_database_connection()
    if not conn:
        print("Не удалось подключиться к базе данных")
        return
    
    c = conn.cursor()
    
    print("=" * 80)
    print(f"ПРОВЕРКА: Чертеж {drawing_number}, Клеймо {weld_mark}")
    print("=" * 80)
    
    # 1. Проверка в wl_china
    print("\n1. Данные в wl_china:")
    print("-" * 80)
    c.execute("""
        SELECT 
            Номер_чертежа,
            N_Линии,
            Клеймо_сварщика_корневой_слой,
            Клеймо_сварщика_заполнение_облицовка,
            Результаты_АКТ_ВИК,
            LENGTH(Результаты_АКТ_ВИК) as vik_length,
            TRIM(COALESCE(Результаты_АКТ_ВИК, '')) as vik_trimmed,
            Номер_сварного_шва,
            Дата_сварки
        FROM wl_china 
        WHERE Номер_чертежа = ?
          AND (
              Клеймо_сварщика_корневой_слой = ? 
              OR Клеймо_сварщика_заполнение_облицовка = ?
          )
        ORDER BY Номер_сварного_шва
    """, (drawing_number, weld_mark, weld_mark))
    
    wl_china_rows = c.fetchall()
    if not wl_china_rows:
        print(f"  Записей не найдено в wl_china для чертежа {drawing_number} и клейма {weld_mark}")
    else:
        print(f"  Найдено записей в wl_china: {len(wl_china_rows)}")
        
        # Статистика по Результаты_АКТ_ВИК
        vik_statuses = {}
        for row in wl_china_rows:
            vik_result = row[4]  # Результаты_АКТ_ВИК
            vik_trimmed = row[6]  # vik_trimmed
            
            status = vik_result if vik_result else 'NULL/пусто'
            if status not in vik_statuses:
                vik_statuses[status] = 0
            vik_statuses[status] += 1
            
            # Показываем первые 5 записей
            if len([r for r in wl_china_rows if r == row]) <= 5:
                print(f"\n  Шов: {row[7]}, Линия: {row[1]}")
                print(f"    Клеймо корневой: {row[2]}, Клеймо заполнение: {row[3]}")
                print(f"    Результаты_АКТ_ВИК: '{row[4]}' (длина: {row[5]}, после TRIM: '{row[6]}')")
                print(f"    Дата сварки: {row[8]}")
        
        print(f"\n  Статистика по Результаты_АКТ_ВИК:")
        for status, count in vik_statuses.items():
            print(f"    '{status}': {count} швов")
        
        # Подсчет с текущей логикой
        c.execute("""
            SELECT COUNT(*) 
            FROM wl_china 
            WHERE Номер_чертежа = ?
              AND (
                  Клеймо_сварщика_корневой_слой = ? 
                  OR Клеймо_сварщика_заполнение_облицовка = ?
              )
              AND TRIM(COALESCE(Результаты_АКТ_ВИК, '')) = 'Заказ отправлен'
        """, (drawing_number, weld_mark, weld_mark))
        zayavlen_count = c.fetchone()[0]
        print(f"\n  С условием TRIM(COALESCE(Результаты_АКТ_ВИК, '')) = 'Заказ отправлен': {zayavlen_count} швов")
    
    # 2. Проверка в сварено_сварщиком
    print("\n2. Данные в сварено_сварщиком:")
    print("-" * 80)
    c.execute("""
        SELECT 
            N_Линии,
            _Линия,
            Номер_чертежа,
            клейма_сварщиков,
            заявлен_вик,
            годен_вик,
            не_годен_вик,
            не_подан_вик,
            ВИК,
            Всего_сваренно_сварщиком
        FROM сварено_сварщиком 
        WHERE Номер_чертежа = ?
          AND клейма_сварщиков LIKE ?
        ORDER BY N_Линии
    """, (drawing_number, f'%{weld_mark}%'))
    
    svarenno_rows = c.fetchall()
    if not svarenno_rows:
        print(f"  Записей не найдено в сварено_сварщиком для чертежа {drawing_number} и клейма {weld_mark}")
    else:
        print(f"  Найдено записей в сварено_сварщиком: {len(svarenno_rows)}")
        for row in svarenno_rows:
            print(f"\n  Линия: {row[0]} ({row[1]})")
            print(f"    Клейма: {row[3]}")
            print(f"    Всего сварено: {row[9]}")
            print(f"    заявлен_вик: {row[4]}")
            print(f"    годен_вик: {row[5]}")
            print(f"    не_годен_вик: {row[6]}")
            print(f"    не_подан_вик: {row[7]}")
            print(f"    ВИК: {row[8]}")
    
    # 3. Проверка группировки - какие клейма объединяются
    print("\n3. Группировка по клеймам в wl_china:")
    print("-" * 80)
    c.execute("""
        SELECT 
            N_Линии,
            COUNT(*) as total_welds,
            GROUP_CONCAT(DISTINCT Клеймо_сварщика_корневой_слой) as root_marks,
            GROUP_CONCAT(DISTINCT Клеймо_сварщика_заполнение_облицовка) as fill_marks,
            SUM(CASE WHEN TRIM(COALESCE(Результаты_АКТ_ВИК, '')) = 'Заказ отправлен' THEN 1 ELSE 0 END) as zayavlen,
            SUM(CASE WHEN TRIM(COALESCE(Результаты_АКТ_ВИК, '')) = 'годен' THEN 1 ELSE 0 END) as goden,
            SUM(CASE WHEN TRIM(COALESCE(Результаты_АКТ_ВИК, '')) = 'Не годен' THEN 1 ELSE 0 END) as ne_goden
        FROM wl_china 
        WHERE Номер_чертежа = ?
          AND (
              Клеймо_сварщика_корневой_слой = ? 
              OR Клеймо_сварщика_заполнение_облицовка = ?
          )
          AND Дата_сварки IS NOT NULL 
          AND TRIM(Дата_сварки) <> ''
        GROUP BY N_Линии
    """, (drawing_number, weld_mark, weld_mark))
    
    grouped = c.fetchall()
    for row in grouped:
        print(f"\n  Линия: {row[0]}")
        print(f"    Всего швов: {row[1]}")
        print(f"    Клейма корневые: {row[2]}")
        print(f"    Клейма заполнение: {row[3]}")
        print(f"    Заявлен ВИК: {row[4]}")
        print(f"    Годен: {row[5]}")
        print(f"    Не годен: {row[6]}")
    
    conn.close()
    print("\n" + "=" * 80)

if __name__ == "__main__":
    drawing = "GCC-NAG-DDD-12460-12-1500-TK-ISO-00001"
    weld_mark = "2Z08"
    check_drawing_and_weld(drawing, weld_mark)

