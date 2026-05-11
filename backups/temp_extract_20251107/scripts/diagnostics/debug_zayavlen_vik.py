#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Детальная проверка почему заявлен_вик не считается
"""

import sqlite3
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, project_root)

from scripts.utilities.db_utils import get_database_connection

conn = get_database_connection()
c = conn.cursor()

drawing = "GCC-NAG-DDD-12460-12-1500-TK-ISO-00001"
weld_mark = "2Z08"

print("=" * 80)
print(f"Проверка для чертежа: {drawing}, клеймо: {weld_mark}")
print("=" * 80)

# 1. Данные из wl_china
print("\n1. Данные из wl_china:")
c.execute("""
    SELECT 
        Номер_чертежа,
        N_Линии,
        Клеймо_сварщика_корневой_слой,
        Клеймо_сварщика_заполнение_облицовка,
        Метод_сварки_корневой_слой,
        Метод_сварки_заполнение_облицовка,
        Тип_соединения_российский_стандарт,
        Результаты_АКТ_ВИК,
        Дата_сварки
    FROM wl_china 
    WHERE Номер_чертежа = ?
      AND (
          Клеймо_сварщика_корневой_слой = ? 
          OR Клеймо_сварщика_заполнение_облицовка = ?
      )
""", (drawing, weld_mark, weld_mark))

rows = c.fetchall()
for row in rows:
    print(f"\n  Шов:")
    print(f"    Чертеж: {row[0]}")
    print(f"    Линия: {row[1]}")
    print(f"    Клеймо корневой: {row[2]}")
    print(f"    Клеймо заполнение: {row[3]}")
    print(f"    Метод корневой: {row[4]}")
    print(f"    Метод заполнение: {row[5]}")
    print(f"    Тип соединения: {row[6]}")
    print(f"    Результаты_АКТ_ВИК: '{row[7]}'")
    print(f"    Дата_сварки: {row[8]}")

# 2. Проверка как формируются клейма_сварщиков, методы_сварки, типы_сварных_швов
print("\n2. Как формируются агрегированные поля:")
c.execute("""
    SELECT 
        N_Линии,
        (
            SELECT group_concat(part, '/')
            FROM (
                SELECT DISTINCT part
                FROM (
                    SELECT trim(Клеймо_сварщика_корневой_слой) as part 
                    WHERE Клеймо_сварщика_корневой_слой != ''
                    UNION ALL
                    SELECT trim(Клеймо_сварщика_заполнение_облицовка) as part 
                    WHERE Клеймо_сварщика_заполнение_облицовка != ''
                )
                WHERE part IS NOT NULL AND part != ''
            )
        ) as клейма,
        (
            SELECT group_concat(part, '/')
            FROM (
                SELECT DISTINCT part
                FROM (
                    SELECT trim(Метод_сварки_корневой_слой) as part 
                    WHERE Метод_сварки_корневой_слой IS NOT NULL AND Метод_сварки_корневой_слой != ''
                    UNION ALL
                    SELECT trim(Метод_сварки_заполнение_облицовка) as part 
                    WHERE Метод_сварки_заполнение_облицовка IS NOT NULL AND Метод_сварки_заполнение_облицовка != ''
                )
                WHERE part IS NOT NULL AND part != ''
            )
        ) as методы,
        (
            SELECT group_concat(part, '/')
            FROM (
                SELECT DISTINCT part
                FROM (
                    SELECT trim(Тип_соединения_российский_стандарт) as part 
                    WHERE Тип_соединения_российский_стандарт IS NOT NULL AND Тип_соединения_российский_стандарт != ''
                )
                WHERE part IS NOT NULL AND part != ''
            )
        ) as типы,
        SUM(CASE WHEN TRIM(COALESCE(Результаты_АКТ_ВИК, '')) = 'Заказ отправлен' THEN 1 ELSE 0 END) as заявлен_вик
    FROM wl_china
    WHERE Номер_чертежа = ?
      AND (
          Клеймо_сварщика_корневой_слой = ? 
          OR Клеймо_сварщика_заполнение_облицовка = ?
      )
      AND Дата_сварки IS NOT NULL 
      AND TRIM(Дата_сварки) <> ''
      AND (
          Клеймо_сварщика_корневой_слой IS NOT NULL AND Клеймо_сварщика_корневой_слой <> ''
          OR Клеймо_сварщика_заполнение_облицовка IS NOT NULL AND Клеймо_сварщика_заполнение_облицовка <> ''
      )
    GROUP BY N_Линии
""", (drawing, weld_mark, weld_mark))

grouped = c.fetchall()
for row in grouped:
    print(f"\n  Линия: {row[0]}")
    print(f"    Клейма: {row[1]}")
    print(f"    Методы: {row[2]}")
    print(f"    Типы: {row[3]}")
    print(f"    заявлен_вик: {row[4]}")

# 3. Что в таблице сварено_сварщиком
print("\n3. Данные в сварено_сварщиком:")
c.execute("""
    SELECT 
        N_Линии,
        _Линия,
        Номер_чертежа,
        клейма_сварщиков,
        методы_сварки,
        типы_сварных_швов,
        заявлен_вик,
        годен_вик,
        Всего_сваренно_сварщиком
    FROM сварено_сварщиком 
    WHERE Номер_чертежа = ?
      AND клейма_сварщиков LIKE ?
""", (drawing, f'%{weld_mark}%'))

svarenno = c.fetchall()
for row in svarenno:
    print(f"\n  Линия: {row[0]} ({row[1]})")
    print(f"    Клейма: {row[3]}")
    print(f"    Методы: {row[4]}")
    print(f"    Типы: {row[5]}")
    print(f"    заявлен_вик: {row[6]}")
    print(f"    годен_вик: {row[7]}")
    print(f"    Всего: {row[8]}")

conn.close()

