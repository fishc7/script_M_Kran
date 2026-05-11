#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Детальная проверка проблемных записей
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

print("=" * 80)
print("ДЕТАЛЬНАЯ ПРОВЕРКА ПРОБЛЕМНЫХ ЗАПИСЕЙ")
print("=" * 80)

# Проверяем запись 084-MS-0700, чертеж GCC-NAG-DDD-12470-14-1400-TK-ISO-00040
print("\n1. Запись 084-MS-0700, чертеж GCC-NAG-DDD-12470-14-1400-TK-ISO-00040:")
print("-" * 80)

# Что в таблице
c.execute("""
    SELECT 
        N_Линии,
        _Линия,
        Номер_чертежа,
        клейма_сварщиков,
        методы_сварки,
        типы_сварных_швов,
        заявлен_вик,
        Всего_сваренно_сварщиком
    FROM сварено_сварщиком 
    WHERE N_Линии = '084-MS-0700'
      AND Номер_чертежа = 'GCC-NAG-DDD-12470-14-1400-TK-ISO-00040'
""")

svarenno_records = c.fetchall()
print(f"  Записей в сварено_сварщиком: {len(svarenno_records)}")
for record in svarenno_records:
    print(f"\n    Клейма: {record[3]}")
    print(f"    Методы: {record[4]}")
    print(f"    Типы: {record[5]}")
    print(f"    заявлен_вик: {record[6]}")
    print(f"    Всего: {record[7]}")

# Что в wl_china
c.execute("""
    SELECT 
        Клеймо_сварщика_корневой_слой,
        Клеймо_сварщика_заполнение_облицовка,
        Метод_сварки_корневой_слой,
        Метод_сварки_заполнение_облицовка,
        Тип_соединения_российский_стандарт,
        Результаты_АКТ_ВИК,
        COUNT(*) as count
    FROM wl_china
    WHERE N_Линии = '084-MS-0700'
      AND Номер_чертежа = 'GCC-NAG-DDD-12470-14-1400-TK-ISO-00040'
      AND Дата_сварки IS NOT NULL 
      AND TRIM(Дата_сварки) <> ''
    GROUP BY 
        Клеймо_сварщика_корневой_слой,
        Клеймо_сварщика_заполнение_облицовка,
        Метод_сварки_корневой_слой,
        Метод_сварки_заполнение_облицовка,
        Тип_соединения_российский_стандарт,
        Результаты_АКТ_ВИК
    ORDER BY count DESC
""")

wl_china_records = c.fetchall()
print(f"\n  Групп в wl_china: {len(wl_china_records)}")
vik_zayavlen_count = 0
for record in wl_china_records:
    k1, k2, m1, m2, t, vik, count = record
    print(f"\n    Клейма: {k1}/{k2}")
    print(f"    Методы: {m1}/{m2}")
    print(f"    Тип: {t}")
    print(f"    ВИК: {vik}")
    print(f"    Количество: {count}")
    if vik == 'Заказ отправлен':
        vik_zayavlen_count += count

print(f"\n  Всего со статусом 'Заказ отправлен' в wl_china: {vik_zayavlen_count}")

# Проверяем запись где заявлен_вик = 0, но должна быть
print("\n2. Запись 005-PA-0602, чертеж GCC-NAG-DDD-12470-13-1400-TK-ISO-00141:")
print("-" * 80)

c.execute("""
    SELECT 
        N_Линии,
        _Линия,
        Номер_чертежа,
        клейма_сварщиков,
        методы_сварки,
        типы_сварных_швов,
        заявлен_вик
    FROM сварено_сварщиком 
    WHERE N_Линии = '005-PA-0602'
      AND Номер_чертежа = 'GCC-NAG-DDD-12470-13-1400-TK-ISO-00141'
""")

svarenno_records = c.fetchall()
print(f"  Записей в сварено_сварщиком: {len(svarenno_records)}")
for record in svarenno_records:
    print(f"\n    Клейма: {record[3]}")
    print(f"    Методы: {record[4]}")
    print(f"    Типы: {record[5]}")
    print(f"    заявлен_вик: {record[6]}")

# Что в wl_china для этой записи
c.execute("""
    SELECT 
        Клеймо_сварщика_корневой_слой,
        Клеймо_сварщика_заполнение_облицовка,
        Метод_сварки_корневой_слой,
        Метод_сварки_заполнение_облицовка,
        Тип_соединения_российский_стандарт,
        Результаты_АКТ_ВИК,
        Номер_сварного_шва
    FROM wl_china
    WHERE N_Линии = '005-PA-0602'
      AND Номер_чертежа = 'GCC-NAG-DDD-12470-13-1400-TK-ISO-00141'
      AND TRIM(COALESCE(Результаты_АКТ_ВИК, '')) = 'Заказ отправлен'
      AND Дата_сварки IS NOT NULL 
      AND TRIM(Дата_сварки) <> ''
""")

wl_china_vik = c.fetchall()
print(f"\n  Швов со статусом 'Заказ отправлен' в wl_china: {len(wl_china_vik)}")
for record in wl_china_vik:
    print(f"\n    Шов: {record[6]}")
    print(f"    Клейма: {record[0]}/{record[1]}")
    print(f"    Методы: {record[2]}/{record[3]}")
    print(f"    Тип: {record[4]}")

# Проверяем как эти швы группируются
print("\n  Как эти швы должны группироваться:")
c.execute("""
    SELECT 
        (
            SELECT group_concat(DISTINCT part, '/')
            FROM (
                SELECT trim(Клеймо_сварщика_корневой_слой) as part 
                FROM wl_china wc2
                WHERE wc2.N_Линии = wc.N_Линии
                  AND wc2.Номер_чертежа = wc.Номер_чертежа
                  AND wc2.Дата_сварки IS NOT NULL 
                  AND TRIM(wc2.Дата_сварки) <> ''
                  AND wc2.Клеймо_сварщика_корневой_слой IS NOT NULL 
                  AND wc2.Клеймо_сварщика_корневой_слой != ''
                UNION ALL
                SELECT trim(Клеймо_сварщика_заполнение_облицовка) as part 
                FROM wl_china wc2
                WHERE wc2.N_Линии = wc.N_Линии
                  AND wc2.Номер_чертежа = wc.Номер_чертежа
                  AND wc2.Дата_сварки IS NOT NULL 
                  AND TRIM(wc2.Дата_сварки) <> ''
                  AND wc2.Клеймо_сварщика_заполнение_облицовка IS NOT NULL 
                  AND wc2.Клеймо_сварщика_заполнение_облицовка != ''
            )
            WHERE part IS NOT NULL AND part != ''
        ) as клейма,
        COUNT(*) as total,
        SUM(CASE WHEN TRIM(COALESCE(Результаты_АКТ_ВИК, '')) = 'Заказ отправлен' THEN 1 ELSE 0 END) as zayavlen
    FROM wl_china wc
    WHERE wc.N_Линии = '005-PA-0602'
      AND wc.Номер_чертежа = 'GCC-NAG-DDD-12470-13-1400-TK-ISO-00141'
      AND wc.Дата_сварки IS NOT NULL 
      AND TRIM(wc.Дата_сварки) <> ''
    GROUP BY wc.N_Линии, wc.Номер_чертежа
""")

grouped = c.fetchall()
for record in grouped:
    print(f"\n    Клейма в группе: {record[0]}")
    print(f"    Всего швов: {record[1]}")
    print(f"    Заявлен ВИК: {record[2]}")

conn.close()
print("\n" + "=" * 80)

