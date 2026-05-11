#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тест SQL запроса для проверки заявлен_вик
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

print("Тестирование SQL запроса из create_svarenno_svarshchikom_table.py")
print("=" * 80)

# Тестовый запрос с упрощенной логикой
sql = f"""
SELECT
    wc.N_Линии,
    CASE 
        WHEN wc.N_Линии IS NOT NULL THEN
            CASE 
                WHEN INSTR(CAST(wc.N_Линии AS TEXT), ' ') > 0 THEN 
                    SUBSTR(CAST(wc.N_Линии AS TEXT), 1, INSTR(CAST(wc.N_Линии AS TEXT), ' ') - 1)
                WHEN INSTR(CAST(wc.N_Линии AS TEXT), '(') > 0 THEN 
                    SUBSTR(CAST(wc.N_Линии AS TEXT), 1, INSTR(CAST(wc.N_Линии AS TEXT), '(') - 1)
                ELSE CAST(wc.N_Линии AS TEXT)
            END
        ELSE NULL
    END as _Линия,
    wc.Номер_чертежа,
    (
        SELECT group_concat(part, '/')
        FROM (
            SELECT DISTINCT part
            FROM (
                SELECT trim(wc2.Клеймо_сварщика_корневой_слой) as part 
                FROM wl_china wc2
                WHERE wc2.Номер_чертежа = wc.Номер_чертежа
                  AND wc2.N_Линии = wc.N_Линии
                  AND wc2.Клеймо_сварщика_корневой_слой != ''
                UNION ALL
                SELECT trim(wc2.Клеймо_сварщика_заполнение_облицовка) as part 
                FROM wl_china wc2
                WHERE wc2.Номер_чертежа = wc.Номер_чертежа
                  AND wc2.N_Линии = wc.N_Линии
                  AND wc2.Клеймо_сварщика_заполнение_облицовка != ''
            )
            WHERE part IS NOT NULL AND part != ''
        )
    ) as клейма_сварщиков,
    SUM(CASE WHEN TRIM(COALESCE(wc.Результаты_АКТ_ВИК, '')) = 'Заказ отправлен' THEN 1 ELSE 0 END) as заявлен_вик,
    COUNT(wc.Номер_сварного_шва) as Всего_сваренно_сварщиком
FROM wl_china wc
WHERE wc.Номер_чертежа = ?
  AND (
      wc.Клеймо_сварщика_корневой_слой = ? 
      OR wc.Клеймо_сварщика_заполнение_облицовка = ?
  )
  AND wc.Дата_сварки IS NOT NULL 
  AND TRIM(wc.Дата_сварки) <> ''
  AND (
      wc.Клеймо_сварщика_корневой_слой IS NOT NULL AND wc.Клеймо_сварщика_корневой_слой <> ''
      OR wc.Клеймо_сварщика_заполнение_облицовка IS NOT NULL AND wc.Клеймо_сварщика_заполнение_облицовка <> ''
  )
  AND wc.Номер_сварного_шва IS NOT NULL
GROUP BY 
    wc.N_Линии,
    _Линия,
    wc.Номер_чертежа,
    клейма_сварщиков
HAVING клейма_сварщиков IS NOT NULL
"""

print("\nВыполняю запрос...")
try:
    c.execute(sql, (drawing, weld_mark, weld_mark))
    rows = c.fetchall()
    print(f"\nНайдено записей: {len(rows)}")
    for row in rows:
        print(f"\n  Линия: {row[0]} ({row[1]})")
        print(f"  Чертеж: {row[2]}")
        print(f"  Клейма: {row[3]}")
        print(f"  заявлен_вик: {row[4]}")
        print(f"  Всего: {row[5]}")
except Exception as e:
    print(f"Ошибка: {e}")
    import traceback
    traceback.print_exc()

conn.close()

