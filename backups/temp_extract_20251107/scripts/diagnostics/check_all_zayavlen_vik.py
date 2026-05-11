#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Проверка расчета заявлен_вик для всех записей
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
print("ПРОВЕРКА РАСЧЕТА заявлен_вик ДЛЯ ВСЕХ ЗАПИСЕЙ")
print("=" * 80)

# 1. Общая статистика
print("\n1. Общая статистика:")
print("-" * 80)

# В wl_china
c.execute("""
    SELECT COUNT(*) 
    FROM wl_china 
    WHERE TRIM(COALESCE(Результаты_АКТ_ВИК, '')) = 'Заказ отправлен'
      AND Дата_сварки IS NOT NULL 
      AND TRIM(Дата_сварки) <> ''
""")
total_zayavlen_in_wl_china = c.fetchone()[0]
print(f"  Всего швов со статусом 'Заказ отправлен' в wl_china: {total_zayavlen_in_wl_china}")

# В сварено_сварщиком
c.execute("SELECT SUM(заявлен_вик) FROM сварено_сварщиком")
total_zayavlen_in_svarenno = c.fetchone()[0] or 0
print(f"  Сумма заявлен_вик в сварено_сварщиком: {total_zayavlen_in_svarenno}")

c.execute("SELECT COUNT(*) FROM сварено_сварщиком WHERE заявлен_вик > 0")
records_with_zayavlen = c.fetchone()[0]
print(f"  Записей с заявлен_вик > 0: {records_with_zayavlen}")

c.execute("SELECT COUNT(*) FROM сварено_сварщиком")
total_records = c.fetchone()[0]
print(f"  Всего записей в сварено_сварщиком: {total_records}")

# 2. Проверка для нескольких записей - сравнение с wl_china
print("\n2. Проверка для записей с заявлен_вик > 0 (первые 10):")
print("-" * 80)

c.execute("""
    SELECT 
        N_Линии,
        _Линия,
        Номер_чертежа,
        клейма_сварщиков,
        заявлен_вик,
        Всего_сваренно_сварщиком
    FROM сварено_сварщиком 
    WHERE заявлен_вик > 0
    ORDER BY заявлен_вик DESC
    LIMIT 10
""")

records = c.fetchall()
for i, (n_line, line, drawing, marks, zayavlen, total) in enumerate(records, 1):
    print(f"\n  {i}. Линия: {n_line} ({line}), Чертеж: {drawing}")
    print(f"     Клейма: {marks}")
    print(f"     заявлен_вик в таблице: {zayavlen}")
    print(f"     Всего сварено: {total}")
    
    # Проверяем в wl_china
    c.execute("""
        SELECT COUNT(*) 
        FROM wl_china 
        WHERE N_Линии = ?
          AND Номер_чертежа = ?
          AND TRIM(COALESCE(Результаты_АКТ_ВИК, '')) = 'Заказ отправлен'
          AND Дата_сварки IS NOT NULL 
          AND TRIM(Дата_сварки) <> ''
          AND (
              Клеймо_сварщика_корневой_слой IS NOT NULL AND Клеймо_сварщика_корневой_слой <> ''
              OR Клеймо_сварщика_заполнение_облицовка IS NOT NULL AND Клеймо_сварщика_заполнение_облицовка <> ''
          )
    """, (n_line, drawing))
    count_in_wl_china = c.fetchone()[0]
    
    if count_in_wl_china == zayavlen:
        print(f"     Проверка: OK (в wl_china: {count_in_wl_china})")
    else:
        print(f"     Проверка: НЕ СОВПАДАЕТ! (в wl_china: {count_in_wl_china}, в таблице: {zayavlen})")

# 3. Проверка записей где заявлен_вик = 0, но должны быть заявленные
print("\n3. Проверка записей где заявлен_вик = 0, но в wl_china есть 'Заказ отправлен':")
print("-" * 80)

c.execute("""
    SELECT 
        ss.N_Линии,
        ss._Линия,
        ss.Номер_чертежа,
        ss.клейма_сварщиков,
        COUNT(*) as count_in_wl_china
    FROM сварено_сварщиком ss
    INNER JOIN wl_china wc ON 
        wc.N_Линии = ss.N_Линии 
        AND wc.Номер_чертежа = ss.Номер_чертежа
        AND TRIM(COALESCE(wc.Результаты_АКТ_ВИК, '')) = 'Заказ отправлен'
        AND wc.Дата_сварки IS NOT NULL 
        AND TRIM(wc.Дата_сварки) <> ''
    WHERE ss.заявлен_вик = 0
    GROUP BY ss.N_Линии, ss._Линия, ss.Номер_чертежа, ss.клейма_сварщиков
    LIMIT 10
""")

mismatches = c.fetchall()
if not mismatches:
    print("  Нет расхождений - все правильно!")
else:
    print(f"  Найдено {len(mismatches)} записей с расхождениями:")
    for n_line, line, drawing, marks, count_in_wl_china in mismatches:
        print(f"\n    Линия: {n_line} ({line}), Чертеж: {drawing}")
        print(f"    Клейма: {marks}")
        print(f"    В wl_china есть {count_in_wl_china} швов со статусом 'Заказ отправлен'")
        print(f"    Но в таблице заявлен_вик = 0")

# 4. Проверка наоборот - заявлен_вик > 0, но в wl_china нет
print("\n4. Проверка записей где заявлен_вик > 0, но в wl_china нет 'Заказ отправлен':")
print("-" * 80)

c.execute("""
    SELECT 
        ss.N_Линии,
        ss._Линия,
        ss.Номер_чертежа,
        ss.клейма_сварщиков,
        ss.заявлен_вик
    FROM сварено_сварщиком ss
    WHERE ss.заявлен_вик > 0
      AND NOT EXISTS (
          SELECT 1 
          FROM wl_china wc
          WHERE wc.N_Линии = ss.N_Линии 
            AND wc.Номер_чертежа = ss.Номер_чертежа
            AND TRIM(COALESCE(wc.Результаты_АКТ_ВИК, '')) = 'Заказ отправлен'
            AND wc.Дата_сварки IS NOT NULL 
            AND TRIM(wc.Дата_сварки) <> ''
      )
    LIMIT 10
""")

false_positives = c.fetchall()
if not false_positives:
    print("  Нет ложных срабатываний - все правильно!")
else:
    print(f"  Найдено {len(false_positives)} записей с ложными срабатываниями:")
    for n_line, line, drawing, marks, zayavlen in false_positives:
        print(f"\n    Линия: {n_line} ({line}), Чертеж: {drawing}")
        print(f"    Клейма: {marks}")
        print(f"    В таблице заявлен_вик = {zayavlen}, но в wl_china нет 'Заказ отправлен'")

# 5. Итоговая статистика
print("\n5. Итоговая статистика:")
print("-" * 80)

if total_zayavlen_in_wl_china == total_zayavlen_in_svarenno:
    print(f"  УСПЕХ: Суммы совпадают ({total_zayavlen_in_wl_china} = {total_zayavlen_in_svarenno})")
else:
    diff = total_zayavlen_in_wl_china - total_zayavlen_in_svarenno
    print(f"  ВНИМАНИЕ: Разница {diff} швов")
    print(f"    В wl_china: {total_zayavlen_in_wl_china}")
    print(f"    В сварено_сварщиком: {total_zayavlen_in_svarenno}")

conn.close()
print("\n" + "=" * 80)

