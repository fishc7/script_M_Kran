#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Диагностический скрипт для проверки несоответствий в расчете "Стило(PMI)"
"""

import sqlite3
import os
import sys

# Настройка путей
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, project_root)

from scripts.utilities.db_utils import get_database_connection

def check_stilo_values():
    """Проверка уникальных значений в Результаты_Заключения_Стилоскопирование"""
    conn = get_database_connection()
    c = conn.cursor()
    
    print("=" * 60)
    print("ПРОВЕРКА 1: Уникальные значения Результаты_Заключения_Стилоскопирование")
    print("=" * 60)
    
    c.execute("""
        SELECT DISTINCT Результаты_Заключения_Стилоскопирование, 
               COUNT(*) as count
        FROM wl_china 
        WHERE Результаты_Заключения_Стилоскопирование IS NOT NULL
        GROUP BY Результаты_Заключения_Стилоскопирование
        ORDER BY count DESC
    """)
    
    values = c.fetchall()
    for val, count in values:
        print(f"  '{val}' - {count} записей")
    
    # Проверка вариантов "годен"
    print("\nПроверка вариантов 'годен':")
    variants = ['годен', 'Годен', 'ГОДЕН', 'годен ', ' годен', '  годен  ']
    for variant in variants:
        c.execute("""
            SELECT COUNT(*) 
            FROM wl_china 
            WHERE UPPER(TRIM(COALESCE(Результаты_Заключения_Стилоскопирование, ''))) = UPPER(?)
        """, (variant,))
        count = c.fetchone()[0]
        if count > 0:
            print(f"  Найдено записей с вариантом '{variant}': {count}")
    
    conn.close()

def check_ceil_formula_difference():
    """Сравнение формул ceil - текущая vs унифицированная"""
    conn = get_database_connection()
    c = conn.cursor()
    
    print("\n" + "=" * 60)
    print("ПРОВЕРКА 2: Сравнение формул ceil")
    print("=" * 60)
    
    query = """
    SELECT 
        ss.id,
        ss._Линия,
        ss.Всего_сваренно_сварщиком,
        ss.не_годен_рк,
        ss.годен_стилоскопирование,
        ss."Стило(PMI)" as текущее_значение,
        он."СТ (Стилоскоп) Стилоскопирование / PMI" as процент_из_основнойНК,
        -- Расчет по текущей формуле (сложная)
        CASE 
            WHEN EXISTS (
                SELECT 1 FROM основнаяНК он2
                WHERE он2."Номер линии / Line No" = ss._Линия
                  AND он2."СТ (Стилоскоп) Стилоскопирование / PMI" LIKE '%*%'
            ) THEN 2
            ELSE (
                CASE 
                    WHEN (
                        ((MAX(ss.Всего_сваренно_сварщиком - ss.не_годен_рк, 0)) * COALESCE(CAST(REPLACE(REPLACE(он."СТ (Стилоскоп) Стилоскопирование / PMI", '%', ''), ',', '.') AS REAL), 0)) / 100.0
                    ) > CAST((
                        ((MAX(ss.Всего_сваренно_сварщиком - ss.не_годен_рк, 0)) * COALESCE(CAST(REPLACE(REPLACE(он."СТ (Стилоскоп) Стилоскопирование / PMI", '%', ''), ',', '.') AS REAL), 0)) / 100.0
                    ) AS INT)
                    THEN CAST((
                        ((MAX(ss.Всего_сваренно_сварщиком - ss.не_годен_рк, 0)) * COALESCE(CAST(REPLACE(REPLACE(он."СТ (Стилоскоп) Стилоскопирование / PMI", '%', ''), ',', '.') AS REAL), 0)) / 100.0
                    ) AS INT) + 1
                    ELSE CAST((
                        ((MAX(ss.Всего_сваренно_сварщиком - ss.не_годен_рк, 0)) * COALESCE(CAST(REPLACE(REPLACE(он."СТ (Стилоскоп) Стилоскопирование / PMI", '%', ''), ',', '.') AS REAL), 0)) / 100.0
                    ) AS INT)
                END
            )
        END - COALESCE(ss.годен_стилоскопирование, 0) as расчет_текущей_формулы,
        -- Расчет по унифицированной формуле (как для РК/ПВК/МПД)
        CASE 
            WHEN EXISTS (
                SELECT 1 FROM основнаяНК он2
                WHERE он2."Номер линии / Line No" = ss._Линия
                  AND он2."СТ (Стилоскоп) Стилоскопирование / PMI" LIKE '%*%'
            ) THEN 2
            ELSE CAST((
                (MAX(ss.Всего_сваренно_сварщиком - ss.не_годен_рк, 0)) * COALESCE(CAST(REPLACE(REPLACE(он."СТ (Стилоскоп) Стилоскопирование / PMI", '%', ''), ',', '.') AS REAL), 0) + 99
            ) / 100 AS INT)
        END - COALESCE(ss.годен_стилоскопирование, 0) as расчет_унифицированной_формулы
    FROM сварено_сварщиком ss
    LEFT JOIN основнаяНК он ON он."Номер линии / Line No" = ss._Линия
    WHERE ss."Стило(PMI)" IS NOT NULL
      AND он."СТ (Стилоскоп) Стилоскопирование / PMI" IS NOT NULL
      AND он."СТ (Стилоскоп) Стилоскопирование / PMI" NOT LIKE '%*%'
    LIMIT 20
    """
    
    c.execute(query)
    results = c.fetchall()
    
    print(f"\nНайдено записей для сравнения: {len(results)}")
    print("\nСравнение формул (первые 10 записей):\n")
    
    mismatches = 0
    for row in results[:10]:
        id_val, линия, всего, не_годен_рк, годен, текущее, процент, расчет_текущей, расчет_унифицированной = row
        
        if расчет_текущей != расчет_унифицированной:
            mismatches += 1
            print(f"  ID {id_val}, Линия {линия}:")
            print(f"    Всего: {всего}, не_годен_рк: {не_годен_рк}, годен: {годен}")
            print(f"    Процент: {процент}")
            print(f"    Текущее значение: {текущее}")
            print(f"    Расчет текущей формулой: {расчет_текущей}")
            print(f"    Расчет унифицированной: {расчет_унифицированной}")
            print(f"    !!! НЕСООТВЕТСТВИЕ!")
            print()
    
    if mismatches == 0:
        print("  OK: Формулы дают одинаковый результат для проверенных записей")
    
    conn.close()

def check_goden_variants():
    """Проверка учета вариантов 'годен'"""
    conn = get_database_connection()
    c = conn.cursor()
    
    print("\n" + "=" * 60)
    print("ПРОВЕРКА 3: Учет вариантов 'годен'")
    print("=" * 60)
    
    # Текущий подсчет (строгое совпадение)
    c.execute("""
        SELECT SUM(CASE WHEN Результаты_Заключения_Стилоскопирование = 'годен' THEN 1 ELSE 0 END)
        FROM wl_china
        WHERE Дата_сварки IS NOT NULL AND TRIM(Дата_сварки) <> ''
    """)
    current_count = c.fetchone()[0] or 0
    
    # Нормализованный подсчет (UPPER + TRIM)
    c.execute("""
        SELECT SUM(CASE WHEN UPPER(TRIM(COALESCE(Результаты_Заключения_Стилоскопирование, ''))) = 'ГОДЕН' 
            THEN 1 ELSE 0 END)
        FROM wl_china
        WHERE Дата_сварки IS NOT NULL AND TRIM(Дата_сварки) <> ''
    """)
    normalized_count = c.fetchone()[0] or 0
    
    print(f"\nТекущий подсчет (строгое совпадение 'годен'): {current_count}")
    print(f"Нормализованный подсчет (UPPER+TRIM): {normalized_count}")
    
    if normalized_count > current_count:
        print(f"\n!!! ОБНАРУЖЕНО: Пропущено {normalized_count - current_count} записей с вариантами 'годен'!")
        
        # Показать примеры пропущенных
        c.execute("""
            SELECT DISTINCT Результаты_Заключения_Стилоскопирование, COUNT(*)
            FROM wl_china
            WHERE Дата_сварки IS NOT NULL AND TRIM(Дата_сварки) <> ''
              AND UPPER(TRIM(COALESCE(Результаты_Заключения_Стилоскопирование, ''))) = 'ГОДЕН'
              AND Результаты_Заключения_Стилоскопирование != 'годен'
            GROUP BY Результаты_Заключения_Стилоскопирование
        """)
        variants = c.fetchall()
        print("\nПропущенные варианты:")
        for variant, count in variants:
            print(f"  '{variant}' - {count} записей")
    else:
        print("\n  OK: Все варианты 'годен' учтены корректно")
    
    conn.close()

def check_pmi_calculation():
    """Проверка расчета Стило(PMI) на соответствие текущей формуле"""
    conn = get_database_connection()
    c = conn.cursor()
    
    print("\n" + "=" * 60)
    print("ПРОВЕРКА 4: Соответствие текущих значений формуле")
    print("=" * 60)
    
    # Упрощенная проверка - сравнение с текущей формулой
    query = """
    SELECT COUNT(*) 
    FROM сварено_сварщиком ss
    WHERE ss."Стило(PMI)" IS NOT NULL
      AND ABS(ss."Стило(PMI)" - (
          MAX(
              CASE 
                  WHEN EXISTS (
                      SELECT 1 FROM основнаяНК он
                      WHERE он."Номер линии / Line No" = ss._Линия
                        AND он."СТ (Стилоскоп) Стилоскопирование / PMI" LIKE '%*%'
                  ) THEN 2
                  ELSE CAST((
                      (MAX(ss.Всего_сваренно_сварщиком - ss.не_годен_рк, 0)) * COALESCE((
                          SELECT CAST(REPLACE(REPLACE(он."СТ (Стилоскоп) Стилоскопирование / PMI", '%', ''), ',', '.') AS REAL)
                          FROM основнаяНК он
                          WHERE он."Номер линии / Line No" = ss._Линия
                          LIMIT 1
                      ), 0) + 99) / 100 AS INT)
                  END - COALESCE(ss.годен_стилоскопирование, 0),
              0
          )
      )) > 0.01
    """
    
    c.execute(query)
    mismatches = c.fetchone()[0]
    
    print(f"\nЗаписей, не соответствующих унифицированной формуле: {mismatches}")
    
    if mismatches > 0:
        print("  !!! Обнаружены несоответствия!")
    
    conn.close()

if __name__ == "__main__":
    import sys
    import io
    
    # Настройка кодировки для Windows
    if sys.platform == 'win32':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    
    print("Диагностика расчетов 'Стило(PMI)' и 'годен_стилоскопирование'\n")
    
    try:
        check_stilo_values()
        check_goden_variants()
        check_ceil_formula_difference()
        check_pmi_calculation()
        
        print("\n" + "=" * 60)
        print("Диагностика завершена")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nОШИБКА: {e}")
        import traceback
        traceback.print_exc()

