#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Анализ расхождения между количеством негодных записей в статистике и weld_repair_log
"""

import sqlite3
import os
from pathlib import Path

def get_db_connection():
    """Создает соединение с базой данных"""
    project_root = Path(__file__).parent
    db_path = project_root / 'BD_Kingisepp' / 'M_Kran_Kingesepp.db'
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def analyze_defects_discrepancy():
    """Анализирует расхождение между статистикой и weld_repair_log"""
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("🔍 АНАЛИЗ РАСХОЖДЕНИЯ МЕЖДУ СТАТИСТИКОЙ И WELD_REPAIR_LOG")
    print("=" * 80)
    
    # 1. Получаем статистику из logs_lnk (как в функции get_logs_lnk_stats)
    print("\n📊 СТАТИСТИКА ИЗ LOGS_LNK:")
    print("-" * 40)
    
    # РК дефекты: Ремонт, Вырез, Вырезать
    cursor.execute('SELECT COUNT(*) FROM logs_lnk WHERE "Статус_РК" LIKE "%Не годен%"')
    rk_defects = cursor.fetchone()[0]
    print(f"РК дефекты (Ремонт/Вырез): {rk_defects}")
    
    # РК Н/П (неофициальный ремонт или вырез)
    cursor.execute('SELECT COUNT(*) FROM logs_lnk WHERE "Статус_РК" = "Н/П"')
    rk_np = cursor.fetchone()[0]
    print(f"РК Н/П (неофициальный): {rk_np}")
    
    # Всего негодных по статистике
    total_defects_stats = rk_defects + rk_np
    print(f"ВСЕГО НЕГОДНЫХ (по статистике): {total_defects_stats}")
    
    # 2. Проверяем количество записей в weld_repair_log
    print("\n📋 ЗАПИСИ В WELD_REPAIR_LOG:")
    print("-" * 40)
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='weld_repair_log'")
    table_exists = cursor.fetchone()
    
    if table_exists:
        cursor.execute("SELECT COUNT(*) FROM weld_repair_log")
        weld_repair_count = cursor.fetchone()[0]
        print(f"Всего записей в weld_repair_log: {weld_repair_count}")
        
        # Получаем несколько примеров записей для анализа
        cursor.execute("SELECT app_row_id, 'Чертеж', 'Номер_стыка', 'Статус_РК' FROM weld_repair_log LIMIT 5")
        sample_records = cursor.fetchall()
        print(f"Примеры записей в weld_repair_log:")
        for record in sample_records:
            print(f"  - app_row_id: {record[0]}")
    else:
        print("Таблица weld_repair_log не существует")
        weld_repair_count = 0
    
    # 3. Анализируем расхождение
    print("\n🔍 АНАЛИЗ РАСХОЖДЕНИЯ:")
    print("-" * 40)
    
    if weld_repair_count > total_defects_stats:
        print(f"❌ ПРОБЛЕМА: В weld_repair_log больше записей ({weld_repair_count}) чем негодных по статистике ({total_defects_stats})")
        print(f"   Разница: {weld_repair_count - total_defects_stats} записей")
        
        # Проверяем, какие записи есть в weld_repair_log, но не должны быть по статистике
        print("\n🔍 ДЕТАЛЬНЫЙ АНАЛИЗ:")
        
        # Получаем app_row_id из weld_repair_log
        cursor.execute("SELECT app_row_id FROM weld_repair_log")
        weld_repair_ids = [row[0] for row in cursor.fetchall()]
        
        # Проверяем статусы этих записей в logs_lnk
        if weld_repair_ids:
            placeholders = ','.join(['?' for _ in weld_repair_ids])
            cursor.execute(f'''
                SELECT app_row_id, "Статус_РК", "Статус_ВИК", "Чертеж", "Номер_стыка"
                FROM logs_lnk 
                WHERE app_row_id IN ({placeholders})
            ''', weld_repair_ids)
            
            records_in_logs = cursor.fetchall()
            print(f"Найдено {len(records_in_logs)} записей в logs_lnk из {len(weld_repair_ids)} в weld_repair_log")
            
            # Анализируем статусы
            status_counts = {}
            non_defect_records = []
            
            for record in records_in_logs:
                status_rk = record['Статус_РК'] or ''
                status_vik = record['Статус_ВИК'] or ''
                
                # Проверяем, является ли запись негодной
                is_defect = (
                    'емонт' in status_rk or 'ырез' in status_rk or 
                    status_rk == 'Н/П' or
                    (status_vik != 'Годен' and status_vik != '' and status_vik is not None)
                )
                
                if not is_defect:
                    non_defect_records.append({
                        'app_row_id': record['app_row_id'],
                        'status_rk': status_rk,
                        'status_vik': status_vik,
                        'drawing': record['Чертеж'],
                        'joint': record['Номер_стыка']
                    })
                
                status_key = f"РК: {status_rk}, ВИК: {status_vik}"
                status_counts[status_key] = status_counts.get(status_key, 0) + 1
            
            print(f"\n📈 СТАТИСТИКА СТАТУСОВ:")
            for status, count in status_counts.items():
                print(f"  {status}: {count}")
            
            if non_defect_records:
                print(f"\n⚠️ ЗАПИСИ, КОТОРЫЕ НЕ ДОЛЖНЫ БЫТЬ В WELD_REPAIR_LOG:")
                for record in non_defect_records[:10]:  # Показываем первые 10
                        # Безопасно кодируем значения для вывода
                    safe_status_rk = str(record['status_rk']).replace('\u221a', 'V').replace('\u2116', 'N')
                    safe_status_vik = str(record['status_vik']).replace('\u221a', 'V').replace('\u2116', 'N')
                    safe_drawing = str(record['drawing']).replace('\u221a', 'V').replace('\u2116', 'N')
                    safe_joint = str(record['joint']).replace('\u221a', 'V').replace('\u2116', 'N')
                    print(f"  - app_row_id: {record['app_row_id']}, РК: '{safe_status_rk}', ВИК: '{safe_status_vik}', Чертеж: {safe_drawing}, Стык: {safe_joint}")
                if len(non_defect_records) > 10:
                    print(f"  ... и еще {len(non_defect_records) - 10} записей")
    
    else:
        print(f"✅ Количество записей корректное: weld_repair_log ({weld_repair_count}) <= статистика ({total_defects_stats})")
    
    # 4. Дополнительная проверка - все ли негодные записи перенесены
    print("\n🔍 ПРОВЕРКА ПОЛНОТЫ ПЕРЕНОСА:")
    print("-" * 40)
    
    # Получаем все негодные записи из logs_lnk
    cursor.execute('''
        SELECT app_row_id, "Статус_РК", "Статус_ВИК", "Чертеж", "Номер_стыка"
        FROM logs_lnk 
        WHERE ("Статус_РК" LIKE "%Не годен%" OR "Статус_РК" = "Н/П")
        OR ("Статус_ВИК" != "Годен" AND "Статус_ВИК" != "" AND "Статус_ВИК" IS NOT NULL)
    ''')
    
    all_defect_records = cursor.fetchall()
    print(f"Всего негодных записей в logs_lnk: {len(all_defect_records)}")
    
    if weld_repair_count > 0:
        # Проверяем, какие негодные записи НЕ перенесены
        defect_ids = [record['app_row_id'] for record in all_defect_records]
        placeholders = ','.join(['?' for _ in defect_ids])
        
        cursor.execute(f'''
            SELECT app_row_id FROM weld_repair_log 
            WHERE app_row_id IN ({placeholders})
        ''', defect_ids)
        
        transferred_ids = [row[0] for row in cursor.fetchall()]
        not_transferred = [rid for rid in defect_ids if rid not in transferred_ids]
        
        print(f"Перенесено в weld_repair_log: {len(transferred_ids)}")
        print(f"НЕ перенесено: {len(not_transferred)}")
        
        if not_transferred:
            print(f"\n⚠️ НЕ ПЕРЕНЕСЕННЫЕ ЗАПИСИ:")
            for record in all_defect_records:
                if record['app_row_id'] in not_transferred:
                    # Безопасно кодируем значения для вывода
                    safe_status_rk = str(record['Статус_РК']).replace('\u221a', 'V').replace('\u2116', 'N')
                    safe_status_vik = str(record['Статус_ВИК']).replace('\u221a', 'V').replace('\u2116', 'N')
                    safe_drawing = str(record['Чертеж']).replace('\u221a', 'V').replace('\u2116', 'N')
                    safe_joint = str(record['Номер_стыка']).replace('\u221a', 'V').replace('\u2116', 'N')
                    print(f"  - app_row_id: {record['app_row_id']}, РК: '{safe_status_rk}', ВИК: '{safe_status_vik}', Чертеж: {safe_drawing}, Стык: {safe_joint}")
    
    conn.close()
    
    print("\n" + "=" * 80)
    print("✅ АНАЛИЗ ЗАВЕРШЕН")

if __name__ == "__main__":
    analyze_defects_discrepancy()
