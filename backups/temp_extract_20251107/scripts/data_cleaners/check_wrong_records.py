#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Проверка записей в weld_repair_log с неправильными статусами
"""

import sqlite3
from pathlib import Path

def get_db_connection():
    """Создает соединение с базой данных"""
    project_root = Path(__file__).parent
    db_path = project_root / 'BD_Kingisepp' / 'M_Kran_Kingesepp.db'
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def check_wrong_records():
    """Проверяет записи в weld_repair_log с неправильными статусами"""
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("🔍 ПРОВЕРКА ЗАПИСЕЙ В WELD_REPAIR_LOG С НЕПРАВИЛЬНЫМИ СТАТУСАМИ")
    print("=" * 70)
    
    # Получаем все записи из weld_repair_log
    cursor.execute("SELECT app_row_id FROM weld_repair_log")
    weld_repair_ids = [row[0] for row in cursor.fetchall()]
    
    if not weld_repair_ids:
        print("❌ Таблица weld_repair_log пуста")
        return
    
    print(f"📋 Всего записей в weld_repair_log: {len(weld_repair_ids)}")
    
    # Проверяем статусы этих записей в logs_lnk
    placeholders = ','.join(['?' for _ in weld_repair_ids])
    cursor.execute(f'''
        SELECT app_row_id, "Статус_РК", "Статус_ВИК", "Чертеж", "Номер_стыка"
        FROM logs_lnk 
        WHERE app_row_id IN ({placeholders})
    ''', weld_repair_ids)
    
    records_in_logs = cursor.fetchall()
    print(f"📊 Найдено {len(records_in_logs)} записей в logs_lnk")
    
    # Анализируем статусы
    correct_records = []
    wrong_records = []
    
    for record in records_in_logs:
        status_rk = record['Статус_РК'] or ''
        status_vik = record['Статус_ВИК'] or ''
        
        # Проверяем, является ли запись НЕГОДНОЙ по критериям статистики
        is_defect = (
            'Не годен' in status_rk or 
            status_rk == 'Н/П'
        )
        
        if is_defect:
            correct_records.append(record)
        else:
            wrong_records.append({
                'app_row_id': record['app_row_id'],
                'status_rk': status_rk,
                'status_vik': status_vik,
                'drawing': record['Чертеж'],
                'joint': record['Номер_стыка']
            })
    
    print(f"\n✅ Правильных записей: {len(correct_records)}")
    print(f"❌ Неправильных записей: {len(wrong_records)}")
    
    if wrong_records:
        print(f"\n📋 ДЕТАЛИЗАЦИЯ НЕПРАВИЛЬНЫХ ЗАПИСЕЙ:")
        print("-" * 50)
        
        # Группируем по статусам
        status_groups = {}
        for record in wrong_records:
            status_key = f"РК: '{record['status_rk']}', ВИК: '{record['status_vik']}'"
            if status_key not in status_groups:
                status_groups[status_key] = []
            status_groups[status_key].append(record)
        
        for status, records in status_groups.items():
            print(f"\n🔸 {status}: {len(records)} записей")
            for record in records[:3]:  # Показываем первые 3 примера
                print(f"   - app_row_id: {record['app_row_id']}, Чертеж: {record['drawing']}, Стык: {record['joint']}")
            if len(records) > 3:
                print(f"   ... и еще {len(records) - 3} записей")
    
    # Проверяем, сколько негодных записей НЕ в weld_repair_log
    cursor.execute('''
        SELECT app_row_id, "Статус_РК", "Статус_ВИК", "Чертеж", "Номер_стыка"
        FROM logs_lnk 
        WHERE ("Статус_РК" LIKE "%Не годен%" OR "Статус_РК" = "Н/П")
    ''')
    
    all_defect_records = cursor.fetchall()
    defect_ids_in_logs = [record['app_row_id'] for record in all_defect_records]
    
    missing_records = []
    for record in all_defect_records:
        if record['app_row_id'] not in [r['app_row_id'] for r in correct_records]:
            missing_records.append(record)
    
    print(f"\n📊 АНАЛИЗ НЕДОСТАЮЩИХ ЗАПИСЕЙ:")
    print(f"Всего негодных записей в logs_lnk: {len(all_defect_records)}")
    print(f"Правильных записей в weld_repair_log: {len(correct_records)}")
    print(f"Недостающих записей: {len(missing_records)}")
    
    if missing_records:
        print(f"\n📋 ПРИМЕРЫ НЕДОСТАЮЩИХ ЗАПИСЕЙ:")
        for record in missing_records[:5]:
            print(f"   - app_row_id: {record['app_row_id']}, РК: '{record['Статус_РК']}', ВИК: '{record['Статус_ВИК']}', Чертеж: {record['Чертеж']}")
    
    conn.close()
    
    print(f"\n" + "=" * 70)
    print("✅ ПРОВЕРКА ЗАВЕРШЕНА")

if __name__ == "__main__":
    check_wrong_records()






