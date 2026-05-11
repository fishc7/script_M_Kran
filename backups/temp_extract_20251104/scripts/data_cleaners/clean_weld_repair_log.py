#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Очистка weld_repair_log от записей, которые не должны там быть
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

def clean_weld_repair_log():
    """Очищает weld_repair_log от неправильных записей"""
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("🧹 ОЧИСТКА WELD_REPAIR_LOG ОТ НЕПРАВИЛЬНЫХ ЗАПИСЕЙ")
    print("=" * 80)
    
    # Проверяем существование таблицы
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='weld_repair_log'")
    if not cursor.fetchone():
        print("❌ Таблица weld_repair_log не существует")
        return
    
    # Получаем все записи из weld_repair_log
    cursor.execute("SELECT app_row_id FROM weld_repair_log")
    weld_repair_ids = [row[0] for row in cursor.fetchall()]
    
    print(f"Всего записей в weld_repair_log: {len(weld_repair_ids)}")
    
    if not weld_repair_ids:
        print("✅ Таблица weld_repair_log пуста")
        return
    
    # Проверяем статусы этих записей в logs_lnk
    placeholders = ','.join(['?' for _ in weld_repair_ids])
    cursor.execute(f'''
        SELECT app_row_id, "Статус_РК", "Статус_ВИК", "Чертеж", "Номер_стыка"
        FROM logs_lnk 
        WHERE app_row_id IN ({placeholders})
    ''', weld_repair_ids)
    
    records_in_logs = cursor.fetchall()
    print(f"Найдено записей в logs_lnk: {len(records_in_logs)}")
    
    # Определяем записи для удаления
    records_to_delete = []
    valid_records = []
    
    for record in records_in_logs:
        status_rk = record['Статус_РК'] or ''
        status_vik = record['Статус_ВИК'] or ''
        
        # Проверяем, является ли запись НЕГОДНОЙ по критериям статистики
        is_defect = (
            'Не годен' in status_rk or 
            status_rk == 'Н/П'
        )
        
        if is_defect:
            valid_records.append(record['app_row_id'])
        else:
            records_to_delete.append({
                'app_row_id': record['app_row_id'],
                'status_rk': status_rk,
                'status_vik': status_vik,
                'drawing': record['Чертеж'],
                'joint': record['Номер_стыка']
            })
    
    print(f"\n📊 РЕЗУЛЬТАТ АНАЛИЗА:")
    print(f"✅ Правильных записей: {len(valid_records)}")
    print(f"❌ Записей для удаления: {len(records_to_delete)}")
    
    if records_to_delete:
        print(f"\n🗑️ ЗАПИСИ ДЛЯ УДАЛЕНИЯ:")
        for record in records_to_delete:
            # Безопасно кодируем значения для вывода
            safe_status_rk = str(record['status_rk']).replace('\u221a', 'V').replace('\u2116', 'N')
            safe_status_vik = str(record['status_vik']).replace('\u221a', 'V').replace('\u2116', 'N')
            safe_drawing = str(record['drawing']).replace('\u221a', 'V').replace('\u2116', 'N')
            safe_joint = str(record['joint']).replace('\u221a', 'V').replace('\u2116', 'N')
            print(f"  - app_row_id: {record['app_row_id']}, РК: '{safe_status_rk}', ВИК: '{safe_status_vik}', Чертеж: {safe_drawing}, Стык: {safe_joint}")
        
        # Группируем по причинам удаления
        reasons = {}
        for record in records_to_delete:
            if record['status_rk'] == '' and record['status_vik'] == '':
                reason = "Пустые статусы РК и ВИК"
            elif 'Пересвет' in record['status_rk']:
                reason = "Статус РК: Пересвет"
            elif 'Не соответствует' in record['status_rk']:
                reason = "Статус РК: Не соответствует"
            else:
                # Безопасно кодируем значения для вывода
                safe_status_rk = str(record['status_rk']).replace('\u221a', 'V').replace('\u2116', 'N')
                safe_status_vik = str(record['status_vik']).replace('\u221a', 'V').replace('\u2116', 'N')
                reason = f"Другие статусы: РК='{safe_status_rk}', ВИК='{safe_status_vik}'"
            
            reasons[reason] = reasons.get(reason, 0) + 1
        
        print(f"\n📈 ГРУППИРОВКА ПО ПРИЧИНАМ:")
        for reason, count in reasons.items():
            print(f"  {reason}: {count}")
        
        # Запрашиваем подтверждение
        print(f"\n⚠️ ВНИМАНИЕ: Будет удалено {len(records_to_delete)} записей из weld_repair_log!")
        response = input("Продолжить удаление? (y/N): ")
        
        if response.lower() == 'y':
            # Удаляем неправильные записи
            delete_ids = [record['app_row_id'] for record in records_to_delete]
            placeholders = ','.join(['?' for _ in delete_ids])
            
            cursor.execute(f"DELETE FROM weld_repair_log WHERE app_row_id IN ({placeholders})", delete_ids)
            deleted_count = cursor.rowcount
            
            # Сохраняем изменения
            conn.commit()
            
            print(f"✅ Удалено записей: {deleted_count}")
            
            # Проверяем результат
            cursor.execute("SELECT COUNT(*) FROM weld_repair_log")
            remaining_count = cursor.fetchone()[0]
            print(f"Осталось записей в weld_repair_log: {remaining_count}")
            
            # Проверяем соответствие статистике
            cursor.execute('SELECT COUNT(*) FROM logs_lnk WHERE "Статус_РК" LIKE "%Не годен%"')
            rk_defects = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM logs_lnk WHERE "Статус_РК" = "Н/П"')
            rk_np = cursor.fetchone()[0]
            
            expected_count = rk_defects + rk_np
            print(f"Ожидаемое количество по статистике: {expected_count}")
            
            if remaining_count == expected_count:
                print("✅ Количество записей теперь соответствует статистике!")
            else:
                print(f"⚠️ Все еще есть расхождение: {remaining_count} vs {expected_count}")
        else:
            print("❌ Удаление отменено")
    else:
        print("✅ Все записи в weld_repair_log корректны")
    
    conn.close()
    print("\n" + "=" * 80)
    print("✅ ОЧИСТКА ЗАВЕРШЕНА")

if __name__ == "__main__":
    clean_weld_repair_log()

