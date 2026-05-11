#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Быстрая проверка статуса синхронизации weld_repair_log
"""

import sqlite3
from pathlib import Path

def get_db_connection():
    """Создает соединение с базой данных"""
    project_root = Path(__file__).parent.parent.parent
    db_path = project_root / 'database' / 'BD_Kingisepp' / 'M_Kran_Kingesepp.db'
    
    conn = sqlite3.connect(db_path)
    return conn

def check_weld_repair_status():
    """Быстрая проверка статуса синхронизации"""
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("🔍 БЫСТРАЯ ПРОВЕРКА СТАТУСА WELD_REPAIR_LOG")
    print("=" * 50)
    
    # Получаем статистику
    cursor.execute('SELECT COUNT(*) FROM logs_lnk WHERE "Статус_РК" LIKE "%Не годен%"')
    rk_defects = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM logs_lnk WHERE "Статус_РК" = "Н/П"')
    rk_np = cursor.fetchone()[0]
    
    total_defects_stats = rk_defects + rk_np
    
    # Проверяем weld_repair_log
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='weld_repair_log'")
    table_exists = cursor.fetchone()
    
    if table_exists:
        cursor.execute("SELECT COUNT(*) FROM weld_repair_log")
        weld_repair_count = cursor.fetchone()[0]
    else:
        weld_repair_count = 0
    
    # Выводим результат
    print(f"📊 Статистика негодных записей: {total_defects_stats}")
    print(f"📋 Записей в weld_repair_log: {weld_repair_count}")
    
    if weld_repair_count == total_defects_stats:
        print("✅ СТАТУС: ОК - Количество записей соответствует")
    elif weld_repair_count > total_defects_stats:
        print(f"❌ СТАТУС: ПРОБЛЕМА - В weld_repair_log больше на {weld_repair_count - total_defects_stats}")
        print("   Рекомендация: запустите python sync_weld_repair_log.py")
    else:
        print(f"❌ СТАТУС: ПРОБЛЕМА - В weld_repair_log меньше на {total_defects_stats - weld_repair_count}")
        print("   Рекомендация: запустите python sync_weld_repair_log.py")
    
    conn.close()

if __name__ == "__main__":
    check_weld_repair_status()
