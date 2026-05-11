#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Автоматическая проверка системы M_Kran
Рекомендации по поддержанию согласованности данных
"""

import sqlite3
from pathlib import Path
from datetime import datetime
import subprocess
import sys

def get_db_connection():
    """Создает соединение с базой данных"""
    project_root = Path(__file__).parent
    db_path = project_root / 'BD_Kingisepp' / 'M_Kran_Kingesepp.db'
    
    conn = sqlite3.connect(db_path)
    return conn

def run_consistency_check():
    """Запускает проверку согласованности"""
    try:
        result = subprocess.run([sys.executable, 'test_consistency.py'], 
                              capture_output=True, text=True, timeout=30)
        return result.stdout
    except Exception as e:
        return f"Ошибка при запуске проверки: {e}"

def auto_check_system():
    """Автоматическая проверка системы"""
    
    print("🤖 АВТОМАТИЧЕСКАЯ ПРОВЕРКА СИСТЕМЫ M_KRAN")
    print("=" * 60)
    print(f"Время проверки: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. Быстрая проверка статуса
    print("\n📊 БЫСТРАЯ ПРОВЕРКА СТАТУСА:")
    print("-" * 40)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Статистика
    cursor.execute('SELECT COUNT(*) FROM logs_lnk WHERE "Статус_РК" LIKE "%Не годен%"')
    rk_defects = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM logs_lnk WHERE "Статус_РК" = "Н/П"')
    rk_np = cursor.fetchone()[0]
    
    total_stats = rk_defects + rk_np
    
    # Weld_repair_log
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='weld_repair_log'")
    table_exists = cursor.fetchone()
    
    if table_exists:
        cursor.execute("SELECT COUNT(*) FROM weld_repair_log")
        weld_repair_count = cursor.fetchone()[0]
    else:
        weld_repair_count = 0
    
    print(f"📈 Статистика негодных записей: {total_stats}")
    print(f"📋 Записей в weld_repair_log: {weld_repair_count}")
    
    if weld_repair_count == total_stats:
        print("✅ СТАТУС: ОК - Синхронизация в порядке")
        status_ok = True
    else:
        print(f"❌ СТАТУС: ПРОБЛЕМА - Расхождение: {weld_repair_count - total_stats}")
        status_ok = False
    
    conn.close()
    
    # 2. Рекомендации
    print(f"\n💡 РЕКОМЕНДАЦИИ ПО ПОДДЕРЖАНИЮ СИСТЕМЫ:")
    print("-" * 40)
    
    print("1️⃣ РЕГУЛЯРНЫЕ ПРОВЕРКИ:")
    print("   • Еженедельно запускайте: python test_consistency.py")
    print("   • Ежемесячно запускайте: python sync_weld_repair_log.py")
    print("   • При массовых операциях с данными - сразу после")
    
    print("\n2️⃣ МОНИТОРИНГ В ВЕБ-ПРИЛОЖЕНИИ:")
    print("   • Сравнивайте число в карточке 'ВСЕГО НЕГОДНЫХ'")
    print("   • Кликайте на карточку и проверяйте количество записей")
    print("   • Если числа не совпадают - запускайте синхронизацию")
    
    print("\n3️⃣ ПРОФИЛАКТИЧЕСКИЕ МЕРЫ:")
    print("   • Перед массовым переносом данных проверяйте статус")
    print("   • После обновления данных запускайте проверку")
    print("   • Ведите лог изменений в системе")
    
    print("\n4️⃣ АВТОМАТИЗАЦИЯ:")
    print("   • Настройте планировщик задач для регулярных проверок")
    print("   • Создайте уведомления при обнаружении расхождений")
    print("   • Настройте автоматический запуск синхронизации")
    
    # 3. Текущие действия
    if not status_ok:
        print(f"\n🚨 ТРЕБУЕТСЯ ВМЕШАТЕЛЬСТВО:")
        print("-" * 40)
        print("❌ Обнаружено расхождение в данных!")
        print("🔧 Рекомендуемые действия:")
        print("   1. Запустите: python sync_weld_repair_log.py")
        print("   2. Проверьте результат: python test_consistency.py")
        print("   3. Если проблема повторяется - обратитесь к разработчику")
    else:
        print(f"\n✅ СИСТЕМА РАБОТАЕТ КОРРЕКТНО")
        print("-" * 40)
        print("🎯 Все данные синхронизированы")
        print("📅 Следующая проверка рекомендуется через неделю")
    
    # 4. Информация о системе
    print(f"\n📋 ИНФОРМАЦИЯ О СИСТЕМЕ:")
    print("-" * 40)
    print("🔧 Доступные скрипты:")
    print("   • test_consistency.py - полная проверка согласованности")
    print("   • sync_weld_repair_log.py - синхронизация данных")
    print("   • check_weld_repair_status.py - быстрая проверка")
    print("   • auto_check_system.py - этот скрипт")
    
    print(f"\n📞 ПОДДЕРЖКА:")
    print("   • При возникновении проблем обращайтесь к разработчику")
    print("   • Сохраняйте логи ошибок для анализа")
    print("   • Регулярно обновляйте систему")
    
    print(f"\n" + "=" * 60)
    print("✅ АВТОМАТИЧЕСКАЯ ПРОВЕРКА ЗАВЕРШЕНА")

if __name__ == "__main__":
    auto_check_system()






