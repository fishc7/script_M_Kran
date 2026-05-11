#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Функции нормализации для столбцов Статус_ВИК, ВИК, Статус_РК, РК

РЕФАКТОРИНГ: Использует единый модуль scripts.core.database для подключения к БД
"""

import os
import sys

# Добавляем путь к scripts модулю для правильного импорта
current_dir = os.path.dirname(os.path.abspath(__file__))
scripts_dir = os.path.dirname(current_dir)
if scripts_dir not in sys.path:
    sys.path.insert(0, scripts_dir)

# Импортируем единый модуль подключения к БД
try:
    # Пробуем импортировать через scripts.core
    from scripts.core.database import get_database_connection, DatabaseConnection
except ImportError:
    # Fallback: пробуем добавить путь к core напрямую
    core_dir = os.path.join(scripts_dir, 'core')
    if core_dir not in sys.path:
        sys.path.insert(0, core_dir)
    from core.database import get_database_connection, DatabaseConnection

def normalize_vik_status():
    """
    Нормализация столбцов Статус_ВИК и ВИК в таблице logs_lnk
    """
    print("🚀 Начинаем нормализацию столбцов Статус_ВИК и ВИК")
    
    # Маппинг для приведения статусов к общему виду
    status_mapping = {
        'Годен': 'Годен',
        'Н/П': 'Н/П',
        'Ремонт': 'Не годен',
        'Пересвет': 'Пересвет',
        'Не соответствует': 'Не соответствует',
        'Вырезать': 'Не годен',
        'Заявлен': 'Заказ отправлен'
    }
    
    conn = get_database_connection()
    if not conn:
        print("❌ Не удалось подключиться к базе данных")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Проверяем существование таблицы logs_lnk
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='logs_lnk'")
        if not cursor.fetchone():
            print("❌ Таблица 'logs_lnk' не существует")
            return False
        
        # Проверяем существование столбцов
        cursor.execute("PRAGMA table_info(logs_lnk)")
        columns_info = cursor.fetchall()
        column_names = [col[1] for col in columns_info]
        
        if 'Статус_ВИК' not in column_names:
            print("❌ Столбец 'Статус_ВИК' не найден в таблице logs_lnk")
            return False
        
        if 'ВИК' not in column_names:
            print("❌ Столбец 'ВИК' не найден в таблице logs_lnk")
            return False
        
        print("✅ Столбцы 'Статус_ВИК' и 'ВИК' найдены в таблице")
        
        # Получаем общее количество записей
        cursor.execute("SELECT COUNT(*) FROM logs_lnk")
        total_records = cursor.fetchone()[0]
        print(f"📊 Всего записей в таблице logs_lnk: {total_records}")
        
        # Шаг 1: Заполняем пустые Статус_ВИК значениями из столбца ВИК
        print("🔄 Шаг 1: Заполняем пустые Статус_ВИК значениями из столбца ВИК")
        
        cursor.execute("""
            SELECT COUNT(*) FROM logs_lnk 
            WHERE ("Статус_ВИК" IS NULL OR "Статус_ВИК" = '' OR "Статус_ВИК" = 'NULL')
            AND ("ВИК" IS NOT NULL AND "ВИК" != '' AND "ВИК" != 'NULL')
        """)
        empty_status_count = cursor.fetchone()[0]
        print(f"📊 Найдено записей с пустым Статус_ВИК и непустым ВИК: {empty_status_count}")
        
        if empty_status_count > 0:
            cursor.execute("""
                UPDATE logs_lnk 
                SET "Статус_ВИК" = "ВИК"
                WHERE ("Статус_ВИК" IS NULL OR "Статус_ВИК" = '' OR "Статус_ВИК" = 'NULL')
                AND ("ВИК" IS NOT NULL AND "ВИК" != '' AND "ВИК" != 'NULL')
            """)
            updated_count = cursor.rowcount
            print(f"✅ Обновлено записей: {updated_count}")
        else:
            print("ℹ️ Нет записей для обновления на шаге 1")
        
        # Шаг 2: Нормализуем значения Статус_ВИК согласно маппингу
        print("🔄 Шаг 2: Нормализуем значения Статус_ВИК согласно маппингу")
        
        # Получаем уникальные значения Статус_ВИК для анализа
        cursor.execute("""
            SELECT DISTINCT "Статус_ВИК" 
            FROM logs_lnk 
            WHERE "Статус_ВИК" IS NOT NULL AND "Статус_ВИК" != '' AND "Статус_ВИК" != 'NULL'
        """)
        unique_statuses = [row[0] for row in cursor.fetchall()]
        print(f"📊 Уникальные значения Статус_ВИК: {unique_statuses}")
        
        # Применяем маппинг
        total_normalized = 0
        for old_status, new_status in status_mapping.items():
            if old_status != new_status:  # Обновляем только если значения отличаются
                cursor.execute("""
                    UPDATE logs_lnk 
                    SET "Статус_ВИК" = ? 
                    WHERE "Статус_ВИК" = ?
                """, (new_status, old_status))
                updated_count = cursor.rowcount
                if updated_count > 0:
                    print(f"✅ '{old_status}' → '{new_status}': {updated_count} записей")
                    total_normalized += updated_count
        
        print(f"📊 Всего нормализовано записей: {total_normalized}")
        
        # Сохраняем изменения
        conn.commit()
        print("✅ Изменения сохранены в базе данных")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при нормализации: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
        return False
    
    finally:
        if 'conn' in locals():
            conn.close()

def normalize_rk_status():
    """
    Нормализация столбцов Статус_РК и РК в таблице logs_lnk
    """
    print("🚀 Начинаем нормализацию столбцов Статус_РК и РК")
    
    # Маппинг для приведения статусов к общему виду
    status_mapping = {
        'Годен': 'Годен',
        'Н/П': 'Н/П',
        'Ремонт': 'Не годен',
        'Пересвет': 'Пересвет',
        'Не соответствует': 'Не соответствует',
        'Вырезать': 'Не годен',
        'Заявлен': 'Заказ отправлен'
    }
    
    conn = get_database_connection()
    if not conn:
        print("❌ Не удалось подключиться к базе данных")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Проверяем существование таблицы logs_lnk
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='logs_lnk'")
        if not cursor.fetchone():
            print("❌ Таблица 'logs_lnk' не существует")
            return False
        
        # Проверяем существование столбцов
        cursor.execute("PRAGMA table_info(logs_lnk)")
        columns_info = cursor.fetchall()
        column_names = [col[1] for col in columns_info]
        
        if 'Статус_РК' not in column_names:
            print("❌ Столбец 'Статус_РК' не найден в таблице logs_lnk")
            return False
        
        if 'РК' not in column_names:
            print("❌ Столбец 'РК' не найден в таблице logs_lnk")
            return False
        
        print("✅ Столбцы 'Статус_РК' и 'РК' найдены в таблице")
        
        # Получаем общее количество записей
        cursor.execute("SELECT COUNT(*) FROM logs_lnk")
        total_records = cursor.fetchone()[0]
        print(f"📊 Всего записей в таблице logs_lnk: {total_records}")
        
        # Шаг 1: Заполняем пустые Статус_РК значениями из столбца РК
        print("🔄 Шаг 1: Заполняем пустые Статус_РК значениями из столбца РК")
        
        cursor.execute("""
            SELECT COUNT(*) FROM logs_lnk 
            WHERE ("Статус_РК" IS NULL OR "Статус_РК" = '' OR "Статус_РК" = 'NULL')
            AND ("РК" IS NOT NULL AND "РК" != '' AND "РК" != 'NULL')
        """)
        empty_status_count = cursor.fetchone()[0]
        print(f"📊 Найдено записей с пустым Статус_РК и непустым РК: {empty_status_count}")
        
        if empty_status_count > 0:
            cursor.execute("""
                UPDATE logs_lnk 
                SET "Статус_РК" = "РК"
                WHERE ("Статус_РК" IS NULL OR "Статус_РК" = '' OR "Статус_РК" = 'NULL')
                AND ("РК" IS NOT NULL AND "РК" != '' AND "РК" != 'NULL')
            """)
            updated_count = cursor.rowcount
            print(f"✅ Обновлено записей: {updated_count}")
        else:
            print("ℹ️ Нет записей для обновления на шаге 1")
        
        # Шаг 2: Нормализуем значения Статус_РК согласно маппингу
        print("🔄 Шаг 2: Нормализуем значения Статус_РК согласно маппингу")
        
        # Получаем уникальные значения Статус_РК для анализа
        cursor.execute("""
            SELECT DISTINCT "Статус_РК" 
            FROM logs_lnk 
            WHERE "Статус_РК" IS NOT NULL AND "Статус_РК" != '' AND "Статус_РК" != 'NULL'
        """)
        unique_statuses = [row[0] for row in cursor.fetchall()]
        print(f"📊 Уникальные значения Статус_РК: {unique_statuses}")
        
        # Применяем маппинг
        total_normalized = 0
        for old_status, new_status in status_mapping.items():
            if old_status != new_status:  # Обновляем только если значения отличаются
                cursor.execute("""
                    UPDATE logs_lnk 
                    SET "Статус_РК" = ? 
                    WHERE "Статус_РК" = ?
                """, (new_status, old_status))
                updated_count = cursor.rowcount
                if updated_count > 0:
                    print(f"✅ '{old_status}' → '{new_status}': {updated_count} записей")
                    total_normalized += updated_count
        
        print(f"📊 Всего нормализовано записей: {total_normalized}")
        
        # Сохраняем изменения
        conn.commit()
        print("✅ Изменения сохранены в базе данных")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при нормализации: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
        return False
    
    finally:
        if 'conn' in locals():
            conn.close()
