#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для нормализации столбцов Статус_ВИК и ВИК в таблице logs_lnk

Задачи:
1. Если Статус_ВИК пустой, то брать не пустое значение из столбца ВИК
2. Привести значения Статус_ВИК к общему виду согласно маппингу
"""

import sqlite3
import logging
from datetime import datetime
import os
import sys

# Добавляем путь к корневой директории проекта
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

def get_database_connection():
    """
    Создает подключение к базе данных с автоматическим определением пути
    """
    import os
    
    # Получаем текущую директорию
    current_dir = os.getcwd()
    
    # Пробуем разные варианты путей для новой структуры проекта
    possible_paths = [
        # Если запускаем из корневой папки проекта (новая структура)
        os.path.join(current_dir, 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Если запускаем из папки scripts
        os.path.join(current_dir, '..', 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Если запускаем из папки scripts/data_loaders
        os.path.join(current_dir, '..', '..', 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Если запускаем из папки web/app
        os.path.join(current_dir, '..', '..', 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Если запускаем из папки desktop/qt_app
        os.path.join(current_dir, '..', '..', 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Старые пути для совместимости
        os.path.join(current_dir, 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        os.path.join(current_dir, '..', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
    ]
    
    for path in possible_paths:
        abs_path = os.path.abspath(path)
        if os.path.exists(abs_path):
            return sqlite3.connect(abs_path)
    
    # Если не нашли, возвращаем None
    return None

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/normalize_vik_status_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def normalize_vik_status():
    """
    Нормализация столбцов Статус_ВИК и ВИК в таблице logs_lnk
    """
    logger.info("🚀 Начинаем нормализацию столбцов Статус_ВИК и ВИК")
    
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
        logger.error("❌ Не удалось подключиться к базе данных")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Проверяем существование таблицы logs_lnk
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='logs_lnk'")
        if not cursor.fetchone():
            logger.error("❌ Таблица 'logs_lnk' не существует")
            return False
        
        # Проверяем существование столбцов
        cursor.execute("PRAGMA table_info(logs_lnk)")
        columns_info = cursor.fetchall()
        column_names = [col[1] for col in columns_info]
        
        if 'Статус_ВИК' not in column_names:
            logger.error("❌ Столбец 'Статус_ВИК' не найден в таблице logs_lnk")
            return False
        
        if 'ВИК' not in column_names:
            logger.error("❌ Столбец 'ВИК' не найден в таблице logs_lnk")
            return False
        
        logger.info("✅ Столбцы 'Статус_ВИК' и 'ВИК' найдены в таблице")
        
        # Получаем общее количество записей
        cursor.execute("SELECT COUNT(*) FROM logs_lnk")
        total_records = cursor.fetchone()[0]
        logger.info(f"📊 Всего записей в таблице logs_lnk: {total_records}")
        
        # Шаг 1: Заполняем пустые Статус_ВИК значениями из столбца ВИК
        logger.info("🔄 Шаг 1: Заполняем пустые Статус_ВИК значениями из столбца ВИК")
        
        cursor.execute("""
            SELECT COUNT(*) FROM logs_lnk 
            WHERE ("Статус_ВИК" IS NULL OR "Статус_ВИК" = '' OR "Статус_ВИК" = 'NULL')
            AND ("ВИК" IS NOT NULL AND "ВИК" != '' AND "ВИК" != 'NULL')
        """)
        empty_status_count = cursor.fetchone()[0]
        logger.info(f"📊 Найдено записей с пустым Статус_ВИК и непустым ВИК: {empty_status_count}")
        
        if empty_status_count > 0:
            cursor.execute("""
                UPDATE logs_lnk 
                SET "Статус_ВИК" = "ВИК"
                WHERE ("Статус_ВИК" IS NULL OR "Статус_ВИК" = '' OR "Статус_ВИК" = 'NULL')
                AND ("ВИК" IS NOT NULL AND "ВИК" != '' AND "ВИК" != 'NULL')
            """)
            updated_count = cursor.rowcount
            logger.info(f"✅ Обновлено записей: {updated_count}")
        else:
            logger.info("ℹ️ Нет записей для обновления на шаге 1")
        
        # Шаг 2: Нормализуем значения Статус_ВИК согласно маппингу
        logger.info("🔄 Шаг 2: Нормализуем значения Статус_ВИК согласно маппингу")
        
        # Получаем уникальные значения Статус_ВИК для анализа
        cursor.execute("""
            SELECT DISTINCT "Статус_ВИК" 
            FROM logs_lnk 
            WHERE "Статус_ВИК" IS NOT NULL AND "Статус_ВИК" != '' AND "Статус_ВИК" != 'NULL'
        """)
        unique_statuses = [row[0] for row in cursor.fetchall()]
        logger.info(f"📊 Уникальные значения Статус_ВИК: {unique_statuses}")
        
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
                    logger.info(f"✅ '{old_status}' → '{new_status}': {updated_count} записей")
                    total_normalized += updated_count
        
        logger.info(f"📊 Всего нормализовано записей: {total_normalized}")
        
        # Проверяем результат
        cursor.execute("""
            SELECT "Статус_ВИК", COUNT(*) as count
            FROM logs_lnk 
            WHERE "Статус_ВИК" IS NOT NULL AND "Статус_ВИК" != '' AND "Статус_ВИК" != 'NULL'
            GROUP BY "Статус_ВИК"
            ORDER BY count DESC
        """)
        final_statuses = cursor.fetchall()
        
        logger.info("📊 Финальная статистика по Статус_ВИК:")
        for status, count in final_statuses:
            logger.info(f"   '{status}': {count} записей")
        
        # Сохраняем изменения
        conn.commit()
        logger.info("✅ Изменения сохранены в базе данных")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка при нормализации: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
        return False
    
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    """Основная функция"""
    logger.info("=" * 60)
    logger.info("НОРМАЛИЗАЦИЯ СТОЛБЦОВ СТАТУС_ВИК И ВИК")
    logger.info("=" * 60)
    
    success = normalize_vik_status()
    
    if success:
        logger.info("✅ Нормализация завершена успешно")
    else:
        logger.error("❌ Нормализация завершена с ошибками")
    
    logger.info("=" * 60)

if __name__ == "__main__":
    main()

# Для запуска через GUI
def run_script():
    """Функция для запуска скрипта через GUI"""
    main()
