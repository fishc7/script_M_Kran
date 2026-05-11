#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для обновления данных в таблице weld_repair_log
Обновляет поля "Размер выборки (длина, ширина, глубина), мм" и "Способ и результаты контроля выборки"
на основе данных из таблицы logs_lnk по полю app_row_id
"""

import sqlite3
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/update_weld_repair_log.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Получает соединение с базой данных"""
    try:
        conn = sqlite3.connect('database/BD_Kingisepp/M_Kran_Kingesepp.db')
        conn.row_factory = sqlite3.Row  # Для доступа к полям по имени
        return conn
    except Exception as e:
        logger.error(f"Ошибка подключения к базе данных: {e}")
        return None

def update_weld_repair_log():
    """Обновляет данные в таблице weld_repair_log"""
    logger.info("Начинаем обновление данных в таблице weld_repair_log")
    
    conn = get_db_connection()
    if not conn:
        logger.error("Не удалось подключиться к базе данных")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Проверяем существование таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='weld_repair_log'")
        if not cursor.fetchone():
            logger.error("Таблица weld_repair_log не существует")
            return False
            
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='logs_lnk'")
        if not cursor.fetchone():
            logger.error("Таблица logs_lnk не существует")
            return False
        
        # Получаем все записи из weld_repair_log
        cursor.execute("SELECT app_row_id FROM weld_repair_log")
        weld_repair_records = cursor.fetchall()
        
        logger.info(f"Найдено {len(weld_repair_records)} записей в таблице weld_repair_log")
        
        updated_count = 0
        not_found_count = 0
        error_count = 0
        
        for record in weld_repair_records:
            app_row_id = record['app_row_id']
            
            try:
                # Получаем данные из logs_lnk для данного app_row_id
                cursor.execute('''
                    SELECT "Примечания_заключений", "Заявленны_виды_контроля"
                    FROM logs_lnk 
                    WHERE app_row_id = ?
                ''', (app_row_id,))
                
                logs_data = cursor.fetchone()
                
                if logs_data:
                    # Обновляем данные в weld_repair_log
                    cursor.execute('''
                        UPDATE weld_repair_log 
                        SET "Размер выборки (длина, ширина, глубина), мм" = ?,
                            "Способ и результаты контроля выборки" = ?
                        WHERE app_row_id = ?
                    ''', (
                        logs_data['Примечания_заключений'] or '',
                        logs_data['Заявленны_виды_контроля'] or '',
                        app_row_id
                    ))
                    
                    updated_count += 1
                    logger.info(f"Обновлена запись app_row_id={app_row_id}")
                    
                else:
                    not_found_count += 1
                    logger.warning(f"Не найдена соответствующая запись в logs_lnk для app_row_id={app_row_id}")
                    
            except Exception as e:
                error_count += 1
                logger.error(f"Ошибка при обновлении записи app_row_id={app_row_id}: {e}")
        
        # Сохраняем изменения
        conn.commit()
        
        logger.info(f"Обновление завершено:")
        logger.info(f"  - Обновлено записей: {updated_count}")
        logger.info(f"  - Не найдено в logs_lnk: {not_found_count}")
        logger.info(f"  - Ошибок: {error_count}")
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при обновлении данных: {e}")
        return False
    finally:
        conn.close()

def main():
    """Основная функция"""
    logger.info("=" * 50)
    logger.info("ЗАПУСК СКРИПТА ОБНОВЛЕНИЯ WELD_REPAIR_LOG")
    logger.info("=" * 50)
    
    success = update_weld_repair_log()
    
    if success:
        logger.info("Скрипт выполнен успешно")
    else:
        logger.error("Скрипт завершился с ошибками")
    
    logger.info("=" * 50)

if __name__ == "__main__":
    main()









