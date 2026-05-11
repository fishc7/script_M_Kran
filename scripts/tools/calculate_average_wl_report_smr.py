#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для расчета средних значений в таблице wl_report_smr

Функции:
1. Расчет среднего значения на каждый день
2. Расчет среднего значения на весь период
3. Экспорт результатов в Excel/CSV
"""

import os
import sys
import sqlite3
import pandas as pd
from datetime import datetime
import logging

# Добавляем пути для импорта модулей
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))  # scripts/tools -> scripts -> project_root

# Добавляем пути в sys.path
for path in [current_dir, project_root]:
    if path not in sys.path:
        sys.path.insert(0, path)

def get_database_path():
    """Возвращает путь к базе данных"""
    if getattr(sys, 'frozen', False):
        base_path = os.path.dirname(sys.executable)
    else:
        base_path = project_root
    
    return os.path.join(base_path, 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db')

def setup_logging():
    """Настройка логирования"""
    log_dir = os.path.join(project_root, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, 'calculate_average_wl_report_smr.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def get_table_columns(conn, table_name='wl_report_smr'):
    """Получает список столбцов таблицы"""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    return [col[1] for col in columns]

def get_numeric_columns(conn, table_name='wl_report_smr'):
    """
    Определяет потенциально числовые столбцы в таблице
    
    Args:
        conn: Подключение к базе данных
        table_name: Имя таблицы
        
    Returns:
        list: Список потенциально числовых столбцов
    """
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns_info = cursor.fetchall()
    
    numeric_columns = []
    for col_info in columns_info:
        col_name = col_info[1]
        col_type = col_info[2].upper()
        
        # Проверяем тип столбца
        if any(num_type in col_type for num_type in ['INT', 'REAL', 'FLOAT', 'DOUBLE', 'NUMERIC']):
            numeric_columns.append(col_name)
        else:
            # Для TEXT столбцов проверяем, можно ли преобразовать в число
            try:
                cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM {table_name} 
                    WHERE "{col_name}" IS NOT NULL 
                    AND "{col_name}" != ''
                    AND CAST("{col_name}" AS REAL) IS NOT NULL
                    LIMIT 100
                """)
                count = cursor.fetchone()[0]
                if count > 0:
                    numeric_columns.append(col_name)
            except:
                pass
    
    return numeric_columns

def calculate_daily_average(conn, numeric_column=None, date_column='Дата_сварки', logger=None):
    """
    Рассчитывает среднее значение на каждый день
    
    Args:
        conn: Подключение к базе данных
        numeric_column: Имя числового столбца (если None, считает количество записей)
        date_column: Имя столбца с датой
        logger: Логгер
        
    Returns:
        pd.DataFrame: DataFrame с результатами
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    try:
        if numeric_column is None:
            # Если столбец не указан, считаем количество записей на каждый день
            query = f"""
                SELECT 
                    "{date_column}" as Дата,
                    COUNT(*) as Количество_записей
                FROM wl_report_smr
                WHERE "{date_column}" IS NOT NULL
                GROUP BY "{date_column}"
                ORDER BY "{date_column}"
            """
        else:
            # Рассчитываем среднее значение числового столбца на каждый день
            query = f"""
                SELECT 
                    "{date_column}" as Дата,
                    COUNT(*) as Количество_записей,
                    AVG(CAST("{numeric_column}" AS REAL)) as Среднее_значение
                FROM wl_report_smr
                WHERE "{date_column}" IS NOT NULL
                    AND "{numeric_column}" IS NOT NULL
                    AND "{numeric_column}" != ''
                GROUP BY "{date_column}"
                ORDER BY "{date_column}"
            """
        
        df = pd.read_sql_query(query, conn)
        logger.info(f"Рассчитано средних значений на каждый день: {len(df)}")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при расчете среднего на каждый день: {e}")
        return pd.DataFrame()

def calculate_period_average(conn, numeric_column=None, date_column='Дата_сварки', logger=None):
    """
    Рассчитывает среднее значение на весь период
    
    Args:
        conn: Подключение к базе данных
        numeric_column: Имя числового столбца (если None, считает среднее количество записей на день)
        date_column: Имя столбца с датой
        logger: Логгер
        
    Returns:
        dict: Словарь с результатами
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    try:
        if numeric_column is None:
            # Если столбец не указан, считаем среднее количество записей на день
            query = f"""
                SELECT 
                    COUNT(*) as Общее_количество_записей,
                    COUNT(DISTINCT "{date_column}") as Количество_дней,
                    CAST(COUNT(*) AS REAL) / NULLIF(COUNT(DISTINCT "{date_column}"), 0) as Среднее_на_день,
                    MIN("{date_column}") as Первая_дата,
                    MAX("{date_column}") as Последняя_дата
                FROM wl_report_smr
                WHERE "{date_column}" IS NOT NULL
            """
        else:
            # Рассчитываем среднее значение числового столбца на весь период
            query = f"""
                SELECT 
                    COUNT(*) as Общее_количество_записей,
                    COUNT(DISTINCT "{date_column}") as Количество_дней,
                    AVG(CAST("{numeric_column}" AS REAL)) as Среднее_значение_за_период,
                    MIN(CAST("{numeric_column}" AS REAL)) as Минимальное_значение,
                    MAX(CAST("{numeric_column}" AS REAL)) as Максимальное_значение,
                    MIN("{date_column}") as Первая_дата,
                    MAX("{date_column}") as Последняя_дата
                FROM wl_report_smr
                WHERE "{date_column}" IS NOT NULL
                    AND "{numeric_column}" IS NOT NULL
                    AND "{numeric_column}" != ''
            """
        
        df = pd.read_sql_query(query, conn)
        if not df.empty:
            result = df.iloc[0].to_dict()
            logger.info(f"Рассчитано среднее значение на весь период")
            return result
        else:
            logger.warning("Не удалось рассчитать среднее значение на весь период")
            return {}
        
    except Exception as e:
        logger.error(f"Ошибка при расчете среднего на весь период: {e}")
        return {}

def export_to_excel(daily_df, period_dict, output_path=None, logger=None):
    """
    Экспортирует результаты в Excel файл
    
    Args:
        daily_df: DataFrame с результатами по дням
        period_dict: Словарь с результатами за период
        output_path: Путь к выходному файлу
        logger: Логгер
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    if output_path is None:
        output_dir = os.path.join(project_root, 'reports')
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(output_dir, f'average_calculation_{timestamp}.xlsx')
    
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Лист с результатами по дням
            if not daily_df.empty:
                daily_df.to_excel(writer, sheet_name='Среднее_по_дням', index=False)
            
            # Лист с результатами за период
            if period_dict:
                period_df = pd.DataFrame([period_dict])
                period_df.to_excel(writer, sheet_name='Среднее_за_период', index=False)
        
        logger.info(f"Результаты экспортированы в: {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"Ошибка при экспорте в Excel: {e}")
        return None

def main():
    """Основная функция"""
    logger = setup_logging()
    logger.info("=== НАЧАЛО РАСЧЕТА СРЕДНИХ ЗНАЧЕНИЙ ===")
    
    # Получаем путь к базе данных
    db_path = get_database_path()
    
    if not os.path.exists(db_path):
        logger.error(f"База данных не найдена: {db_path}")
        return
    
    logger.info(f"Подключение к базе данных: {db_path}")
    conn = sqlite3.connect(db_path)
    
    try:
        # Получаем список столбцов
        columns = get_table_columns(conn)
        logger.info(f"Найдено столбцов в таблице: {len(columns)}")
        
        # Определяем числовые столбцы
        numeric_columns = get_numeric_columns(conn)
        logger.info(f"Найдено потенциально числовых столбцов: {len(numeric_columns)}")
        if numeric_columns:
            logger.info(f"Числовые столбцы: {', '.join(numeric_columns[:10])}...")
        
        # Проверяем наличие столбца с датой
        date_columns = ['Дата_сварки', 'Date_welding', 'Дата сварки']
        date_column = None
        for col in date_columns:
            if col in columns:
                date_column = col
                break
        
        if date_column is None:
            logger.warning("Столбец с датой не найден. Используется 'Дата_сварки' по умолчанию.")
            date_column = 'Дата_сварки'
        else:
            logger.info(f"Используется столбец с датой: {date_column}")
        
        # 1. Расчет среднего на каждый день (количество записей)
        logger.info("\n--- Расчет среднего на каждый день (количество записей) ---")
        daily_count = calculate_daily_average(conn, numeric_column=None, date_column=date_column, logger=logger)
        if not daily_count.empty:
            logger.info(f"Примеры результатов по дням:")
            print(daily_count.head(10).to_string())
        
        # 2. Расчет среднего на весь период (среднее количество записей на день)
        logger.info("\n--- Расчет среднего на весь период (среднее количество записей на день) ---")
        period_count = calculate_period_average(conn, numeric_column=None, date_column=date_column, logger=logger)
        if period_count:
            logger.info("Результаты за весь период:")
            for key, value in period_count.items():
                logger.info(f"  {key}: {value}")
        
        # 3. Если есть числовые столбцы, рассчитываем среднее для них
        if numeric_columns:
            logger.info(f"\n--- Расчет среднего для числовых столбцов ---")
            for num_col in numeric_columns[:5]:  # Обрабатываем первые 5 числовых столбцов
                try:
                    logger.info(f"\nОбработка столбца: {num_col}")
                    
                    # Среднее на каждый день
                    daily_avg = calculate_daily_average(conn, numeric_column=num_col, date_column=date_column, logger=logger)
                    if not daily_avg.empty:
                        logger.info(f"  Среднее на каждый день: {len(daily_avg)} дней")
                    
                    # Среднее за период
                    period_avg = calculate_period_average(conn, numeric_column=num_col, date_column=date_column, logger=logger)
                    if period_avg:
                        avg_value = period_avg.get('Среднее_значение_за_период', 'N/A')
                        logger.info(f"  Среднее за период: {avg_value}")
                except Exception as e:
                    logger.warning(f"  Ошибка при обработке столбца {num_col}: {e}")
        
        # Экспорт результатов
        logger.info("\n--- Экспорт результатов ---")
        output_path = export_to_excel(daily_count, period_count, logger=logger)
        if output_path:
            logger.info(f"Результаты сохранены в: {output_path}")
        
        logger.info("\n=== РАСЧЕТ СРЕДНИХ ЗНАЧЕНИЙ ЗАВЕРШЕН ===")
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении скрипта: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        conn.close()

if __name__ == "__main__":
    main()

