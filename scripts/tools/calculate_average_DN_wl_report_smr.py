#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для расчета средних значений столбца DN в таблице wl_report_smr

Функции:
1. Расчет среднего значения DN на каждый день
2. Расчет среднего значения DN по неделям
3. Расчет среднего значения DN по месяцам
4. Расчет среднего значения DN на весь период
5. Экспорт результатов в Excel в папку D:\Загрузка
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
    
    log_file = os.path.join(log_dir, 'calculate_average_DN_wl_report_smr.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def check_column_exists(conn, column_name='DN', table_name='wl_report_smr'):
    """Проверяет существование столбца в таблице"""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [col[1] for col in cursor.fetchall()]
    return column_name in columns

def calculate_daily_average_DN(conn, date_column='Дата_сварки', dn_column='DN', logger=None):
    """
    Рассчитывает среднее значение DN на каждый день
    
    Args:
        conn: Подключение к базе данных
        date_column: Имя столбца с датой
        dn_column: Имя столбца DN
        logger: Логгер
        
    Returns:
        pd.DataFrame: DataFrame с результатами
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    try:
        query = f"""
            SELECT 
                "{date_column}" as Дата,
                COUNT(*) as Количество_записей,
                AVG(CAST("{dn_column}" AS REAL)) as Среднее_DN,
                MIN(CAST("{dn_column}" AS REAL)) as Минимальное_DN,
                MAX(CAST("{dn_column}" AS REAL)) as Максимальное_DN,
                SUM(CAST("{dn_column}" AS REAL)) as Сумма_DN
            FROM wl_report_smr
            WHERE "{date_column}" IS NOT NULL
                AND "{dn_column}" IS NOT NULL
                AND "{dn_column}" != ''
                AND CAST("{dn_column}" AS REAL) IS NOT NULL
            GROUP BY "{date_column}"
            ORDER BY "{date_column}"
        """
        
        df = pd.read_sql_query(query, conn)
        
        # Добавляем столбец с днём недели
        if not df.empty:
            # Преобразуем дату в datetime
            df['Дата_dt'] = pd.to_datetime(df['Дата'])
            
            # Получаем день недели (0=понедельник, 6=воскресенье)
            days_of_week = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
            df['День_недели'] = df['Дата_dt'].dt.dayofweek.map(lambda x: days_of_week[x])
            
            # Переупорядочиваем столбцы: Дата, День_недели, остальные
            columns_order = ['Дата', 'День_недели', 'Количество_записей', 'Среднее_DN', 
                            'Минимальное_DN', 'Максимальное_DN', 'Сумма_DN']
            df = df[columns_order]
            
            # Удаляем временный столбец
            df = df.drop('Дата_dt', axis=1, errors='ignore')
        
        logger.info(f"Рассчитано средних значений DN на каждый день: {len(df)}")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при расчете среднего DN на каждый день: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()

def calculate_period_average_DN(conn, date_column='Дата_сварки', dn_column='DN', logger=None):
    """
    Рассчитывает среднее значение DN на весь период
    
    Args:
        conn: Подключение к базе данных
        date_column: Имя столбца с датой
        dn_column: Имя столбца DN
        logger: Логгер
        
    Returns:
        dict: Словарь с результатами
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    try:
        query = f"""
            SELECT 
                COUNT(*) as Общее_количество_записей,
                COUNT(DISTINCT "{date_column}") as Количество_дней,
                AVG(CAST("{dn_column}" AS REAL)) as Среднее_DN_за_период,
                MIN(CAST("{dn_column}" AS REAL)) as Минимальное_DN,
                MAX(CAST("{dn_column}" AS REAL)) as Максимальное_DN,
                SUM(CAST("{dn_column}" AS REAL)) as Сумма_DN,
                MIN("{date_column}") as Первая_дата,
                MAX("{date_column}") as Последняя_дата
            FROM wl_report_smr
            WHERE "{date_column}" IS NOT NULL
                AND "{dn_column}" IS NOT NULL
                AND "{dn_column}" != ''
                AND CAST("{dn_column}" AS REAL) IS NOT NULL
        """
        
        df = pd.read_sql_query(query, conn)
        if not df.empty:
            result = df.iloc[0].to_dict()
            logger.info(f"Рассчитано среднее значение DN на весь период")
            return result
        else:
            logger.warning("Не удалось рассчитать среднее значение DN на весь период")
            return {}
        
    except Exception as e:
        logger.error(f"Ошибка при расчете среднего DN на весь период: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {}

def calculate_monthly_average_DN(conn, date_column='Дата_сварки', dn_column='DN', logger=None):
    """
    Рассчитывает среднее значение DN по месяцам
    
    Args:
        conn: Подключение к базе данных
        date_column: Имя столбца с датой
        dn_column: Имя столбца DN
        logger: Логгер
        
    Returns:
        pd.DataFrame: DataFrame с результатами
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    try:
        query = f"""
            SELECT 
                strftime('%Y-%m', "{date_column}") as Год_Месяц,
                COUNT(*) as Количество_записей,
                COUNT(DISTINCT "{date_column}") as Количество_дней,
                AVG(CAST("{dn_column}" AS REAL)) as Среднее_DN,
                MIN(CAST("{dn_column}" AS REAL)) as Минимальное_DN,
                MAX(CAST("{dn_column}" AS REAL)) as Максимальное_DN,
                SUM(CAST("{dn_column}" AS REAL)) as Сумма_DN,
                CAST(COUNT(*) AS REAL) / COUNT(DISTINCT "{date_column}") as Среднее_записей_на_день
            FROM wl_report_smr
            WHERE "{date_column}" IS NOT NULL
                AND "{dn_column}" IS NOT NULL
                AND "{dn_column}" != ''
                AND CAST("{dn_column}" AS REAL) IS NOT NULL
            GROUP BY strftime('%Y-%m', "{date_column}")
            ORDER BY strftime('%Y-%m', "{date_column}")
        """
        
        df = pd.read_sql_query(query, conn)
        
        # Добавляем столбец с названием месяца и годом
        if not df.empty:
            # Преобразуем Год_Месяц в datetime для извлечения месяца и года
            df['Дата_dt'] = pd.to_datetime(df['Год_Месяц'] + '-01')
            
            # Извлекаем год
            df['Год'] = df['Дата_dt'].dt.year.astype(int)
            
            # Названия месяцев
            month_names = {
                1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель',
                5: 'Май', 6: 'Июнь', 7: 'Июль', 8: 'Август',
                9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'
            }
            
            df['Месяц'] = df['Дата_dt'].dt.month.map(month_names)
            
            # Переупорядочиваем столбцы: Год, Год_Месяц, Месяц, остальные
            columns_order = ['Год', 'Год_Месяц', 'Месяц', 'Количество_записей', 'Количество_дней',
                            'Среднее_DN', 'Минимальное_DN', 'Максимальное_DN', 
                            'Сумма_DN', 'Среднее_записей_на_день']
            df = df[columns_order]
            
            # Удаляем временный столбец
            df = df.drop('Дата_dt', axis=1, errors='ignore')
        
        logger.info(f"Рассчитано средних значений DN по месяцам: {len(df)}")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при расчете среднего DN по месяцам: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()

def calculate_weekly_average_DN(conn, date_column='Дата_сварки', dn_column='DN', logger=None):
    """
    Рассчитывает среднее значение DN по неделям (неделя начинается с понедельника)
    
    Args:
        conn: Подключение к базе данных
        date_column: Имя столбца с датой
        dn_column: Имя столбца DN
        logger: Логгер
        
    Returns:
        pd.DataFrame: DataFrame с результатами
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    try:
        # Сначала получаем все данные
        query = f"""
            SELECT 
                "{date_column}" as Дата,
                CAST("{dn_column}" AS REAL) as DN
            FROM wl_report_smr
            WHERE "{date_column}" IS NOT NULL
                AND "{dn_column}" IS NOT NULL
                AND "{dn_column}" != ''
                AND CAST("{dn_column}" AS REAL) IS NOT NULL
        """
        
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            return pd.DataFrame()
        
        # Преобразуем дату в datetime
        df['Дата'] = pd.to_datetime(df['Дата'])
        
        # Вычисляем начало недели (понедельник)
        # В pandas: понедельник = 0, воскресенье = 6
        # Вычитаем количество дней до понедельника
        df['День_недели'] = df['Дата'].dt.dayofweek  # 0 = понедельник, 6 = воскресенье
        df['Начало_недели'] = df['Дата'] - pd.to_timedelta(df['День_недели'], unit='d')
        
        # Вычисляем конец недели (воскресенье)
        df['Конец_недели'] = df['Начало_недели'] + pd.Timedelta(days=6)
        
        # Формируем идентификатор недели (год-неделя)
        df['Год'] = df['Начало_недели'].dt.year
        df['Неделя_года'] = df['Начало_недели'].dt.isocalendar().week
        df['Год_Неделя'] = df['Год'].astype(str) + '-W' + df['Неделя_года'].astype(str).str.zfill(2)
        
        # Группируем по неделям
        weekly_stats = df.groupby('Год_Неделя').agg({
            'Год': 'first',  # Берем год из начала недели
            'Начало_недели': 'min',
            'Конец_недели': 'max',
            'Дата': 'count',
            'DN': ['mean', 'min', 'max', 'sum']
        }).reset_index()
        
        # Переименовываем столбцы
        weekly_stats.columns = [
            'Год_Неделя',
            'Год',
            'Начало_недели',
            'Конец_недели',
            'Количество_записей',
            'Среднее_DN',
            'Минимальное_DN',
            'Максимальное_DN',
            'Сумма_DN'
        ]
        
        # Вычисляем количество уникальных дней в неделе
        days_per_week = df.groupby('Год_Неделя')['Дата'].nunique().reset_index()
        days_per_week.columns = ['Год_Неделя', 'Количество_дней']
        
        # Объединяем
        result_df = weekly_stats.merge(days_per_week, on='Год_Неделя')
        
        # Вычисляем среднее записей на день
        result_df['Среднее_записей_на_день'] = result_df['Количество_записей'] / result_df['Количество_дней']
        
        # Форматируем даты
        result_df['Начало_недели'] = result_df['Начало_недели'].dt.strftime('%Y-%m-%d')
        result_df['Конец_недели'] = result_df['Конец_недели'].dt.strftime('%Y-%m-%d')
        
        # Преобразуем год в целое число
        result_df['Год'] = result_df['Год'].astype(int)
        
        # Сортируем по дате начала недели
        result_df = result_df.sort_values('Начало_недели').reset_index(drop=True)
        
        # Выбираем нужные столбцы в правильном порядке
        result_df = result_df[[
            'Год',
            'Год_Неделя',
            'Начало_недели',
            'Конец_недели',
            'Количество_записей',
            'Количество_дней',
            'Среднее_DN',
            'Минимальное_DN',
            'Максимальное_DN',
            'Сумма_DN',
            'Среднее_записей_на_день'
        ]]
        
        logger.info(f"Рассчитано средних значений DN по неделям (неделя с понедельника): {len(result_df)}")
        return result_df
        
    except Exception as e:
        logger.error(f"Ошибка при расчете среднего DN по неделям: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()

def calculate_yearly_average_DN(conn, date_column='Дата_сварки', dn_column='DN', logger=None):
    """
    Рассчитывает среднее значение DN по годам
    
    Args:
        conn: Подключение к базе данных
        date_column: Имя столбца с датой
        dn_column: Имя столбца DN
        logger: Логгер
        
    Returns:
        pd.DataFrame: DataFrame с результатами
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    try:
        query = f"""
            SELECT 
                strftime('%Y', "{date_column}") as Год,
                COUNT(*) as Количество_записей,
                COUNT(DISTINCT "{date_column}") as Количество_дней,
                COUNT(DISTINCT strftime('%Y-%m', "{date_column}")) as Количество_месяцев,
                AVG(CAST("{dn_column}" AS REAL)) as Среднее_DN,
                MIN(CAST("{dn_column}" AS REAL)) as Минимальное_DN,
                MAX(CAST("{dn_column}" AS REAL)) as Максимальное_DN,
                SUM(CAST("{dn_column}" AS REAL)) as Сумма_DN,
                CAST(COUNT(*) AS REAL) / COUNT(DISTINCT "{date_column}") as Среднее_записей_на_день,
                MIN("{date_column}") as Первая_дата,
                MAX("{date_column}") as Последняя_дата
            FROM wl_report_smr
            WHERE "{date_column}" IS NOT NULL
                AND "{dn_column}" IS NOT NULL
                AND "{dn_column}" != ''
                AND CAST("{dn_column}" AS REAL) IS NOT NULL
            GROUP BY strftime('%Y', "{date_column}")
            ORDER BY strftime('%Y', "{date_column}")
        """
        
        df = pd.read_sql_query(query, conn)
        logger.info(f"Рассчитано средних значений DN по годам: {len(df)}")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при расчете среднего DN по годам: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()

def get_dn_statistics(conn, dn_column='DN', logger=None):
    """
    Получает общую статистику по столбцу DN
    
    Args:
        conn: Подключение к базе данных
        dn_column: Имя столбца DN
        logger: Логгер
        
    Returns:
        dict: Словарь со статистикой
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    try:
        query = f"""
            SELECT 
                COUNT(*) as Всего_записей,
                COUNT("{dn_column}") as Записей_с_DN,
                COUNT(*) - COUNT("{dn_column}") as Записей_без_DN
            FROM wl_report_smr
        """
        
        df = pd.read_sql_query(query, conn)
        if not df.empty:
            return df.iloc[0].to_dict()
        return {}
        
    except Exception as e:
        logger.error(f"Ошибка при получении статистики DN: {e}")
        return {}

def export_to_excel(daily_df, monthly_df, weekly_df, yearly_df, period_dict, stats_dict, output_dir='D:\\Загрузка', logger=None):
    """
    Экспортирует результаты в Excel файл
    
    Args:
        daily_df: DataFrame с результатами по дням
        monthly_df: DataFrame с результатами по месяцам
        weekly_df: DataFrame с результатами по неделям
        yearly_df: DataFrame с результатами по годам
        period_dict: Словарь с результатами за период
        stats_dict: Словарь со статистикой
        output_dir: Директория для сохранения файла
        logger: Логгер
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    # Создаем директорию, если её нет
    os.makedirs(output_dir, exist_ok=True)
    
    # Формируем имя файла с датой и временем
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_dir, f'Среднее_DN_wl_report_smr_{timestamp}.xlsx')
    
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Лист с результатами по дням
            if not daily_df.empty:
                daily_df.to_excel(writer, sheet_name='Среднее_DN_по_дням', index=False)
                logger.info(f"Добавлен лист 'Среднее_DN_по_дням': {len(daily_df)} строк")
            
            # Лист с результатами по неделям
            if not weekly_df.empty:
                weekly_df.to_excel(writer, sheet_name='Среднее_DN_по_неделям', index=False)
                logger.info(f"Добавлен лист 'Среднее_DN_по_неделям': {len(weekly_df)} строк")
            
            # Лист с результатами по месяцам
            if not monthly_df.empty:
                monthly_df.to_excel(writer, sheet_name='Среднее_DN_по_месяцам', index=False)
                logger.info(f"Добавлен лист 'Среднее_DN_по_месяцам': {len(monthly_df)} строк")
            
            # Лист с результатами по годам
            if not yearly_df.empty:
                yearly_df.to_excel(writer, sheet_name='Среднее_DN_по_годам', index=False)
                logger.info(f"Добавлен лист 'Среднее_DN_по_годам': {len(yearly_df)} строк")
            
            # Лист с результатами за период
            if period_dict:
                period_df = pd.DataFrame([period_dict])
                period_df.to_excel(writer, sheet_name='Среднее_DN_за_период', index=False)
                logger.info("Добавлен лист 'Среднее_DN_за_период'")
            
            # Лист со статистикой
            if stats_dict:
                stats_df = pd.DataFrame([stats_dict])
                stats_df.to_excel(writer, sheet_name='Статистика_DN', index=False)
                logger.info("Добавлен лист 'Статистика_DN'")
        
        logger.info(f"Результаты успешно экспортированы в: {output_path}")
        print(f"\n✓ Файл сохранен: {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"Ошибка при экспорте в Excel: {e}")
        import traceback
        logger.error(traceback.format_exc())
        print(f"\n✗ Ошибка при сохранении файла: {e}")
        return None

def main():
    """Основная функция"""
    logger = setup_logging()
    logger.info("=== НАЧАЛО РАСЧЕТА СРЕДНИХ ЗНАЧЕНИЙ DN ===")
    print("=" * 60)
    print("РАСЧЕТ СРЕДНИХ ЗНАЧЕНИЙ СТОЛБЦА DN")
    print("=" * 60)
    
    # Получаем путь к базе данных
    db_path = get_database_path()
    
    if not os.path.exists(db_path):
        error_msg = f"База данных не найдена: {db_path}"
        logger.error(error_msg)
        print(f"\n✗ {error_msg}")
        return
    
    logger.info(f"Подключение к базе данных: {db_path}")
    print(f"\n📁 База данных: {db_path}")
    
    conn = sqlite3.connect(db_path)
    
    try:
        # Проверяем наличие столбца DN
        dn_column = 'DN'
        date_column = 'Дата_сварки'
        
        if not check_column_exists(conn, dn_column):
            error_msg = f"Столбец '{dn_column}' не найден в таблице wl_report_smr"
            logger.error(error_msg)
            print(f"\n✗ {error_msg}")
            
            # Показываем доступные столбцы
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(wl_report_smr)")
            columns = [col[1] for col in cursor.fetchall()]
            print(f"\nДоступные столбцы в таблице (первые 20):")
            for i, col in enumerate(columns[:20], 1):
                print(f"  {i}. {col}")
            if len(columns) > 20:
                print(f"  ... и еще {len(columns) - 20} столбцов")
            return
        
        logger.info(f"Столбец '{dn_column}' найден")
        print(f"✓ Столбец '{dn_column}' найден")
        
        # Проверяем наличие столбца с датой
        date_columns = ['Дата_сварки', 'Date_welding', 'Дата сварки']
        date_column = None
        for col in date_columns:
            if check_column_exists(conn, col):
                date_column = col
                break
        
        if date_column is None:
            error_msg = "Столбец с датой не найден"
            logger.error(error_msg)
            print(f"\n✗ {error_msg}")
            return
        
        logger.info(f"Используется столбец с датой: {date_column}")
        print(f"✓ Столбец с датой: {date_column}")
        
        # Получаем статистику по DN
        print("\n📊 Получение статистики по DN...")
        stats_dict = get_dn_statistics(conn, dn_column, logger)
        if stats_dict:
            print(f"  Всего записей: {stats_dict.get('Всего_записей', 'N/A')}")
            print(f"  Записей с DN: {stats_dict.get('Записей_с_DN', 'N/A')}")
            print(f"  Записей без DN: {stats_dict.get('Записей_без_DN', 'N/A')}")
        
        # 1. Расчет среднего DN на каждый день
        print("\n📅 Расчет среднего DN на каждый день...")
        daily_df = calculate_daily_average_DN(conn, date_column, dn_column, logger)
        if not daily_df.empty:
            print(f"✓ Рассчитано для {len(daily_df)} дней")
            print("\nПримеры результатов (первые 5 дней):")
            print(daily_df.head().to_string(index=False))
        else:
            print("⚠ Нет данных для расчета среднего на каждый день")
        
        # 2. Расчет среднего DN по неделям
        print("\n📅 Расчет среднего DN по неделям...")
        weekly_df = calculate_weekly_average_DN(conn, date_column, dn_column, logger)
        if not weekly_df.empty:
            print(f"✓ Рассчитано для {len(weekly_df)} недель")
            print("\nПримеры результатов (первые 3 недели):")
            print(weekly_df.head(3).to_string(index=False))
        else:
            print("⚠ Нет данных для расчета среднего по неделям")
        
        # 3. Расчет среднего DN по месяцам
        print("\n📅 Расчет среднего DN по месяцам...")
        monthly_df = calculate_monthly_average_DN(conn, date_column, dn_column, logger)
        if not monthly_df.empty:
            print(f"✓ Рассчитано для {len(monthly_df)} месяцев")
            print("\nПримеры результатов (первые 3 месяца):")
            print(monthly_df.head(3).to_string(index=False))
        else:
            print("⚠ Нет данных для расчета среднего по месяцам")
        
        # 4. Расчет среднего DN по годам
        print("\n📅 Расчет среднего DN по годам...")
        yearly_df = calculate_yearly_average_DN(conn, date_column, dn_column, logger)
        if not yearly_df.empty:
            print(f"✓ Рассчитано для {len(yearly_df)} лет")
            print("\nРезультаты по годам:")
            print(yearly_df.to_string(index=False))
        else:
            print("⚠ Нет данных для расчета среднего по годам")
        
        # 5. Расчет среднего DN на весь период
        print("\n📊 Расчет среднего DN на весь период...")
        period_dict = calculate_period_average_DN(conn, date_column, dn_column, logger)
        if period_dict:
            print("✓ Результаты за весь период:")
            print(f"  Общее количество записей: {period_dict.get('Общее_количество_записей', 'N/A')}")
            print(f"  Количество дней: {period_dict.get('Количество_дней', 'N/A')}")
            avg_dn = period_dict.get('Среднее_DN_за_период', None)
            if avg_dn is not None:
                print(f"  Среднее DN за период: {avg_dn:.2f}")
            print(f"  Минимальное DN: {period_dict.get('Минимальное_DN', 'N/A')}")
            print(f"  Максимальное DN: {period_dict.get('Максимальное_DN', 'N/A')}")
            print(f"  Первая дата: {period_dict.get('Первая_дата', 'N/A')}")
            print(f"  Последняя дата: {period_dict.get('Последняя_дата', 'N/A')}")
        else:
            print("⚠ Нет данных для расчета среднего за период")
        
        # 6. Экспорт в Excel
        print("\n💾 Экспорт результатов в Excel...")
        output_path = export_to_excel(daily_df, monthly_df, weekly_df, yearly_df, period_dict, stats_dict, output_dir='D:\\Загрузка', logger=logger)
        
        if output_path:
            logger.info("\n=== РАСЧЕТ СРЕДНИХ ЗНАЧЕНИЙ DN ЗАВЕРШЕН УСПЕШНО ===")
            print("\n" + "=" * 60)
            print("✓ РАСЧЕТ ЗАВЕРШЕН УСПЕШНО")
            print("=" * 60)
        else:
            logger.error("\n=== ОШИБКА ПРИ ЭКСПОРТЕ ===")
            print("\n✗ Ошибка при экспорте результатов")
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении скрипта: {e}")
        import traceback
        logger.error(traceback.format_exc())
        print(f"\n✗ Ошибка: {e}")
    finally:
        conn.close()
        logger.info("Соединение с базой данных закрыто")

if __name__ == "__main__":
    main()

