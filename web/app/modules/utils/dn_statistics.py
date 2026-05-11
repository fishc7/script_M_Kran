#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Модуль для получения статистики по средним значениям DN в таблице wl_report_smr

Функции:
- get_dn_statistics_data() - получение всех статистических данных
"""

import pandas as pd
import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

def get_moscow_time():
    """Получает текущее время в часовом поясе Москвы (UTC+3)"""
    try:
        # Создаем часовой пояс Москвы (UTC+3)
        moscow_tz = timezone(timedelta(hours=3))
        moscow_time = datetime.now(moscow_tz)
        return pd.Timestamp(moscow_time)
    except Exception as e:
        logger.warning(f"Не удалось получить время Москвы, используется системное время: {e}")
        # Используем системное время
        return pd.Timestamp.now()


def get_dn_statistics_data(get_db_connection_func):
    """
    Получает статистику DN для отображения на странице
    
    Args:
        get_db_connection_func: Функция для получения подключения к БД
        
    Returns:
        dict: Словарь со статистикой или None в случае ошибки
    """
    try:
        # Получаем подключение к БД
        conn = get_db_connection_func()
        if not conn:
            logger.error("Не удалось получить подключение к базе данных")
            return None
        
        # Проверяем и создаем таблицы статистики, если их нет
        try:
            ensure_dn_statistics_tables_exist(conn)
        except Exception as e:
            logger.error(f"Ошибка при проверке таблиц статистики DN: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # Продолжаем выполнение, даже если не удалось создать таблицы
            # Но логируем ошибку для диагностики
        
        # Проверяем существование таблицы
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='wl_report_smr'")
        table_exists = cursor.fetchone()
        
        if not table_exists:
            logger.error("Таблица wl_report_smr не найдена в базе данных")
            conn.close()
            return None
        
        # Проверяем наличие столбцов
        cursor.execute("PRAGMA table_info(wl_report_smr)")
        columns_info = cursor.fetchall()
        # Извлекаем имена столбцов (row_factory = sqlite3.Row, поэтому используем ['name'])
        try:
            # Пробуем как Row объект (словарь)
            columns = [col['name'] for col in columns_info]
        except (KeyError, TypeError):
            # Если не Row, пробуем как кортеж (индекс 1 - это имя столбца)
            try:
                columns = [col[1] for col in columns_info]
            except (IndexError, TypeError) as e:
                logger.error(f"Не удалось извлечь имена столбцов: {e}")
                logger.error(f"Тип первого элемента: {type(columns_info[0]) if columns_info else 'пусто'}")
                conn.close()
                return None
        
        if 'DN' not in columns:
            logger.error(f"Столбец 'DN' не найден в таблице wl_report_smr. Доступные столбцы: {', '.join(columns[:20])}")
            conn.close()
            return None
        
        if 'Дата_сварки' not in columns:
            logger.error(f"Столбец 'Дата_сварки' не найден в таблице wl_report_smr. Доступные столбцы: {', '.join(columns[:20])}")
            conn.close()
            return None
        
        date_column = 'Дата_сварки'
        dn_column = 'DN'
        
        stats = {}
        
        # Сначала получаем среднее за весь период (будет использоваться для всех разделов)
        try:
            period_avg_query = f"""
                SELECT 
                    AVG(CAST("{dn_column}" AS REAL)) as Среднее_DN_за_период
                FROM wl_report_smr
                WHERE "{date_column}" IS NOT NULL
                    AND "{dn_column}" IS NOT NULL
                    AND "{dn_column}" != ''
                    AND CAST("{dn_column}" AS REAL) IS NOT NULL
            """
            period_avg_df = pd.read_sql_query(period_avg_query, conn)
            period_avg = None
            if not period_avg_df.empty and period_avg_df.iloc[0]['Среднее_DN_за_период'] is not None:
                period_avg = round(float(period_avg_df.iloc[0]['Среднее_DN_за_период']), 2)
            else:
                logger.warning("Не удалось получить среднее DN за период")
        except Exception as e:
            logger.error(f"Ошибка при получении среднего за период: {e}")
            import traceback
            logger.error(traceback.format_exc())
            period_avg = None
        
        # 1. Последнее значение по дням
        try:
            daily_query = f"""
                SELECT 
                    "{date_column}" as Дата,
                    COUNT(*) as Количество_записей,
                    MIN(CAST("{dn_column}" AS REAL)) as Минимальное_DN,
                    MAX(CAST("{dn_column}" AS REAL)) as Максимальное_DN,
                    SUM(CAST("{dn_column}" AS REAL)) as Сумма_DN
                FROM wl_report_smr
                WHERE "{date_column}" IS NOT NULL
                    AND "{dn_column}" IS NOT NULL
                    AND "{dn_column}" != ''
                    AND CAST("{dn_column}" AS REAL) IS NOT NULL
                GROUP BY "{date_column}"
                ORDER BY "{date_column}" DESC
                LIMIT 1
            """
            daily_df = pd.read_sql_query(daily_query, conn)
            if not daily_df.empty:
                date_dt = pd.to_datetime(daily_df.iloc[0]['Дата'])
                days_of_week = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
                day_of_week = days_of_week[date_dt.dayofweek]
                # Вычисляем среднее DN за этот день
                daily_sum = float(daily_df.iloc[0]['Сумма_DN'])
                daily_count = int(daily_df.iloc[0]['Количество_записей'])
                daily_avg = round(daily_sum / daily_count, 2) if daily_count > 0 else None
                
                stats['last_daily'] = {
                    'дата': daily_df.iloc[0]['Дата'],
                    'день_недели': day_of_week,
                    'количество_записей': daily_count,
                    'среднее_dn': daily_avg,  # Среднее за этот день
                    'минимальное_dn': round(float(daily_df.iloc[0]['Минимальное_DN']), 2),
                    'максимальное_dn': round(float(daily_df.iloc[0]['Максимальное_DN']), 2),
                    'сумма_dn': round(daily_sum, 2)  # Сумма за последний день
                }
            else:
                logger.warning("Не найдено данных для последнего дня")
        except Exception as e:
            logger.error(f"Ошибка при получении последнего значения по дням: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
        # 2. Последнее значение по неделям (используем pandas для правильной группировки)
        # Если текущая неделя не закончилась, берем предыдущую неделю
        try:
            weekly_query = f"""
                SELECT 
                    "{date_column}" as Дата,
                    CAST("{dn_column}" AS REAL) as DN
                FROM wl_report_smr
                WHERE "{date_column}" IS NOT NULL
                    AND "{dn_column}" IS NOT NULL
                    AND "{dn_column}" != ''
                    AND CAST("{dn_column}" AS REAL) IS NOT NULL
                ORDER BY "{date_column}" DESC
            """
            weekly_df = pd.read_sql_query(weekly_query, conn)
            if not weekly_df.empty:
                weekly_df['Дата'] = pd.to_datetime(weekly_df['Дата'])
                weekly_df['День_недели'] = weekly_df['Дата'].dt.dayofweek
                
                # Неделя начинается в понедельник в 8:00 по Москве
                # Если запись в понедельник до 8:00, она относится к предыдущей неделе
                def get_week_start(row):
                    date = row['Дата']
                    day_of_week = row['День_недели']
                    
                    # Вычисляем понедельник этой недели (00:00)
                    monday = date - pd.to_timedelta(day_of_week, unit='d')
                    monday_normalized = monday.normalize()
                    
                    # Если это понедельник и время < 8:00, берем предыдущий понедельник
                    if day_of_week == 0 and date.hour < 8:
                        monday_normalized = monday_normalized - pd.Timedelta(days=7)
                    
                    # Устанавливаем время начала недели: понедельник 8:00
                    week_start = monday_normalized.replace(hour=8, minute=0, second=0, microsecond=0)
                    return week_start
                
                weekly_df['Начало_недели'] = weekly_df.apply(get_week_start, axis=1)
                # Конец недели: воскресенье 17:00 (начало + 6 дней + время до 17:00)
                weekly_df['Конец_недели'] = weekly_df['Начало_недели'] + pd.Timedelta(days=6, hours=9)  # Понедельник 8:00 + 6 дней + 9 часов = Воскресенье 17:00
                
                weekly_df['Год'] = weekly_df['Начало_недели'].dt.year
                weekly_df['Неделя_года'] = weekly_df['Начало_недели'].dt.isocalendar().week
                weekly_df['Год_Неделя'] = weekly_df['Год'].astype(str) + '-W' + weekly_df['Неделя_года'].astype(str).str.zfill(2)
                
                # Группируем по неделям
                weekly_groups = weekly_df.groupby('Год_Неделя').agg({
                    'Начало_недели': 'min',
                    'Конец_недели': 'max',
                    'Дата': 'max',
                    'DN': ['min', 'max', 'sum', 'count']
                }).reset_index()
                weekly_groups.columns = ['Год_Неделя', 'Начало_недели', 'Конец_недели', 'Последняя_дата', 'Минимальное_DN', 'Максимальное_DN', 'Сумма_DN', 'Количество_записей']
                weekly_groups = weekly_groups.sort_values('Начало_недели')
                
                # Проверяем, закончилась ли последняя неделя
                # Используем время Москвы для определения текущей даты и времени
                # Неделя заканчивается в воскресенье в 17:00 по Москве
                moscow_now = get_moscow_time()
                # Получаем текущее время в Москве как tz-naive для сравнения
                if isinstance(moscow_now, pd.Timestamp):
                    if moscow_now.tz is not None:
                        # Убираем timezone, сохраняя дату и время
                        now_naive = moscow_now.tz_localize(None)
                    else:
                        now_naive = moscow_now
                else:
                    # datetime объект
                    now_naive = moscow_now
                    if now_naive.tzinfo is not None:
                        # Убираем timezone, но сохраняем дату и время
                        now_naive = now_naive.replace(tzinfo=None)
                
                last_week_row = weekly_groups.iloc[-1]
                # Конец недели уже установлен как воскресенье 17:00 в функции get_week_start
                last_week_end = pd.to_datetime(last_week_row['Конец_недели'])
                # Приводим к tz-naive для сравнения (убираем timezone)
                if last_week_end.tz is not None:
                    last_week_end = last_week_end.tz_localize(None)
                
                # Неделя считается завершенной, если её конец (воскресенье 17:00) был в прошлом
                # Если конец недели >= сейчас, значит неделя еще не закончилась
                if last_week_end >= now_naive:
                    # Ищем предыдущую завершенную неделю (ищем с конца списка назад)
                    found_completed_week = False
                    for idx in range(len(weekly_groups) - 2, -1, -1):  # Идем с предпоследней недели назад
                        prev_week_row = weekly_groups.iloc[idx]
                        # Конец недели уже установлен как воскресенье 17:00
                        prev_week_end = pd.to_datetime(prev_week_row['Конец_недели'])
                        if prev_week_end.tz is not None:
                            prev_week_end = prev_week_end.tz_localize(None)
                        if prev_week_end < now_naive:  # Эта неделя завершена (конец был в прошлом)
                            last_week_row = prev_week_row
                            found_completed_week = True
                            break
                    
                    if not found_completed_week:
                        logger.warning("Не найдено завершенных недель, пропускаем отображение недельной статистики")
                        # Если не нашли завершенную неделю, не показываем статистику
                        last_week_row = None
                
                if not weekly_groups.empty and last_week_row is not None:
                    # Вычисляем среднее DN за эту неделю
                    weekly_sum = float(last_week_row['Сумма_DN'])
                    weekly_count = int(last_week_row['Количество_записей'])
                    weekly_avg = round(weekly_sum / weekly_count, 2) if weekly_count > 0 else None
                    
                    stats['last_weekly'] = {
                        'год': int(last_week_row['Год_Неделя'].split('-W')[0]),
                        'неделя': last_week_row['Год_Неделя'],
                        'начало_недели': pd.to_datetime(last_week_row['Начало_недели']).strftime('%Y-%m-%d'),
                        'конец_недели': pd.to_datetime(last_week_row['Конец_недели']).strftime('%Y-%m-%d'),
                        'количество_записей': weekly_count,
                        'среднее_dn': weekly_avg,  # Среднее за эту неделю
                        'минимальное_dn': round(float(last_week_row['Минимальное_DN']), 2),
                        'максимальное_dn': round(float(last_week_row['Максимальное_DN']), 2),
                        'сумма_dn': round(weekly_sum, 2)  # Сумма за последнюю завершенную неделю
                    }
            else:
                logger.warning("Не найдено данных для недель")
        except Exception as e:
            logger.error(f"Ошибка при получении последнего значения по неделям: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
        # 2.5. Последнее значение по месяцам
        # Если текущий месяц не закончился, берем предыдущий месяц
        try:
            monthly_query = f"""
                SELECT 
                    strftime('%Y-%m', "{date_column}") as Год_Месяц,
                    COUNT(*) as Количество_записей,
                    COUNT(DISTINCT "{date_column}") as Количество_дней,
                    MIN(CAST("{dn_column}" AS REAL)) as Минимальное_DN,
                    MAX(CAST("{dn_column}" AS REAL)) as Максимальное_DN,
                    SUM(CAST("{dn_column}" AS REAL)) as Сумма_DN,
                    MAX("{date_column}") as Последняя_дата
                FROM wl_report_smr
                WHERE "{date_column}" IS NOT NULL
                    AND "{dn_column}" IS NOT NULL
                    AND "{dn_column}" != ''
                    AND CAST("{dn_column}" AS REAL) IS NOT NULL
                GROUP BY strftime('%Y-%m', "{date_column}")
                ORDER BY strftime('%Y-%m', "{date_column}") DESC
            """
            monthly_df = pd.read_sql_query(monthly_query, conn)
            if not monthly_df.empty:
                # Проверяем, закончился ли текущий месяц
                # Используем время Москвы для определения текущей даты
                today = get_moscow_time()
                current_month_str = today.strftime('%Y-%m')
                last_month_row = monthly_df.iloc[0]
                last_month_str = last_month_row['Год_Месяц']
                
                # Если последний месяц в данных - это текущий месяц, проверяем, закончился ли он
                if last_month_str == current_month_str:
                    # Текущий месяц еще идет, берем предыдущий месяц
                    if len(monthly_df) > 1:
                        last_month_row = monthly_df.iloc[1]  # Берем предыдущий месяц
                        last_month_str = last_month_row['Год_Месяц']
                    # Если только один месяц и он текущий, все равно берем его
                
                # Преобразуем Год_Месяц в datetime для извлечения месяца и года
                date_dt = pd.to_datetime(last_month_str + '-01')
                
                # Названия месяцев
                month_names = {
                    1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель',
                    5: 'Май', 6: 'Июнь', 7: 'Июль', 8: 'Август',
                    9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'
                }
                
                month_name = month_names[date_dt.month]
                
                # Вычисляем среднее DN за этот месяц
                monthly_sum = float(last_month_row['Сумма_DN'])
                monthly_count = int(last_month_row['Количество_записей'])
                monthly_avg = round(monthly_sum / monthly_count, 2) if monthly_count > 0 else None
                
                stats['last_monthly'] = {
                    'год': int(date_dt.year),
                    'год_месяц': last_month_str,
                    'месяц': month_name,
                    'количество_записей': monthly_count,
                    'количество_дней': int(last_month_row['Количество_дней']),
                    'среднее_dn': monthly_avg,  # Среднее за этот месяц
                    'минимальное_dn': round(float(last_month_row['Минимальное_DN']), 2),
                    'максимальное_dn': round(float(last_month_row['Максимальное_DN']), 2),
                    'сумма_dn': round(monthly_sum, 2)  # Сумма за последний завершенный месяц
                }
            else:
                logger.warning("Не найдено данных для месяцев")
        except Exception as e:
            logger.error(f"Ошибка при получении последнего значения по месяцам: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
        # 3. По годам - все значения
        try:
            yearly_query = f"""
                SELECT 
                    strftime('%Y', "{date_column}") as Год,
                    COUNT(*) as Количество_записей,
                    COUNT(DISTINCT "{date_column}") as Количество_дней,
                    COUNT(DISTINCT strftime('%Y-%m', "{date_column}")) as Количество_месяцев,
                    AVG(CAST("{dn_column}" AS REAL)) as Среднее_DN,
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
                GROUP BY strftime('%Y', "{date_column}")
                ORDER BY strftime('%Y', "{date_column}")
            """
            yearly_df = pd.read_sql_query(yearly_query, conn)
            if not yearly_df.empty:
                stats['yearly'] = yearly_df.to_dict('records')
                # Преобразуем числовые значения
                for record in stats['yearly']:
                    record['Год'] = int(record['Год'])
                    record['Количество_записей'] = int(record['Количество_записей'])
                    record['Количество_дней'] = int(record['Количество_дней'])
                    record['Количество_месяцев'] = int(record['Количество_месяцев'])
                    record['Среднее_DN'] = round(float(record['Среднее_DN']), 2) if record['Среднее_DN'] else None
                    record['Минимальное_DN'] = round(float(record['Минимальное_DN']), 2) if record['Минимальное_DN'] else None
                    record['Максимальное_DN'] = round(float(record['Максимальное_DN']), 2) if record['Максимальное_DN'] else None
                    record['Сумма_DN'] = round(float(record['Сумма_DN']), 2) if record['Сумма_DN'] else None
            else:
                logger.warning("Не найдено данных для годов")
        except Exception as e:
            logger.error(f"Ошибка при получении данных по годам: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
        # 4. За весь период
        try:
            period_query = f"""
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
            period_df = pd.read_sql_query(period_query, conn)
            if not period_df.empty:
                stats['period'] = {
                    'общее_количество_записей': int(period_df.iloc[0]['Общее_количество_записей']),
                    'количество_дней': int(period_df.iloc[0]['Количество_дней']),
                    'среднее_dn_за_период': round(float(period_df.iloc[0]['Среднее_DN_за_период']), 2) if period_df.iloc[0]['Среднее_DN_за_период'] else None,
                    'минимальное_dn': round(float(period_df.iloc[0]['Минимальное_DN']), 2) if period_df.iloc[0]['Минимальное_DN'] else None,
                    'максимальное_dn': round(float(period_df.iloc[0]['Максимальное_DN']), 2) if period_df.iloc[0]['Максимальное_DN'] else None,
                    'сумма_dn': round(float(period_df.iloc[0]['Сумма_DN']), 2) if period_df.iloc[0]['Сумма_DN'] else None,
                    'первая_дата': period_df.iloc[0]['Первая_дата'],
                    'последняя_дата': period_df.iloc[0]['Последняя_дата']
                }
            else:
                logger.warning("Не найдено данных за весь период")
        except Exception as e:
            logger.error(f"Ошибка при получении статистики за весь период: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
        # Проверяем, есть ли хотя бы какие-то данные
        if len(stats) == 0:
            logger.warning("Не удалось получить ни одного раздела статистики")
            conn.close()
            return None
        
        # Сохраняем статистику в базу данных (перед закрытием соединения)
        try:
            save_dn_statistics_to_db(conn, stats)
        except Exception as e:
            logger.error(f"Ошибка при сохранении статистики в БД: {e}")
            # Не прерываем выполнение, просто логируем ошибку
        
        conn.close()
        return stats
        
    except Exception as e:
        logger.error(f'Критическая ошибка получения статистики DN: {e}')
        import traceback
        error_trace = traceback.format_exc()
        logger.error(f"Полный стек ошибки:\n{error_trace}")
        
        # Закрываем соединение в случае ошибки
        try:
            if 'conn' in locals() and conn:
                conn.close()
        except:
            pass
        
        return None


def ensure_dn_statistics_tables_exist(conn):
    """
    Проверяет существование таблиц статистики DN и создает их, если они не существуют
    
    Args:
        conn: Подключение к БД
    """
    cursor = conn.cursor()
    
    try:
        # Проверяем существование всех таблиц
        cursor.execute('''
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name IN (
                'dn_statistics_daily', 
                'dn_statistics_weekly', 
                'dn_statistics_monthly', 
                'dn_statistics_yearly', 
                'dn_statistics_period'
            )
        ''')
        existing_tables = {row[0] for row in cursor.fetchall()}
        
        required_tables = {
            'dn_statistics_daily',
            'dn_statistics_weekly',
            'dn_statistics_monthly',
            'dn_statistics_yearly',
            'dn_statistics_period'
        }
        
        # Если не все таблицы существуют, создаем их
        if existing_tables != required_tables:
            logger.warning(f"Не все таблицы статистики DN существуют. Создаю недостающие...")
            
            # 1. Таблица dn_statistics_daily
            if 'dn_statistics_daily' not in existing_tables:
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS dn_statistics_daily (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        дата DATE NOT NULL UNIQUE,
                        день_недели TEXT,
                        количество_записей INTEGER NOT NULL,
                        среднее_dn REAL,
                        минимальное_dn REAL,
                        максимальное_dn REAL,
                        сумма_dn REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_dn_statistics_daily_дата 
                    ON dn_statistics_daily(дата)
                ''')
            
            # 2. Таблица dn_statistics_weekly
            if 'dn_statistics_weekly' not in existing_tables:
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS dn_statistics_weekly (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        год INTEGER NOT NULL,
                        неделя TEXT NOT NULL,
                        начало_недели DATE NOT NULL,
                        конец_недели DATE NOT NULL,
                        количество_записей INTEGER NOT NULL,
                        среднее_dn REAL,
                        минимальное_dn REAL,
                        максимальное_dn REAL,
                        сумма_dn REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(год, неделя)
                    )
                ''')
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_dn_statistics_weekly_период 
                    ON dn_statistics_weekly(год, неделя)
                ''')
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_dn_statistics_weekly_даты 
                    ON dn_statistics_weekly(начало_недели, конец_недели)
                ''')
            
            # 3. Таблица dn_statistics_monthly
            if 'dn_statistics_monthly' not in existing_tables:
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS dn_statistics_monthly (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        год INTEGER NOT NULL,
                        месяц INTEGER NOT NULL,
                        месяц_название TEXT,
                        год_месяц TEXT NOT NULL,
                        количество_записей INTEGER NOT NULL,
                        количество_дней INTEGER,
                        среднее_dn REAL,
                        минимальное_dn REAL,
                        максимальное_dn REAL,
                        сумма_dn REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(год, месяц)
                    )
                ''')
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_dn_statistics_monthly_период 
                    ON dn_statistics_monthly(год, месяц)
                ''')
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_dn_statistics_monthly_год_месяц 
                    ON dn_statistics_monthly(год_месяц)
                ''')
            
            # 4. Таблица dn_statistics_yearly
            if 'dn_statistics_yearly' not in existing_tables:
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS dn_statistics_yearly (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        год INTEGER NOT NULL UNIQUE,
                        количество_записей INTEGER NOT NULL,
                        количество_дней INTEGER,
                        количество_месяцев INTEGER,
                        среднее_dn REAL,
                        минимальное_dn REAL,
                        максимальное_dn REAL,
                        сумма_dn REAL,
                        первая_дата DATE,
                        последняя_дата DATE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_dn_statistics_yearly_год 
                    ON dn_statistics_yearly(год)
                ''')
            
            # 5. Таблица dn_statistics_period
            if 'dn_statistics_period' not in existing_tables:
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS dn_statistics_period (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        общее_количество_записей INTEGER NOT NULL,
                        количество_дней INTEGER,
                        среднее_dn_за_период REAL,
                        минимальное_dn REAL,
                        максимальное_dn REAL,
                        сумма_dn REAL,
                        первая_дата DATE,
                        последняя_дата DATE,
                        calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
            
            conn.commit()
            logger.warning("Таблицы статистики DN успешно созданы")
        
    except Exception as e:
        logger.error(f"Ошибка при проверке/создании таблиц статистики DN: {e}")
        import traceback
        logger.error(traceback.format_exc())
        conn.rollback()


def save_dn_statistics_to_db(conn, stats_data):
    """
    Сохраняет статистику DN в базу данных (отдельные таблицы для каждого типа)
    
    Args:
        conn: Подключение к БД
        stats_data: Словарь со статистикой из get_dn_statistics_data()
    """
    if not stats_data:
        return
    
    cursor = conn.cursor()
    
    try:
        # 1. Сохраняем дневную статистику
        if 'last_daily' in stats_data:
            daily = stats_data['last_daily']
            cursor.execute('''
                INSERT OR REPLACE INTO dn_statistics_daily 
                (дата, день_недели, количество_записей, среднее_dn, 
                 минимальное_dn, максимальное_dn, сумма_dn, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                daily.get('дата'),
                daily.get('день_недели'),
                daily.get('количество_записей'),
                daily.get('среднее_dn'),
                daily.get('минимальное_dn'),
                daily.get('максимальное_dn'),
                daily.get('сумма_dn')
            ))
        
        # 2. Сохраняем недельную статистику
        if 'last_weekly' in stats_data:
            weekly = stats_data['last_weekly']
            cursor.execute('''
                INSERT OR REPLACE INTO dn_statistics_weekly 
                (год, неделя, начало_недели, конец_недели, количество_записей,
                 среднее_dn, минимальное_dn, максимальное_dn, сумма_dn, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                weekly.get('год'),
                weekly.get('неделя'),
                weekly.get('начало_недели'),
                weekly.get('конец_недели'),
                weekly.get('количество_записей'),
                weekly.get('среднее_dn'),
                weekly.get('минимальное_dn'),
                weekly.get('максимальное_dn'),
                weekly.get('сумма_dn')
            ))
        
        # 3. Сохраняем месячную статистику
        if 'last_monthly' in stats_data:
            monthly = stats_data['last_monthly']
            # Извлекаем месяц из год_месяц (формат: '2026-01')
            month_num = None
            if monthly.get('год_месяц'):
                try:
                    month_num = int(monthly['год_месяц'].split('-')[1])
                except:
                    pass
            
            cursor.execute('''
                INSERT OR REPLACE INTO dn_statistics_monthly 
                (год, месяц, месяц_название, год_месяц, количество_записей,
                 количество_дней, среднее_dn, минимальное_dn, максимальное_dn,
                 сумма_dn, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                monthly.get('год'),
                month_num,
                monthly.get('месяц'),
                monthly.get('год_месяц'),
                monthly.get('количество_записей'),
                monthly.get('количество_дней'),
                monthly.get('среднее_dn'),
                monthly.get('минимальное_dn'),
                monthly.get('максимальное_dn'),
                monthly.get('сумма_dn')
            ))
        
        # 4. Сохраняем годовую статистику (все годы)
        if 'yearly' in stats_data:
            for year_data in stats_data['yearly']:
                cursor.execute('''
                    INSERT OR REPLACE INTO dn_statistics_yearly 
                    (год, количество_записей, количество_дней, количество_месяцев,
                     среднее_dn, минимальное_dn, максимальное_dn, сумма_dn,
                     первая_дата, последняя_дата, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    year_data.get('Год'),
                    year_data.get('Количество_записей'),
                    year_data.get('Количество_дней'),
                    year_data.get('Количество_месяцев'),
                    year_data.get('Среднее_DN'),
                    year_data.get('Минимальное_DN'),
                    year_data.get('Максимальное_DN'),
                    year_data.get('Сумма_DN'),
                    year_data.get('Первая_дата'),
                    year_data.get('Последняя_дата')
                ))
        
        # 5. Сохраняем общую статистику за период
        if 'period' in stats_data:
            period = stats_data['period']
            # Удаляем старые записи (должна быть только одна запись)
            cursor.execute('DELETE FROM dn_statistics_period')
            cursor.execute('''
                INSERT INTO dn_statistics_period 
                (общее_количество_записей, количество_дней, среднее_dn_за_период,
                 минимальное_dn, максимальное_dn, сумма_dn, первая_дата,
                 последняя_дата, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                period.get('общее_количество_записей'),
                period.get('количество_дней'),
                period.get('среднее_dn_за_период'),
                period.get('минимальное_dn'),
                period.get('максимальное_dn'),
                period.get('сумма_dn'),
                period.get('первая_дата'),
                period.get('последняя_дата')
            ))
        
        conn.commit()
        
    except Exception as e:
        logger.error(f"Ошибка при сохранении статистики DN в БД: {e}")
        import traceback
        logger.error(traceback.format_exc())
        conn.rollback()

