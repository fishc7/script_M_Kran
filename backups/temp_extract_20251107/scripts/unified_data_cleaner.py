#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Универсальный скрипт для полной очистки данных в таблице wl_report_smr
Выполняет все этапы очистки: КЛЕЙМО, Факт, КЛЕЙМО_1, Ревизия, Unnamed:_16
Создает столбец Фактическое_Клеймо с данными по приоритету
"""

import sqlite3
import pandas as pd
from datetime import datetime
import os
import re
import logging

def setup_logging():
    """Настройка логирования"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"unified_cleaning_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def is_date_value(value):
    """
    Проверяет, является ли значение датой в различных форматах
    
    Args:
        value: Значение для проверки
        
    Returns:
        bool: True если значение похоже на дату, False иначе
    """
    if pd.isna(value) or value is None:
        return False
    
    # Проверяем, является ли значение pandas Timestamp или datetime объектом
    if isinstance(value, pd.Timestamp):
        return True
    
    try:
        if hasattr(value, 'date') or hasattr(value, 'strftime'):
            return True
    except:
        pass
    
    value_str = str(value).strip()
    
    if not value_str:
        return False
    
    # Паттерны для распознавания дат
    date_patterns = [
        # DD.MM.YYYY или DD.MM.YY
        r'^\d{1,2}\.\d{1,2}\.\d{2,4}(\s+\d{1,2}:\d{2}(:\d{2})?)?$',
        # YYYY-MM-DD
        r'^\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}:\d{2})?$',
        # DD/MM/YYYY или DD/MM/YY
        r'^\d{1,2}/\d{1,2}/\d{2,4}(\s+\d{1,2}:\d{2}(:\d{2})?)?$',
        # DD-MM-YYYY или DD-MM-YY
        r'^\d{1,2}-\d{1,2}-\d{2,4}(\s+\d{1,2}:\d{2}(:\d{2})?)?$',
        # Дата в скобках, например: (10.04.25) или 01(10.04.25)
        r'^\d*\s*\(?\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4}\)?$',
    ]
    
    # Проверяем по паттернам
    for pattern in date_patterns:
        if re.match(pattern, value_str, re.IGNORECASE):
            return True
    
    # Пытаемся распарсить как дату
    try:
        # Различные форматы для парсинга
        date_formats = [
            '%d.%m.%Y',
            '%d.%m.%y',
            '%Y-%m-%d',
            '%d/%m/%Y',
            '%d/%m/%y',
            '%d-%m-%Y',
            '%d-%m-%y',
            '%d.%m.%Y %H:%M:%S',
            '%d.%m.%Y %H:%M',
            '%Y-%m-%d %H:%M:%S',
        ]
        
        for fmt in date_formats:
            try:
                datetime.strptime(value_str.split()[0], fmt)  # Берем только дату без времени
                return True
            except (ValueError, IndexError):
                continue
    except:
        pass
    
    return False

def clean_kleimo_column(conn, logger):
    """Очистка столбца КЛЕЙМО"""
    logger.info("=" * 80)
    logger.info("ЭТАП 1: ОЧИСТКА СТОЛБЦА КЛЕЙМО")
    logger.info("=" * 80)
    
    cursor = conn.cursor()
    total_changes = 0
    
    try:
        # Получаем данные из столбцов КЛЕЙМО и Факт
        query = '''
        SELECT rowid, КЛЕЙМО, Факт 
        FROM wl_report_smr 
        WHERE КЛЕЙМО IS NOT NULL AND КЛЕЙМО != ''
        '''
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            logger.info("В столбце КЛЕЙМО нет данных!")
            return 0
        
        logger.info(f"Всего записей с данными в КЛЕЙМО: {len(df)}")
        
        # Этап 1: Удаление скобок и запятых
        logger.info("Этап 1.1: Удаление скобок и запятых...")
        changes_count = 0
        
        for idx, row in df.iterrows():
            kleimo_value = str(row['КЛЕЙМО']).strip()
            original_value = kleimo_value
            
            # Удаляем скобки и запятые
            kleimo_value = re.sub(r'[\(\),]', '', kleimo_value)
            
            if kleimo_value != original_value:
                cursor.execute(
                    'UPDATE wl_report_smr SET КЛЕЙМО = ? WHERE rowid = ?',
                    (kleimo_value, row['rowid'])
                )
                changes_count += 1
                if changes_count <= 10:
                    logger.info(f"  Запись {row['rowid']}: '{original_value}' -> '{kleimo_value}'")
        
        if changes_count > 10:
            logger.info(f"  ... и еще {changes_count - 10} записей")
        
        logger.info(f"Удалено скобок и запятых: {changes_count}")
        total_changes += changes_count
        
        # Этап 2: Замена длинных текстовых значений на значения из Факт
        logger.info("Этап 1.2: Замена длинных текстовых значений...")
        changes_count = 0
        
        long_text_patterns = [
            'стык заварен',
            'есть в ЖСР',
            'В ЖСР С',
            'дубль',
            'перевар',
            'OEES'
        ]
        
        for idx, row in df.iterrows():
            kleimo_value = str(row['КЛЕЙМО']).strip()
            fact_value = str(row['Факт']).strip() if pd.notna(row['Факт']) else None
            
            if fact_value and fact_value != 'None':
                # Проверяем на длинные текстовые паттерны
                for pattern in long_text_patterns:
                    if re.search(pattern, kleimo_value, re.IGNORECASE):
                        cursor.execute(
                            'UPDATE wl_report_smr SET КЛЕЙМО = ? WHERE rowid = ?',
                            (fact_value, row['rowid'])
                        )
                        changes_count += 1
                        if changes_count <= 10:
                            logger.info(f"  Запись {row['rowid']}: '{kleimo_value}' -> '{fact_value}'")
                        break
        
        if changes_count > 10:
            logger.info(f"  ... и еще {changes_count - 10} записей")
        
        logger.info(f"Заменено длинных текстовых значений: {changes_count}")
        total_changes += changes_count
        
        # Этап 3: Замена пустых значений на значения из Факт
        logger.info("Этап 1.3: Замена пустых значений...")
        changes_count = 0
        
        cursor.execute('''
            SELECT rowid, Факт 
            FROM wl_report_smr 
            WHERE (КЛЕЙМО IS NULL OR КЛЕЙМО = '') 
            AND Факт IS NOT NULL AND Факт != ''
        ''')
        
        empty_records = cursor.fetchall()
        
        for rowid, fact_value in empty_records:
            cursor.execute(
                'UPDATE wl_report_smr SET КЛЕЙМО = ? WHERE rowid = ?',
                (fact_value, rowid)
            )
            changes_count += 1
            if changes_count <= 10:
                logger.info(f"  Запись {rowid}: NULL -> '{fact_value}'")
        
        if changes_count > 10:
            logger.info(f"  ... и еще {changes_count - 10} записей")
        
        logger.info(f"Заменено пустых значений: {changes_count}")
        total_changes += changes_count
        
        # Этап 4: Стандартизация формата (удаление пробелов, верхний регистр, замена O на 0)
        logger.info("Этап 1.4: Стандартизация формата...")
        changes_count = 0
        
        cursor.execute('SELECT rowid, КЛЕЙМО FROM wl_report_smr WHERE КЛЕЙМО IS NOT NULL AND КЛЕЙМО != ""')
        kleimo_records = cursor.fetchall()
        
        for rowid, kleimo_value in kleimo_records:
            original_value = kleimo_value
            
            # Удаляем все пробелы
            kleimo_value = kleimo_value.replace(" ", "")
            
            # Преобразуем в верхний регистр
            kleimo_value = kleimo_value.upper()
            
            # Заменяем O/o на 0
            kleimo_value = re.sub(r'[ОOоo]', '0', kleimo_value)
            
            if kleimo_value != original_value:
                cursor.execute(
                    'UPDATE wl_report_smr SET КЛЕЙМО = ? WHERE rowid = ?',
                    (kleimo_value, rowid)
                )
                changes_count += 1
                if changes_count <= 10:
                    logger.info(f"  Запись {rowid}: '{original_value}' -> '{kleimo_value}'")
        
        if changes_count > 10:
            logger.info(f"  ... и еще {changes_count - 10} записей")
        
        logger.info(f"Стандартизировано значений: {changes_count}")
        total_changes += changes_count
        
        # Этап 5: Удаление дат из столбца КЛЕЙМО
        logger.info("Этап 1.5: Удаление дат из столбца КЛЕЙМО...")
        changes_count = 0
        
        cursor.execute('SELECT rowid, КЛЕЙМО FROM wl_report_smr WHERE КЛЕЙМО IS NOT NULL AND КЛЕЙМО != ""')
        kleimo_records = cursor.fetchall()
        
        for rowid, kleimo_value in kleimo_records:
            if is_date_value(kleimo_value):
                cursor.execute(
                    'UPDATE wl_report_smr SET КЛЕЙМО = NULL WHERE rowid = ?',
                    (rowid,)
                )
                changes_count += 1
                if changes_count <= 10:
                    logger.info(f"  Запись {rowid}: '{kleimo_value}' -> NULL (дата)")
        
        if changes_count > 10:
            logger.info(f"  ... и еще {changes_count - 10} записей")
        
        logger.info(f"Удалено дат из КЛЕЙМО: {changes_count}")
        total_changes += changes_count
        
        logger.info(f"ИТОГО ИЗМЕНЕНИЙ В КЛЕЙМО: {total_changes}")
        return total_changes
        
    except Exception as e:
        logger.error(f"Ошибка при очистке КЛЕЙМО: {e}")
        return 0

def clean_fact_column(conn, logger):
    """
    Очистка столбца Факт
    
    Удаляет все значения содержащие слово "перевар" в любом регистре 
    (ПЕРЕВАР, Перевар, перевар, пЕрЕвАр) с любым количеством пробелов.
    Все такие записи заменяются на NULL.
    """
    logger.info("=" * 80)
    logger.info("ЭТАП 2: ОЧИСТКА СТОЛБЦА ФАКТ")
    logger.info("=" * 80)
    
    cursor = conn.cursor()
    
    try:
        # Удаляем все значения содержащие слово "перевар" в любом регистре
        # (ПЕРЕВАР, Перевар, перевар, пЕрЕвАр - это одно и то же)
        # Удаляются также все пробелы перед проверкой
        cursor.execute('''
            SELECT rowid, Факт 
            FROM wl_report_smr 
            WHERE Факт IS NOT NULL 
            AND LOWER(REPLACE(Факт, ' ', '')) LIKE '%перевар%'
        ''')
        
        perevар_records = cursor.fetchall()
        
        logger.info(f"Найдено записей с 'перевар' (в любом регистре) в Факт: {len(perevар_records)}")
        
        changes_count = 0
        for rowid, fact_value in perevар_records:
            cursor.execute(
                'UPDATE wl_report_smr SET Факт = NULL WHERE rowid = ?',
                (rowid,)
            )
            changes_count += 1
            if changes_count <= 10:
                logger.info(f"  Запись {rowid}: '{fact_value}' -> NULL")
        
        if changes_count > 10:
            logger.info(f"  ... и еще {changes_count - 10} записей")
        
        logger.info(f"УДАЛЕНО ЗАПИСЕЙ С 'ПЕРЕВАР' (в любом регистре) В ФАКТ: {changes_count}")
        total_changes = changes_count
        
        # Этап 2.2: Удаление дат из столбца Факт
        logger.info("Этап 2.2: Удаление дат из столбца Факт...")
        changes_count = 0
        
        cursor.execute('SELECT rowid, Факт FROM wl_report_smr WHERE Факт IS NOT NULL AND Факт != ""')
        fact_records = cursor.fetchall()
        
        for rowid, fact_value in fact_records:
            if is_date_value(fact_value):
                cursor.execute(
                    'UPDATE wl_report_smr SET Факт = NULL WHERE rowid = ?',
                    (rowid,)
                )
                changes_count += 1
                if changes_count <= 10:
                    logger.info(f"  Запись {rowid}: '{fact_value}' -> NULL (дата)")
        
        if changes_count > 10:
            logger.info(f"  ... и еще {changes_count - 10} записей")
        
        logger.info(f"Удалено дат из Факт: {changes_count}")
        total_changes += changes_count
        
        logger.info(f"ИТОГО ИЗМЕНЕНИЙ В ФАКТ: {total_changes}")
        return total_changes
        
    except Exception as e:
        logger.error(f"Ошибка при очистке Факт: {e}")
        return 0

def clean_multiple_columns(conn, logger):
    """Очистка столбцов КЛЕЙМО_1, Ревизия, Unnamed:_16"""
    logger.info("=" * 80)
    logger.info("ЭТАП 3: ОЧИСТКА СТОЛБЦОВ КЛЕЙМО_1, РЕВИЗИЯ, UNNAMED:_16")
    logger.info("=" * 80)
    
    cursor = conn.cursor()
    total_changes = 0
    
    # Список нежелательных данных
    unwanted_data = [
        'ткс',
        'TKC',
        'ПЕРЕВАР',
        '04.07.2025 0:00',
        'Долг',
        '22.04.25 (02)',
        '25.06.2024 0:00',
        '06.02.2025 0:00',
        'Перевар 25.06.2024',
        'Врезка',
        'Переварпо новой ревизии',
        'по новой ревизии',
        'Перевар(ревизия)',
        '01(10.04.25)',
        '00(27.06.25)',
        '2025-07-04 00:00:00',
        '2025-02-06 00:00:00',
        '2025-04-10 00:00:00',
        '2024-06-25 00:00:00',
        '2024-08-27 00:00:00',
        '2024-07-31 00:00:00',
        '2025-04-22 00:00:00',
        '2024-06-22 00:00:00',
        '2024-09-12 00:00:00',
        '2024-08-19 00:00:00',
        '2024-04-22 00:00:00',
        '2024-04-25 00:00:00',
        '2024-06-27 00:00:00',
        '2025-04-25 00:00:00',
        '45772',
        'Ремонт',
        '2025-08-02 00:00:00',
        'Врезка',
        'ПЕРЕВАР ,ФЛАНЦЫ БЫЛИ ЗАВАРЕНЫ,',
        'НЕ ТОГО ИСПОЛНЕНИЯ'
    ]
    
    # Столбцы для очистки
    columns_to_clean = ['КЛЕЙМО_1', 'Ревизия', 'Unnamed:_16']
    
    logger.info(f"Список нежелательных данных ({len(unwanted_data)} элементов):")
    for i, data in enumerate(unwanted_data[:10], 1):
        logger.info(f"  {i:2d}. '{data}'")
    if len(unwanted_data) > 10:
        logger.info(f"  ... и еще {len(unwanted_data) - 10} элементов")
    
    logger.info(f"Столбцы для очистки: {', '.join(columns_to_clean)}")
    
    # Очищаем каждый столбец
    for column in columns_to_clean:
        logger.info(f"\n{'-'*70}")
        logger.info(f"ОЧИСТКА СТОЛБЦА: {column}")
        logger.info(f"{'-'*70}")
        
        try:
            # Получаем данные из столбца
            query = f'SELECT rowid, "{column}" FROM wl_report_smr WHERE "{column}" IS NOT NULL AND "{column}" != \'\''
            df = pd.read_sql_query(query, conn)
            
            if df.empty:
                logger.info(f"В столбце {column} нет данных!")
                continue
            
            total_records = len(df)
            logger.info(f"Всего записей с данными в столбце {column}: {total_records}")
            
            # Ищем записи с нежелательными данными
            unwanted_records = []
            date_records = []
            
            for idx, row in df.iterrows():
                column_value = str(row[column]).strip()
                
                # Проверяем, является ли значение датой
                if is_date_value(column_value):
                    date_records.append((row['rowid'], column_value))
                    unwanted_records.append((row['rowid'], column_value))
                    continue
                
                # Проверяем точное совпадение
                if column_value in unwanted_data:
                    unwanted_records.append((row['rowid'], column_value))
                else:
                    # Проверяем частичное совпадение (без учета регистра)
                    for unwanted in unwanted_data:
                        if re.search(re.escape(unwanted), column_value, re.IGNORECASE):
                            unwanted_records.append((row['rowid'], column_value))
                            break
            
            logger.info(f"Найдено записей с нежелательными данными: {len(unwanted_records)}")
            if date_records:
                logger.info(f"  Из них распознано как даты: {len(date_records)}")
            
            if unwanted_records:
                logger.info(f"Примеры найденных записей:")
                for i, (rowid, value) in enumerate(unwanted_records[:10], 1):
                    logger.info(f"  {i:2d}. Запись {rowid}: '{value}'")
                
                if len(unwanted_records) > 10:
                    logger.info(f"  ... и еще {len(unwanted_records) - 10} записей")
                
                # Выполняем очистку
                logger.info(f"Выполняем очистку:")
                cleaned_count = 0
                
                for rowid, value in unwanted_records:
                    # Устанавливаем значение в NULL
                    cursor.execute(
                        f'UPDATE wl_report_smr SET "{column}" = NULL WHERE rowid = ?',
                        (rowid,)
                    )
                    cleaned_count += 1
                    if cleaned_count <= 10:  # Показываем первые 10 очищенных записей
                        logger.info(f"  Запись {rowid}: '{value}' -> NULL")
                
                if cleaned_count > 10:
                    logger.info(f"  ... и еще {cleaned_count - 10} записей очищено")
                
                logger.info(f"УСПЕШНО ОЧИЩЕНО ЗАПИСЕЙ В СТОЛБЦЕ {column}: {cleaned_count}")
                total_changes += cleaned_count
                
            else:
                logger.info("Записей с нежелательными данными не найдено!")
                
        except Exception as e:
            logger.error(f"Ошибка при обработке столбца {column}: {e}")
            continue
    
    logger.info(f"ИТОГО ИЗМЕНЕНИЙ В МНОЖЕСТВЕННЫХ СТОЛБЦАХ: {total_changes}")
    return total_changes

def create_factual_kleimo_column(conn, logger):
    """
    Создание столбца Фактическое_Клеймо с данными по приоритету
    
    ЛОГИКА ПРИОРИТЕТНОСТИ:
    Функция проверяет столбцы по порядку и берет ПЕРВОЕ НЕПУСТОЕ значение.
    Как только находит значение - цикл прерывается (break), остальные столбцы не проверяются.
    
    ПРИОРИТЕТЫ (от высшего к низшему):
    1. Unnamed:_16      - самый высокий приоритет
    2. Ревизия          - высокий приоритет
    3. КЛЕЙМО_1         - средний приоритет
    4. Факт             - средний приоритет
    5. КЛЕЙМО           - самый низкий приоритет (резервный источник)
    
    ПРИМЕРЫ РАБОТЫ:
    Случай 1: Unnamed:_16 = 'К12345', Ревизия = '', КЛЕЙМО_1 = 'К67890', Факт = 'К11111', КЛЕЙМО = 'К22222'
       Результат: 'К12345' (из Unnamed:_16)
    
    Случай 2: Unnamed:_16 = NULL, Ревизия = 'К55555', КЛЕЙМО_1 = '', Факт = 'К33333', КЛЕЙМО = 'К22222'
       Результат: 'К55555' (из Ревизия)
    
    Случай 3: Unnamed:_16 = NULL, Ревизия = NULL, КЛЕЙМО_1 = NULL, Факт = 'К99999', КЛЕЙМО = 'К33333'
       Результат: 'К99999' (из Факт)
    
    Случай 4: Unnamed:_16 = NULL, Ревизия = NULL, КЛЕЙМО_1 = NULL, Факт = NULL, КЛЕЙМО = 'К33333'
       Результат: 'К33333' (из КЛЕЙМО)
    """
    logger.info("=" * 80)
    logger.info("ЭТАП 4: СОЗДАНИЕ СТОЛБЦА ФАКТИЧЕСКОЕ_КЛЕЙМО")
    logger.info("=" * 80)
    
    cursor = conn.cursor()
    
    try:
        # Проверяем, существует ли столбец Фактическое_Клеймо
        cursor.execute("PRAGMA table_info(wl_report_smr)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'Фактическое_Клеймо' not in columns:
            logger.info("Создаем столбец Фактическое_Клеймо...")
            cursor.execute("ALTER TABLE wl_report_smr ADD COLUMN Фактическое_Клеймо TEXT")
            logger.info("Столбец Фактическое_Клеймо создан!")
        else:
            logger.info("Столбец Фактическое_Клеймо уже существует!")
        
        # Получаем все данные из приоритетных столбцов
        query = '''
        SELECT rowid, "Unnamed:_16", "Ревизия", "КЛЕЙМО_1", "Факт", "КЛЕЙМО"
        FROM wl_report_smr
        '''
        df = pd.read_sql_query(query, conn)
        
        logger.info(f"Обрабатываем {len(df)} записей...")
        
        # Приоритеты столбцов:
        # 1. Unnamed:_16 (приоритет 1)
        # 2. Ревизия (приоритет 2) 
        # 3. КЛЕЙМО_1 (приоритет 3)
        # 4. Факт (приоритет 4)
        # 5. КЛЕЙМО (приоритет 5)
        
        priority_columns = ['Unnamed:_16', 'Ревизия', 'КЛЕЙМО_1', 'Факт', 'КЛЕЙМО']
        
        filled_count = 0
        source_stats = {col: 0 for col in priority_columns}
        skipped_dates_count = 0
        skipped_dates_by_column = {col: 0 for col in priority_columns}
        
        for idx, row in df.iterrows():
            factual_value = None
            source_column = None
            
            # Проверяем столбцы по приоритету
            for col in priority_columns:
                value = row[col]
                if pd.notna(value) and str(value).strip() != '':
                    value_str = str(value).strip()
                    # Пропускаем даты - они не должны попадать в Фактическое_Клеймо
                    if is_date_value(value_str):
                        skipped_dates_count += 1
                        skipped_dates_by_column[col] += 1
                        continue
                    factual_value = value_str
                    source_column = col
                    break
            
            # Обновляем запись
            if factual_value:
                cursor.execute(
                    'UPDATE wl_report_smr SET "Фактическое_Клеймо" = ? WHERE rowid = ?',
                    (factual_value, row['rowid'])
                )
                filled_count += 1
                source_stats[source_column] += 1
                
                # Показываем первые 10 примеров
                if filled_count <= 10:
                    logger.info(f"  Запись {row['rowid']}: '{factual_value}' (из {source_column})")
        
        if filled_count > 10:
            logger.info(f"  ... и еще {filled_count - 10} записей")
        
        logger.info(f"\nРЕЗУЛЬТАТЫ ЗАПОЛНЕНИЯ ФАКТИЧЕСКОЕ_КЛЕЙМО:")
        logger.info(f"Всего заполнено записей: {filled_count}")
        logger.info(f"Источники данных:")
        
        for i, col in enumerate(priority_columns, 1):
            logger.info(f"  {i}. {col}: {source_stats[col]} записей")
        
        # Статистика по пропущенным датам
        if skipped_dates_count > 0:
            logger.info(f"\nПРОПУЩЕНО ДАТ (не попадают в Фактическое_Клеймо): {skipped_dates_count}")
            for col in priority_columns:
                if skipped_dates_by_column[col] > 0:
                    logger.info(f"  - {col}: {skipped_dates_by_column[col]} дат пропущено")
        
        # Статистика по пустым записям
        cursor.execute('SELECT COUNT(*) FROM wl_report_smr WHERE "Фактическое_Клеймо" IS NULL OR "Фактическое_Клеймо" = ""')
        empty_count = cursor.fetchone()[0]
        logger.info(f"Пустых записей: {empty_count}")
        
        return filled_count
        
    except Exception as e:
        logger.error(f"Ошибка при создании столбца Фактическое_Клеймо: {e}")
        return 0

def get_final_statistics(conn, logger):
    """Получение финальной статистики"""
    logger.info("=" * 80)
    logger.info("ФИНАЛЬНАЯ СТАТИСТИКА")
    logger.info("=" * 80)
    
    cursor = conn.cursor()
    columns_to_check = ['КЛЕЙМО', 'Факт', 'КЛЕЙМО_1', 'Ревизия', 'Unnamed:_16', 'Фактическое_Клеймо']
    
    logger.info("Статистика по столбцам:")
    logger.info("-" * 60)
    
    for column in columns_to_check:
        try:
            cursor.execute(f'SELECT COUNT(*) FROM wl_report_smr WHERE "{column}" IS NOT NULL AND "{column}" != \'\'')
            with_data = cursor.fetchone()[0]
            
            cursor.execute(f'SELECT COUNT(*) FROM wl_report_smr WHERE "{column}" IS NULL OR "{column}" = \'\'')
            empty = cursor.fetchone()[0]
            
            total = with_data + empty
            percentage = (with_data / total) * 100 if total > 0 else 0
            
            logger.info(f"{column:20}: {with_data:5} записей с данными, {empty:5} пустых ({percentage:5.1f}%)")
        except Exception as e:
            logger.error(f"{column:20}: ОШИБКА - {e}")

def main():
    """Основная функция"""
    logger = setup_logging()
    
    db_path = "../database/BD_Kingisepp/M_Kran_Kingesepp.db"
    
    if not os.path.exists(db_path):
        logger.error(f"База данных не найдена: {db_path}")
        return
    
    logger.info("ЗАПУСК УНИВЕРСАЛЬНОЙ ОЧИСТКИ ДАННЫХ")
    logger.info("=" * 80)
    logger.info(f"База данных: {db_path}")
    logger.info(f"Таблица: wl_report_smr")
    logger.info(f"Время начала: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    conn = sqlite3.connect(db_path)
    
    try:
        # Выполняем все этапы очистки
        total_changes_kleimo = clean_kleimo_column(conn, logger)
        total_changes_fact = clean_fact_column(conn, logger)
        total_changes_multiple = clean_multiple_columns(conn, logger)
        total_changes_factual = create_factual_kleimo_column(conn, logger)
        
        # Применяем все изменения
        conn.commit()
        
        # Получаем финальную статистику
        get_final_statistics(conn, logger)
        
        # Создаем финальный отчет (только в лог)
        logger.info("=" * 80)
        logger.info("ФИНАЛЬНЫЙ ОТЧЕТ")
        logger.info("=" * 80)
        logger.info(f"РЕЗУЛЬТАТЫ ОЧИСТКИ:")
        logger.info(f"1. Столбец КЛЕЙМО: {total_changes_kleimo} изменений")
        logger.info(f"2. Столбец Факт: {total_changes_fact} изменений")
        logger.info(f"3. Столбцы КЛЕЙМО_1, Ревизия, Unnamed:_16: {total_changes_multiple} изменений")
        logger.info(f"4. Столбец Фактическое_Клеймо: {total_changes_factual} записей заполнено")
        logger.info(f"")
        logger.info(f"ОБЩЕЕ КОЛИЧЕСТВО ИЗМЕНЕНИЙ: {total_changes_kleimo + total_changes_fact + total_changes_multiple}")
        logger.info(f"ЗАПОЛНЕНО ЗАПИСЕЙ В ФАКТИЧЕСКОЕ_КЛЕЙМО: {total_changes_factual}")
        logger.info("=" * 80)
        
        logger.info("=" * 80)
        logger.info("ОЧИСТКА ЗАВЕРШЕНА УСПЕШНО!")
        logger.info(f"Общее количество изменений: {total_changes_kleimo + total_changes_fact + total_changes_multiple}")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении очистки: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
