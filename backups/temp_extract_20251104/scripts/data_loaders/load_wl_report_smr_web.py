
# Импорты будут обработаны позже в коде
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Оптимизированная версия скрипта для загрузки данных из Excel в базу данных SQLite.
Адаптирована для работы в веб-интерфейсе с улучшенной обработкой ошибок.
"""

import os
import sys
import pandas as pd
from datetime import datetime
import glob
import re
import sqlite3
from sqlalchemy import create_engine
import logging
import traceback

# Добавляем пути для импорта модулей
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))  # scripts/data_loaders -> scripts -> project_root
utilities_dir = os.path.join(project_root, 'scripts', 'utilities')

# Добавляем пути в sys.path
for path in [current_dir, utilities_dir, project_root]:
    if path not in sys.path:
        sys.path.insert(0, path)

def sync_words_kleimo_fact_table(logger):
    """
    Синхронизирует данные из таблицы wl_report_smr в таблицу слов_клейм_факт
    
    Args:
        logger: Логгер для записи информации
        
    Returns:
        int: Количество обновленных записей
    """
    try:
        logger.info("Начинаем синхронизацию с таблицей слов_клейм_факт...")
        
        db_path = get_database_path()
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            # Проверяем существование таблицы слов_клейм_факт
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='слов_клейм_факт'")
            if not cursor.fetchone():
                logger.warning("Таблица слов_клейм_факт не найдена. Синхронизация пропущена.")
                return 0
            
            # Проверяем существование столбца Фактическое_Клеймо в wl_report_smr
            cursor.execute("PRAGMA table_info(wl_report_smr)")
            wl_columns = [col[1] for col in cursor.fetchall()]
            
            if 'Фактическое_Клеймо' not in wl_columns:
                logger.warning("Столбец Фактическое_Клеймо не найден в wl_report_smr. Синхронизация пропущена.")
                return 0
            
            # Получаем уникальные значения из wl_report_smr
            cursor.execute("""
                SELECT DISTINCT Фактическое_Клеймо 
                FROM wl_report_smr 
                WHERE Фактическое_Клеймо IS NOT NULL 
                AND Фактическое_Клеймо != ''
            """)
            
            wl_values = [row[0] for row in cursor.fetchall()]
            logger.info(f"Найдено {len(wl_values)} уникальных значений в wl_report_smr")
            
            # Получаем существующие значения в слов_клейм_факт
            cursor.execute('SELECT "Фактическое_Клеймо" FROM слов_клейм_факт WHERE "Фактическое_Клеймо" IS NOT NULL')
            existing_values = [row[0] for row in cursor.fetchall()]
            logger.info(f"Найдено {len(existing_values)} существующих значений в слов_клейм_факт")
            
            # Находим новые значения для добавления
            new_values = [value for value in wl_values if value not in existing_values]
            logger.info(f"Найдено {len(new_values)} новых значений для добавления")
            
            if new_values:
                # Добавляем новые записи
                insert_count = 0
                for value in new_values:
                    try:
                        cursor.execute(
                            'INSERT INTO слов_клейм_факт (Фактическое_Клеймо, Примечание) VALUES (?, ?)',
                            (value, 'Новая запись')
                        )
                        insert_count += 1
                        
                        # Показываем первые 10 примеров
                        if insert_count <= 10:
                            logger.info(f"  Добавлено: '{value}'")
                    except sqlite3.IntegrityError as e:
                        logger.warning(f"Не удалось добавить '{value}': {e}")
                        continue
                
                if insert_count > 10:
                    logger.info(f"  ... и еще {insert_count - 10} записей")
                
                logger.info(f"Успешно добавлено {insert_count} новых записей в слов_клейм_факт")
                
                # Применяем изменения
                conn.commit()
                logger.info("Изменения применены!")
                
                return insert_count
            else:
                logger.info("Новых значений для добавления не найдено")
                return 0
                
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Ошибка при синхронизации с слов_клейм_факт: {str(e)}")
        return 0

def run_data_cleaning(logger):
    """
    Запускает скрипт очистки данных unified_data_cleaner.py
    
    Выполняет 4 этапа очистки данных в таблице wl_report_smr:
    
    ЭТАП 10.1: Очистка столбца КЛЕЙМО
        - Удаление скобок и запятых из значений
        - Замена длинных текстовых значений ('стык заварен', 'есть в ЖСР', 'дубль', 'перевар', 'OEES') на значения из столбца Факт
        - Заполнение пустых значений данными из столбца Факт
        - Стандартизация: удаление пробелов, верхний регистр, замена O/о на 0
    
    ЭТАП 10.2: Очистка столбца Факт
        - Удаление всех значений содержащих слово "перевар" в любом регистре
        - ПЕРЕВАР, Перевар, перевар, пЕрЕвАр - это одно и то же
        - Все такие записи заменяются на NULL
    
    ЭТАП 10.3: Очистка столбцов КЛЕЙМО_1, Ревизия, Unnamed:_16
        - Удаление нежелательных данных по списку (ТКС, ПЕРЕВАР, даты, 'Долг', 'Врезка', 'Ремонт', '45772' и др.)
    
    ЭТАП 10.4: Создание столбца Фактическое_Клеймо
        - Приоритет 1: Unnamed:_16
        - Приоритет 2: Ревизия
        - Приоритет 3: КЛЕЙМО_1
        - Приоритет 4: Факт
        - Приоритет 5: КЛЕЙМО
    
    Args:
        logger: Логгер для записи информации
        
    Returns:
        bool: True если очистка прошла успешно, False в противном случае
    """
    try:
        logger.info("Импортируем модуль очистки данных...")
        
        # Импортируем функции из unified_data_cleaner
        import importlib.util
        
        # Путь к скрипту очистки
        cleaner_path = os.path.join(project_root, 'scripts', 'unified_data_cleaner.py')
        
        if not os.path.exists(cleaner_path):
            raise FileNotFoundError(f"Скрипт очистки не найден: {cleaner_path}")
        
        # Загружаем модуль
        spec = importlib.util.spec_from_file_location("unified_data_cleaner", cleaner_path)
        cleaner_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cleaner_module)
        
        logger.info("Запускаем очистку данных...")
        
        # Выполняем очистку данных
        # Создаем временный логгер для скрипта очистки
        import logging
        temp_logger = logging.getLogger('data_cleaner')
        temp_logger.setLevel(logging.INFO)
        
        # Выполняем основные функции очистки
        db_path = get_database_path()
        
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"База данных не найдена: {db_path}")
        
        import sqlite3
        conn = sqlite3.connect(db_path)
        
        try:
            # ЭТАП 10.1: Очистка столбца КЛЕЙМО
            # - Удаление скобок и запятых
            # - Замена длинных текстовых значений на данные из Факт
            # - Заполнение пустых значений из Факт
            # - Стандартизация (пробелы, регистр, O→0)
            logger.info("=== ЭТАП 10.1: Очистка столбца КЛЕЙМО ===")
            total_changes_kleimo = cleaner_module.clean_kleimo_column(conn, temp_logger)
            
            # ЭТАП 10.2: Очистка столбца Факт
            # - Удаление всех значений содержащих "перевар" в любом регистре
            # - ПЕРЕВАР, Перевар, перевар, пЕрЕвАр - это одно и то же
            # - Все заменяются на NULL
            logger.info("=== ЭТАП 10.2: Очистка столбца Факт ===")
            total_changes_fact = cleaner_module.clean_fact_column(conn, temp_logger)
            
            # ЭТАП 10.3: Очистка столбцов КЛЕЙМО_1, Ревизия, Unnamed:_16
            # - Удаление нежелательных данных (ТКС, ПЕРЕВАР, даты, 'Долг', 'Врезка', 'Ремонт', '45772' и др.)
            logger.info("=== ЭТАП 10.3: Очистка множественных столбцов ===")
            total_changes_multiple = cleaner_module.clean_multiple_columns(conn, temp_logger)
            
            # ЭТАП 10.4: Создание столбца Фактическое_Клеймо
            # - Приоритет: Unnamed:_16 → Ревизия → КЛЕЙМО_1 → Факт → КЛЕЙМО
            logger.info("=== ЭТАП 10.4: Создание столбца Фактическое_Клеймо ===")
            total_changes_factual = cleaner_module.create_factual_kleimo_column(conn, temp_logger)
            
            # Применяем все изменения
            conn.commit()
            
            # Получаем финальную статистику
            logger.info("=== ФИНАЛЬНАЯ СТАТИСТИКА ===")
            cleaner_module.get_final_statistics(conn, temp_logger)
            
            logger.info("Очистка данных завершена успешно:")
            logger.info(f"  - Изменений в КЛЕЙМО: {total_changes_kleimo}")
            logger.info(f"  - Изменений в Факт: {total_changes_fact}")
            logger.info(f"  - Изменений в множественных столбцах: {total_changes_multiple}")
            logger.info(f"  - Заполнено записей в Фактическое_Клеймо: {total_changes_factual}")
            
            return True
            
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Ошибка при выполнении очистки данных: {str(e)}")
        raise

# Определяем функции локально для избежания проблем с импортом
def clean_column_name(col):
    """Очищает название столбца от специальных символов"""
    if pd.isna(col):
        return 'unnamed_column'
    return str(col).replace(' ', '_').replace('-', '_').replace('.', '_').replace('\n', '_').replace('\r', '_')

def get_excel_paths():
    """Возвращает пути к Excel файлам"""
    return {
        'smr_svarka': "D:/МК_Кран/МК_Кран_Кингесеп/СМР/отчет_площадка/сварка"
    }

def get_database_path():
    """Возвращает путь к базе данных"""
    # Определяем базовый путь проекта
    if getattr(sys, 'frozen', False):
        # Если запущено из EXE
        base_path = os.path.dirname(sys.executable)
    else:
        # Если запущено из .py
        base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    return os.path.join(base_path, 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db')

def validate_path(path, description=""):
    """Проверяет существование пути"""
    exists = os.path.exists(path)
    return exists, path, ""

def get_script_log_path(script_name):
    """Создает путь к лог-файлу"""
    # Определяем базовый путь проекта
    if getattr(sys, 'frozen', False):
        # Если запущено из EXE
        base_path = os.path.dirname(sys.executable)
    else:
        # Если запущено из .py
        base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    log_dir = os.path.join(base_path, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    return os.path.join(log_dir, f'{script_name}.log')

def clean_data_values(df):
    """Очищает данные в DataFrame"""
    # Простая очистка данных
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).str.strip()
    return df

def split_connection_column(df, logger):
    """
    Разделяет столбец СОЕДИНЕНИЕ_ на Деталь1 и Деталь2 по разделителю + или -
    
    Args:
        df: DataFrame с данными
        logger: Логгер для записи информации
        
    Returns:
        DataFrame с добавленными столбцами Деталь1 и Деталь2
    """
    # Ищем столбец СОЕДИНЕНИЕ_ (возможные варианты названий)
    connection_columns = ['СОЕДИНЕНИЕ_', 'СОЕДИНЕНИЕ', 'Connection', 'CONNECTION', 'Соединение', 'Соединение_']
    connection_col = None
    
    for col in connection_columns:
        if col in df.columns:
            connection_col = col
            break
    
    if connection_col is None:
        logger.warning("Столбец 'СОЕДИНЕНИЕ_' не найден. Столбцы Деталь1 и Деталь2 не будут созданы.")
        return df
    
    logger.info(f"Найден столбец для разделения: {connection_col}")
    
    # Создаем копию DataFrame для безопасного изменения
    df_split = df.copy()
    
    # Создаем новые столбцы
    df_split['Деталь1'] = None
    df_split['Деталь2'] = None
    
    # Обрабатываем каждое значение
    total_rows = len(df_split)
    split_count = 0
    not_split_count = 0
    
    for idx, value in df_split[connection_col].items():
        if pd.isna(value) or str(value).strip() == '':
            continue
            
        value_str = str(value).strip()
        
        # Ищем разделители + или -
        # Приоритет: сначала +, потом -
        if '+' in value_str:
            parts = value_str.split('+', 1)  # Разделяем только по первому +
            if len(parts) == 2:
                part1 = parts[0].strip()
                part2 = parts[1].strip()
                if part1 and part2:  # Проверяем, что обе части не пустые
                    df_split.at[idx, 'Деталь1'] = part1
                    df_split.at[idx, 'Деталь2'] = part2
                    split_count += 1
                    continue
        
        elif '-' in value_str:
            parts = value_str.split('-', 1)  # Разделяем только по первому -
            if len(parts) == 2:
                part1 = parts[0].strip()
                part2 = parts[1].strip()
                if part1 and part2:  # Проверяем, что обе части не пустые
                    df_split.at[idx, 'Деталь1'] = part1
                    df_split.at[idx, 'Деталь2'] = part2
                    split_count += 1
                    continue
        
        # Если не удалось разделить
        not_split_count += 1
    
    logger.info(f"Разделение столбца '{connection_col}' завершено:")
    logger.info(f"  - Всего строк: {total_rows}")
    logger.info(f"  - Успешно разделено: {split_count}")
    logger.info(f"  - Не удалось разделить: {not_split_count}")
    
    # Показываем примеры разделения
    examples = df_split[df_split['Деталь1'].notna()][[connection_col, 'Деталь1', 'Деталь2']].head(5)
    if not examples.empty:
        logger.info("Примеры разделения:")
        for idx, row in examples.iterrows():
            logger.info(f"  '{row[connection_col]}' -> '{row['Деталь1']}' + '{row['Деталь2']}'")
    
    # Показываем примеры значений, которые не удалось разделить
    not_split_examples = df_split[df_split['Деталь1'].isna() & df_split[connection_col].notna()][connection_col].head(5)
    if not not_split_examples.empty:
        logger.info("Примеры значений, которые не удалось разделить:")
        for value in not_split_examples:
            logger.info(f"  '{value}' - не содержит разделителей + или -")
    
    return df_split

def format_date_columns(df, logger):
    """
    Форматирует столбцы с датами в формат YYYY-MM-DD
    
    Args:
        df: DataFrame с данными
        logger: Логгер для записи информации
        
    Returns:
        DataFrame с отформатированными датами
    """
    # Ищем столбцы с датами (возможные варианты названий)
    date_columns = ['Дата_сварки', 'Дата сварки', 'Date_welding', 'Welding_date', 'Дата_сварки_', 'Date_welding_']
    
    for col_name in date_columns:
        if col_name in df.columns:
            logger.info(f"Найден столбец с датой: {col_name}")
            
            # Создаем копию для безопасного изменения
            df_formatted = df.copy()
            
            # Обрабатываем столбец с датами
            original_values = df_formatted[col_name]
            formatted_values = []
            
            for value in original_values:
                if pd.isna(value) or value == '' or str(value).strip() == '':
                    formatted_values.append(None)
                else:
                    try:
                        # Пробуем разные форматы дат
                        value_str = str(value).strip()
                        
                        # Если уже в формате YYYY-MM-DD, оставляем как есть
                        if re.match(r'^\d{4}-\d{2}-\d{2}$', value_str):
                            formatted_values.append(value_str)
                        # Если в формате YYYY-MM-DDTHH:MM:SS, извлекаем только дату
                        elif 'T' in value_str:
                            date_part = value_str.split('T')[0]
                            if re.match(r'^\d{4}-\d{2}-\d{2}$', date_part):
                                formatted_values.append(date_part)
                            else:
                                formatted_values.append(None)
                        # Если в формате DD.MM.YYYY, конвертируем
                        elif re.match(r'^\d{1,2}\.\d{1,2}\.\d{4}$', value_str):
                            try:
                                date_obj = datetime.strptime(value_str, "%d.%m.%Y")
                                formatted_values.append(date_obj.strftime("%Y-%m-%d"))
                            except:
                                formatted_values.append(None)
                        # Если в формате DD/MM/YYYY, конвертируем
                        elif re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', value_str):
                            try:
                                date_obj = datetime.strptime(value_str, "%d/%m/%Y")
                                formatted_values.append(date_obj.strftime("%Y-%m-%d"))
                            except:
                                formatted_values.append(None)
                        # Если это уже datetime объект
                        elif isinstance(value, datetime):
                            formatted_values.append(value.strftime("%Y-%m-%d"))
                        else:
                            # Пробуем автоматическое распознавание
                            try:
                                date_obj = pd.to_datetime(value_str)
                                formatted_values.append(date_obj.strftime("%Y-%m-%d"))
                            except:
                                logger.warning(f"Не удалось распознать дату: '{value_str}'")
                                formatted_values.append(None)
                    except Exception as e:
                        logger.warning(f"Ошибка при обработке даты '{value}': {e}")
                        formatted_values.append(None)
            
            # Обновляем столбец
            df_formatted[col_name] = formatted_values
            
            # Подсчитываем статистику
            total_values = len(formatted_values)
            valid_dates = sum(1 for v in formatted_values if v is not None)
            invalid_dates = total_values - valid_dates
            
            logger.info(f"Форматирование столбца '{col_name}':")
            logger.info(f"  - Всего значений: {total_values}")
            logger.info(f"  - Успешно отформатировано: {valid_dates}")
            logger.info(f"  - Не удалось отформатировать: {invalid_dates}")
            
            # Показываем примеры
            examples = [v for v in formatted_values[:5] if v is not None]
            if examples:
                logger.info(f"Примеры отформатированных дат: {examples}")
            
            return df_formatted
    
    logger.info("Столбцы с датами не найдены")
    return df

def print_column_cleaning_report(original_columns, cleaned_columns):
    """Выводит отчет об очистке названий столбцов"""
    print("Отчет об очистке названий столбцов:")
    for orig, cleaned in zip(original_columns, cleaned_columns):
        if orig != cleaned:
            print(f"  '{orig}' -> '{cleaned}'")

def setup_logging():
    """Настройка логирования для веб-интерфейса"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def get_latest_excel_file(directory):
    """Получает самый свежий Excel файл с датой в имени"""
    try:
        excel_files = glob.glob(os.path.join(directory, "*.xlsx"))
        print(f"Найдено Excel файлов: {len(excel_files)}")
        
        dated_files = []
        date_pattern = re.compile(r'(\d{1,2}\.\d{1,2}\.\d{4})')
        
        for file in excel_files:
            filename = os.path.basename(file)
            match = date_pattern.search(filename)
            if match:
                try:
                    date = datetime.strptime(match.group(1), "%d.%m.%Y")
                    dated_files.append((file, date))
                    print(f"Найден файл с датой: {filename} -> {date}")
                except Exception as e:
                    print(f"Ошибка парсинга даты в файле {filename}: {e}")
                    continue
        
        if not dated_files:
            raise FileNotFoundError("В директории не найдены Excel файлы с датами")
        
        latest_file = max(dated_files, key=lambda x: x[1])[0]
        print(f"Выбран самый свежий файл: {os.path.basename(latest_file)}")
        return latest_file
        
    except Exception as e:
        print(f"Ошибка при поиске файлов: {e}")
        raise

def clean_joint_data(joint_text):
    """Очищает данные от пробелов"""
    if not joint_text:
        return None
    
    joint_text = str(joint_text)
    cleaned_text = re.sub(r'[\s\u00A0\u2000-\u200F\u2028-\u202F\u205F-\u206F]+', '', joint_text)
    return cleaned_text if cleaned_text else None

def extract_shortened_iso(drawing_value):
    """
    Извлекает сокращенный ISO из значения столбца Чертеж
    
    Примеры:
    60-12-03(16) -> 60-12-3
    70-13-44(2) -> 70-13-44
    
    Args:
        drawing_value: Значение из столбца Чертеж
        
    Returns:
        Сокращенный ISO или None если не удалось извлечь
    """
    if pd.isna(drawing_value) or not drawing_value:
        return None
    
    drawing_str = str(drawing_value).strip()
    
    # Специальная обработка для единичных случаев
    if drawing_str == "70-12-6811)":
        return "70-12-68"
    elif drawing_str == "70-12-10312)":
        return "70-12-103"
    
    # Паттерн для поиска ISO: XX-XX-XX(XX) или XX-XX-XX или XX-XX-XX(XX)
    # Где XX - цифры, а (XX) - опциональная часть в скобках (с открывающей скобкой или без)
    pattern = r'(\d+-\d+-\d+)(?:\(?(\d+)\)?)?'
    match = re.search(pattern, drawing_str)
    
    if match:
        base_iso = match.group(1)  # Основная часть (например, 60-12-03)
        bracket_part = match.group(2)  # Часть в скобках (например, 16)
        
        if bracket_part:
            # Если есть часть в скобках, убираем ведущие нули из основной части
            # и добавляем только если скобочная часть не равна 0
            parts = base_iso.split('-')
            if len(parts) == 3:
                part1, part2, part3 = parts
                # Убираем ведущие нули из третьей части
                part3_clean = str(int(part3)) if part3.isdigit() else part3
                shortened = f"{part1}-{part2}-{part3_clean}"
                return shortened
        else:
            # Если нет скобок, просто убираем ведущие нули из третьей части
            parts = base_iso.split('-')
            if len(parts) == 3:
                part1, part2, part3 = parts
                part3_clean = str(int(part3)) if part3.isdigit() else part3
                shortened = f"{part1}-{part2}-{part3_clean}"
                return shortened
    
    return None

def add_shortened_iso_column(df, logger):
    """
    Добавляет столбец _сокращен_ISO на основе данных из столбца Чертеж
    
    Args:
        df: DataFrame с данными
        logger: Логгер для записи информации
        
    Returns:
        DataFrame с добавленным столбцом _сокращен_ISO
    """
    # Ищем столбец Чертеж (возможные варианты названий)
    drawing_columns = ['Чертеж', 'Drawing', 'Чертёж', 'Чертеж_', 'Drawing_']
    drawing_col = None
    
    for col in drawing_columns:
        if col in df.columns:
            drawing_col = col
            break
    
    if drawing_col is None:
        logger.warning("Столбец 'Чертеж' не найден. Столбец _сокращен_ISO не будет создан.")
        return df
    
    logger.info(f"Найден столбец для извлечения ISO: {drawing_col}")
    
    # Создаем новый столбец
    df['_сокращен_ISO'] = df[drawing_col].apply(extract_shortened_iso)
    
    # Подсчитываем статистику
    total_rows = len(df)
    non_null_iso = df['_сокращен_ISO'].notna().sum()
    null_iso = total_rows - non_null_iso
    
    logger.info(f"Создан столбец _сокращен_ISO:")
    logger.info(f"  - Всего строк: {total_rows}")
    logger.info(f"  - Успешно извлечено ISO: {non_null_iso}")
    logger.info(f"  - Не удалось извлечь ISO: {null_iso}")
    
    # Показываем несколько примеров
    examples = df[df['_сокращен_ISO'].notna()][[drawing_col, '_сокращен_ISO']].head(5)
    if not examples.empty:
        logger.info("Примеры извлечения ISO:")
        for idx, row in examples.iterrows():
            logger.info(f"  '{row[drawing_col]}' -> '{row['_сокращен_ISO']}'")
    
    return df

def add_iso_from_log_piping_pto(df, logger):
    """
    Добавляет столбец ISO в таблицу wl_report_smr на основе сопоставления с Log_Piping_PTO
    
    Args:
        df: DataFrame с данными wl_report_smr
        logger: Логгер для записи информации
        
    Returns:
        DataFrame с добавленным столбцом ISO
    """
    logger.info("Начинаем сопоставление данных с таблицей Log_Piping_PTO...")
    
    try:
        # Подключаемся к базе данных
        db_path = get_database_path()
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Проверяем существование таблицы Log_Piping_PTO
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Log_Piping_PTO'")
        if not cursor.fetchone():
            logger.warning("Таблица Log_Piping_PTO не найдена. Столбец ISO не будет создан.")
            return df
        
        # Проверяем существование необходимых столбцов в Log_Piping_PTO
        cursor.execute("PRAGMA table_info(Log_Piping_PTO)")
        log_columns = [col[1] for col in cursor.fetchall()]
        
        required_columns = ['ключь_жср_смр', 'ISO']
        missing_columns = [col for col in required_columns if col not in log_columns]
        
        if missing_columns:
            logger.warning(f"В таблице Log_Piping_PTO отсутствуют столбцы: {missing_columns}. Столбец ISO не будет создан.")
            return df
        
        # Получаем данные из Log_Piping_PTO для сопоставления
        cursor.execute("""
            SELECT ключь_жср_смр, ISO 
            FROM Log_Piping_PTO 
            WHERE ключь_жср_смр IS NOT NULL 
            AND ключь_жср_смр != '' 
            AND ISO IS NOT NULL 
            AND ISO != ''
        """)
        
        log_data = cursor.fetchall()
        logger.info(f"Найдено {len(log_data)} записей в Log_Piping_PTO для сопоставления")
        
        if not log_data:
            logger.warning("В таблице Log_Piping_PTO нет данных для сопоставления.")
            return df
        
        # Создаем словарь для быстрого поиска
        iso_mapping = {}
        for key, iso in log_data:
            iso_mapping[key] = iso
        
        # Добавляем столбец ISO в DataFrame
        df['ISO'] = df['_сокращен_ISO'].map(iso_mapping)
        
        # Подсчитываем статистику
        total_rows = len(df)
        matched_rows = df['ISO'].notna().sum()
        unmatched_rows = total_rows - matched_rows
        
        logger.info(f"Сопоставление завершено:")
        logger.info(f"  - Всего строк: {total_rows}")
        logger.info(f"  - Успешно сопоставлено: {matched_rows}")
        logger.info(f"  - Не сопоставлено: {unmatched_rows}")
        
        # Показываем несколько примеров сопоставления
        examples = df[df['ISO'].notna()][['_сокращен_ISO', 'ISO']].head(5)
        if not examples.empty:
            logger.info("Примеры сопоставления:")
            for idx, row in examples.iterrows():
                logger.info(f"  '{row['_сокращен_ISO']}' -> '{row['ISO']}'")
        
        # Показываем несколько примеров несопоставленных значений
        unmatched_examples = df[df['ISO'].isna() & df['_сокращен_ISO'].notna()]['_сокращен_ISO'].head(5)
        if not unmatched_examples.empty:
            logger.info("Примеры несопоставленных значений:")
            for value in unmatched_examples:
                logger.info(f"  '{value}' - не найден в Log_Piping_PTO")
        
        conn.close()
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при сопоставлении с Log_Piping_PTO: {str(e)}")
        return df

def clean_line_column_data(df, logger):
    """
    Очищает данные в столбце ЛИНИЯ от всех видов пробелов
    
    Args:
        df: DataFrame с данными
        logger: Логгер для записи информации
        
    Returns:
        DataFrame с очищенными данными в столбце ЛИНИЯ
    """
    # Ищем столбец ЛИНИЯ (возможные варианты названий)
    line_columns = ['ЛИНИЯ', 'Линия', 'LINE', 'Line', 'ЛИНИЯ_', 'Линия_']
    line_col = None
    
    for col in line_columns:
        if col in df.columns:
            line_col = col
            break
    
    if line_col is None:
        logger.warning("Столбец 'ЛИНИЯ' не найден. Очистка не выполнена.")
        return df
    
    logger.info(f"Найден столбец для очистки: {line_col}")
    
    # Создаем копию DataFrame для безопасного изменения
    df_cleaned = df.copy()
    
    # Применяем очистку к столбцу
    original_values = df_cleaned[line_col].astype(str)
    cleaned_values = original_values.apply(clean_joint_data)  # Используем ту же функцию очистки
    
    # Подсчитываем количество измененных значений
    changed_mask = (original_values != cleaned_values) & (cleaned_values.notna())
    cleaned_count = changed_mask.sum()
    
    # Обновляем данные
    df_cleaned[line_col] = cleaned_values
    
    if cleaned_count > 0:
        logger.info(f"Очищено от пробелов {cleaned_count} записей в столбце '{line_col}'")
        
        # Показываем первые 5 примеров очистки
        examples = df[changed_mask].head(5)
        for idx, row in examples.iterrows():
            original = original_values.loc[idx]
            cleaned = cleaned_values.loc[idx]
            logger.info(f"  Пример: '{original}' -> '{cleaned}'")
    else:
        logger.info(f"В столбце '{line_col}' пробелы не найдены")
    
    return df_cleaned

def clean_joint_column_data(df, logger):
    """Очищает данные в столбце с номерами стыков"""
    joint_columns = ['_Стыка', 'Номер_стыка', 'Стык', 'Weld_number', 'Welded_joint_No', 'Joint_number', 'Weld_No', 'No_Стыка']
    
    for col in joint_columns:
        if col in df.columns:
            logger.info(f"Найден столбец с номерами стыков: {col}")
            df_cleaned = df.copy()
            original_values = df_cleaned[col].astype(str)
            cleaned_values = original_values.apply(clean_joint_data)
            changed_mask = (original_values != cleaned_values) & (cleaned_values.notna())
            cleaned_count = changed_mask.sum()
            logger.info(f"Очищено значений в столбце {col}: {cleaned_count}")
            df_cleaned[col] = cleaned_values
            return df_cleaned
    
    logger.warning("Столбец с номерами стыков не найден")
    return df

def get_existing_columns(conn):
    """Получает список существующих столбцов в таблице"""
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(wl_report_smr)")
    columns = cursor.fetchall()
    return [col[1] for col in columns]

def add_missing_columns(conn, excel_columns, logger):
    """Добавляет недостающие столбцы в таблицу"""
    cursor = conn.cursor()
    existing_columns = get_existing_columns(conn)
    
    logger.info(f"Существующие столбцы в базе данных: {len(existing_columns)}")
    
    added_columns = []
    for col in excel_columns:
        safe_col = f'"{col}"'
        if safe_col not in existing_columns:
            try:
                cursor.execute(f'ALTER TABLE wl_report_smr ADD COLUMN {safe_col} TEXT')
                added_columns.append(col)
                logger.info(f"Добавлен новый столбец: {col}")
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    logger.error(f"Ошибка при добавлении столбца {col}: {e}")
    
    if added_columns:
        conn.commit()
        logger.info(f"Всего добавлено новых столбцов: {len(added_columns)}")
    else:
        logger.info("Новые столбцы не найдены")
    
    return added_columns

def load_excel_to_db():
    """Основная функция загрузки данных"""
    logger = setup_logging()
    logger.info("=== НАЧАЛО ЗАГРУЗКИ ДАННЫХ ===")
    
    try:
        # Получаем пути
        excel_paths = get_excel_paths()
        excel_dir = excel_paths['smr_svarka']
        db_path = get_database_path()
        
        logger.info(f"Директория с Excel файлами: {excel_dir}")
        logger.info(f"Путь к базе данных: {db_path}")
        
        # Проверяем существование путей
        if not os.path.exists(excel_dir):
            raise FileNotFoundError(f"Директория не найдена: {excel_dir}")
        
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"База данных не найдена: {db_path}")
        
        # Получаем самый свежий Excel файл
        latest_file = get_latest_excel_file(excel_dir)
        logger.info(f"Обрабатывается файл: {latest_file}")
        
        # Читаем Excel файл с ограничением на количество строк для тестирования
        logger.info("Читаем Excel файл...")
        df = pd.read_excel(latest_file, sheet_name="ЖСР")
        logger.info(f"Прочитано строк из Excel: {len(df)}")
        
        # Очищаем имена столбцов
        original_columns = df.columns.tolist()
        df.columns = [clean_column_name(col) for col in df.columns]
        
        logger.info(f"Найдено столбцов в Excel файле: {len(df.columns)}")
        logger.info("Столбцы в Excel файле:")
        for i, (orig_col, clean_col) in enumerate(zip(original_columns, df.columns), 1):
            logger.info(f"{i:2d}. {orig_col} -> {clean_col}")
        
        # Очищаем данные от пробелов в столбце с номерами стыков
        logger.info("Начинаем очистку данных от пробелов...")
        df = clean_joint_column_data(df, logger)
        logger.info("Очистка данных завершена")
        
        # Очищаем данные от пробелов в столбце ЛИНИЯ
        logger.info("Начинаем очистку столбца ЛИНИЯ от пробелов...")
        df = clean_line_column_data(df, logger)
        logger.info("Очистка столбца ЛИНИЯ завершена")
        
        # Разделяем столбец СОЕДИНЕНИЕ_ на Деталь1 и Деталь2
        logger.info("Начинаем разделение столбца СОЕДИНЕНИЕ_...")
        df = split_connection_column(df, logger)
        logger.info("Разделение столбца СОЕДИНЕНИЕ_ завершено")
        
        # Добавляем столбец _сокращен_ISO на основе данных из столбца Чертеж
        logger.info("Начинаем создание столбца _сокращен_ISO...")
        df = add_shortened_iso_column(df, logger)
        logger.info("Создание столбца _сокращен_ISO завершено")
        
        # Добавляем столбец ISO на основе сопоставления с Log_Piping_PTO
        logger.info("Начинаем создание столбца ISO...")
        df = add_iso_from_log_piping_pto(df, logger)
        logger.info("Создание столбца ISO завершено")
        
        # Форматируем столбцы с датами в формат YYYY-MM-DD
        logger.info("Начинаем форматирование столбцов с датами...")
        df = format_date_columns(df, logger)
        logger.info("Форматирование столбцов с датами завершено")
        
        # Подключаемся к базе данных
        logger.info("Подключаемся к базе данных...")
        conn = sqlite3.connect(db_path)
        
        try:
            # Проверяем существование таблицы
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='wl_report_smr'")
            table_exists = cursor.fetchone() is not None
            
            if not table_exists:
                logger.info("Таблица 'wl_report_smr' не существует. Создаем новую таблицу...")
                engine = create_engine(f'sqlite:///{db_path}')
                df['date_load'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                df.to_sql('wl_report_smr', engine, if_exists='replace', index=False)
                logger.info("Таблица создана и данные загружены")
            else:
                logger.info("Таблица 'wl_report_smr' существует. Проверяем новые столбцы...")
                
                # Добавляем недостающие столбцы
                added_columns = add_missing_columns(conn, df.columns, logger)
                
                # Добавляем колонку с датой загрузки
                df['date_load'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # Загружаем данные, заменяя существующие
                logger.info("Загружаем данные в базу...")
                engine = create_engine(f'sqlite:///{db_path}')
                df.to_sql('wl_report_smr', engine, if_exists='replace', index=False)
                logger.info("Данные успешно обновлены в таблице 'wl_report_smr'")
        
        finally:
            conn.close()
        
        logger.info(f"Обработано строк: {len(df)}")
        logger.info("=== ЗАГРУЗКА ДАННЫХ ЗАВЕРШЕНА УСПЕШНО ===")
        print(f"Скрипт успешно завершён. Загружено строк: {len(df)}")
        
        # Запускаем очистку данных после загрузки
        logger.info("=== НАЧИНАЕМ ОЧИСТКУ ДАННЫХ ===")
        try:
            run_data_cleaning(logger)
            logger.info("=== ОЧИСТКА ДАННЫХ ЗАВЕРШЕНА УСПЕШНО ===")
        except Exception as cleaning_error:
            logger.error(f"Ошибка при очистке данных: {str(cleaning_error)}")
            logger.error(f"Полный стек ошибки очистки:\n{traceback.format_exc()}")
            print(f"ПРЕДУПРЕЖДЕНИЕ: Очистка данных не выполнена: {str(cleaning_error)}")
        
        # Синхронизируем данные с таблицей слов_клейм_факт
        logger.info("=== НАЧИНАЕМ СИНХРОНИЗАЦИЮ С СЛОВ_КЛЕЙМ_ФАКТ ===")
        try:
            sync_count = sync_words_kleimo_fact_table(logger)
            logger.info(f"=== СИНХРОНИЗАЦИЯ ЗАВЕРШЕНА УСПЕШНО: {sync_count} записей ===")
        except Exception as sync_error:
            logger.error(f"Ошибка при синхронизации: {str(sync_error)}")
            logger.error(f"Полный стек ошибки синхронизации:\n{traceback.format_exc()}")
            print(f"ПРЕДУПРЕЖДЕНИЕ: Синхронизация не выполнена: {str(sync_error)}")
        
    except Exception as e:
        error_msg = f"Ошибка при загрузке данных: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Полный стек ошибки:\n{traceback.format_exc()}")
        print(f"ОШИБКА: {error_msg}")
        raise

def run_script():
    """Функция для запуска скрипта через GUI/веб-интерфейс"""
    try:
        load_excel_to_db()
        return True
    except Exception as e:
        print(f"Ошибка выполнения скрипта: {str(e)}")
        return False

if __name__ == "__main__":
    try:
        load_excel_to_db()
    except Exception as e:
        print(f"Ошибка: {str(e)}")
        traceback.print_exc()
