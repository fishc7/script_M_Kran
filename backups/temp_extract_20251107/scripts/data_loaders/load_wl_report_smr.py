
# Импортируем утилиты
try:
    # Пробуем относительный импорт (для запуска через web-приложение)
    from ..utilities.db_utils import clean_column_name
    from ..utilities.path_utils import get_excel_paths
except ImportError:
    # Если не работает, используем абсолютный импорт (для прямого запуска)
    import sys
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    utilities_dir = os.path.join(os.path.dirname(current_dir), 'utilities')
    if utilities_dir not in sys.path:
        sys.path.insert(0, utilities_dir)
    
    from db_utils import clean_column_name
    from path_utils import get_excel_paths
"""
Скрипт для загрузки данных из Excel файла в базу данных SQLite.
Функционал:
1. Поиск самого свежего Excel файла в указанной директории по дате в имени файла (формат DD.MM.YYYY)
2. Чтение листа "ЖСР" из найденного файла
3. Автоматическое добавление новых столбцов в базу данных
4. Загрузка данных в таблицу базы данных 'wl_report_smr'
5. Автоматическая очистка данных от пробелов в столбце с номерами стыков
6. Логирование всех изменений

ВНИМАНИЕ: Очистка данных теперь встроена в этот скрипт, отдельный файл load_wl_report_smr_with_cleaning.py больше не нужен.
"""

import os
import pandas as pd
from datetime import datetime
import glob
import re
import sqlite3
from sqlalchemy import create_engine
import logging

# Импортируем дополнительные утилиты
try:
    from ..utilities.path_utils import get_database_path, get_script_log_path, validate_path
except ImportError:
    # Если не работает, используем абсолютный импорт
    import sys
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    utilities_dir = os.path.join(os.path.dirname(current_dir), 'utilities')
    if utilities_dir not in sys.path:
        sys.path.insert(0, utilities_dir)
    
    from path_utils import get_database_path, get_script_log_path, validate_path

# Настройка логирования
def setup_logging():
    """Настройка логирования"""
    # Получаем путь к лог-файлу через утилиту
    log_filename = get_script_log_path("load_wl_report_smr")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8', mode='a'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def clean_joint_data(joint_text):
    """
    Очищает данные от всех видов пробелов (обычные, неразрывные, табуляции и т.д.)
    
    Args:
        joint_text: Исходный текст
        
    Returns:
        Очищенный текст без пробелов
    """
    if not joint_text:
        return None
    
    # Преобразуем в строку
    joint_text = str(joint_text)
    
    # Удаляем все виды пробелов:
    # \s - все пробельные символы (пробелы, табуляции, переносы строк)
    # \u00A0 - неразрывный пробел
    # \u2000-\u200F - различные виды пробелов Unicode
    # \u2028-\u202F - пробелы и разделители строк Unicode
    # \u205F-\u206F - пробелы и форматирование Unicode
    cleaned_text = re.sub(r'[\s\u00A0\u2000-\u200F\u2028-\u202F\u205F-\u206F]+', '', joint_text)
    
    return cleaned_text if cleaned_text else None

def clean_joint_column_data(df, logger):
    """
    Очищает данные в столбце с номерами стыков от пробелов
    
    Args:
        df: DataFrame с данными
        logger: Логгер для записи информации
        
    Returns:
        DataFrame с очищенными данными
    """
    # Ищем столбец с номерами стыков
    joint_columns = ['_Стыка', 'Номер_стыка', 'Стык', 'Weld_number', 'Welded_joint_No', 'Joint_number', 'Weld_No']
    
    cleaned_count = 0
    joint_column_found = None
    
    for col in joint_columns:
        if col in df.columns:
            joint_column_found = col
            logger.info(f"Найден столбец с номерами стыков: {col}")
            break
    
    if joint_column_found:
        # Создаем копию DataFrame для безопасного изменения
        df_cleaned = df.copy()
        
        # Применяем очистку к столбцу
        original_values = df_cleaned[joint_column_found].astype(str)
        cleaned_values = original_values.apply(clean_joint_data)
        
        # Подсчитываем количество измененных значений
        changed_mask = (original_values != cleaned_values) & (cleaned_values.notna())
        cleaned_count = changed_mask.sum()
        
        # Обновляем данные
        df_cleaned[joint_column_found] = cleaned_values
        
        if cleaned_count > 0:
            logger.info(f"Очищено от пробелов {cleaned_count} записей в столбце '{joint_column_found}'")
            
            # Показываем первые 5 примеров очистки
            examples = df[changed_mask].head(5)
            for idx, row in examples.iterrows():
                original = original_values.loc[idx]
                cleaned = cleaned_values.loc[idx]
                logger.info(f"  Пример: '{original}' -> '{cleaned}'")
        else:
            logger.info(f"В столбце '{joint_column_found}' пробелы не найдены")
        
        return df_cleaned
    else:
        logger.warning("Столбец с номерами стыков не найден. Доступные столбцы:")
        for col in df.columns:
            logger.warning(f"  - {col}")
        return df

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

def get_latest_excel_file(directory):
    # Получаем все Excel файлы в директории
    excel_files = glob.glob(os.path.join(directory, "*.xlsx"))
    
    # Фильтруем файлы, содержащие дату в имени (ищем DD.MM.YYYY или D.MM.YYYY)
    dated_files = []
    date_pattern = re.compile(r'(\d{1,2}\.\d{1,2}\.\d{4})')
    for file in excel_files:
        filename = os.path.basename(file)
        match = date_pattern.search(filename)
        if match:
            try:
                date = datetime.strptime(match.group(1), "%d.%m.%Y")
                dated_files.append((file, date))
            except Exception:
                continue
    if not dated_files:
        raise FileNotFoundError("В директории не найдены Excel файлы с датами")
    # Сортируем по дате и получаем самый свежий файл
    latest_file = max(dated_files, key=lambda x: x[1])[0]
    return latest_file

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
    for col in existing_columns:
        logger.info(f"  - {col}")
    
    added_columns = []
    for col in excel_columns:
        # Экранируем специальные символы в названиях столбцов
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
    # Настройка логирования
    logger = setup_logging()
    logger.info("Начало загрузки данных из Excel в базу данных")
    
    # Отладочная информация
    current_dir = os.getcwd()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    logger.info(f"Текущая рабочая директория: {current_dir}")
    logger.info(f"Директория скрипта: {script_dir}")
    
    # Получаем пути через утилиту
    excel_paths = get_excel_paths()
    excel_dir = excel_paths['smr_svarka']
    db_path = get_database_path()
    
    # Валидируем пути
    excel_exists, excel_dir, excel_error = validate_path(excel_dir, "Директория с Excel файлами")
    if not excel_exists:
        logger.error(excel_error)
        raise FileNotFoundError(excel_error)
    
    db_exists, db_path, db_error = validate_path(db_path, "База данных")
    if not db_exists:
        logger.error(db_error)
        raise FileNotFoundError(db_error)
    
    logger.info(f"Директория с Excel файлами: {excel_dir}")
    
    logger.info(f"Путь к базе данных: {db_path}")
    
    try:
        # Получаем самый свежий Excel файл
        latest_file = get_latest_excel_file(excel_dir)
        logger.info(f"Обрабатывается файл: {latest_file}")
        
        # Проверяем существование файла
        if not os.path.exists(latest_file):
            error_msg = f"Excel файл не найден: {latest_file}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        # Читаем лист "ЖСР" из Excel
        df = pd.read_excel(latest_file, sheet_name="ЖСР")
        logger.info(f"Прочитано строк из Excel: {len(df)}")
        
        # Очищаем имена столбцов
        original_columns = df.columns.tolist()
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Выводим информацию о столбцах
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
        
        # Добавляем столбец _сокращен_ISO на основе данных из столбца Чертеж
        logger.info("Начинаем создание столбца _сокращен_ISO...")
        df = add_shortened_iso_column(df, logger)
        logger.info("Создание столбца _сокращен_ISO завершено")
        
        # Добавляем столбец ISO на основе сопоставления с Log_Piping_PTO
        logger.info("Начинаем создание столбца ISO...")
        df = add_iso_from_log_piping_pto(df, logger)
        logger.info("Создание столбца ISO завершено")
        
        # Подключаемся к базе данных
        conn = sqlite3.connect(db_path)
        
        try:
            # Проверяем существование таблицы
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='wl_report_smr'")
            table_exists = cursor.fetchone() is not None
            
            if not table_exists:
                logger.info("Таблица 'wl_report_smr' не существует. Создаем новую таблицу...")
                # Создаем таблицу с помощью pandas
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
                engine = create_engine(f'sqlite:///{db_path}')
                df.to_sql('wl_report_smr', engine, if_exists='replace', index=False)
                logger.info("Данные успешно обновлены в таблице 'wl_report_smr'")
        
        finally:
            conn.close()
        
        print("Скрипт успешно завершён. Загружено строк:", len(df))
        logger.info(f"Обработано строк: {len(df)}")
        logger.info("Загрузка данных завершена успешно")
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        load_excel_to_db()
    except Exception as e:
        print(f"Ошибка: {str(e)}")
        import traceback
        traceback.print_exc()

# Для запуска через GUI
def run_script():
    """Функция для запуска скрипта через GUI"""
    try:
        load_excel_to_db()
    except Exception as e:
        print(f"Ошибка: {str(e)}")
        import traceback
        traceback.print_exc()
