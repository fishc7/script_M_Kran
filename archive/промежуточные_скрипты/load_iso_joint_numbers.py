import os
from db_utils import clean_column_name, clean_data_values, print_column_cleaning_report
import pandas as pd
from pathlib import Path
import datetime
import sqlite3
import logging

# Настройка логирования
def setup_logging():
    """Настройка логирования для отслеживания процесса загрузки"""
    log_filename = f"logs/load_iso_joint_numbers_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def load_excel_data(file_path, sheet_name="код_для_удаления"):
    """
    Загружает данные из Excel файла с указанного листа
    
    Args:
        file_path (str): Путь к Excel файлу
        sheet_name (str): Название листа для загрузки
    
    Returns:
        pandas.DataFrame: Загруженные данные
    """
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"Загружаю данные из файла: {file_path}")
        logger.info(f"Лист для загрузки: {sheet_name}")
        
        # Проверяем существование файла
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Файл не найден: {file_path}")
        
        # Загружаем данные из Excel
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        
        logger.info(f"Успешно загружено {len(df)} строк и {len(df.columns)} столбцов")
        logger.info(f"Столбцы: {list(df.columns)}")
        
        # Выводим первые несколько строк для проверки
        logger.info("Первые 5 строк данных:")
        logger.info(df.head().to_string())
        
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных из Excel: {e}")
        raise

def clean_data(df):
    """
    Очищает и подготавливает данные
    
    Args:
        df (pandas.DataFrame): Исходные данные
    
    Returns:
        pandas.DataFrame: Очищенные данные
    """
    logger = logging.getLogger(__name__)
    
    logger.info("Начинаю очистку данных...")
    
    # Удаляем полностью пустые строки
    initial_rows = len(df)
    df = df.dropna(how='all')
    logger.info(f"Удалено {initial_rows - len(df)} полностью пустых строк")
    
    # Удаляем полностью пустые столбцы
    initial_cols = len(df.columns)
    df = df.dropna(axis=1, how='all')
    logger.info(f"Удалено {initial_cols - len(df.columns)} полностью пустых столбцов")
    
    # Очищаем названия столбцов
    df.columns = df.columns.str.strip()
    
    # Заполняем NaN значения пустыми строками для текстовых столбцов
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].fillna('')
    
    logger.info(f"После очистки: {len(df)} строк и {len(df.columns)} столбцов")
    logger.info(f"Столбцы после очистки: {list(df.columns)}")
    
    return df

def analyze_data(df):
    """
    Анализирует загруженные данные
    
    Args:
        df (pandas.DataFrame): Данные для анализа
    """
    logger = logging.getLogger(__name__)
    
    logger.info("=== АНАЛИЗ ДАННЫХ ===")
    logger.info(f"Общее количество строк: {len(df)}")
    logger.info(f"Общее количество столбцов: {len(df.columns)}")
    
    # Информация о столбцах
    logger.info("\nИнформация о столбцах:")
    for col in df.columns:
        non_null_count = df[col].count()
        null_count = df[col].isnull().sum()
        unique_count = df[col].nunique()
        
        logger.info(f"  {col}:")
        logger.info(f"    - Непустых значений: {non_null_count}")
        logger.info(f"    - Пустых значений: {null_count}")
        logger.info(f"    - Уникальных значений: {unique_count}")
        
        # Показываем примеры уникальных значений для первых 5 столбцов
        if unique_count <= 10 and unique_count > 0:
            unique_values = df[col].dropna().unique()
            logger.info(f"    - Уникальные значения: {list(unique_values)}")
    
    # Статистика по типам данных
    logger.info(f"\nТипы данных:")
    logger.info(df.dtypes.to_string())

def create_table_in_db(conn, df, table_name="код_для_удаления"):
    """
    Создает таблицу в базе данных на основе структуры DataFrame
    
    Args:
        conn: Подключение к базе данных
        df (pandas.DataFrame): Данные для создания таблицы
        table_name (str): Название таблицы (по умолчанию имя листа)
    
    Returns:
        bool: True если таблица создана успешно
    """
    logger = logging.getLogger(__name__)
    
    try:
        cursor = conn.cursor()
        
        # Удаляем существующую таблицу, если она есть
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        logger.info(f"Старая таблица {table_name} удалена")
        
        # Очищаем названия столбцов
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Создаем SQL для создания таблицы
        columns_sql = ['id INTEGER PRIMARY KEY AUTOINCREMENT']
        
        for col in df.columns:
            # Все столбцы создаем как TEXT для универсальности
            columns_sql.append(f'"{col}" TEXT')
        
        # Добавляем столбец с датой загрузки
        columns_sql.append('Дата_загрузки TEXT')
        
        create_table_sql = f'''
        CREATE TABLE {table_name} (
            {', '.join(columns_sql)}
        )
        '''
        
        cursor.execute(create_table_sql)
        conn.commit()
        
        logger.info(f"Таблица {table_name} успешно создана!")
        logger.info(f"Столбцы: {list(df.columns)}")
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при создании таблицы: {e}")
        return False

def load_data_to_db(conn, df, table_name="код_для_удаления"):
    """
    Загружает данные в базу данных
    
    Args:
        conn: Подключение к базе данных
        df (pandas.DataFrame): Данные для загрузки
        table_name (str): Название таблицы (по умолчанию имя листа)
    
    Returns:
        bool: True если данные загружены успешно
    """
    logger = logging.getLogger(__name__)
    
    try:
        cursor = conn.cursor()
        
        # Очищаем названия столбцов
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Подготавливаем данные для вставки
        insert_data = []
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info("Подготавливаем данные для вставки...")
        
        for index, row in df.iterrows():
            # Обрабатываем каждую строку, заменяя NaN на None (NULL в БД)
            row_data = []
            for col in df.columns:
                value = row.get(col)
                if pd.isna(value):
                    row_data.append(None)
                else:
                    row_data.append(str(value))
            
            # Добавляем дату загрузки
            row_data.append(current_time)
            insert_data.append(tuple(row_data))
            
            # Логируем прогресс каждые 100 записей
            if (index + 1) % 100 == 0:
                logger.info(f"Обработано {index + 1} записей из {len(df)}")
        
        # Создаем SQL запрос для вставки
        columns = list(df.columns) + ['Дата_загрузки']
        placeholders = ', '.join(['?' for _ in columns])
        columns_str = ', '.join([f'"{col}"' for col in columns])
        
        insert_query = f'''
        INSERT INTO {table_name} (
            {columns_str}
        )
        VALUES ({placeholders})
        '''
        
        logger.info("Начинаем вставку данных в базу...")
        cursor.executemany(insert_query, insert_data)
        conn.commit()
        
        logger.info(f"Успешно загружено {len(insert_data)} записей в таблицу {table_name}!")
        
        # Показываем примеры загруженных записей
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
        sample_rows = cursor.fetchall()
        
        logger.info("Примеры загруженных записей:")
        for i, row in enumerate(sample_rows, 1):
            logger.info(f"  {i}. ID: {row[0]}, данные: {row[1:6]}...")
        
        # Показываем статистику
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_count = cursor.fetchone()[0]
        logger.info(f"Всего записей в таблице {table_name}: {total_count}")
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных в базу: {e}")
        import traceback
        logger.error("Полный стек ошибки:")
        logger.error(traceback.format_exc())
        return False

def verify_data_in_db(conn, table_name="код_для_удаления"):
    """
    Проверяет корректность загрузки данных в базу
    
    Args:
        conn: Подключение к базе данных
        table_name (str): Название таблицы (по умолчанию имя листа)
    
    Returns:
        bool: True если данные загружены корректно
    """
    logger = logging.getLogger(__name__)
    
    try:
        cursor = conn.cursor()
        
        # Проверяем количество записей
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_count = cursor.fetchone()[0]
        
        # Проверяем наличие данных
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
        sample = cursor.fetchone()
        
        # Проверяем структуру таблицы
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        logger.info(f"Проверка таблицы {table_name}:")
        logger.info(f"  - Количество записей: {total_count}")
        logger.info(f"  - Количество столбцов: {len(columns)}")
        logger.info(f"  - Столбцы: {[col[1] for col in columns]}")
        
        if total_count > 0 and sample:
            logger.info(f"✓ Проверка успешна: данные загружены корректно")
            return True
        else:
            logger.error("✗ Проверка не прошла: данные не загружены")
            return False
            
    except Exception as e:
        logger.error(f"Ошибка при проверке данных: {e}")
        return False

def find_excel_file(base_path, filename_pattern="номерация стыков по iso"):
    """
    Ищет Excel файл по маске в указанной папке
    
    Args:
        base_path (str): Базовый путь для поиска
        filename_pattern (str): Маска для поиска файла
    
    Returns:
        str: Полный путь к найденному файлу или None
    """
    logger = logging.getLogger(__name__)
    
    try:
        base_path = Path(base_path)
        if not base_path.exists():
            logger.error(f"Папка не найдена: {base_path}")
            return None
        
        logger.info(f"Ищем Excel файл с маской '{filename_pattern}' в: {base_path}")
        
        # Ищем файлы Excel с указанной маской
        excel_files = []
        for file_path in base_path.rglob("*.xlsx"):
            if filename_pattern.lower() in file_path.name.lower():
                excel_files.append(file_path)
        
        # Также ищем .xls файлы
        for file_path in base_path.rglob("*.xls"):
            if filename_pattern.lower() in file_path.name.lower():
                excel_files.append(file_path)
        
        if excel_files:
            # Сортируем по дате изменения (новые файлы первыми)
            excel_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            found_file = excel_files[0]
            logger.info(f"Найден файл: {found_file}")
            return str(found_file)
        else:
            logger.warning(f"Excel файлы с маской '{filename_pattern}' не найдены в {base_path}")
            return None
            
    except Exception as e:
        logger.error(f"Ошибка при поиске файла: {e}")
        return None

def get_user_file_path():
    """
    Запрашивает у пользователя путь к файлу
    
    Returns:
        str: Путь к файлу, введенный пользователем
    """
    logger = logging.getLogger(__name__)
    
    logger.info("="*60)
    logger.info("ВВОД ПУТИ К EXCEL ФАЙЛУ")
    logger.info("="*60)
    logger.info("Введите полный путь к Excel файлу с номерацией стыков по ISO")
    logger.info("Пример: D:\\МК_Кран\\МК_Кран_Кингесеп\\ПТО\\файл.xlsx")
    logger.info("Или нажмите Enter для автоматического поиска")
    logger.info("="*60)
    
    user_path = input("Путь к файлу: ").strip()
    
    if user_path:
        logger.info(f"Пользователь ввел путь: {user_path}")
        return user_path
    else:
        logger.info("Пользователь не ввел путь, используем автоматический поиск")
        return None

def main():
    """
    Основная функция для загрузки данных из Excel файла в базу данных
    """
    # Настройка логирования
    logger = setup_logging()
    
    logger.info("=== ЗАГРУЗКА ДАННЫХ ИЗ EXCEL В БАЗУ ДАННЫХ ===")
    logger.info("Начинаю процесс загрузки данных из Excel файла в базу данных...")
    
    # Пути к файлам
    base_search_path = r"D:\МК_Кран\МК_Кран_Кингесеп\ПТО"
    sheet_name = "код_для_удаления"
    db_path = r"D:\МК_Кран\script_M_Kran\BD_Kingisepp\M_Kran_Kingesepp.db"
    table_name = sheet_name  # Используем имя листа как имя таблицы

    try:
        # Определяем путь к Excel файлу
        excel_file_path = None
        
        # Сначала пробуем найти файл автоматически
        excel_file_path = find_excel_file(base_search_path, "номерация стыков по iso")
        
        # Если файл не найден автоматически, запрашиваем у пользователя
        if not excel_file_path:
            logger.info("Автоматический поиск файла не удался")
            user_path = get_user_file_path()
            if user_path:
                excel_file_path = user_path
            else:
                # Пробуем другие варианты поиска
                logger.info("Пробуем альтернативные варианты поиска...")
                excel_file_path = find_excel_file(base_search_path, "номерация")
                if not excel_file_path:
                    excel_file_path = find_excel_file(base_search_path, "стыков")
                if not excel_file_path:
                    excel_file_path = find_excel_file(base_search_path, "iso")
        
        if not excel_file_path:
            raise FileNotFoundError("Excel файл не найден. Проверьте путь и попробуйте снова.")
        
        # Проверяем существование файлов
        if not os.path.exists(excel_file_path):
            raise FileNotFoundError(f"Excel файл не найден: {excel_file_path}")
        
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"База данных не найдена: {db_path}")
        
        logger.info(f"Excel файл найден: {excel_file_path}")
        logger.info(f"База данных найдена: {db_path}")
        
        # Загружаем данные из Excel
        df = load_excel_data(excel_file_path, sheet_name)
        
        # Очищаем данные
        df_cleaned = clean_data(df)
        
        # Анализируем данные
        analyze_data(df_cleaned)
        
        # Подключаемся к базе данных
        logger.info("Подключение к базе данных...")
        conn = sqlite3.connect(db_path)
        
        # Создаем таблицу в базе данных
        if not create_table_in_db(conn, df_cleaned, table_name):
            raise Exception("Не удалось создать таблицу в базе данных")
        
        # Загружаем данные в базу
        if not load_data_to_db(conn, df_cleaned, table_name):
            raise Exception("Не удалось загрузить данные в базу данных")
        
        # Проверяем загрузку
        if not verify_data_in_db(conn, table_name):
            raise Exception("Проверка загрузки данных не прошла")
        
        logger.info("=== ЗАГРУЗКА ЗАВЕРШЕНА УСПЕШНО ===")
        logger.info(f"Данные загружены в таблицу: {table_name}")
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Соединение с базой данных закрыто")

if __name__ == "__main__":
    main() 