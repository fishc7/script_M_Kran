
# Импортируем утилиты
try:
    # Пробуем относительный импорт (для запуска через web-приложение)
    from ..utilities.db_utils import get_database_connection
except ImportError:
    # Если не работает, используем абсолютный импорт (для прямого запуска)
    import sys
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    utilities_dir = os.path.join(os.path.dirname(current_dir), 'utilities')
    if utilities_dir not in sys.path:
        sys.path.insert(0, utilities_dir)
    
    from db_utils import get_database_connection
"""
Скрипт для извлечения дубликатов сварных швов из таблицы wl_china
и сохранения их в отдельную таблицу duplicates_wl_china.

Функционал:
1. ПЕРВАЯ ПРОВЕРКА: Поиск дубликатов в wl_china по полям "Номер_чертежа" и "Номер_сварного_шва" + id
2. ВТОРАЯ ПРОВЕРКА: Поиск дубликатов в wl_china по полям "Номер_чертежа" и "Номер_сварного_шва" (без учета id)
3. Создание таблицы duplicates_wl_china если она не существует
4. Загрузка только новых дубликатов (без повторной загрузки существующих)
5. Логирование всех операций в файл
6. Сохранение дубликатов в базу данных без вывода в консоль
7. Генерация отчета по дубликатам из wl_china
"""

import os
import sqlite3
import pandas as pd
from datetime import datetime
import logging
import sys

# Добавляем текущую директорию в путь для импорта
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

try:
    
except ImportError:
    # Если db_utils не найден, создаем простую функцию подключения
    def get_database_connection():
        db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'BD_Kingisepp', 'M_Kran_Kingesepp.db')
        return sqlite3.connect(db_path)

def setup_logging():
    """Настройка логирования"""
    log_filename = "logs/extract_duplicates_wl_china.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8', mode='a')
        ]
    )
    return logging.getLogger(__name__)

def create_duplicates_table(conn, table_name, logger):
    """
    Создает таблицу дубликатов если она не существует
    
    Args:
        conn: Подключение к базе данных
        table_name: Имя таблицы дубликатов
        logger: Логгер для записи информации
    """
    cursor = conn.cursor()
    
    # Проверяем, существует ли таблица
    cursor.execute(f"""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='{table_name}'
    """)
    
    if cursor.fetchone() is None:
        # Создаем таблицу с упрощенной структурой
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                "блок_" TEXT,
                "Номер_сварного_шва" TEXT,
                "Дата_сварки" TEXT,
                "_Линии" TEXT,
                "Номер_чертежа" TEXT,
                "_Номер_сварного_шва" TEXT,
                "duplicate_group_id" INTEGER,
                "duplicate_count" INTEGER,
                "extraction_date" TEXT,
                "check_type" TEXT,
                "original_id_china" INTEGER,
                "_Что_со_стыком_повторяющимся??!!" TEXT
            )
        """)
        
        conn.commit()
        logger.info(f"Создана новая таблица {table_name}")
    else:
        # Проверяем, есть ли новые столбцы
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in cursor.fetchall()]
        
        # Добавляем недостающие столбцы
        if 'original_id_china' not in columns:
            try:
                cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN original_id_china INTEGER')
                logger.info(f"Добавлен столбец original_id_china в таблицу {table_name}")
            except Exception as e:
                logger.warning(f"Не удалось добавить столбец original_id_china: {e}")
        
        if 'check_type' not in columns:
            try:
                cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN check_type TEXT')
                logger.info(f"Добавлен столбец check_type в таблицу {table_name}")
            except Exception as e:
                logger.warning(f"Не удалось добавить столбец check_type: {e}")
        
        if '_Что_со_стыком_повторяющимся??!!' not in columns:
            try:
                cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN "_Что_со_стыком_повторяющимся??!!" TEXT')
                logger.info(f"Добавлен столбец '_Что_со_стыком_повторяющимся??!!' в таблицу {table_name}")
            except Exception as e:
                logger.warning(f"Не удалось добавить столбец '_Что_со_стыком_повторяющимся??!!': {e}")
        
        conn.commit()
        logger.info(f"Таблица {table_name} уже существует, проверены и добавлены недостающие столбцы")

def get_duplicates_wl_china(conn, logger):
    """
    Извлекает дубликаты из таблицы wl_china по полям "Номер_чертежа" и "_Номер_сварного_шва"
    
    Args:
        conn: Подключение к базе данных
        logger: Логгер для записи информации
        
    Returns:
        DataFrame с дубликатами
    """
    query = """
    WITH duplicates AS (
        SELECT 
            *,
            COUNT(*) OVER (PARTITION BY Номер_чертежа, _Номер_сварного_шва) AS cnt
        FROM 
            wl_china
    )
    SELECT 
        блок_,
        Номер_сварного_шва,
        Дата_сварки,
        _Линии,
        Номер_чертежа,
        _Номер_сварного_шва,
        cnt as duplicate_count,
        id as original_id_china
    FROM 
        duplicates
    WHERE 
        cnt > 1
    ORDER BY 
        Номер_чертежа, _Номер_сварного_шва
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        logger.info(f"Найдено {len(df)} записей дубликатов в wl_china по полям 'Номер_чертежа' и '_Номер_сварного_шва'")
        return df
    except Exception as e:
        logger.error(f"Ошибка при поиске дубликатов в wl_china: {e}")
        return pd.DataFrame()

def get_existing_duplicates(conn, table_name, logger):
    """
    Получает существующие дубликаты из таблицы
    
    Args:
        conn: Подключение к базе данных
        table_name: Имя таблицы дубликатов
        logger: Логгер для записи информации
        
    Returns:
        DataFrame с существующими дубликатами
    """
    query = f"""
    SELECT 
        "блок_", "Номер_сварного_шва", "Дата_сварки", "_Линии", "Номер_чертежа", 
        "_Номер_сварного_шва", "original_id_china", "check_type"
    FROM {table_name}
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        logger.info(f"Получено {len(df)} существующих записей из таблицы {table_name}")
        return df
    except Exception as e:
        logger.error(f"Ошибка при получении существующих дубликатов: {e}")
        return pd.DataFrame()

def filter_new_duplicates(duplicates_df, existing_duplicates_df, logger):
    """
    Фильтрует новые дубликаты, исключая уже существующие
    
    Args:
        duplicates_df: DataFrame с найденными дубликатами
        existing_duplicates_df: DataFrame с существующими дубликатами
        logger: Логгер для записи информации
        
    Returns:
        DataFrame с новыми дубликатами
    """
    if existing_duplicates_df.empty:
        logger.info("Нет существующих дубликатов, все найденные записи являются новыми")
        return duplicates_df
    
    if duplicates_df.empty:
        logger.info("Нет новых дубликатов для добавления")
        return duplicates_df
    
    # Создаем ключ для сравнения
    def create_compare_key(row):
        return f"{row['Номер_чертежа']}_{row['_Номер_сварного_шва']}_{row['original_id_china']}"
    
    duplicates_df['compare_key'] = duplicates_df.apply(create_compare_key, axis=1)
    existing_duplicates_df['compare_key'] = existing_duplicates_df.apply(create_compare_key, axis=1)
    
    # Фильтруем новые записи
    existing_keys = set(existing_duplicates_df['compare_key'])
    new_duplicates = duplicates_df[~duplicates_df['compare_key'].isin(existing_keys)]
    
    # Удаляем временный столбец
    new_duplicates = new_duplicates.drop('compare_key', axis=1)
    
    logger.info(f"Отфильтровано {len(new_duplicates)} новых записей из {len(duplicates_df)} найденных")
    
    return new_duplicates

def insert_duplicates_to_db(conn, duplicates_df, table_name, logger):
    """
    Вставляет дубликаты в таблицу базы данных
    
    Args:
        conn: Подключение к базе данных
        duplicates_df: DataFrame с дубликатами для вставки
        table_name: Имя таблицы для вставки
        logger: Логгер для записи информации
    """
    if duplicates_df.empty:
        logger.info("Нет новых дубликатов для вставки")
        return
    
    cursor = conn.cursor()
    
    try:
        # Генерируем group_id для каждой группы дубликатов
        # Группируем по 'Номер_чертежа' и '_Номер_сварного_шва'
        duplicates_df['group_key'] = duplicates_df['Номер_чертежа'].astype(str) + '|' + duplicates_df['_Номер_сварного_шва'].astype(str)
        
        # Создаем словарь для сопоставления group_key с group_id
        unique_groups = duplicates_df['group_key'].unique()
        group_id_mapping = {group: i + 1 for i, group in enumerate(unique_groups)}
        
        # Добавляем group_id к DataFrame
        duplicates_df['duplicate_group_id'] = duplicates_df['group_key'].map(group_id_mapping)
        
        # Удаляем временный столбец group_key
        duplicates_df = duplicates_df.drop('group_key', axis=1)
        
        # Добавляем дату извлечения
        duplicates_df['extraction_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Добавляем тип проверки
        duplicates_df['check_type'] = 'поиск по Номер_чертежа и Номер_сварного_шва'
        
        # Вставляем данные в таблицу
        duplicates_df.to_sql(table_name, conn, if_exists='append', index=False)
        
        logger.info(f"Успешно добавлено {len(duplicates_df)} новых записей дубликатов в таблицу {table_name}")
        
    except Exception as e:
        logger.error(f"Ошибка при вставке дубликатов в таблицу {table_name}: {e}")
        raise

def generate_summary_report(conn, table_name, logger):
    """
    Генерирует сводный отчет по дубликатам
    
    Args:
        conn: Подключение к базе данных
        table_name: Имя таблицы дубликатов
        logger: Логгер для записи информации
    """
    cursor = conn.cursor()
    
    logger.info("=" * 60)
    logger.info("СВОДНЫЙ ОТЧЕТ ПО ДУБЛИКАТАМ WL_CHINA")
    logger.info("=" * 60)
    
    try:
        # Общая статистика
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_records = cursor.fetchone()[0]
        
        cursor.execute(f"""
            SELECT COUNT(DISTINCT duplicate_group_id) 
            FROM {table_name}
        """)
        total_groups = cursor.fetchone()[0]
        
        logger.info(f"Общее количество записей дубликатов: {total_records}")
        logger.info(f"Количество групп дубликатов: {total_groups}")
        
        # Статистика по типам проверок
        cursor.execute(f"""
            SELECT check_type, COUNT(*) as count
            FROM {table_name}
            GROUP BY check_type
            ORDER BY count DESC
        """)
        check_stats = cursor.fetchall()
        
        if check_stats:
            logger.info("\nСтатистика по типам проверок:")
            for check_type, count in check_stats:
                logger.info(f"  {check_type}: {count} записей")
        
        # Статистика по номерам чертежей
        cursor.execute(f"""
            SELECT "Номер_чертежа", COUNT(*) as count
            FROM {table_name}
            GROUP BY "Номер_чертежа"
            ORDER BY count DESC
            LIMIT 10
        """)
        drawing_stats = cursor.fetchall()
        
        if drawing_stats:
            logger.info("\nТоп-10 номеров чертежей по количеству дубликатов:")
            for drawing, count in drawing_stats:
                logger.info(f"  {drawing}: {count} дубликатов")
        
        # Примеры дубликатов
        cursor.execute(f"""
            SELECT "Номер_чертежа", "_Номер_сварного_шва", duplicate_count, check_type
            FROM {table_name}
            ORDER BY duplicate_count DESC
            LIMIT 5
        """)
        examples = cursor.fetchall()
        
        if examples:
            logger.info("\nПримеры дубликатов (топ-5 по количеству):")
            for drawing, styk, count, check_type in examples:
                logger.info(f"  Чертеж: {drawing}, Стык: {styk}, Количество: {count}, Проверка: {check_type}")
        
    except Exception as e:
        logger.error(f"Ошибка при генерации отчета: {e}")
    
    logger.info("=" * 60)

def clean_empty_values_in_duplicates_after_extraction(conn, logger):
    """
    Очищает пустые значения в таблице дубликатов wl_china после извлечения
    
    Args:
        conn: Подключение к базе данных
        logger: Логгер для записи информации
    """
    logger.info("=" * 60)
    logger.info("АВТОМАТИЧЕСКАЯ ОЧИСТКА ПУСТЫХ ЗНАЧЕНИЙ В ТАБЛИЦЕ ДУБЛИКАТОВ WL_CHINA")
    logger.info("=" * 60)
    
    table_name = 'duplicates_wl_china'
    cursor = conn.cursor()
    
    # Проверяем существование таблицы
    cursor.execute(f"""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='{table_name}'
    """)
    
    if not cursor.fetchone():
        logger.info(f"Таблица {table_name} не существует")
        return
    
    logger.info(f"Очистка таблицы: {table_name}")
    
    # Получаем информацию о столбцах таблицы
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns_info = cursor.fetchall()
    
    # Получаем только текстовые столбцы
    text_columns = []
    for col_info in columns_info:
        col_name = col_info[1]
        col_type = col_info[2].upper()
        if 'TEXT' in col_type or 'CHAR' in col_type or 'VARCHAR' in col_type:
            text_columns.append(col_name)
    
    if not text_columns:
        logger.info(f"В таблице {table_name} нет текстовых столбцов для очистки")
        return
    
    total_cleaned = 0
    
    # Обрабатываем каждый текстовый столбец
    for column in text_columns:
        try:
            # Экранируем название столбца кавычками для безопасного использования в SQL
            quoted_column = f'"{column}"'
            
            # Подсчитываем количество пустых значений
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM {table_name} 
                WHERE {quoted_column} IS NOT NULL 
                AND TRIM({quoted_column}) = ''
            """)
            empty_count = cursor.fetchone()[0]
            
            if empty_count > 0:
                # Заменяем пустые значения на NULL
                cursor.execute(f"""
                    UPDATE {table_name} 
                    SET {quoted_column} = NULL 
                    WHERE {quoted_column} IS NOT NULL 
                    AND TRIM({quoted_column}) = ''
                """)
                
                updated_rows = cursor.rowcount
                total_cleaned += updated_rows
                
                logger.info(f"  Столбец '{column}': очищено {updated_rows} записей")
            else:
                logger.info(f"  Столбец '{column}': пустых значений не найдено")
                
        except Exception as e:
            logger.error(f"Ошибка при очистке столбца '{column}' в таблице {table_name}: {e}")
    
    conn.commit()
    logger.info(f"Всего очищено записей: {total_cleaned}")
    logger.info("Автоматическая очистка пустых значений завершена")
    logger.info("=" * 60)

def main():
    """Основная функция скрипта"""
    logger = setup_logging()
    logger.info("Начало работы скрипта извлечения дубликатов wl_china")
    
    try:
        # Подключение к базе данных
        conn = get_database_connection()
        logger.info("Подключение к базе данных установлено")
        
        # Создание таблицы дубликатов
        create_duplicates_table(conn, 'duplicates_wl_china', logger)
        
        # Поиск дубликатов
        logger.info("=" * 60)
        logger.info("ПОИСК ДУБЛИКАТОВ")
        logger.info("=" * 60)
        
        logger.info("Поиск дубликатов в wl_china по полям 'Номер_чертежа' и '_Номер_сварного_шва'")
        wl_china_duplicates = get_duplicates_wl_china(conn, logger)
        
        if wl_china_duplicates.empty:
            logger.info("Дубликаты не найдены в wl_china")
            return
        
        logger.info(f"Найдено {len(wl_china_duplicates)} записей дубликатов в wl_china")
        
        # Получение существующих дубликатов
        existing_duplicates_df = get_existing_duplicates(conn, 'duplicates_wl_china', logger)
        
        # Фильтрация новых дубликатов
        new_duplicates_df = filter_new_duplicates(wl_china_duplicates, existing_duplicates_df, logger)
        
        # Вставка новых дубликатов
        if not new_duplicates_df.empty:
            insert_duplicates_to_db(conn, new_duplicates_df, 'duplicates_wl_china', logger)
            logger.info(f"Новые записи дубликатов успешно сохранены в таблицу duplicates_wl_china")
        else:
            logger.info(f"Все найденные записи дубликатов уже существуют в таблице duplicates_wl_china")
        
        # Генерация отчета
        generate_summary_report(conn, 'duplicates_wl_china', logger)
        
        # Автоматическая очистка пустых значений в таблице дубликатов
        clean_empty_values_in_duplicates_after_extraction(conn, logger)
        
        conn.commit()
        conn.close()
        logger.info("Работа скрипта завершена успешно")
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении скрипта: {e}")
        raise

def run_script():
    """Функция для запуска скрипта через лаунчер"""
    main()

if __name__ == "__main__":
    main() 