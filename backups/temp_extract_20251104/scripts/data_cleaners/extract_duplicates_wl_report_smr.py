
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
Скрипт для извлечения дубликатов сварных швов из таблицы wl_report_smr
и сохранения их в отдельную таблицу duplicates_wl_report_smr.

Функционал:
1. ПЕРВАЯ ПРОВЕРКА: Поиск дубликатов в wl_report_smr по полям "_ISO" и "_Стыка" + id_smr
2. ВТОРАЯ ПРОВЕРКА: Поиск дубликатов в wl_report_smr по полям "_ISO" и "_Номер_стыка" + id_smr
3. Создание таблицы duplicates_wl_report_smr если она не существует
4. Загрузка только новых дубликатов (без повторной загрузки существующих)
5. Логирование всех операций в файл
6. Сохранение дубликатов в базу данных без вывода в консоль
7. Генерация отчета по дубликатам из wl_report_smr
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
        conn = sqlite3.connect(db_path, timeout=30.0)  # Увеличиваем timeout
        # Настройки для предотвращения блокировки
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=10000")
        conn.execute("PRAGMA temp_store=MEMORY")
        return conn

def setup_logging():
    """Настройка логирования"""
    log_filename = "logs/extract_duplicates_wl_report_smr.log"
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
        # Создаем таблицу с оригинальными названиями столбцов wl_report_smr
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                "Титул" TEXT,
                "_Стыка" TEXT,
                "Дата_сварки" TEXT,
                "ЛИНИЯ" TEXT,
                "_ISO" TEXT,
                "_ОБОЗНАЧЕНИЕ_ШВАРНОГО_ШВА" TEXT,
                "_Номер_стыка" TEXT,
                "duplicate_group_id" INTEGER,
                "duplicate_count" INTEGER,
                "extraction_date" TEXT,
                "check_type" TEXT,
                "original_id_smr" INTEGER,
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
        if 'original_id_smr' not in columns:
            try:
                cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN original_id_smr INTEGER')
                logger.info(f"Добавлен столбец original_id_smr в таблицу {table_name}")
            except Exception as e:
                logger.warning(f"Не удалось добавить столбец original_id_smr: {e}")
        
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

def get_duplicates_wl_report_smr_first(conn, logger):
    """
    ПЕРВАЯ проверка: Извлекает дубликаты из таблицы wl_report_smr по полям "_ISO" и "_Стыка"
    
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
            COUNT(*) OVER (PARTITION BY _ISO, _Стыка) AS cnt
        FROM 
            wl_report_smr
        WHERE 
            _ISO IS NOT NULL AND _ISO != '' 
            AND _Стыка IS NOT NULL AND _Стыка != ''
    )
    SELECT 
        Титул, 
        _Стыка, 
        Дата_сварки, 
        ЛИНИЯ, 
        _ISO, 
        _ОБОЗНАЧЕНИЕ_ШВАРНОГО_ШВА, 
        _Номер_стыка, 
        cnt as duplicate_count,
        id_smr as original_id_smr
    FROM 
        duplicates
    WHERE 
        cnt > 1
    ORDER BY 
        _ISO, _Стыка
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        logger.info(f"ПЕРВАЯ ПРОВЕРКА: Найдено {len(df)} записей дубликатов в wl_report_smr по полям '_ISO' и '_Стыка'")
        return df
    except Exception as e:
        logger.error(f"Ошибка при выполнении первой проверки для wl_report_smr: {e}")
        return pd.DataFrame()

def get_duplicates_wl_report_smr_second(conn, logger):
    """
    ВТОРАЯ проверка: Извлекает дубликаты из таблицы wl_report_smr по полям "_ISO" и "_Номер_стыка"
    
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
            COUNT(*) OVER (PARTITION BY _ISO, _Номер_стыка) AS cnt
        FROM 
            wl_report_smr
        WHERE 
            _ISO IS NOT NULL AND _ISO != '' 
            AND _Номер_стыка IS NOT NULL AND _Номер_стыка != ''
    )
    SELECT 
        Титул, 
        _Стыка, 
        Дата_сварки, 
        ЛИНИЯ, 
        _ISO, 
        _ОБОЗНАЧЕНИЕ_ШВАРНОГО_ШВА, 
        _Номер_стыка, 
        cnt as duplicate_count,
        id_smr as original_id_smr
    FROM 
        duplicates
    WHERE 
        cnt > 1
    ORDER BY 
        _ISO, _Номер_стыка
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        logger.info(f"ВТОРАЯ ПРОВЕРКА: Найдено {len(df)} записей дубликатов в wl_report_smr по полям '_ISO' и '_Номер_стыка'")
        return df
    except Exception as e:
        logger.error(f"Ошибка при выполнении второй проверки для wl_report_smr: {e}")
        return pd.DataFrame()



def insert_duplicates_to_db(conn, duplicates_df, table_name, logger):
    """
    Вставляет дубликаты в таблицу базы данных с оптимизированной обработкой
    
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
        # Используем комбинацию ISO + Номер_стыка + Стыка для уникальности
        duplicates_df['duplicate_group_id'] = duplicates_df.groupby(['_ISO', '_Номер_стыка', '_Стыка']).ngroup()
        
        # Альтернативный способ: используем check_type для различения групп
        # duplicates_df['duplicate_group_id'] = duplicates_df.groupby(['_ISO', '_Номер_стыка', 'check_type']).ngroup()
        
        logger.info(f"Создано {duplicates_df['duplicate_group_id'].nunique()} уникальных групп дубликатов")
        
        # Фильтруем строки с пустыми ключевыми полями
        initial_count = len(duplicates_df)
        duplicates_df = duplicates_df[
            duplicates_df['_ISO'].notnull() & (duplicates_df['_ISO'] != '') &
            duplicates_df['_Стыка'].notnull() & (duplicates_df['_Стыка'] != '') &
            duplicates_df['_Номер_стыка'].notnull() & (duplicates_df['_Номер_стыка'] != '')
        ]
        filtered_count = len(duplicates_df)
        
        if initial_count != filtered_count:
            logger.info(f"Отфильтровано {initial_count - filtered_count} строк с пустыми ключевыми полями")
            print(f"ℹ️ Отфильтровано {initial_count - filtered_count} строк с пустыми ключевыми полями")
        
        # Добавляем дату извлечения
        extraction_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duplicates_df['extraction_date'] = extraction_date
        
        # Получаем все существующие original_id_smr для быстрой проверки
        cursor.execute(f"SELECT original_id_smr, \"_Что_со_стыком_повторяющимся??!!\" FROM {table_name}")
        existing_records = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Подготавливаем данные для batch операций
        update_data = []
        insert_data = []
        
        for idx, row in duplicates_df.iterrows():
            original_id = row['original_id_smr']
            
            # Подготавливаем данные для вставки/обновления
            row_data = (
                row['Титул'], row['_Стыка'], row['Дата_сварки'], row['ЛИНИЯ'], 
                row['_ISO'], row['_ОБОЗНАЧЕНИЕ_ШВАРНОГО_ШВА'], row['_Номер_стыка'], 
                row['duplicate_group_id'], row['duplicate_count'], row['extraction_date'], 
                row['check_type'], original_id
            )
            
            if original_id in existing_records:
                # Обновляем существующую запись
                update_data.append(row_data)
            else:
                # Вставляем новую запись
                insert_data.append(row_data + ('',))  # Добавляем пустую заметку
        
        # Выполняем batch операции
        if update_data:
            cursor.executemany(f"""
                UPDATE {table_name} SET
                    Титул = ?, _Стыка = ?, Дата_сварки = ?, ЛИНИЯ = ?, 
                    _ISO = ?, _ОБОЗНАЧЕНИЕ_ШВАРНОГО_ШВА = ?, _Номер_стыка = ?, 
                    duplicate_group_id = ?, duplicate_count = ?, extraction_date = ?, 
                    check_type = ?
                WHERE original_id_smr = ?
            """, update_data)
            logger.info(f"Обновлено {len(update_data)} существующих записей")
        
        if insert_data:
            cursor.executemany(f"""
                INSERT INTO {table_name} (
                    Титул, _Стыка, Дата_сварки, ЛИНИЯ, _ISO, 
                    _ОБОЗНАЧЕНИЕ_ШВАРНОГО_ШВА, _Номер_стыка, duplicate_group_id, 
                    duplicate_count, extraction_date, check_type, original_id_smr,
                    "_Что_со_стыком_повторяющимся??!!"
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, insert_data)
            logger.info(f"Вставлено {len(insert_data)} новых записей")
        
        conn.commit()
        logger.info(f"Успешно обработано {len(duplicates_df)} записей дубликатов в таблицу {table_name}")
        
    except Exception as e:
        logger.error(f"Ошибка при вставке дубликатов в таблицу {table_name}: {e}")
        conn.rollback()
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
    logger.info("СВОДНЫЙ ОТЧЕТ ПО ДУБЛИКАТАМ WL_REPORT_SMR")
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
        
        # Статистика по ISO
        cursor.execute(f"""
            SELECT "_ISO", COUNT(*) as count
            FROM {table_name}
            GROUP BY "_ISO"
            ORDER BY count DESC
            LIMIT 10
        """)
        iso_stats = cursor.fetchall()
        
        if iso_stats:
            logger.info("\nТоп-10 ISO по количеству дубликатов:")
            for iso, count in iso_stats:
                logger.info(f"  {iso}: {count} дубликатов")
        
        # Примеры дубликатов
        cursor.execute(f"""
            SELECT "_ISO", "_Номер_стыка", duplicate_count, check_type
            FROM {table_name}
            ORDER BY duplicate_count DESC
            LIMIT 5
        """)
        examples = cursor.fetchall()
        
        if examples:
            logger.info("\nПримеры дубликатов (топ-5 по количеству):")
            for iso, styk, count, check_type in examples:
                logger.info(f"  ISO: {iso}, Стык: {styk}, Количество: {count}, Проверка: {check_type}")
        
    except Exception as e:
        logger.error(f"Ошибка при генерации отчета: {e}")
    
    logger.info("=" * 60)

def clean_empty_values_in_duplicates_after_extraction(conn, logger):
    """
    Очищает пустые значения в таблице дубликатов wl_report_smr после извлечения
    
    Args:
        conn: Подключение к базе данных
        logger: Логгер для записи информации
    """
    logger.info("=" * 60)
    logger.info("АВТОМАТИЧЕСКАЯ ОЧИСТКА ПУСТЫХ ЗНАЧЕНИЙ В ТАБЛИЦЕ ДУБЛИКАТОВ WL_REPORT_SMR")
    logger.info("=" * 60)
    
    table_name = 'duplicates_wl_report_smr'
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
    logger.info("Начало работы скрипта извлечения дубликатов wl_report_smr")
    
    try:
        # Подключение к базе данных
        conn = get_database_connection()
        logger.info("Подключение к базе данных установлено")
        
        # Создание таблицы дубликатов
        create_duplicates_table(conn, 'duplicates_wl_report_smr', logger)
        
        # Сбор всех дубликатов
        all_duplicates = []
        
        # ПЕРВАЯ ПРОВЕРКА
        logger.info("=" * 60)
        logger.info("ПЕРВАЯ ПРОВЕРКА")
        logger.info("=" * 60)
        
        logger.info("Первая проверка дубликатов в wl_report_smr по полям '_ISO' и '_Стыка'")
        wl_report_first = get_duplicates_wl_report_smr_first(conn, logger)
        if not wl_report_first.empty:
            wl_report_first['check_type'] = 'проверка wl_report_smr по ISO+Стыка'
            all_duplicates.append(wl_report_first)
            logger.info(f"Добавлено {len(wl_report_first)} записей из первой проверки")
        
        # ВТОРАЯ ПРОВЕРКА
        logger.info("=" * 60)
        logger.info("ВТОРАЯ ПРОВЕРКА")
        logger.info("=" * 60)
        
        logger.info("Вторая проверка дубликатов в wl_report_smr по полям '_ISO' и '_Номер_стыка'")
        wl_report_second = get_duplicates_wl_report_smr_second(conn, logger)
        if not wl_report_second.empty:
            wl_report_second['check_type'] = 'проверка wl_report_smr по ISO+Номер_стыка'
            all_duplicates.append(wl_report_second)
            logger.info(f"Добавлено {len(wl_report_second)} записей из второй проверки")
        
        if not all_duplicates:
            logger.info("Дубликаты не найдены в wl_report_smr")
            return
        
        # Объединение всех дубликатов
        combined_duplicates = pd.concat(all_duplicates, ignore_index=True)
        logger.info(f"Общее количество найденных дубликатов в wl_report_smr: {len(combined_duplicates)}")
        
        # ДЕДУПЛИКАЦИЯ: Удаляем дублирующиеся записи по original_id_smr
        initial_count = len(combined_duplicates)
        
        # Анализ пересечений между проверками
        if not wl_report_first.empty and not wl_report_second.empty:
            first_ids = set(wl_report_first['original_id_smr'].tolist())
            second_ids = set(wl_report_second['original_id_smr'].tolist())
            intersection_ids = first_ids.intersection(second_ids)
            
            if intersection_ids:
                logger.info(f"АНАЛИЗ ПЕРЕСЕЧЕНИЙ: {len(intersection_ids)} записей попадают в обе проверки")
                logger.info(f"  Первая проверка (ISO+Стыка): {len(first_ids)} уникальных записей")
                logger.info(f"  Вторая проверка (ISO+Номер_стыка): {len(second_ids)} уникальных записей")
                logger.info(f"  Пересечение: {len(intersection_ids)} записей")
                print(f"ℹ️ АНАЛИЗ: {len(intersection_ids)} записей попадают в обе проверки")
        
        combined_duplicates = combined_duplicates.drop_duplicates(subset=['original_id_smr'], keep='first')
        final_count = len(combined_duplicates)
        
        if initial_count != final_count:
            removed_count = initial_count - final_count
            logger.info(f"ДЕДУПЛИКАЦИЯ: Удалено {removed_count} дублирующихся записей (было {initial_count}, стало {final_count})")
            print(f"ℹ️ ДЕДУПЛИКАЦИЯ: Удалено {removed_count} дублирующихся записей")
        
        logger.info(f"Финальное количество уникальных дубликатов в wl_report_smr: {len(combined_duplicates)}")
        
        # Вставка/обновление дубликатов (INSERT OR REPLACE по original_id_smr)
        insert_duplicates_to_db(conn, combined_duplicates, 'duplicates_wl_report_smr', logger)
        logger.info(f"Записи дубликатов успешно вставлены/обновлены в таблицу duplicates_wl_report_smr")
        
        # Генерация отчета
        generate_summary_report(conn, 'duplicates_wl_report_smr', logger)
        
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