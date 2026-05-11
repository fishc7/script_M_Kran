
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
Скрипт для редактирования дубликатов в отдельных таблицах duplicates_wl_report_smr и duplicates_wl_china.

Функционал:
1. Просмотр дубликатов из отдельных таблиц duplicates_wl_report_smr и duplicates_wl_china
2. Редактирование поля "_Что_со_стыком_повторяющимся??!!" 
3. Обновление данных в таблицах дубликатов duplicates_wl_report_smr и duplicates_wl_china
4. Логирование всех изменений
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
    log_filename = "logs/edit_duplicates_and_update.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8', mode='a')
        ]
    )
    return logging.getLogger(__name__)

def get_duplicates_for_editing(conn, table_name, logger):
    """
    Получает дубликаты из указанной таблицы для редактирования
    
    Args:
        conn: Подключение к базе данных
        table_name: Имя таблицы дубликатов
        logger: Логгер для записи информации
        
    Returns:
        DataFrame с дубликатами для редактирования
    """
    if table_name == 'duplicates_wl_report_smr':
        query = """
        SELECT 
            ROWID,
            "Титул",
            "_Стыка", 
            "Дата_сварки",
            "ЛИНИЯ",
            "_ISO",
            "_ОБОЗНАЧЕНИЕ_ШВАРНОГО_ШВА",
            "_Номер_стыка",
            "duplicate_group_id",
            "duplicate_count",
            "extraction_date",
            "check_type",
            "original_id_smr",
            COALESCE("_Что_со_стыком_повторяющимся??!!", '') as "_Что_со_стыком_повторяющимся??!!"
        FROM duplicates_wl_report_smr
        ORDER BY "_ISO", "_Номер_стыка"
        """
    elif table_name == 'duplicates_wl_china':
        query = """
        SELECT 
            ROWID,
            "блок_" as "Титул",
            "Номер_сварного_шва" as "_Стыка", 
            "Дата_сварки",
            "_Линии" as "ЛИНИЯ",
            "Номер_чертежа" as "_ISO",
            "_ОБОЗНАЧЕНИЕ_ШВАРНОГО_ШВА",
            "_Номер_сварного_шва" as "_Номер_стыка",
            "duplicate_group_id",
            "duplicate_count",
            "extraction_date",
            "check_type",
            "original_id_china",
            COALESCE("_Что_со_стыком_повторяющимся??!!", '') as "_Что_со_стыком_повторяющимся??!!"
        FROM duplicates_wl_china
        ORDER BY "Номер_чертежа", "_Номер_сварного_шва"
        """
    else:
        logger.error(f"Неизвестная таблица: {table_name}")
        return pd.DataFrame()
    
    try:
        df = pd.read_sql_query(query, conn)
        logger.info(f"Получено {len(df)} записей дубликатов из {table_name} для редактирования")
        return df
    except Exception as e:
        logger.error(f"Ошибка при получении дубликатов из {table_name}: {e}")
        return pd.DataFrame()

def display_duplicates(df, table_name, logger):
    """
    Отображает дубликаты в удобном формате
    
    Args:
        df: DataFrame с дубликатами
        table_name: Имя таблицы
        logger: Логгер для записи информации
    """
    if df.empty:
        logger.info("Нет дубликатов для отображения")
        return
    
    logger.info("=" * 80)
    logger.info(f"ДУБЛИКАТЫ ДЛЯ РЕДАКТИРОВАНИЯ ИЗ ТАБЛИЦЫ {table_name.upper()}")
    logger.info("=" * 80)
    
    # Группируем по ISO и номеру стыка
    grouped = df.groupby(['_ISO', '_Номер_стыка'])
    
    for (iso, styk), group in grouped:
        logger.info(f"\nISO: {iso} | Стык: {styk}")
        logger.info("-" * 60)
        
        for idx, row in group.iterrows():
            logger.info(f"ROWID: {row['ROWID']}")
            logger.info(f"Тип проверки: {row['check_type']}")
            logger.info(f"Титул: {row['Титул']}")
            logger.info(f"Стык: {row['_Стыка']}")
            logger.info(f"Линия: {row['ЛИНИЯ']}")
            logger.info(f"Дата сварки: {row['Дата_сварки']}")
            logger.info(f"Обозначение шва: {row['_ОБОЗНАЧЕНИЕ_ШВАРНОГО_ШВА']}")
            logger.info(f"Заметка: {row['_Что_со_стыком_повторяющимся??!!']}")
            if 'original_id_smr' in row and pd.notna(row['original_id_smr']):
                logger.info(f"ID_SMR: {row['original_id_smr']}")
            if 'original_id_china' in row and pd.notna(row['original_id_china']):
                logger.info(f"ID_China: {row['original_id_china']}")
            logger.info("-" * 40)

def update_duplicate_note(conn, table_name, rowid, new_note, logger):
    """
    Обновляет заметку для дубликата
    
    Args:
        conn: Подключение к базе данных
        table_name: Имя таблицы дубликатов
        rowid: ROWID записи в таблице дубликатов
        new_note: Новая заметка
        logger: Логгер для записи информации
        
    Returns:
        bool: True если обновление прошло успешно
    """
    cursor = conn.cursor()
    
    try:
        # Проверяем текущее значение перед обновлением
        cursor.execute(f"""
            SELECT "_Что_со_стыком_повторяющимся??!!" 
            FROM {table_name} 
            WHERE ROWID = ?
        """, (rowid,))
        
        current_value = cursor.fetchone()
        if current_value:
            logger.info(f"Текущее значение для ROWID {rowid} в {table_name}: '{current_value[0]}'")
        else:
            logger.warning(f"Запись с ROWID {rowid} не найдена в {table_name} при проверке")
            return False
        
        # Выполняем обновление
        cursor.execute(f"""
            UPDATE {table_name} 
            SET "_Что_со_стыком_повторяющимся??!!" = ?
            WHERE ROWID = ?
        """, (new_note, rowid))
        
        if cursor.rowcount > 0:
            logger.info(f"Обновлена заметка для ROWID {rowid} в {table_name}: '{current_value[0]}' -> '{new_note}'")
            return True
        else:
            logger.warning(f"Запись с ROWID {rowid} не найдена в {table_name} при обновлении")
            return False
            
    except Exception as e:
        logger.error(f"Ошибка при обновлении заметки для ROWID {rowid} в {table_name}: {e}")
        return False

def update_source_table_wl_report_smr(conn, original_id_smr, new_note, logger):
    """
    Обновляет данные в таблице duplicates_wl_report_smr
    
    Args:
        conn: Подключение к базе данных
        original_id_smr: ID записи в таблице wl_report_smr
        new_note: Новая заметка
        logger: Логгер для записи информации
        
    Returns:
        bool: True если обновление прошло успешно
    """
    cursor = conn.cursor()
    
    try:
        # Обновляем данные в таблице дубликатов duplicates_wl_report_smr
        cursor.execute("""
            UPDATE duplicates_wl_report_smr 
            SET "_Что_со_стыком_повторяющимся??!!" = ?
            WHERE original_id_smr = ?
        """, (new_note, original_id_smr))
        
        duplicates_updated = cursor.rowcount > 0
        
        if duplicates_updated:
            logger.info(f"Обновлена запись в duplicates_wl_report_smr (id_smr={original_id_smr}): {new_note}")
        else:
            logger.warning(f"Запись с id_smr={original_id_smr} не найдена в duplicates_wl_report_smr")
        
        return duplicates_updated
            
    except Exception as e:
        logger.error(f"Ошибка при обновлении duplicates_wl_report_smr (id_smr={original_id_smr}): {e}")
        return False

def update_source_table_wl_china(conn, original_id_china, new_note, logger):
    """
    Обновляет данные в таблице duplicates_wl_china
    
    Args:
        conn: Подключение к базе данных
        original_id_china: ID записи в таблице wl_china
        new_note: Новая заметка
        logger: Логгер для записи информации
        
    Returns:
        bool: True если обновление прошло успешно
    """
    cursor = conn.cursor()
    
    try:
        # Обновляем данные в таблице дубликатов duplicates_wl_china
        cursor.execute("""
            UPDATE duplicates_wl_china 
            SET "_Что_со_стыком_повторяющимся??!!" = ?
            WHERE original_id_china = ?
        """, (new_note, original_id_china))
        
        duplicates_updated = cursor.rowcount > 0
        
        if duplicates_updated:
            logger.info(f"Обновлена запись в duplicates_wl_china (id={original_id_china}): {new_note}")
        else:
            logger.warning(f"Запись с id={original_id_china} не найдена в duplicates_wl_china")
        
        return duplicates_updated
            
    except Exception as e:
        logger.error(f"Ошибка при обновлении duplicates_wl_china (id={original_id_china}): {e}")
        return False

def update_all_duplicates_in_group(conn, table_name, group_data, new_note, logger):
    """
    Обновляет все дубликаты в группе (одинаковые ISO и номер стыка)
    
    Args:
        conn: Подключение к базе данных
        table_name: Имя таблицы дубликатов
        group_data: DataFrame с дубликатами одной группы
        new_note: Новая заметка
        logger: Логгер для записи информации
        
    Returns:
        dict: Статистика обновлений
    """
    stats = {
        'duplicates_updated': 0,
        'errors': 0
    }
    
    for idx, row in group_data.iterrows():
        rowid = row['ROWID']
        
        # Обновляем в таблице дубликатов
        if update_duplicate_note(conn, table_name, rowid, new_note, logger):
            stats['duplicates_updated'] += 1
        else:
            stats['errors'] += 1
    
    return stats

def interactive_edit_mode(conn, table_name, logger):
    """
    Интерактивный режим редактирования дубликатов
    
    Args:
        conn: Подключение к базе данных
        table_name: Имя таблицы дубликатов
        logger: Логгер для записи информации
    """
    logger.info("=" * 80)
    logger.info(f"ИНТЕРАКТИВНЫЙ РЕЖИМ РЕДАКТИРОВАНИЯ ТАБЛИЦЫ {table_name.upper()}")
    logger.info("=" * 80)
    
    # Получаем дубликаты
    duplicates_df = get_duplicates_for_editing(conn, table_name, logger)
    
    if duplicates_df.empty:
        logger.info(f"Нет дубликатов для редактирования в таблице {table_name}")
        return
    
    # Отображаем дубликаты
    display_duplicates(duplicates_df, table_name, logger)
    
    # Группируем по ISO и номеру стыка
    grouped = duplicates_df.groupby(['_ISO', '_Номер_стыка'])
    
    total_groups = len(grouped)
    logger.info(f"\nВсего групп дубликатов: {total_groups}")
    
    for i, ((iso, styk), group) in enumerate(grouped, 1):
        logger.info(f"\n{'='*60}")
        logger.info(f"ГРУППА {i}/{total_groups}: ISO={iso}, Стык={styk}")
        logger.info(f"{'='*60}")
        
        # Показываем записи в группе
        for idx, row in group.iterrows():
            logger.info(f"ROWID: {row['ROWID']} | Тип проверки: {row['check_type']} | Заметка: {row['_Что_со_стыком_повторяющимся??!!']}")
        
        # Запрашиваем новую заметку
        current_note = group.iloc[0]['_Что_со_стыком_повторяющимся??!!']
        logger.info(f"\nТекущая заметка для группы: '{current_note}'")
        
        # В реальном приложении здесь был бы input(), но для автоматизации используем логирование
        new_note = input(f"Введите новую заметку для группы (или Enter для пропуска): ").strip()
        
        if new_note:
            # Обновляем все записи в группе
            stats = update_all_duplicates_in_group(conn, table_name, group, new_note, logger)
            
            logger.info(f"Статистика обновления группы {i}:")
            logger.info(f"  - Обновлено в {table_name}: {stats['duplicates_updated']}")
            logger.info(f"  - Ошибок: {stats['errors']}")
        else:
            logger.info("Группа пропущена")

def batch_update_mode(conn, table_name, logger):
    """
    Пакетный режим обновления - применяет одну заметку ко всем дубликатам
    
    Args:
        conn: Подключение к базе данных
        table_name: Имя таблицы дубликатов
        logger: Логгер для записи информации
    """
    logger.info("=" * 80)
    logger.info(f"ПАКЕТНЫЙ РЕЖИМ ОБНОВЛЕНИЯ ТАБЛИЦЫ {table_name.upper()}")
    logger.info("=" * 80)
    
    # Получаем дубликаты
    duplicates_df = get_duplicates_for_editing(conn, table_name, logger)
    
    if duplicates_df.empty:
        logger.info(f"Нет дубликатов для обновления в таблице {table_name}")
        return
    
    # Запрашиваем общую заметку
    new_note = input("Введите общую заметку для всех дубликатов: ").strip()
    
    if not new_note:
        logger.info("Заметка не введена, обновление отменено")
        return
    
    # Группируем по ISO и номеру стыка
    grouped = duplicates_df.groupby(['_ISO', '_Номер_стыка'])
    
    total_groups = len(grouped)
    total_updated = 0
    
    logger.info(f"Применение заметки '{new_note}' к {total_groups} группам дубликатов...")
    
    for i, ((iso, styk), group) in enumerate(grouped, 1):
        logger.info(f"Обработка группы {i}/{total_groups}: ISO={iso}, Стык={styk}")
        
        # Обновляем все записи в группе
        stats = update_all_duplicates_in_group(conn, table_name, group, new_note, logger)
        total_updated += stats['duplicates_updated']
    
    logger.info(f"Пакетное обновление завершено. Обновлено записей: {total_updated}")

def main():
    """Основная функция скрипта"""
    logger = setup_logging()
    logger.info("Начало работы скрипта редактирования дубликатов")
    
    try:
        # Подключение к базе данных
        conn = get_database_connection()
        logger.info("Подключение к базе данных установлено")
        
        # Выбор таблицы для редактирования
        print("\nВыберите таблицу для редактирования:")
        print("1. duplicates_wl_report_smr")
        print("2. duplicates_wl_china")
        print("3. Обе таблицы")
        
        table_choice = input("Введите номер выбора (1-3): ").strip()
        
        if table_choice == "1":
            table_name = "duplicates_wl_report_smr"
            process_table_editing(conn, table_name, logger)
        elif table_choice == "2":
            table_name = "duplicates_wl_china"
            process_table_editing(conn, table_name, logger)
        elif table_choice == "3":
            # Обработка обеих таблиц
            for table_name in ["duplicates_wl_report_smr", "duplicates_wl_china"]:
                logger.info(f"\n{'='*80}")
                logger.info(f"РЕДАКТИРОВАНИЕ ТАБЛИЦЫ {table_name.upper()}")
                logger.info(f"{'='*80}")
                process_table_editing(conn, table_name, logger)
        else:
            logger.error("Неверный выбор таблицы")
            return
        
        conn.commit()
        conn.close()
        logger.info("Работа скрипта завершена успешно")
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении скрипта: {e}")
        raise

def process_table_editing(conn, table_name, logger):
    """
    Обрабатывает редактирование таблицы дубликатов
    
    Args:
        conn: Подключение к базе данных
        table_name: Имя таблицы
        logger: Логгер для записи информации
    """
    # Выбор режима работы
    print(f"\nВыберите режим работы для таблицы {table_name}:")
    print("1. Интерактивный режим (редактирование каждой группы отдельно)")
    print("2. Пакетный режим (применение одной заметки ко всем дубликатам)")
    
    mode = input("Введите номер режима (1 или 2): ").strip()
    
    if mode == "1":
        interactive_edit_mode(conn, table_name, logger)
    elif mode == "2":
        batch_update_mode(conn, table_name, logger)
    else:
        logger.error("Неверный режим работы")

if __name__ == "__main__":
    main()

def run_script():
    """Функция для запуска скрипта из главной формы"""
    main() 