
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
Скрипт для просмотра дубликатов из отдельных таблиц duplicates_wl_report_smr и duplicates_wl_china.

Функционал:
1. Просмотр дубликатов из таблицы duplicates_wl_report_smr
2. Просмотр дубликатов из таблицы duplicates_wl_china
3. Фильтрация по различным критериям
4. Экспорт результатов в Excel
5. Статистика по дубликатам для каждой таблицы отдельно
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
    log_filename = "logs/view_duplicates.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8', mode='a')
        ]
    )
    return logging.getLogger(__name__)

def get_duplicates_from_table(conn, table_name, logger):
    """
    Получает дубликаты из указанной таблицы
    
    Args:
        conn: Подключение к базе данных
        table_name: Имя таблицы дубликатов
        logger: Логгер для записи информации
        
    Returns:
        DataFrame с дубликатами
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
        logger.info(f"Получено {len(df)} записей дубликатов из таблицы {table_name}")
        return df
    except Exception as e:
        logger.error(f"Ошибка при получении дубликатов из {table_name}: {e}")
        return pd.DataFrame()

def filter_duplicates(df, filter_type, filter_value, logger):
    """
    Фильтрует дубликаты по различным критериям
    
    Args:
        df: DataFrame с дубликатами
        filter_type: Тип фильтра ('iso', 'line', 'note', 'check_type')
        filter_value: Значение для фильтрации
        logger: Логгер для записи информации
        
    Returns:
        DataFrame с отфильтрованными дубликатами
    """
    if df.empty:
        return df
    
    if filter_type == 'iso':
        filtered_df = df[df['_ISO'].str.contains(filter_value, case=False, na=False)]
    elif filter_type == 'line':
        filtered_df = df[df['ЛИНИЯ'].str.contains(filter_value, case=False, na=False)]
    elif filter_type == 'note':
        filtered_df = df[df['_Что_со_стыком_повторяющимся??!!'].str.contains(filter_value, case=False, na=False)]
    elif filter_type == 'check_type':
        filtered_df = df[df['check_type'].str.contains(filter_value, case=False, na=False)]
    else:
        logger.warning(f"Неизвестный тип фильтра: {filter_type}")
        return df
    
    logger.info(f"Применен фильтр '{filter_type}' со значением '{filter_value}': найдено {len(filtered_df)} записей")
    return filtered_df

def display_duplicates_summary(df, table_name, logger):
    """
    Отображает сводку по дубликатам
    
    Args:
        df: DataFrame с дубликатами
        table_name: Имя таблицы
        logger: Логгер для записи информации
    """
    if df.empty:
        logger.info("Нет дубликатов для отображения")
        return
    
    logger.info("=" * 80)
    logger.info(f"СВОДКА ПО ДУБЛИКАТАМ ИЗ ТАБЛИЦЫ {table_name.upper()}")
    logger.info("=" * 80)
    
    # Общая статистика
    total_records = len(df)
    total_groups = df.groupby(['_ISO', '_Номер_стыка']).ngroups
    
    logger.info(f"Общее количество записей: {total_records}")
    logger.info(f"Количество групп дубликатов: {total_groups}")
    
    # Статистика по типам проверок
    check_type_stats = df['check_type'].value_counts()
    logger.info("\nСтатистика по типам проверок:")
    for check_type, count in check_type_stats.items():
        logger.info(f"  {check_type}: {count} записей")
    
    # Статистика по линиям
    line_stats = df['ЛИНИЯ'].value_counts().head(10)
    logger.info("\nТоп-10 линий по количеству дубликатов:")
    for line, count in line_stats.items():
        logger.info(f"  {line}: {count} записей")
    
    # Статистика по ISO
    iso_stats = df['_ISO'].value_counts().head(10)
    logger.info("\nТоп-10 ISO по количеству дубликатов:")
    for iso, count in iso_stats.items():
        logger.info(f"  {iso}: {count} записей")
    
    # Статистика по заметкам
    note_stats = df['_Что_со_стыком_повторяющимся??!!'].value_counts()
    logger.info("\nСтатистика по заметкам:")
    for note, count in note_stats.items():
        if note:  # Показываем только непустые заметки
            logger.info(f"  '{note}': {count} записей")
    
    empty_notes = (df['_Что_со_стыком_повторяющимся??!!'] == '').sum()
    logger.info(f"  Записей без заметок: {empty_notes}")

def display_duplicates_detailed(df, logger, limit=50):
    """
    Отображает детальную информацию о дубликатах
    
    Args:
        df: DataFrame с дубликатами
        logger: Логгер для записи информации
        limit: Максимальное количество записей для отображения
    """
    if df.empty:
        logger.info("Нет дубликатов для отображения")
        return
    
    logger.info("=" * 80)
    logger.info("ДЕТАЛЬНАЯ ИНФОРМАЦИЯ О ДУБЛИКАТАХ")
    logger.info("=" * 80)
    
    # Ограничиваем количество записей для отображения
    display_df = df.head(limit)
    
    # Группируем по ISO и номеру стыка
    grouped = display_df.groupby(['_ISO', '_Номер_стыка'])
    
    for i, ((iso, styk), group) in enumerate(grouped, 1):
        logger.info(f"\nГРУППА {i}: ISO={iso}, Стык={styk}")
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
    
    if len(df) > limit:
        logger.info(f"\n... и еще {len(df) - limit} записей (показано {limit})")

def export_to_excel(df, filename, logger):
    """
    Экспортирует дубликаты в Excel файл
    
    Args:
        df: DataFrame с дубликатами
        filename: Имя файла для экспорта
        logger: Логгер для записи информации
    """
    if df.empty:
        logger.info("Нет данных для экспорта")
        return
    
    try:
        # Создаем Excel файл с несколькими листами
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Основной лист с дубликатами
            df.to_excel(writer, sheet_name='Дубликаты', index=False)
            
            # Лист со сводкой
            summary_data = []
            
            # Статистика по типам проверок
            check_type_stats = df['check_type'].value_counts()
            for check_type, count in check_type_stats.items():
                summary_data.append(['Тип проверки', check_type, count])
            
            # Статистика по линиям
            line_stats = df['ЛИНИЯ'].value_counts().head(20)
            for line, count in line_stats.items():
                summary_data.append(['Линия', line, count])
            
            # Статистика по ISO
            iso_stats = df['_ISO'].value_counts().head(20)
            for iso, count in iso_stats.items():
                summary_data.append(['ISO', iso, count])
            
            summary_df = pd.DataFrame(summary_data, columns=['Тип', 'Значение', 'Количество'])
            summary_df.to_excel(writer, sheet_name='Сводка', index=False)
            
            # Лист с группами дубликатов
            grouped = df.groupby(['_ISO', '_Номер_стыка'])
            group_data = []
            
            for (iso, styk), group in grouped:
                group_data.append([
                    iso, styk, len(group),
                    group['check_type'].iloc[0],
                    group['_Что_со_стыком_повторяющимся??!!'].iloc[0]
                ])
            
            group_df = pd.DataFrame(group_data, columns=['ISO', 'Номер_стыка', 'Количество_дубликатов', 'Тип_проверки', 'Заметка'])
            group_df.to_excel(writer, sheet_name='Группы_дубликатов', index=False)
        
        logger.info(f"Данные экспортированы в файл: {filename}")
        
    except Exception as e:
        logger.error(f"Ошибка при экспорте в Excel: {e}")

def main():
    """Основная функция скрипта"""
    logger = setup_logging()
    logger.info("Начало работы скрипта просмотра дубликатов")
    
    try:
        # Подключение к базе данных
        conn = get_database_connection()
        logger.info("Подключение к базе данных установлено")
        
        # Выбор таблицы для просмотра
        print("\nВыберите таблицу для просмотра:")
        print("1. duplicates_wl_report_smr")
        print("2. duplicates_wl_china")
        print("3. Обе таблицы")
        
        table_choice = input("Введите номер выбора (1-3): ").strip()
        
        if table_choice == "1":
            table_name = "duplicates_wl_report_smr"
            duplicates_df = get_duplicates_from_table(conn, table_name, logger)
            if not duplicates_df.empty:
                display_duplicates_summary(duplicates_df, table_name, logger)
                process_table_operations(duplicates_df, table_name, logger)
        
        elif table_choice == "2":
            table_name = "duplicates_wl_china"
            duplicates_df = get_duplicates_from_table(conn, table_name, logger)
            if not duplicates_df.empty:
                display_duplicates_summary(duplicates_df, table_name, logger)
                process_table_operations(duplicates_df, table_name, logger)
        
        elif table_choice == "3":
            # Обработка обеих таблиц
            for table_name in ["duplicates_wl_report_smr", "duplicates_wl_china"]:
                logger.info(f"\n{'='*80}")
                logger.info(f"ОБРАБОТКА ТАБЛИЦЫ {table_name.upper()}")
                logger.info(f"{'='*80}")
                
                duplicates_df = get_duplicates_from_table(conn, table_name, logger)
                if not duplicates_df.empty:
                    display_duplicates_summary(duplicates_df, table_name, logger)
                    process_table_operations(duplicates_df, table_name, logger)
                else:
                    logger.info(f"Таблица {table_name} пуста или не существует")
        
        else:
            logger.error("Неверный выбор таблицы")
            return
        
        conn.close()
        logger.info("Работа скрипта завершена успешно")
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении скрипта: {e}")
        raise

def process_table_operations(df, table_name, logger):
    """
    Обрабатывает операции с таблицей дубликатов
    
    Args:
        df: DataFrame с дубликатами
        table_name: Имя таблицы
        logger: Логгер для записи информации
    """
    # Выбор режима работы
    print(f"\nВыберите режим работы для таблицы {table_name}:")
    print("1. Просмотр детальной информации")
    print("2. Фильтрация дубликатов")
    print("3. Экспорт в Excel")
    print("4. Все вышеперечисленное")
    
    mode = input("Введите номер режима (1-4): ").strip()
    
    if mode == "1":
        display_duplicates_detailed(df, logger)
    elif mode == "2":
        print("\nТипы фильтров:")
        print("1. По ISO")
        print("2. По типу проверки")
        print("3. По линии")
        print("4. По заметке")
        
        filter_choice = input("Введите номер типа фильтра (1-4): ").strip()
        filter_value = input("Введите значение для фильтрации: ").strip()
        
        filter_map = {'1': 'iso', '2': 'check_type', '3': 'line', '4': 'note'}
        filter_type = filter_map.get(filter_choice, 'iso')
        
        filtered_df = filter_duplicates(df, filter_type, filter_value, logger)
        if not filtered_df.empty:
            display_duplicates_detailed(filtered_df, logger)
    
    elif mode == "3":
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{table_name}_export_{timestamp}.xlsx"
        export_to_excel(df, filename, logger)
    
    elif mode == "4":
        display_duplicates_detailed(df, logger)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{table_name}_export_{timestamp}.xlsx"
        export_to_excel(df, filename, logger)
    
    else:
        logger.error("Неверный режим работы")

if __name__ == "__main__":
    main()

def run_script():
    """Функция для запуска скрипта из главной формы"""
    main() 