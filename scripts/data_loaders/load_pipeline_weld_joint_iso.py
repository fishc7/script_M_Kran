import sqlite3
import os
import sys
import pandas as pd
from datetime import datetime
import logging

# Импортируем утилиты
try:
    # Пробуем относительный импорт (для запуска через web-приложение)
    from ..utilities.db_utils import get_database_connection
    from ..utilities.path_utils import get_log_path
except ImportError:
    # Если не работает, используем абсолютный импорт (для прямого запуска)
    import sys
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    utilities_dir = os.path.join(os.path.dirname(current_dir), 'utilities')
    if utilities_dir not in sys.path:
        sys.path.insert(0, utilities_dir)
    
    from db_utils import get_database_connection
    from path_utils import get_log_path



# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(get_log_path('load_pipeline_weld_joint_iso'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_existing_columns(cursor, table_name):
    """
    Получает список существующих столбцов в таблице
    """
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    return [col[1] for col in columns]

def add_missing_columns(cursor, table_name, required_columns):
    """
    Добавляет недостающие столбцы в таблицу
    """
    existing_columns = get_existing_columns(cursor, table_name)
    added_columns = []
    
    for column_name in required_columns:
        if column_name not in existing_columns:
            try:
                cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN "{column_name}" TEXT')
                added_columns.append(column_name)
                logger.info(f"✅ Добавлен новый столбец: {column_name}")
            except sqlite3.OperationalError as e:
                logger.warning(f"Не удалось добавить столбец {column_name}: {e}")
    
    if added_columns:
        logger.info(f"📊 Добавлено новых столбцов: {len(added_columns)}")
        logger.info(f"📋 Новые столбцы: {', '.join(added_columns)}")
    else:
        logger.info("ℹ️ Новые столбцы не требуются")
    
    return added_columns

def get_existing_records_hash(cursor, table_name, key_columns):
    """
    Получает хеш существующих записей для проверки дубликатов
    """
    try:
        # Формируем запрос для получения ключевых столбцов
        columns_str = ', '.join([f'"{col}"' for col in key_columns])
        query = f'SELECT {columns_str} FROM {table_name}'
        
        cursor.execute(query)
        existing_records = cursor.fetchall()
        
        # Создаем множество хешей для быстрого поиска
        existing_hashes = set()
        for record in existing_records:
            # Создаем хеш из значений ключевых столбцов
            record_hash = '|'.join([str(val) if val is not None else '' for val in record])
            existing_hashes.add(record_hash)
        
        logger.info(f"📊 Найдено {len(existing_hashes)} существующих записей для проверки дубликатов")
        return existing_hashes
        
    except Exception as e:
        logger.error(f"Ошибка при получении существующих записей: {e}")
        return set()

def check_duplicates_and_updates(df, existing_hashes, key_columns):
    """
    Проверяет дубликаты в данных и возвращает новые записи и записи для обновления
    """
    logger.info("🔍 Проверка дубликатов и обновлений...")
    logger.info(f"🔑 Используемые ключевые столбцы: {key_columns}")
    logger.info(f"📊 Найдено существующих хешей: {len(existing_hashes)}")
    
    new_records = []
    update_records = []
    duplicate_count = 0
    
    # Показываем несколько примеров существующих хешей для отладки
    if existing_hashes:
        sample_hashes = list(existing_hashes)[:3]
        logger.info(f"📋 Примеры существующих хешей:")
        for i, hash_val in enumerate(sample_hashes, 1):
            logger.info(f"   {i}. {hash_val}")
    
    for i, (index, row) in enumerate(df.iterrows(), 1):
        # Создаем хеш записи из ключевых столбцов (приводим все к строковому типу)
        record_values = []
        for col in key_columns:
            if col in df.columns:
                value = row.get(col)
                # Приводим все значения к строковому типу для корректного сравнения
                record_values.append(str(value) if pd.notna(value) else '')
            else:
                record_values.append('')
        
        record_hash = '|'.join(record_values)
        
        # Показываем первые несколько хешей новых записей для отладки
        if i <= 3:
            logger.info(f"📝 Хеш записи {i}: {record_hash}")
        
        # Проверяем, есть ли такая запись уже в базе
        if record_hash in existing_hashes:
            duplicate_count += 1
            # Добавляем в список для обновления (может содержать новые столбцы)
            update_records.append((index, row))
            if duplicate_count <= 5:  # Показываем первые 5 дубликатов
                logger.info(f"🔄 Найден дубликат для обновления: {' | '.join(record_values[:3])}...")
        else:
            new_records.append((index, row))
    
    logger.info(f"📊 Результат проверки дубликатов:")
    logger.info(f"   - Всего записей в Excel: {len(df)}")
    logger.info(f"   - Дубликатов найдено: {duplicate_count}")
    logger.info(f"   - Новых записей для загрузки: {len(new_records)}")
    logger.info(f"   - Записей для обновления: {len(update_records)}")
    
    if duplicate_count > 0:
        logger.info(f"ℹ️ Дубликаты будут обновлены, новые записи загружены")
    
    # Если все записи считаются дубликатами, показываем предупреждение
    if len(new_records) == 0 and len(df) > 0:
        logger.warning("⚠️ ВНИМАНИЕ: Все записи считаются дубликатами!")
        logger.warning("⚠️ Будут выполнены только обновления существующих записей")
    
    return new_records, update_records

def update_existing_records(cursor, update_records, df, existing_columns):
    """
    Обновляет существующие записи новыми данными из Excel
    """
    if not update_records:
        logger.info("ℹ️ Нет записей для обновления")
        return 0
    
    logger.info("🔄 Начинаем обновление существующих записей...")
    
    # Создаем индекс Excel по ключевым полям для быстрого поиска
    excel_index = {}
    for index, row in df.iterrows():
        # Приводим все значения к строковому типу для корректного сравнения
        titul = str(row['Титул']) if pd.notna(row['Титул']) else ''
        iso = str(row['ISO']) if pd.notna(row['ISO']) else ''
        liniya = str(row['Линия']) if pd.notna(row['Линия']) else ''
        klyuch = str(row['ключь_жср_смр']) if pd.notna(row['ключь_жср_смр']) else ''
        styuk = str(row['стык']) if pd.notna(row['стык']) else ''
        
        key = (titul, iso, liniya, klyuch, styuk)
        
        # Сохраняем все данные строки для обновления
        row_data = {}
        for col in df.columns:
            value = row.get(col)
            if pd.notna(value):
                row_data[col] = str(value)
            else:
                row_data[col] = None
        excel_index[key] = row_data
    
    logger.info(f"📊 Создан индекс Excel с {len(excel_index)} записями")
    
    # Получаем все существующие записи для обновления
    cursor.execute('SELECT id, "Титул", "ISO", "Линия", "ключь_жср_смр", "стык" FROM pipeline_weld_joint_iso ORDER BY id')
    existing_records = cursor.fetchall()
    
    updated_count = 0
    not_found_count = 0
    
    for record in existing_records:
        record_id, titul, iso, liniya, klyuch, styuk = record
        # Приводим все значения к строковому типу
        key = (str(titul) if titul else '', str(iso) if iso else '', str(liniya) if liniya else '', 
               str(klyuch) if klyuch else '', str(styuk) if styuk else '')
        
        if key in excel_index:
            new_data = excel_index[key]
            
            # Формируем SET часть UPDATE запроса только для новых столбцов
            set_parts = []
            values = []
            
            for col in existing_columns:
                if col != 'id' and col in new_data and new_data[col] is not None:
                    set_parts.append(f'"{col}" = ?')
                    values.append(new_data[col])
            
            if set_parts:
                values.append(record_id)  # Добавляем ID для WHERE условия
                update_query = f'UPDATE pipeline_weld_joint_iso SET {", ".join(set_parts)} WHERE id = ?'
                
                cursor.execute(update_query, values)
                updated_count += 1
                
                if updated_count <= 5:  # Показываем первые 5 обновлений
                    logger.info(f"   ✅ ID {record_id}: обновлено {len(set_parts)} столбцов")
        else:
            not_found_count += 1
            if not_found_count <= 5:  # Показываем первые 5 ненайденных
                logger.info(f"   ❌ ID {record_id}: не найден в Excel - {key}")
    
    logger.info(f"📊 Результаты обновления:")
    logger.info(f"   - Обновлено записей: {updated_count}")
    logger.info(f"   - Не найдено в Excel: {not_found_count}")
    logger.info(f"   - Всего обработано: {len(existing_records)}")
    
    return updated_count

def create_pipeline_weld_joint_iso_table():
    """
    Создает таблицу pipeline_weld_joint_iso для хранения данных о номерации стыков по ISO
    """
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        logger.info("Подключение к базе данных успешно!")
        
        # Проверяем существование таблицы
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pipeline_weld_joint_iso'")
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            # Создаем таблицу pipeline_weld_joint_iso
            create_table_sql = '''
            CREATE TABLE pipeline_weld_joint_iso (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                "Титул" TEXT,
                "ISO" TEXT,
                "Линия" TEXT,
                "ключь_жср_смр" TEXT,
                "Линия2" TEXT,
                "стык" TEXT,
                "Код_удаления" TEXT,
                "лист" TEXT,
                "повтор" TEXT,
                "открыть" TEXT,
                "Дата_загрузки" TEXT
            )
            '''
            
            cursor.execute(create_table_sql)
            logger.info("✅ Таблица pipeline_weld_joint_iso создана!")
        else:
            logger.info("ℹ️ Таблица pipeline_weld_joint_iso уже существует")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при создании таблицы: {e}")
        import traceback
        logger.error("Полный стек ошибки:")
        logger.error(traceback.format_exc())
        return False

def load_excel_data_to_db(force_load=False):
    """
    Загружает данные из Excel файла в таблицу pipeline_weld_joint_iso
    
    Args:
        force_load (bool): Если True, загружает все данные без проверки дубликатов
    """
    excel_file_path = r"D:\МК_Кран\МК_Кран_Кингесеп\ПТО\номерация стыков по iso\номерация_стыков_по_iso_12460_v01.xlsx"
    
    try:
        # Проверяем существование файла
        if not os.path.exists(excel_file_path):
            logger.error(f"Файл не найден: {excel_file_path}")
            return False
        
        logger.info(f"Читаем Excel файл: {excel_file_path}")
        
        # Читаем Excel файл
        df = pd.read_excel(excel_file_path)
        
        logger.info(f"Прочитано {len(df)} строк из Excel файла")
        logger.info(f"Столбцы в Excel: {df.columns.tolist()}")
        
        # Показываем первые несколько строк для отладки
        logger.info("📋 Первые 3 строки из Excel:")
        for i in range(min(3, len(df))):
            row = df.iloc[i]
            logger.info(f"   {i+1}. {dict(row.head())}")
        
        # Подключаемся к базе данных
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Определяем требуемые столбцы (все столбцы из Excel + системные)
        excel_columns = list(df.columns)
        required_columns = ['id']
        required_columns.extend(excel_columns)
        required_columns.append('Дата_загрузки')
        
        # Добавляем недостающие столбцы
        added_columns = add_missing_columns(cursor, 'pipeline_weld_joint_iso', required_columns)
        
        # Получаем актуальный список столбцов после добавления новых
        existing_columns = get_existing_columns(cursor, 'pipeline_weld_joint_iso')
        logger.info(f"📊 Столбцы в таблице: {existing_columns}")
        
        # Определяем ключевые столбцы для проверки дубликатов
        key_columns = ['Титул', 'ISO', 'Линия', 'ключь_жср_смр', 'стык']
        available_key_columns = [col for col in key_columns if col in existing_columns]
        
        if not available_key_columns:
            logger.warning("⚠️ Не найдены ключевые столбцы для проверки дубликатов")
            available_key_columns = ['Титул']  # Используем только Титул как запасной вариант
        
        logger.info(f"🔑 Ключевые столбцы для проверки дубликатов: {available_key_columns}")
        
        if force_load:
            logger.info("🔄 Принудительная загрузка: проверка дубликатов отключена")
            new_records = [(index, row) for index, row in df.iterrows()]
            update_records = []
        else:
            # Получаем существующие записи для проверки дубликатов
            existing_hashes = get_existing_records_hash(cursor, 'pipeline_weld_joint_iso', available_key_columns)
            
            # Проверяем дубликаты и получаем новые записи и записи для обновления
            new_records, update_records = check_duplicates_and_updates(df, existing_hashes, available_key_columns)
        
        if not new_records and not update_records:
            logger.info("ℹ️ Все записи уже существуют в базе данных. Загрузка не требуется.")
            if not force_load:
                logger.info("💡 Для принудительной загрузки используйте параметр force_load=True")
            conn.close()
            return True
        
        # Если есть записи для обновления, но нет новых записей
        if not new_records and update_records:
            logger.info("ℹ️ Новых записей нет, но есть записи для обновления.")
            # Обновляем существующие записи новыми данными
            updated_count = update_existing_records(cursor, update_records, df, existing_columns)
            conn.commit()
            
            if updated_count > 0:
                logger.info(f"✅ Успешно обновлено {updated_count} существующих записей!")
            else:
                logger.info("ℹ️ Обновления не потребовались.")
            
            conn.close()
            return True
        
        # Подготавливаем данные для вставки
        insert_data = []
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for i, (index, row) in enumerate(new_records, 1):
            # Создаем словарь с данными строки
            row_data = {}
            
            # Добавляем данные из Excel
            for col in df.columns:
                value = row.get(col)
                if pd.notna(value):
                    row_data[col] = str(value)
                else:
                    row_data[col] = None
            
            # Добавляем системные данные
            row_data['Дата_загрузки'] = current_time
            
            # Создаем кортеж значений только для столбцов без id
            values = []
            for col in existing_columns:
                if col != 'id':  # Пропускаем столбец id
                    values.append(row_data.get(col))
            
            insert_data.append(tuple(values))
            
            # Логируем прогресс каждые 100 записей
            if i % 100 == 0:
                logger.info(f"Обработано {i} новых записей из {len(new_records)}")
        
        # Создаем динамический INSERT запрос
        columns_for_insert = [col for col in existing_columns if col != 'id']
        placeholders = ', '.join(['?' for _ in columns_for_insert])
        columns_str = ', '.join([f'"{col}"' for col in columns_for_insert])
        
        insert_query = f'''
        INSERT INTO pipeline_weld_joint_iso ({columns_str})
        VALUES ({placeholders})
        '''
        
        logger.info("Начинаем вставку новых данных...")
        logger.info(f"📝 SQL запрос: {insert_query}")
        
        cursor.executemany(insert_query, insert_data)
        
        # Обновляем существующие записи новыми данными
        updated_count = update_existing_records(cursor, update_records, df, existing_columns)
        
        conn.commit()
        
        logger.info(f"✅ Успешно загружено {len(insert_data)} новых записей в таблицу pipeline_weld_joint_iso!")
        if updated_count > 0:
            logger.info(f"✅ Успешно обновлено {updated_count} существующих записей!")
        
        # Показываем примеры загруженных записей
        cursor.execute("SELECT * FROM pipeline_weld_joint_iso ORDER BY id DESC LIMIT 5")
        sample_rows = cursor.fetchall()
        
        logger.info("📋 Примеры последних загруженных записей:")
        for i, row in enumerate(sample_rows, 1):
            logger.info(f"  {i}. ID: {row[0]}, Титул: {row[1] if len(row) > 1 else 'N/A'}, ISO: {row[2] if len(row) > 2 else 'N/A'}")
        
        # Показываем статистику
        cursor.execute("SELECT COUNT(*) FROM pipeline_weld_joint_iso")
        total_count = cursor.fetchone()[0]
        logger.info(f"📊 Всего записей в таблице pipeline_weld_joint_iso: {total_count}")
        
        # Статистика по уникальным значениям (если столбцы существуют)
        for col in ['Титул', 'ISO', 'стык']:
            if col in existing_columns:
                cursor.execute(f'SELECT COUNT(DISTINCT "{col}") as unique_{col} FROM pipeline_weld_joint_iso')
                unique_count = cursor.fetchone()[0]
                logger.info(f"📈 Уникальных {col}: {unique_count}")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {e}")
        import traceback
        logger.error("Полный стек ошибки:")
        logger.error(traceback.format_exc())
        return False

def verify_data_loading():
    """
    Проверяет корректность загрузки данных
    """
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Проверяем количество записей
        cursor.execute("SELECT COUNT(*) FROM pipeline_weld_joint_iso")
        total_count = cursor.fetchone()[0]
        
        # Проверяем наличие данных
        cursor.execute("SELECT * FROM pipeline_weld_joint_iso LIMIT 1")
        sample = cursor.fetchone()
        
        conn.close()
        
        if total_count > 0 and sample:
            logger.info(f"✓ Проверка успешна: загружено {total_count} записей")
            return True
        else:
            logger.error("✗ Проверка не прошла: данные не загружены")
            return False
            
    except Exception as e:
        logger.error(f"Ошибка при проверке данных: {e}")
        return False

def main(force_load=False):
    """
    Основная функция для запуска скрипта
    
    Args:
        force_load (bool): Если True, загружает все данные без проверки дубликатов
    """
    logger.info("=" * 60)
    logger.info("Начало загрузки данных в таблицу pipeline_weld_joint_iso")
    if force_load:
        logger.info("🔄 РЕЖИМ ПРИНУДИТЕЛЬНОЙ ЗАГРУЗКИ (проверка дубликатов отключена)")
    logger.info("=" * 60)
    
    # Создаем таблицу
    if not create_pipeline_weld_joint_iso_table():
        logger.error("Не удалось создать таблицу. Завершение работы.")
        return
    
    # Загружаем данные
    if not load_excel_data_to_db(force_load=force_load):
        logger.error("Не удалось загрузить данные. Завершение работы.")
        return
    
    # Проверяем загрузку
    if not verify_data_loading():
        logger.error("Проверка загрузки не прошла.")
        return
    
    logger.info("=" * 60)
    logger.info("Загрузка данных завершена успешно!")
    logger.info("=" * 60)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Загрузка данных в таблицу pipeline_weld_joint_iso')
    parser.add_argument('--force', action='store_true', 
                       help='Принудительная загрузка без проверки дубликатов')
    parser.add_argument('--verify-only', action='store_true',
                       help='Только проверка данных без загрузки')
    
    args = parser.parse_args()
    
    if args.verify_only:
        logger.info("=" * 60)
        logger.info("ПРОВЕРКА ДАННЫХ В ТАБЛИЦЕ pipeline_weld_joint_iso")
        logger.info("=" * 60)
        if verify_data_loading():
            logger.info("✅ Проверка данных прошла успешно!")
        else:
            logger.error("❌ Проверка данных не прошла!")
        sys.exit(0)
    else:
        main(force_load=args.force)

# Для запуска через GUI
def run_script():
    """Функция для запуска скрипта через GUI"""
    main()

def run_script_force():
    """Функция для запуска скрипта через GUI с принудительной загрузкой"""
    main(force_load=True) 