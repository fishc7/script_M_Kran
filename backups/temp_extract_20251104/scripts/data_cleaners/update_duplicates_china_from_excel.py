
# Импортируем утилиты
try:
    # Пробуем относительный импорт (для запуска через web-приложение)
    from ..utilities.db_utils import get_database_connection
    from ..utilities.path_utils import get_database_path
except ImportError:
    # Если не работает, используем абсолютный импорт (для прямого запуска)
    import sys
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    utilities_dir = os.path.join(os.path.dirname(current_dir), 'utilities')
    if utilities_dir not in sys.path:
        sys.path.insert(0, utilities_dir)
    
    from db_utils import get_database_connection
    from path_utils import get_database_path
"""
Скрипт для обновления данных в таблице duplicates_wl_china из Excel файла.

Функционал:
1. Чтение данных из Excel файла из папки D:\МК_Кран
2. Обновление столбца "_Что_со_стыком_повторяющимся??!!" в таблице duplicates_wl_china
3. Сопоставление записей по ключевому полю original_id_china
4. Логирование всех операций обновления
5. Создание отчета об обновлениях
"""

import os
import sqlite3
import pandas as pd
from datetime import datetime
import logging
import sys
import glob
import argparse
from utilities.path_utils import get_excel_paths

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
    log_filename = "logs/update_duplicates_china_from_excel.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8', mode='a')
        ]
    )
    return logging.getLogger(__name__)

def read_excel_data(file_path, logger):
    """
    Читает данные из Excel файла
    
    Args:
        file_path: Путь к Excel файлу
        logger: Логгер для записи информации
        
    Returns:
        DataFrame с данными из Excel
    """
    try:
        # Пробуем прочитать все листы
        excel_file = pd.ExcelFile(file_path)
        logger.info(f"Found sheets in Excel file: {excel_file.sheet_names}")
        
        # Ищем лист с данными дубликатов
        target_sheet = None
        for sheet_name in excel_file.sheet_names:
            if isinstance(sheet_name, str) and any(keyword in sheet_name.lower() for keyword in ['дубли', 'china', 'китай']):
                target_sheet = sheet_name
                break
        
        if target_sheet is None:
            # Если не нашли подходящий лист, берем первый
            target_sheet = excel_file.sheet_names[0]
        
        logger.info(f"Using sheet: {target_sheet}")
        
        # Читаем данные
        df = pd.read_excel(file_path, sheet_name=target_sheet)
        logger.info(f"Read {len(df)} rows from Excel file")
        logger.info(f"Columns in Excel: {list(df.columns)}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error reading Excel file: {e}")
        return pd.DataFrame()

def prepare_excel_data(df, logger):
    """
    Подготавливает данные из Excel для сопоставления с базой данных по паре (Номер чертежа, Номер сварного шва)
    
    Args:
        df: DataFrame с данными из Excel
        logger: Логгер для записи информации
        
    Returns:
        DataFrame с подготовленными данными
    """
    # Проверяем наличие столбца "Номер чертежа"
    chertezha_column = None
    if 'Номер чертежа' in df.columns:
        chertezha_column = 'Номер чертежа'
    else:
        logger.error(f"Column 'Номер чертежа' not found in Excel file. Available columns: {list(df.columns)}")
        print(f"Column 'Номер чертежа' not found. Available columns: {list(df.columns)}")
        return pd.DataFrame()
    
    # Проверяем наличие столбца "Номер сварного шва"
    shva_column = None
    if 'Номер сварного шва' in df.columns:
        shva_column = 'Номер сварного шва'
    else:
        logger.error(f"Column 'Номер сварного шва' not found in Excel file. Available columns: {list(df.columns)}")
        print(f"Column 'Номер сварного шва' not found. Available columns: {list(df.columns)}")
        return pd.DataFrame()
    
    # Проверяем наличие столбца с заметками
    note_column = None
    note_columns = ['_Что_со_стыком_повторяющимся??!!', 'Что_со_стыком_повторяющимся', 'Заметка', 'Примечание', 'Комментарий', 'Статус стыка']
    
    for col in df.columns:
        col_str = str(col)
        if any(note_col in col_str for note_col in note_columns):
            note_column = col
            break
    
    if note_column is None:
        logger.error(f"Column with notes not found in Excel file. Available columns: {list(df.columns)}")
        print(f"Column with notes not found. Available columns: {list(df.columns)}")
        return pd.DataFrame()
    
    logger.info(f"Found columns: {chertezha_column}, {shva_column}, {note_column}")
    print(f"Found columns: {chertezha_column}, {shva_column}, {note_column}")
    
    # Создаем новый DataFrame только с нужными столбцами
    result_df = pd.DataFrame()
    result_df['Номер_чертежа'] = df[chertezha_column].fillna('').astype(str).str.strip()
    result_df['Номер_сварного_шва'] = df[shva_column].fillna('').astype(str).str.strip()
    result_df['_Что_со_стыком_повторяющимся??!!'] = df[note_column].fillna('')
    
    # Удаляем строки с пустыми значениями
    result_df = result_df[(result_df['Номер_чертежа'] != '') & (result_df['Номер_сварного_шва'] != '')]
    logger.info(f"Prepared {len(result_df)} rows for update by pair (Номер чертежа, Номер сварного шва)")
    print(f"Prepared {len(result_df)} rows for update")
    
    return result_df

def get_duplicates_from_db(conn, logger):
    """
    Получает все записи из таблицы duplicates_wl_china
    
    Args:
        conn: Подключение к базе данных
        logger: Логгер для записи информации
        
    Returns:
        DataFrame с дубликатами из базы данных
    """
    query = """
    SELECT 
        ROWID,
        "Номер_чертежа",
        "Номер_сварного_шва",
        "_Что_со_стыком_повторяющимся??!!"
    FROM duplicates_wl_china
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        logger.info(f"Received {len(df)} records from duplicates_wl_china table")
        return df
    except Exception as e:
        logger.error(f"Error receiving data from database: {e}")
        return pd.DataFrame()

def match_and_update_records(conn, excel_df, db_df, logger, force_update=False):
    """
    Для каждой строки из Excel обновляет столбец _Что_со_стыком_повторяющимся??!! в базе,
    если совпадает пара (Номер_чертежа, Номер_сварного_шва). Не использует DataFrame из базы, только прямой UPDATE.
    
    Args:
        conn: Подключение к базе данных
        excel_df: DataFrame с данными из Excel
        db_df: DataFrame с данными из базы (не используется)
        logger: Логгер
        force_update: Если True, обновляет все записи, даже если они уже обновлены
    """
    cursor = conn.cursor()
    stats = {
        'total_excel_records': len(excel_df),
        'updated_records': 0,
        'skipped_records': 0,
        'errors': 0,
        'not_found_records': 0,
        'already_updated_records': 0
    }
    logger.info("Starting update by pair (Номер_чертежа, Номер_сварного_шва)...")
    print(f"Starting update for {len(excel_df)} records...")
    
    # Показываем несколько примеров пар из Excel
    print("Examples of pairs (чертеж, шов) from Excel:")
    for i, (idx, excel_row) in enumerate(excel_df.iterrows()):
        if i < 5:  # Показываем первые 5
            print(f"   - ({excel_row['Номер_чертежа']}, {excel_row['Номер_сварного_шва']})")
    
    # Проверяем, есть ли совпадения в базе
    print("Checking for records in the database...")
    cursor = conn.cursor()
    for i, (idx, excel_row) in enumerate(excel_df.iterrows()):
        if i < 3:  # Проверяем первые 3
            nomer_chertezha = excel_row['Номер_чертежа']
            nomer_shva = excel_row['Номер_сварного_шва']
            cursor.execute('SELECT COUNT(*) FROM duplicates_wl_china WHERE "Номер_чертежа" = ? AND "Номер_сварного_шва" = ?', (nomer_chertezha, nomer_shva))
            count = cursor.fetchone()[0]
            print(f"   Pair ({nomer_chertezha}, {nomer_shva}): {count} records found in database")
    
    for idx, excel_row in excel_df.iterrows():
        try:
            nomer_chertezha = excel_row['Номер_чертежа']
            nomer_shva = excel_row['Номер_сварного_шва']
            new_note = excel_row['_Что_со_стыком_повторяющимся??!!'] or ''
            
            # Получаем текущее значение
            cursor.execute('SELECT "_Что_со_стыком_повторяющимся??!!" FROM duplicates_wl_china WHERE "Номер_чертежа" = ? AND "Номер_сварного_шва" = ?', (nomer_chertezha, nomer_shva))
            row = cursor.fetchone()
            
            if row is None:
                stats['not_found_records'] += 1
                if stats['not_found_records'] <= 5:  # Показываем только первые 5 пропущенных
                    print(f"Pair not found: ({nomer_chertezha}, {nomer_shva})")
                logger.warning(f"Pair not found: ({nomer_chertezha}, {nomer_shva})")
                continue
            current_note = row[0] or ''
            if force_update or current_note != new_note:
                cursor.execute('UPDATE duplicates_wl_china SET "_Что_со_стыком_повторяющимся??!!" = ? WHERE "Номер_чертежа" = ? AND "Номер_сварного_шва" = ?', (new_note, nomer_chertezha, nomer_shva))
                if cursor.rowcount > 0:
                    stats['updated_records'] += 1
                    logger.info(f"Updated pair ({nomer_chertezha}, {nomer_shva}): '{current_note}' -> '{new_note}'")
                    if stats['updated_records'] <= 3:  # Показываем первые 3 обновления
                        print(f"Updated pair ({nomer_chertezha}, {nomer_shva}): '{current_note}' -> '{new_note}'")
                else:
                    stats['errors'] += 1
                    logger.warning(f"Failed to update pair: ({nomer_chertezha}, {nomer_shva})")
            else:
                stats['already_updated_records'] += 1
                if stats['already_updated_records'] <= 3:  # Показываем первые 3 уже обновленных
                    print(f"Pair ({nomer_chertezha}, {nomer_shva}) already updated: '{current_note}'")
                logger.debug(f"Pair ({nomer_chertezha}, {nomer_shva}) does not require update")
        except Exception as e:
            stats['errors'] += 1
            logger.error(f"Error processing record {idx}: {e}")
    
    conn.commit()
    
    # Показываем итоговую статистику
    print(f"\nTOTAL STATISTICS:")
    print(f"   Total records in Excel: {stats['total_excel_records']}")
    print(f"   Updated records: {stats['updated_records']}")
    print(f"   Already updated: {stats['already_updated_records']}")
    print(f"   Not found in database: {stats['not_found_records']}")
    print(f"   Errors: {stats['errors']}")
    
    return stats

def generate_update_report(stats, logger):
    """
    Генерирует отчет об обновлении
    
    Args:
        stats: Словарь со статистикой обновления
        logger: Логгер для записи информации
    """
    logger.info("=" * 80)
    logger.info("UPDATE REPORT")
    logger.info("=" * 80)
    logger.info(f"Total records in Excel file: {stats['total_excel_records']}")
    logger.info(f"Updated records: {stats['updated_records']}")
    logger.info(f"Already updated: {stats['already_updated_records']}")
    logger.info(f"Not found in database: {stats['not_found_records']}")
    logger.info(f"Errors: {stats['errors']}")
    logger.info("=" * 80)

def check_data_status(conn, logger):
    """
    Проверяет текущее состояние данных в таблице duplicates_wl_china
    
    Args:
        conn: Подключение к базе данных
        logger: Логгер для записи информации
    """
    cursor = conn.cursor()
    
    # Общая статистика
    cursor.execute('SELECT COUNT(*) FROM duplicates_wl_china')
    total_records = cursor.fetchone()[0]
    
    # Записи с заметками
    cursor.execute('SELECT COUNT(*) FROM duplicates_wl_china WHERE "_Что_со_стыком_повторяющимся??!!" IS NOT NULL AND "_Что_со_стыком_повторяющимся??!!" != ""')
    records_with_notes = cursor.fetchone()[0]
    
    # Записи без заметок
    cursor.execute('SELECT COUNT(*) FROM duplicates_wl_china WHERE "_Что_со_стыком_повторяющимся??!!" IS NULL OR "_Что_со_стыком_повторяющимся??!!" = ""')
    records_without_notes = cursor.fetchone()[0]
    
    # Статистика по типам заметок
    cursor.execute('''
        SELECT "_Что_со_стыком_повторяющимся??!!", COUNT(*) 
        FROM duplicates_wl_china 
        WHERE "_Что_со_стыком_повторяющимся??!!" IS NOT NULL AND "_Что_со_стыком_повторяющимся??!!" != ""
        GROUP BY "_Что_со_стыком_повторяющимся??!!"
        ORDER BY COUNT(*) DESC
    ''')
    note_types = cursor.fetchall()
    
    print("=" * 80)
    print("DATA STATUS IN duplicates_wl_china TABLE")
    print("=" * 80)
    print(f"Total records: {total_records}")
    print(f"Records with notes: {records_with_notes}")
    print(f"Records without notes: {records_without_notes}")
    if total_records > 0:
        print(f"Note fill rate: {(records_with_notes/total_records*100):.1f}%")
    print("\nNote type distribution:")
    for note_type, count in note_types:
        print(f"  '{note_type}': {count} records")
    print("=" * 80)
    
    logger.info(f"Checking data status: total {total_records}, with notes {records_with_notes}")
    
    return {
        'total': total_records,
        'with_notes': records_with_notes,
        'without_notes': records_without_notes,
        'note_types': note_types
    }

def main():
    """Основная функция"""
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(description='Обновление данных дубликатов China из Excel файла')
    parser.add_argument('--force', action='store_true', help='Принудительное обновление всех записей')
    parser.add_argument('--check', action='store_true', help='Только проверить статус данных без обновления')
    parser.add_argument('--excel-path', type=str, help='Путь к Excel файлу (по умолчанию ищется в D:\\МК_Кран)')
    
    args = parser.parse_args()
    
    logger = setup_logging()
    
    logger.info("Starting script to update China duplicates from Excel file")
    print(f"Current working directory: {os.getcwd()}")
    logger.info(f"Current working directory: {os.getcwd()}")
    if args.force:
        logger.info("Force update mode enabled")
    if args.check:
        logger.info("Check data status mode enabled")
    
    try:
        db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'BD_Kingisepp', 'M_Kran_Kingesepp.db')
        print(f"Database path: {os.path.abspath(db_path)}")
        logger.info(f"Database path: {os.path.abspath(db_path)}")
        conn = sqlite3.connect(db_path)
        logger.info("Database connection established")
        
        # Если включен режим проверки, только проверяем статус
        if args.check:
            check_data_status(conn, logger)
            return
        
        # Ищем Excel файл
        if args.excel_path:
            excel_file = args.excel_path
            if not os.path.exists(excel_file):
                error_msg = f"Specified Excel file not found: {excel_file}"
                print(f"{error_msg}")
                logger.error(error_msg)
                return
            print(f"Using specified Excel file: {excel_file}")
        else:
            excel_dir = r"D:\МК_Кран"
            excel_file = None
        
        print(f"Searching for Excel files in folder: {excel_dir}")
        logger.info(f"Searching for Excel files in folder: {excel_dir}")
        
        if not os.path.exists(excel_dir):
            error_msg = f"Folder {excel_dir} not found"
            print(f"{error_msg}")
            logging.error(error_msg)
            return
        
        # Показываем все файлы в папке
        all_files = os.listdir(excel_dir)
        excel_files = [f for f in all_files if f.endswith(('.xlsx', '.xls'))]
        print(f"Found Excel files in folder: {len(excel_files)}")
        print(f"All files in folder {excel_dir}:")
        for file in all_files[:10]:  # Показываем первые 10 файлов
            print(f"   - {file}")
        print(f"Excel files:")
        for file in excel_files:
            print(f"   - {file}")
        
        # Ищем файлы Excel с названиями, содержащими "дубли" и "china" или "китай"
        patterns = [
            "*дубли*china*.xlsx",
            "*дубли*china*.xls",
            "*china*дубли*.xlsx",
            "*china*дубли*.xls",
            "*китай*дубли*.xlsx",
            "*китай*дубли*.xls",
            "*дубли*китай*.xlsx",
            "*дубли*китай*.xls",
            "*китайский*дубли*.xlsx",
            "*китайский*дубли*.xls",
            "*дубли*китайский*.xlsx",
            "*дубли*китайский*.xls"
        ]
        
        for pattern in patterns:
            files = glob.glob(os.path.join(excel_dir, pattern))
            if files:
                # Возвращаем самый новый файл
                latest_file = max(files, key=os.path.getctime)
                excel_file = latest_file
                print(f"Found Excel file by pattern '{pattern}': {latest_file}")
                logging.info(f"Found Excel file: {latest_file}")
                break
        
        if excel_file is None:
            error_msg = f"Excel file with China duplicates not found in folder {excel_dir}"
            print(f"{error_msg}")
            print("Ensure there are files in the folder with names containing 'дубли' and 'china'")
            print(f"Checking patterns:")
            for pattern in patterns:
                files = glob.glob(os.path.join(excel_dir, pattern))
                print(f"   {pattern}: {len(files)} files found")
                for file in files:
                    print(f"     - {file}")
            logging.error(error_msg)
            return
        
        # Читаем данные из Excel
        print(f"Reading data from Excel file: {excel_file}")
        print(f"File size: {os.path.getsize(excel_file)} bytes")
        excel_df = read_excel_data(excel_file, logger)
        if excel_df.empty:
            error_msg = "Failed to read data from Excel file"
            print(f"{error_msg}")
            logger.error(error_msg)
            return
        
        print(f"Read {len(excel_df)} rows from Excel")
        print(f"Columns in Excel file: {list(excel_df.columns)}")
        
        # Проверяем первые несколько строк
        print("First 3 rows from Excel:")
        for i in range(min(3, len(excel_df))):
            print(f"   Row {i+1}: {dict(excel_df.iloc[i])}")
        
        print(f"Read {len(excel_df)} rows from Excel")
        print(f"Columns in Excel file: {list(excel_df.columns)}")
        
        # Подготавливаем данные из Excel
        print("🔧 Preparing Excel data...")
        prepared_excel_df = prepare_excel_data(excel_df, logger)
        if prepared_excel_df.empty:
            error_msg = "Failed to prepare data from Excel file"
            print(f"❌ {error_msg}")
            logger.error(error_msg)
            return
        
        print(f"✅ Prepared {len(prepared_excel_df)} rows for update")
        
        # Получаем данные из базы данных
        db_df = get_duplicates_from_db(conn, logger)
        if db_df.empty:
            logger.error("Failed to receive data from database")
            return
        
        print(f"📊 Found {len(db_df)} records in database")
        print("📋 Examples of pairs (чертеж, шов) from database:")
        for i, (idx, row) in enumerate(db_df.iterrows()):
            if i < 5:  # Показываем первые 5
                print(f"   - ({row['Номер_чертежа']}, {row['Номер_сварного_шва']})")
        
        # Сопоставляем и обновляем записи
        force_update = args.force
        if force_update:
            print("🔄 FORCE UPDATE MODE - all records will be updated")
        stats = match_and_update_records(conn, prepared_excel_df, db_df, logger, force_update)
        
        # Генерируем отчет
        generate_update_report(stats, logger)
        
        logger.info("Update completed successfully")
        print("✅ Script completed successfully. Updated rows:", stats['updated_records'])
        
    except Exception as e:
        logger.error(f"Critical error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main()

def run_script():
    """Функция для запуска скрипта из главной формы"""
    main() 