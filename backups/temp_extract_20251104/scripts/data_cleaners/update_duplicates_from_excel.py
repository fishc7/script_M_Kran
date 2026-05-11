
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
Скрипт для обновления данных в таблице duplicates_wl_report_smr из Excel файла.

Функционал:
1. Чтение данных из Excel файла из папки D:\МК_Кран
2. Обновление столбца "_Что_со_стыком_повторяющимся??!!" в таблице duplicates_wl_report_smr
3. Сопоставление записей по ключевым полям (_ISO, _Номер_стыка, _Стыка)
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
    log_filename = "logs/update_duplicates_from_excel.log"
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
        logger.info(f"Найдены листы в Excel файле: {excel_file.sheet_names}")
        
        # Ищем лист с данными дубликатов
        target_sheet = None
        for sheet_name in excel_file.sheet_names:
            if isinstance(sheet_name, str) and any(keyword in sheet_name.lower() for keyword in ['дубли', 'smr', 'отчет']):
                target_sheet = sheet_name
                break
        
        if target_sheet is None:
            # Если не нашли подходящий лист, берем первый
            target_sheet = excel_file.sheet_names[0]
        
        logger.info(f"Используем лист: {target_sheet}")
        
        # Читаем данные
        df = pd.read_excel(file_path, sheet_name=target_sheet)
        logger.info(f"Прочитано {len(df)} строк из Excel файла")
        logger.info(f"Столбцы в Excel: {list(df.columns)}")
        
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при чтении Excel файла: {e}")
        return pd.DataFrame()

def prepare_excel_data(df, logger):
    """
    Подготавливает данные из Excel для сопоставления с базой данных
    
    Args:
        df: DataFrame с данными из Excel
        logger: Логгер для записи информации
        
    Returns:
        DataFrame с подготовленными данными
    """
    # Ищем столбец Original ID в Excel
    id_column = None
    if 'Original ID' in df.columns:
        id_column = 'Original ID'
    elif 'original_id_smr' in df.columns:
        id_column = 'original_id_smr'
    else:
        # Если нет 'Original ID', ищем похожие названия
        id_columns = ['original_id', 'id_smr']
        for col in df.columns:
            if isinstance(col, str):
                col_str = col
            else:
                col_str = str(col)
            if any(str(id_col).lower() in col_str.lower() for id_col in id_columns):
                id_column = col
                break
    
    if id_column is None:
        logger.error(f"Столбец с ID не найден в Excel файле. Доступные столбцы: {list(df.columns)}")
        print(f"❌ Столбец с ID не найден. Доступные столбцы: {list(df.columns)}")
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
        logger.error(f"Столбец с заметками не найден в Excel файле. Доступные столбцы: {list(df.columns)}")
        print(f"❌ Столбец с заметками не найден. Доступные столбцы: {list(df.columns)}")
        return pd.DataFrame()
    
    logger.info(f"Найдены столбцы: {id_column}, {note_column}")
    print(f"✅ Найдены столбцы: {id_column}, {note_column}")
    
    # Создаем новый DataFrame только с нужными столбцами
    result_df = pd.DataFrame()
    result_df['original_id_smr'] = pd.to_numeric(df[id_column], errors='coerce')
    result_df['_Что_со_стыком_повторяющимся??!!'] = df[note_column].fillna('')
    
    # Удаляем строки с пустыми ID
    result_df = result_df.dropna(subset=['original_id_smr'])
    logger.info(f"Подготовлено {len(result_df)} строк для обновления по original_id_smr")
    print(f"✅ Подготовлено {len(result_df)} строк для обновления")
    
    return result_df

def get_duplicates_from_db(conn, logger):
    """
    Получает все записи из таблицы duplicates_wl_report_smr
    
    Args:
        conn: Подключение к базе данных
        logger: Логгер для записи информации
        
    Returns:
        DataFrame с дубликатами из базы данных
    """
    query = """
    SELECT 
        ROWID,
        "original_id_smr",
        "_ISO",
        "_Стыка",
        "_Номер_стыка",
        "_Что_со_стыком_повторяющимся??!!"
    FROM duplicates_wl_report_smr
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        logger.info(f"Получено {len(df)} записей из таблицы duplicates_wl_report_smr")
        return df
    except Exception as e:
        logger.error(f"Ошибка при получении данных из базы: {e}")
        return pd.DataFrame()

def match_and_update_records(conn, excel_df, db_df, logger, force_update=False):
    """
    Для каждой строки из Excel обновляет столбец _Что_со_стыком_повторяющимся??!! в базе,
    если совпадает original_id_smr. Не использует DataFrame из базы, только прямой UPDATE.
    
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
    logger.info("Начинаем обновление по original_id_smr...")
    print(f"🔄 Начинаем обновление {len(excel_df)} записей...")
    
    # Показываем несколько примеров ID из Excel
    print("📋 Примеры ID из Excel:")
    for i, (idx, excel_row) in enumerate(excel_df.iterrows()):
        if i < 5:  # Показываем первые 5
            print(f"   - original_id_smr: {excel_row['original_id_smr']}")
    
    for idx, excel_row in excel_df.iterrows():
        try:
            original_id = int(excel_row['original_id_smr'])
            new_note = excel_row['_Что_со_стыком_повторяющимся??!!'] or ''
            
            # Получаем текущее значение
            cursor.execute('SELECT "_Что_со_стыком_повторяющимся??!!" FROM duplicates_wl_report_smr WHERE original_id_smr = ?', (original_id,))
            row = cursor.fetchone()
            
            if row is None:
                stats['not_found_records'] += 1
                if stats['not_found_records'] <= 5:  # Показываем только первые 5 пропущенных
                    print(f"⚠️ Не найдена запись с original_id_smr {original_id}")
                logger.warning(f"Не найдена запись с original_id_smr {original_id}")
                continue
            current_note = row[0] or ''
            if force_update or current_note != new_note:
                cursor.execute('UPDATE duplicates_wl_report_smr SET "_Что_со_стыком_повторяющимся??!!" = ? WHERE original_id_smr = ?', (new_note, original_id))
                if cursor.rowcount > 0:
                    stats['updated_records'] += 1
                    logger.info(f"Обновлена запись original_id_smr {original_id}: '{current_note}' -> '{new_note}'")
                    if stats['updated_records'] <= 3:  # Показываем первые 3 обновления
                        print(f"✅ Обновлена запись {original_id}: '{current_note}' -> '{new_note}'")
                else:
                    stats['errors'] += 1
                    logger.warning(f"Не удалось обновить запись original_id_smr {original_id}")
            else:
                stats['already_updated_records'] += 1
                if stats['already_updated_records'] <= 3:  # Показываем первые 3 уже обновленных
                    print(f"ℹ️ Запись {original_id} уже обновлена: '{current_note}'")
                logger.debug(f"Запись original_id_smr {original_id} не требует обновления")
        except Exception as e:
            stats['errors'] += 1
            logger.error(f"Ошибка при обработке записи {idx}: {e}")
    
    conn.commit()
    
    # Показываем итоговую статистику
    print(f"\n📊 ИТОГОВАЯ СТАТИСТИКА:")
    print(f"   Всего записей в Excel: {stats['total_excel_records']}")
    print(f"   Обновлено записей: {stats['updated_records']}")
    print(f"   Уже обновлены: {stats['already_updated_records']}")
    print(f"   Не найдено в базе: {stats['not_found_records']}")
    print(f"   Ошибок: {stats['errors']}")
    
    return stats

def generate_update_report(stats, logger):
    """
    Генерирует отчет об обновлениях
    
    Args:
        stats: Словарь со статистикой обновлений
        logger: Логгер для записи информации
    """
    logger.info("=" * 80)
    logger.info("ОТЧЕТ ОБ ОБНОВЛЕНИИ ДАННЫХ")
    logger.info("=" * 80)
    logger.info(f"Всего записей в Excel файле: {stats['total_excel_records']}")
    logger.info(f"Обновлено записей: {stats['updated_records']}")
    logger.info(f"Уже обновлены: {stats['already_updated_records']}")
    logger.info(f"Не найдено в базе: {stats['not_found_records']}")
    logger.info(f"Ошибок: {stats['errors']}")
    logger.info("=" * 80)

def check_data_status(conn, logger):
    """
    Проверяет текущее состояние данных в таблице duplicates_wl_report_smr
    
    Args:
        conn: Подключение к базе данных
        logger: Логгер для записи информации
    """
    cursor = conn.cursor()
    
    # Общая статистика
    cursor.execute('SELECT COUNT(*) FROM duplicates_wl_report_smr')
    total_records = cursor.fetchone()[0]
    
    # Записи с заметками
    cursor.execute('SELECT COUNT(*) FROM duplicates_wl_report_smr WHERE "_Что_со_стыком_повторяющимся??!!" IS NOT NULL AND "_Что_со_стыком_повторяющимся??!!" != ""')
    records_with_notes = cursor.fetchone()[0]
    
    # Записи без заметок
    cursor.execute('SELECT COUNT(*) FROM duplicates_wl_report_smr WHERE "_Что_со_стыком_повторяющимся??!!" IS NULL OR "_Что_со_стыком_повторяющимся??!!" = ""')
    records_without_notes = cursor.fetchone()[0]
    
    # Статистика по типам заметок
    cursor.execute('''
        SELECT "_Что_со_стыком_повторяющимся??!!", COUNT(*) 
        FROM duplicates_wl_report_smr 
        WHERE "_Что_со_стыком_повторяющимся??!!" IS NOT NULL AND "_Что_со_стыком_повторяющимся??!!" != ""
        GROUP BY "_Что_со_стыком_повторяющимся??!!"
        ORDER BY COUNT(*) DESC
    ''')
    note_types = cursor.fetchall()
    
    print("=" * 80)
    print("СТАТУС ДАННЫХ В ТАБЛИЦЕ duplicates_wl_report_smr")
    print("=" * 80)
    print(f"Всего записей: {total_records}")
    print(f"Записей с заметками: {records_with_notes}")
    print(f"Записей без заметок: {records_without_notes}")
    print(f"Процент заполненности: {(records_with_notes/total_records*100):.1f}%")
    print("\nРаспределение по типам заметок:")
    for note_type, count in note_types:
        print(f"  '{note_type}': {count} записей")
    print("=" * 80)
    
    logger.info(f"Проверка статуса данных: всего {total_records}, с заметками {records_with_notes}")
    
    return {
        'total': total_records,
        'with_notes': records_with_notes,
        'without_notes': records_without_notes,
        'note_types': note_types
    }

def main():
    """Основная функция"""
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(description='Обновление данных дубликатов из Excel файла')
    parser.add_argument('--force', action='store_true', help='Принудительное обновление всех записей')
    parser.add_argument('--check', action='store_true', help='Только проверить статус данных без обновления')
    parser.add_argument('--excel-path', type=str, help='Путь к Excel файлу (по умолчанию ищется в D:\\МК_Кран)')
    
    args = parser.parse_args()
    
    logger = setup_logging()
    
    logger.info("Запуск скрипта обновления дубликатов из Excel файла")
    if args.force:
        logger.info("Режим принудительного обновления включен")
    if args.check:
        logger.info("Режим проверки статуса данных включен")
    
    try:
        db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'BD_Kingisepp', 'M_Kran_Kingesepp.db')
        print(f"Путь к базе данных: {os.path.abspath(db_path)}")
        logger.info(f"Путь к базе данных: {os.path.abspath(db_path)}")
        conn = sqlite3.connect(db_path)
        logger.info("Подключение к базе данных установлено")
        
        # Если включен режим проверки, только проверяем статус
        if args.check:
            check_data_status(conn, logger)
            return
        
        # Ищем Excel файл
        if args.excel_path:
            excel_file = args.excel_path
            if not os.path.exists(excel_file):
                error_msg = f"Указанный Excel файл не найден: {excel_file}"
                print(f"❌ {error_msg}")
                logger.error(error_msg)
                return
            print(f"✅ Используем указанный Excel файл: {excel_file}")
        else:
            excel_dir = r"D:\МК_Кран"
            excel_file = None
        
        print(f"🔍 Ищем Excel файлы в папке: {excel_dir}")
        logger.info(f"🔍 Ищем Excel файлы в папке: {excel_dir}")
        
        if not os.path.exists(excel_dir):
            error_msg = f"Папка {excel_dir} не найдена"
            print(f"❌ {error_msg}")
            logging.error(error_msg)
            return
        
        # Показываем все файлы в папке
        all_files = os.listdir(excel_dir)
        excel_files = [f for f in all_files if f.endswith(('.xlsx', '.xls'))]
        print(f"📁 Найдено Excel файлов в папке: {len(excel_files)}")
        for file in excel_files:
            print(f"   - {file}")
        
        # Ищем файлы Excel с названиями, содержащими "дубли" и "отчет smr"
        patterns = [
            "*дубли*отчет*smr*.xlsx",
            "*дубли*отчет*smr*.xls",
            "*отчет*smr*дубли*.xlsx",
            "*отчет*smr*дубли*.xls",
            "*smr*дубли*.xlsx",
            "*smr*дубли*.xls"
        ]
        
        for pattern in patterns:
            files = glob.glob(os.path.join(excel_dir, pattern))
            if files:
                # Возвращаем самый новый файл
                latest_file = max(files, key=os.path.getctime)
                excel_file = latest_file
                print(f"✅ Найден Excel файл по шаблону '{pattern}': {latest_file}")
                logging.info(f"Найден Excel файл: {latest_file}")
                break
        
        if excel_file is None:
            error_msg = f"Excel файл с дубликатами не найден в папке {excel_dir}"
            print(f"❌ {error_msg}")
            print("💡 Убедитесь, что в папке есть файлы с названиями, содержащими 'дубли' и 'отчет smr'")
            logging.error(error_msg)
            return
        
        # Читаем данные из Excel
        print(f"📖 Читаем данные из Excel файла: {excel_file}")
        excel_df = read_excel_data(excel_file, logger)
        if excel_df.empty:
            error_msg = "Не удалось прочитать данные из Excel файла"
            print(f"❌ {error_msg}")
            logger.error(error_msg)
            return
        
        print(f"📊 Прочитано строк из Excel: {len(excel_df)}")
        print(f"📋 Столбцы в Excel файле: {list(excel_df.columns)}")
        
        # Подготавливаем данные из Excel
        print("🔧 Подготавливаем данные из Excel...")
        prepared_excel_df = prepare_excel_data(excel_df, logger)
        if prepared_excel_df.empty:
            error_msg = "Не удалось подготовить данные из Excel файла"
            print(f"❌ {error_msg}")
            logger.error(error_msg)
            return
        
        print(f"✅ Подготовлено строк для обновления: {len(prepared_excel_df)}")
        
        # Получаем данные из базы данных
        db_df = get_duplicates_from_db(conn, logger)
        if db_df.empty:
            logger.error("Не удалось получить данные из базы данных")
            return
        
        print(f"📊 Найдено записей в базе данных: {len(db_df)}")
        print("📋 Примеры ID из базы данных:")
        for i, (idx, row) in enumerate(db_df.iterrows()):
            if i < 5:  # Показываем первые 5
                print(f"   - original_id_smr: {row['original_id_smr']}")
        
        # Сопоставляем и обновляем записи
        force_update = args.force
        if force_update:
            print("🔄 РЕЖИМ ПРИНУДИТЕЛЬНОГО ОБНОВЛЕНИЯ - все записи будут обновлены")
        stats = match_and_update_records(conn, prepared_excel_df, db_df, logger, force_update)
        
        # Генерируем отчет
        generate_update_report(stats, logger)
        
        logger.info("Обновление завершено успешно")
        print("✅ Скрипт успешно завершён. Обновлено строк:", stats['updated_records'])
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Соединение с базой данных закрыто")

if __name__ == "__main__":
    main()

def run_script():
    """Функция для запуска скрипта из главной формы"""
    main() 