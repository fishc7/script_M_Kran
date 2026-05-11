"""
Заявки на неразрушающий контроль (НК) от компании М_Кран
===========================================================

Этот скрипт загружает данные о заявках на проведение неразрушающего контроля
сварных соединений от компании М_Кран в базу данных.

Источники данных:
- Старый формат: D:\МК_Кран\МК_Кран_Кингесеп\НК\Заявки_НК\Заявки_excel_старого вида
- Новый формат: D:\МК_Кран\МК_Кран_Кингесеп\НК\Заявки_НК\Заявки_excel

Функциональность:
- Инкрементальная загрузка (проверка уже обработанных файлов)
- Поддержка двух форматов Excel файлов
- Фильтрация по непустым номерам стыков
- Автоматическое создание таблицы work_order_log_NDT
- Детальная статистика обработки

Автор: AI Assistant
Версия: 1.0
"""

import pandas as pd
import sqlite3
import os
import logging
from datetime import datetime
import json
import sys

# Добавляем пути для импорта модулей
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))  # scripts/data_loaders -> scripts -> project_root
utilities_dir = os.path.join(project_root, 'scripts', 'utilities')

# Добавляем пути в sys.path
for path in [current_dir, utilities_dir, project_root]:
    if path not in sys.path:
        sys.path.insert(0, path)

# Определяем функцию get_log_path локально
def get_log_path(script_name):
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

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(get_log_path('load_work_order_log_NDT'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def get_old_format_mapping():
    """Возвращает маппинг для старых файлов"""
    return {
        '1': 'No',
        '3': 'Зона Area',
        '5': 'Шифр Проекта Project Code',
        '8': 'Исполнительная схема обозначения сварных соединений As-built drawing number',
        '6': 'Лист',
        '7': 'Rev',
        '4': '№ Линии Line number',
        '12': 'Номер стыка Welded joint No',
        '15': 'Диаметр 1, мм. Pipe diameter',
        '16': 'Толщина стенки 1, мм. wall thickness',
        '17': 'Диаметр 2, мм. Pipe diameter',
        '18': 'Толщина стенки 2, мм. wall thickness',
        '21': 'Дата сварки Welding date',
        '10': 'ОБЪЕМ КОНТРОЛЯ СВАРНЫХ ШВОВ Weld control range',
        '23': '№ клейма сварщика 1 Welder1',
        '24': '№ клейма сварщика 2 Welder2',
        '14': 'Тип соединения стыковое ,угловое и т.д Type of welds',
        '22': 'Способ сварки method welding',
        '9': 'Категория трубопровода Pipeline category',
        '26': 'Вид контроля ВИК, РК, ПВК,МК Tyoe of control NDT',
        '2': 'Наименование титула Facility Item Name'
    }

def get_new_format_mapping():
    """Возвращает маппинг для новых файлов"""
    return {
        '1': 'No',
        '2': 'Зона Area',
        '3': 'Шифр Проекта Project Code',
        '4': 'Исполнительная схема обозначения сварных соединений As-built drawing number',
        '5': 'Лист',
        '6': 'Rev',
        '7': '№ Линии Line number',
        '8': 'Номер стыка Welded joint No',
        '9': 'Диаметр 1, мм. Pipe diameter',
        '10': 'Толщина стенки 1, мм. wall thickness',
        '11': 'Диаметр 2, мм. Pipe diameter',
        '12': 'Толщина стенки 2, мм. wall thickness',
        '13': 'Материал1 Material Science1',
        '14': 'Материал2 Material Science2',
        '15': 'Дата сварки Welding date',
        '16': 'ОБЪЕМ КОНТРОЛЯ СВАРНЫХ ШВОВ Weld control range',
        '17': '№ клейма сварщика 1 Welder1',
        '18': '№ клейма сварщика 2 Welder2',
        '19': 'Тип соединения стыковое ,угловое и т.д Type of welds',
        '20': 'Способ сварки method welding',
        '21': 'Категория трубопровода Pipeline category',
        '22': 'Вид контроля ВИК, РК, ПВК,МК Tyoe of control NDT',
        '23': 'Наименование титула Facility Item Name',
        '24': 'команда Team',
        '25': 'Отраслевые стандарты industry'
    }

def get_database_columns():
    """Возвращает точные названия колонок из базы данных"""
    return [
        'No',
        'Зона Area',
        'Шифр Проекта Project Code',
        'Исполнительная схема обозначения сварных соединений As-built drawing number',
        'Лист',
        'Rev',
        '№ Линии Line number',
        'Номер стыка Welded joint No',
        'Диаметр 1, мм. Pipe diameter',
        'Толщина стенки 1, мм. wall thickness',
        'Диаметр 2, мм. Pipe diameter',
        'Толщина стенки 2, мм. wall thickness',
        'Материал1 Material Science1',
        'Материал2 Material Science2',
        'Дата сварки Welding date',
        'ОБЪЕМ КОНТРОЛЯ СВАРНЫХ ШВОВ Weld control range',
        '№ клейма сварщика 1 Welder1',
        '№ клейма сварщика 2 Welder2',
        'Тип соединения стыковое ,угловое и т.д Type of welds',
        'Способ сварки method welding',
        'Категория трубопровода Pipeline category',
        'Вид контроля ВИК, РК, ПВК,МК Tyoe of control NDT',
        'Наименование титула Facility Item Name',
        'команда Team',
        'Отраслевые стандарты industry'
    ]

def table_exists(db_path):
    """Проверяет существование таблицы work_order_log_NDT"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='work_order_log_NDT'
        """)
        exists = cursor.fetchone() is not None
        conn.close()
        return exists
    except Exception as e:
        logging.error(f"Ошибка проверки существования таблицы: {e}")
        return False

def create_table(db_path):
    """Создает таблицу work_order_log_NDT если она не существует"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получаем список колонок из базы данных
        db_columns = get_database_columns()
        
        # Создаем SQL для создания таблицы
        columns_sql = []
        for col in db_columns:
            # Экранируем специальные символы в названиях колонок
            safe_col = f'"{col}"'
            columns_sql.append(f'{safe_col} TEXT')
        
        # Добавляем дополнительные колонки
        columns_sql.append('"source_file" TEXT')
        columns_sql.append('"load_date" TEXT')
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS work_order_log_NDT (
            {', '.join(columns_sql)}
        )
        """
        
        cursor.execute(create_sql)
        conn.commit()
        conn.close()
        
        logging.info("Таблица work_order_log_NDT создана успешно")
        return True
        
    except Exception as e:
        logging.error(f"Ошибка создания таблицы: {e}")
        return False

def get_processed_files(db_path):
    """Получает список уже обработанных файлов из базы данных"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT source_file FROM work_order_log_NDT WHERE source_file IS NOT NULL')
        processed_files = {row[0] for row in cursor.fetchall()}
        conn.close()
        return processed_files
    except Exception as e:
        logging.error(f"Ошибка получения списка обработанных файлов: {e}")
        return set()

def process_old_format_file(file_path):
    """Обрабатывает файл старого формата (данные с 5-й строки)"""
    try:
        # Читаем Excel файл
        df = pd.read_excel(file_path, header=None)
        
        # Получаем маппинг для старых файлов
        mapping = get_old_format_mapping()
        db_columns = get_database_columns()
        
        # Создаем новый DataFrame с правильными колонками
        result_data = {}
        for db_col in db_columns:
            result_data[db_col] = []
        
        # Заполняем данные начиная с 5-й строки (индекс 4)
        for row_idx in range(4, len(df)):
            row_data = {}
            for db_col in db_columns:
                row_data[db_col] = None
            
            # Заполняем данные из найденных соответствий
            for col_num_str, db_col in mapping.items():
                col_num = int(col_num_str) - 1  # Переводим в 0-based индекс
                if col_num < len(df.columns) and row_idx < len(df):
                    value = df.iloc[row_idx, col_num]
                    if pd.notna(value):
                        row_data[db_col] = str(value)
            
            # Проверяем, что номер стыка не пустой
            joint_number = row_data.get('Номер стыка Welded joint No')
            if joint_number and str(joint_number).strip() and str(joint_number).strip() != 'nan':
                # Добавляем строку в результат только если номер стыка не пустой
                for db_col in db_columns:
                    result_data[db_col].append(row_data[db_col])
        
        result_df = pd.DataFrame(result_data)
        return result_df
        
    except Exception as e:
        logging.error(f"Ошибка обработки старого формата файла {file_path}: {e}")
        return None

def process_new_format_file(file_path):
    """Обрабатывает файл нового формата (данные с 4-й строки)"""
    try:
        # Читаем Excel файл начиная с 4-й строки (индекс 3)
        df = pd.read_excel(file_path, header=2, skiprows=[2])  # Заголовки в 3-й строке (индекс 2), данные с 4-й (индекс 3)
        
        # Получаем маппинг для новых файлов
        mapping = get_new_format_mapping()
        db_columns = get_database_columns()
        
        # Создаем новый DataFrame с правильными колонками
        result_data = {}
        for db_col in db_columns:
            result_data[db_col] = []
        
        # Заполняем данные
        for _, row in df.iterrows():
            row_data = {}
            for db_col in db_columns:
                row_data[db_col] = None
            
            # Заполняем данные из найденных соответствий
            for col_num_str, db_col in mapping.items():
                col_num = int(col_num_str) - 1  # Переводим в 0-based индекс
                if col_num < len(row):
                    value = row.iloc[col_num]
                    if pd.notna(value):
                        row_data[db_col] = str(value)
            
            # Проверяем, что номер стыка не пустой
            joint_number = row_data.get('Номер стыка Welded joint No')
            if joint_number and str(joint_number).strip() and str(joint_number).strip() != 'nan':
                # Добавляем строку в результат только если номер стыка не пустой
                for db_col in db_columns:
                    result_data[db_col].append(row_data[db_col])
        
        result_df = pd.DataFrame(result_data)
        return result_df
        
    except Exception as e:
        logging.error(f"Ошибка обработки нового формата файла {file_path}: {e}")
        return None

def load_data_to_database(df, source_file, db_path):
    """Загружает данные в базу данных"""
    try:
        conn = sqlite3.connect(db_path)
        
        # Добавляем информацию об источнике (полный путь к файлу)
        df['source_file'] = source_file
        df['load_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Загружаем данные
        df.to_sql('work_order_log_NDT', conn, if_exists='append', index=False)
        
        conn.close()
        logging.info(f"Загружено {len(df)} заявок НК из файла {source_file}")
        return True
        
    except Exception as e:
        logging.error(f"Ошибка загрузки в БД: {e}")
        return False

def main():
    """Основная функция инкрементальной загрузки заявок на неразрушающий контроль (НК) от компании М_Кран"""
    
    # Определяем базовый путь проекта
    if getattr(sys, 'frozen', False):
        # Если запущено из EXE
        base_path = os.path.dirname(sys.executable)
    else:
        # Если запущено из .py
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Пути к файлам и папкам (используем относительные пути)
    old_format_dir = os.path.join(base_path, 'МК_Кран_Кингесеп', 'НК', 'Заявки_НК', 'Заявки_excel_старого вида')
    new_format_dir = os.path.join(base_path, 'МК_Кран_Кингесеп', 'НК', 'Заявки_НК', 'Заявки_excel')
    db_path = os.path.join(base_path, 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db')
    
    # Альтернативные пути для разных вариантов структуры
    if not os.path.exists(old_format_dir):
        old_format_dir = r'D:\МК_Кран\МК_Кран_Кингесеп\НК\Заявки_НК\Заявки_excel_старого вида'
    if not os.path.exists(new_format_dir):
        new_format_dir = r'D:\МК_Кран\МК_Кран_Кингесеп\НК\Заявки_НК\Заявки_excel'
    if not os.path.exists(db_path):
        # Пробуем разные варианты путей к базе данных
        possible_db_paths = [
            os.path.join(base_path, 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
            os.path.join('..', 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
            os.path.join('..', '..', 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
            r'D:\МК_Кран\script_M_Kran\database\BD_Kingisepp\M_Kran_Kingesepp.db'
        ]
        
        for path in possible_db_paths:
            if os.path.exists(path):
                db_path = path
                break
        else:
            # Если ни один путь не найден, используем последний как fallback
            db_path = possible_db_paths[-1]
    
    logging.info(f"Базовый путь: {base_path}")
    logging.info(f"Путь к БД: {db_path}")
    logging.info(f"Путь к старым файлам: {old_format_dir}")
    logging.info(f"Путь к новым файлам: {new_format_dir}")
    
    # Проверяем и создаем таблицу если она не существует
    if not table_exists(db_path):
        logging.info("Таблица work_order_log_NDT не найдена, создаем новую...")
        if not create_table(db_path):
            logging.error("Не удалось создать таблицу. Завершение работы.")
            print("ОШИБКА: Не удалось создать таблицу work_order_log_NDT")
            return
    else:
        logging.info("Таблица work_order_log_NDT найдена")
    
    # Получаем список уже обработанных файлов
    processed_files = get_processed_files(db_path)
    logging.info(f"Найдено {len(processed_files)} уже обработанных файлов")
    
    # Статистика
    stats = {
        'old_files_processed': 0,
        'new_files_processed': 0,
        'old_files_loaded': 0,
        'new_files_loaded': 0,
        'old_files_skipped': 0,
        'new_files_skipped': 0,
        'errors': []
    }
    
    # Обрабатываем файлы старого формата
    if os.path.exists(old_format_dir):
        logging.info(f"Обработка файлов старого формата в папке: {old_format_dir}")
        
        for file in os.listdir(old_format_dir):
            if file.endswith(('.xlsx', '.xls')):
                file_path = os.path.join(old_format_dir, file)
                
                # Проверяем, был ли файл уже обработан
                if file_path in processed_files:
                    logging.info(f"Пропуск уже обработанной заявки НК: {file}")
                    stats['old_files_skipped'] += 1
                    continue
                
                logging.info(f"Обработка заявки НК (старый формат): {file}")
                stats['old_files_processed'] += 1
                
                try:
                    df = process_old_format_file(file_path)
                    if df is not None and len(df) > 0:
                        if load_data_to_database(df, file_path, db_path):
                            stats['old_files_loaded'] += 1
                        else:
                            stats['errors'].append(f"Ошибка загрузки в БД: {file}")
                    else:
                        logging.info(f"Нет данных с непустыми номерами стыков для загрузки: {file}")
                except Exception as e:
                    stats['errors'].append(f"Ошибка обработки {file}: {e}")
    else:
        logging.warning(f"Папка старых файлов не найдена: {old_format_dir}")
    
    # Обрабатываем файлы нового формата
    if os.path.exists(new_format_dir):
        logging.info(f"Обработка файлов нового формата в папке: {new_format_dir}")
        
        for file in os.listdir(new_format_dir):
            if file.endswith(('.xlsx', '.xls')):
                file_path = os.path.join(new_format_dir, file)
                
                # Проверяем, был ли файл уже обработан
                if file_path in processed_files:
                    logging.info(f"Пропуск уже обработанной заявки НК: {file}")
                    stats['new_files_skipped'] += 1
                    continue
                
                logging.info(f"Обработка заявки НК (новый формат): {file}")
                stats['new_files_processed'] += 1
                
                try:
                    df = process_new_format_file(file_path)
                    if df is not None and len(df) > 0:
                        if load_data_to_database(df, file_path, db_path):
                            stats['new_files_loaded'] += 1
                        else:
                            stats['errors'].append(f"Ошибка загрузки в БД: {file}")
                    else:
                        logging.info(f"Нет данных с непустыми номерами стыков для загрузки: {file}")
                except Exception as e:
                    stats['errors'].append(f"Ошибка обработки {file}: {e}")
    else:
        logging.warning(f"Папка новых файлов не найдена: {new_format_dir}")
    
    # Сохраняем статистику
    stats_file = os.path.join(os.path.dirname(__file__), 'unified_loading_stats.json')
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    
    # Выводим итоговую статистику
    print("\n" + "="*60)
    print("ИТОГОВАЯ СТАТИСТИКА ЗАГРУЗКИ ЗАЯВОК НК ОТ М_КРАН")
    print("="*60)
    print(f"Уже обработанных файлов: {len(processed_files)}")
    print(f"Файлов старого формата обработано: {stats['old_files_processed']}")
    print(f"Файлов старого формата загружено: {stats['old_files_loaded']}")
    print(f"Файлов старого формата пропущено: {stats['old_files_skipped']}")
    print(f"Файлов нового формата обработано: {stats['new_files_processed']}")
    print(f"Файлов нового формата загружено: {stats['new_files_loaded']}")
    print(f"Файлов нового формата пропущено: {stats['new_files_skipped']}")
    print(f"Всего ошибок: {len(stats['errors'])}")
    print("\n💡 Инкрементальная загрузка: проверяется поле source_file")
    print("💡 Фильтрация: загружаются только строки с непустыми номерами стыков")
    print("💡 В поле source_file сохраняется полный путь к файлу")
    print("💡 Автоматическое создание таблицы: если таблица не существует, она создается автоматически")
    
    if stats['errors']:
        print("\nОШИБКИ:")
        for error in stats['errors']:
            print(f"  - {error}")
    
    # Добавляем финальное сообщение о результате
    total_processed = stats['old_files_processed'] + stats['new_files_processed']
    total_loaded = stats['old_files_loaded'] + stats['new_files_loaded']
    
    if total_loaded > 0:
        print(f"\n✅ УСПЕШНО: Загружено {total_loaded} файлов из {total_processed} обработанных")
    elif total_processed > 0:
        print(f"\n⚠️ ВНИМАНИЕ: Обработано {total_processed} файлов, но ничего не загружено (возможно, все файлы уже обработаны)")
    else:
        print(f"\n❌ ОШИБКА: Не удалось обработать ни одного файла")

def run_script():
    """Функция для запуска скрипта из GUI приложения"""
    main()

if __name__ == "__main__":
    main() 
    