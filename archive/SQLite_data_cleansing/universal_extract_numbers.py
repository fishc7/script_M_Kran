import sqlite3
import os
import re
import sys

# Настройка кодировки для правильной работы с кириллическими символами
if sys.platform.startswith('win'):
    import locale
    try:
        locale.setlocale(locale.LC_ALL, 'Russian_Russia.1251')
    except:
        try:
            locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')
        except:
            pass
    
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    os.environ['PYTHONLEGACYWINDOWSSTDIO'] = 'utf-8'

def safe_print_path(path):
    """Безопасно выводит путь с правильной кодировкой"""
    try:
        return str(path)
    except UnicodeEncodeError:
        try:
            return path.encode('utf-8', errors='replace').decode('utf-8')
        except:
            return os.path.basename(path)

def extract_number_from_text(text):
    """
    Универсальная функция извлечения чисел из текста
    Поддерживает форматы: 'S01', 'S02', '123', 'Joint-123', 'Стык 456' и т.д.
    Убирает ведущие нули из извлеченных чисел.
    """
    if not text:
        return None
    
    text = str(text).strip()
    
    # Убираем названия столбцов, если они попало в данные
    if text in ['Номер_стыка_Welded_joint_No_', 'Welded_joint_No', 'Joint_No', 'Номер стыка', 'Номер_сварного_шва']:
        return None
    
    # Паттерн 1: S01, S02, S123, F04 (буква + цифры)
    match = re.search(r'[A-Z](\d+)', text, re.IGNORECASE)
    if match:
        # Убираем ведущие нули
        number = match.group(1)
        return str(int(number))
    
    # Паттерн 2: любые цифры в строке
    match = re.search(r'(\d+)', text)
    if match:
        # Убираем ведущие нули
        number = match.group(1)
        return str(int(number))
    
    return None

def get_database_tables(db_path):
    """Получает список таблиц в базе данных"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        conn.close()
        return [table[0] for table in tables]
    except Exception as e:
        print(f"❌ Ошибка при получении таблиц: {e}")
        return []

def get_table_columns(db_path, table_name):
    """Получает список столбцов в таблице"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # Экранируем имя таблицы обратными кавычками
        cursor.execute(f"PRAGMA table_info(`{table_name}`)")
        columns = cursor.fetchall()
        conn.close()
        return [col[1] for col in columns]
    except Exception as e:
        print(f"❌ Ошибка при получении столбцов: {e}")
        return []

def process_column(db_path, table_name, source_column, target_column):
    """Обрабатывает один столбец в таблице"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Проверяем существование таблицы
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if not cursor.fetchone():
            print(f"⚠️ Таблица '{table_name}' не существует.")
            return 0
        
        # Проверяем существование исходного столбца
        cursor.execute(f"PRAGMA table_info(`{table_name}`)")
        columns = [col[1] for col in cursor.fetchall()]
        if source_column not in columns:
            print(f"⚠️ Столбец '{source_column}' не найден в таблице '{table_name}'.")
            return 0
        
        # Добавляем целевой столбец, если его нет
        try:
            cursor.execute(f'ALTER TABLE `{table_name}` ADD COLUMN "{target_column}" TEXT')
            print(f"✅ Добавлен новый столбец '{target_column}' в {table_name}")
        except Exception as e:
            if "duplicate column name" in str(e).lower():
                print(f"ℹ️ Столбец '{target_column}' уже существует в {table_name}")
            else:
                print(f"⚠️ Ошибка при добавлении столбца: {e}")
        
        # Получаем данные для обработки
        cursor.execute(f'SELECT rowid, "{source_column}" FROM `{table_name}` WHERE "{source_column}" IS NOT NULL')
        records = cursor.fetchall()
        print(f"📊 Найдено записей для обработки в {table_name}: {len(records)}")
        
        # Обрабатываем записи
        updated_count = 0
        for record in records:
            rowid, text_value = record
            extracted_number = extract_number_from_text(text_value)
            if extracted_number is not None:
                cursor.execute(f'UPDATE `{table_name}` SET "{target_column}" = ? WHERE rowid = ?', (extracted_number, rowid))
                updated_count += 1
        
        # Сохраняем изменения
        conn.commit()
        conn.close()
        
        print(f"✅ Обработано записей в {table_name}: {updated_count}")
        return updated_count
        
    except Exception as e:
        print(f"❌ Ошибка при обработке столбца {source_column} в таблице {table_name}: {e}")
        return 0

def main():
    """Главная функция для универсального извлечения чисел"""
    print("=== Универсальное извлечение чисел из столбцов ===")
    
    # Параметры можно передавать через аргументы командной строки или переменные окружения
    db_path = os.environ.get('EXTRACT_DB_PATH')
    table_name = os.environ.get('EXTRACT_TABLE_NAME')
    source_column = os.environ.get('EXTRACT_SOURCE_COLUMN')
    target_column = os.environ.get('EXTRACT_TARGET_COLUMN')
    
    if not all([db_path, table_name, source_column, target_column]):
        print("❌ Не все параметры указаны. Используйте переменные окружения:")
        print("  EXTRACT_DB_PATH - путь к базе данных")
        print("  EXTRACT_TABLE_NAME - название таблицы")
        print("  EXTRACT_SOURCE_COLUMN - исходный столбец")
        print("  EXTRACT_TARGET_COLUMN - целевой столбец")
        return
    
    print(f"📁 База данных: {safe_print_path(db_path)}")
    print(f"📋 Таблица: {table_name}")
    print(f"📊 Исходный столбец: {source_column}")
    print(f"🎯 Целевой столбец: {target_column}")
    
    # Проверяем существование файла базы данных
    if db_path is None or not os.path.exists(db_path):
        print(f"❌ Файл базы данных не найден: {safe_print_path(db_path)}")
        return
    
    # Обрабатываем столбец
    updated_count = process_column(db_path, table_name, source_column, target_column)
    
    if updated_count > 0:
        print(f"\n✅ Извлечение завершено успешно! Обработано записей: {updated_count}")
    else:
        print(f"\n⚠️ Извлечение завершено, но данные не были найдены или обработаны.")

def run_script():
    """Функция для запуска скрипта через лаунчер"""
    try:
        print("🚀 Запуск универсального скрипта извлечения чисел...")
        main()
        print("✅ Универсальный скрипт извлечения чисел завершен успешно!")
    except Exception as e:
        print(f"❌ Ошибка при запуске скрипта: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    run_script() 