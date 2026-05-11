import sqlite3
import os
import pandas as pd
import re
from pathlib import Path

def clean_column_name(col_name):
    # Заменяем переносы строк на пробелы
    col_name = str(col_name).replace('\n', ' ')
    # Заменяем множественные пробелы на один пробел
    col_name = re.sub(r'\s+', ' ', col_name)
    # Убираем пробелы в начале и конце
    col_name = col_name.strip()
    return col_name

def create_database():
    # Получаем абсолютный путь к корню проекта (на 4 уровня выше от скрипта)
    script_dir = Path(__file__).parent
    # Путь к основной базе данных в корне проекта
    db_path = script_dir.parent.parent.parent.parent / "BD_Kingisepp" / "M_Kran_Kingesepp.db"
    
    # Создаем директорию, если она не существует
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Подключаемся к базе данных
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    return conn, cursor

def add_missing_columns(cursor, table_name, columns):
    # Получаем существующие столбцы
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = {row[1] for row in cursor.fetchall()}
    
    # Добавляем отсутствующие столбцы
    for col in columns:
        if col not in existing_columns:
            print(f"Добавление нового столбца: {col}")
            cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" TEXT')

def create_and_import():
    conn = None
    try:
        # Получаем абсолютный путь к текущему скрипту
        script_dir = Path(__file__).parent
        # Путь к Excel файлу относительно скрипта
        excel_file = script_dir.parent.parent / "naks_подробнее.xlsx"
        
        # Проверка существования файла
        if not excel_file.exists():
            print(f"Ошибка: Файл не найден: {excel_file}")
            return

        # Подключение к базе данных
        print("Подключение к базе данных...")
        conn, cursor = create_database()
        
        # Чтение Excel файла
        print("Чтение Excel файла...")
        df = pd.read_excel(excel_file)
        
        # Удаляем полностью пустые строки
        df = df.dropna(how='all')
        
        # Создаем словарь для маппинга оригинальных названий на очищенные
        column_mapping = {}
        seen_cleaned_names = {}
        for col in df.columns:
            clean_name = clean_column_name(col)
            if clean_name in seen_cleaned_names:
                seen_cleaned_names[clean_name] += 1
                clean_name = f"{clean_name}_{seen_cleaned_names[clean_name]}"
            else:
                seen_cleaned_names[clean_name] = 1
            column_mapping[col] = clean_name
        
        # Выводим сопоставление оригинальных и очищенных названий столбцов
        print("\nСопоставление оригинальных и очищенных названий столбцов:")
        for orig, clean in column_mapping.items():
            print(f'  "{orig}"  -->  "{clean}"')
        
        # Исключаем столбцы с неинформативными именами
        filtered_mapping = {orig: clean for orig, clean in column_mapping.items() if clean}
        
        # Переименовываем столбцы в DataFrame
        df = df.rename(columns=filtered_mapping)
        df = df[list(filtered_mapping.values())]
        
        # Удаляем дубликаты по номеру удостоверения
        if '_УДОСТОВИРЕНИЕ_НАКС_' in df.columns:
            initial_count = len(df)
            df = df.drop_duplicates(subset=['_УДОСТОВИРЕНИЕ_НАКС_'], keep='first')
            if len(df) < initial_count:
                print(f"Удалено {initial_count - len(df)} дубликатов из Excel файла")
        
        # Проверяем существование таблицы
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='naks_details'")
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            # Создание SQL для создания таблицы
            columns = []
            for original_col, clean_col in filtered_mapping.items():
                if clean_col == "_УДОСТОВИРЕНИЕ_НАКС_":
                    columns.append(f'"{clean_col}" TEXT UNIQUE')
                else:
                    columns.append(f'"{clean_col}" TEXT')
            
            create_table_sql = f'''
            CREATE TABLE IF NOT EXISTS naks_details (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                {', '.join(columns)}
            )
            '''
            
            # Создание таблицы
            print("Создание таблицы naks_details...")
            cursor.execute(create_table_sql)
            conn.commit()
        else:
            # Добавляем отсутствующие столбцы
            print("Проверка и добавление отсутствующих столбцов...")
            add_missing_columns(cursor, 'naks_details', list(filtered_mapping.values()))
            conn.commit()
        
        # Получаем существующие номера удостоверений
        cursor.execute("SELECT _УДОСТОВИРЕНИЕ_НАКС_ FROM naks_details")
        existing_certificates = {row[0] for row in cursor.fetchall() if row[0] is not None}
        
        # Фильтруем только новые записи, исключая пустые значения
        if '_УДОСТОВИРЕНИЕ_НАКС_' in df.columns:
            # Удаляем строки с пустыми номерами удостоверений
            df = df.dropna(subset=['_УДОСТОВИРЕНИЕ_НАКС_'])
            new_records = df[~df['_УДОСТОВИРЕНИЕ_НАКС_'].isin(existing_certificates)]
        else:
            print("Предупреждение: Столбец '_УДОСТОВИРЕНИЕ_НАКС_' не найден")
            new_records = df
        
        if len(new_records) > 0:
            print(f"Найдено {len(new_records)} новых записей для импорта...")
            # Импорт только новых данных
            new_records.to_sql('naks_details', conn, if_exists='append', index=False)
            print(f"Импортировано {len(new_records)} новых записей.")
        else:
            print("Новых записей для импорта не найдено.")
        
        # Проверка общего количества записей
        cursor.execute("SELECT COUNT(*) FROM naks_details")
        total_count = cursor.fetchone()[0]
        print(f"Всего записей в базе: {total_count}")
        
        # Сохранение изменений и закрытие соединения
        conn.commit()
        conn.close()
        print("Операция завершена успешно.")
        
    except Exception as e:
        print(f"Ошибка: {str(e)}")
        if conn:
            try:
                conn.close()
            except:
                pass

if __name__ == "__main__":
    create_and_import() 
    