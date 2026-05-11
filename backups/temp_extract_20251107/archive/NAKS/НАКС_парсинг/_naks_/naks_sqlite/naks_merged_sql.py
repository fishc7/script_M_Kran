import sqlite3
import os
import pandas as pd
import re

def clean_column_name(col_name):
    # Заменяем переносы строк на пробелы
    col_name = str(col_name).replace('\n', ' ')
    # Заменяем множественные пробелы на один пробел
    col_name = re.sub(r'\s+', ' ', col_name)
    # Убираем пробелы в начале и конце
    col_name = col_name.strip()
    return col_name

def create_database():
    # Путь к базе данных
    db_path = r"D:\WL_project\ST_data\st_naks"
    
    # Создаем директорию, если она не существует
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Подключаемся к базе данных
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    return conn, cursor

def update_table_structure(cursor, table_name, columns):
    # Получаем существующие столбцы
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = {row[1] for row in cursor.fetchall()}
    
    # Добавляем новые столбцы
    for col in columns:
        if col not in existing_columns:
            print(f"Добавление нового столбца: {col}")
            cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" TEXT')

def create_and_import():
    try:
        # Проверка существования файла
        excel_file = r"D:\WL_project\НАКС_парсинг\naks_merged.xlsx"
        if not os.path.exists(excel_file):
            print(f"Ошибка: Файл не найден: {excel_file}")
            return

        # Подключение к базе данных
        print("Подключение к базе данных...")
        conn, cursor = create_database()
        
        # Чтение Excel файла
        print("Чтение Excel файла...")
        df = pd.read_excel(excel_file)
        
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
        
        # Проверяем существование таблицы
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='naks_merged'")
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            # Создание SQL для создания таблицы
            columns = []
            for original_col, clean_col in filtered_mapping.items():
                columns.append(f'"{clean_col}" TEXT')
            
            create_table_sql = f'''
            CREATE TABLE IF NOT EXISTS naks_merged (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                {', '.join(columns)}
            )
            '''
            
            # Создание таблицы
            print("Создание таблицы naks_merged...")
            cursor.execute(create_table_sql)
            conn.commit()
        else:
            # Обновляем структуру существующей таблицы
            print("Обновление структуры таблицы...")
            update_table_structure(cursor, 'naks_merged', list(filtered_mapping.values()))
            conn.commit()
        
        # Проверяем существующие записи
        print("Проверка существующих записей...")
        cursor.execute("SELECT _УДОСТОВИРЕНИЕ_НАКС_ FROM naks_merged")
        existing_certificates = {str(row[0]) for row in cursor.fetchall()}
        
        # Подготовка данных для вставки
        new_records = []
        skipped_records = 0
        
        for _, row in df.iterrows():
            # Проверяем только по номеру удостоверения
            certificate_number = str(row['_УДОСТОВИРЕНИЕ_НАКС_'])
            
            if certificate_number not in existing_certificates:
                new_records.append(row)
            else:
                skipped_records += 1
        
        if new_records:
            # Создаем DataFrame только с новыми записями
            new_df = pd.DataFrame(new_records)
            
            # Импорт только новых данных
            print(f"Импорт {len(new_records)} новых записей...")
            new_df.to_sql('naks_merged', conn, if_exists='append', index=False)
            
            # Проверка количества импортированных записей
            cursor.execute("SELECT COUNT(*) FROM naks_merged")
            total_count = cursor.fetchone()[0]
            
            print(f"\nИмпорт успешно завершен!")
            print(f"Всего записей в базе: {total_count}")
            print(f"Добавлено новых записей: {len(new_records)}")
            print(f"Пропущено существующих записей: {skipped_records}")
        else:
            print("\nНет новых записей для импорта.")
            print(f"Пропущено существующих записей: {skipped_records}")
        
        # Сохранение изменений и закрытие соединения
        conn.commit()
        conn.close()
        
    except Exception as e:
        print(f"Ошибка: {str(e)}")
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    create_and_import() 