import sqlite3
import os
import pandas as pd
import re

def create_database():
    # Путь к базе данных
    db_path = r"../../../../BD_Kingisepp/M_Kran_Kingesepp.db"
    
    # Проверяем существование файла базы данных
    if not os.path.exists(db_path):
        print(f"Ошибка: База данных не найдена по пути: {os.path.abspath(db_path)}")
        return
    
    print(f"База данных найдена: {os.path.abspath(db_path)}")
    
    # Создаем директорию, если она не существует
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Подключаемся к базе данных
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Проверяем существование таблицы
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='naks_home'")
    if not cursor.fetchone():
        # Создаем таблицу только если она не существует
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS naks_home (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            "Номер удостоверения" TEXT,
            "Доп. Атт." TEXT,
            "Окончание срока действия удостоверения" TEXT,
            "Cрок продления" TEXT,
            "ФИО" TEXT,
            "Шифр клейма" TEXT,
            "Место работы" TEXT,
            "Должность" TEXT,
            "AЦ" TEXT,
            "AП" TEXT,
            "Дата аттестации" TEXT,
            "Вид деятельности" TEXT,
            "Область аттестации (Подробнее)" TEXT,
            "Вид аттестации" TEXT
        )
        ''')
        conn.commit()
        print("Таблица naks_home создана успешно")
    else:
        print("Таблица naks_home уже существует")
    
    conn.close()

def clean_data(df):
    # Удаляем лишние пробелы во всех строковых столбцах
    for col in df.columns:
        if df[col].dtype == 'object':  # если столбец содержит строки
            df[col] = df[col].str.strip() if df[col].dtype == 'object' else df[col]
    
    # Удаляем текст "Подробнее" из столбца "Область аттестации (Подробнее)" в любом регистре
    if 'Область аттестации (Подробнее)' in df.columns:
        df['Область аттестации (Подробнее)'] = df['Область аттестации (Подробнее)'].apply(
            lambda x: re.sub(r'подробнее', '', str(x), flags=re.IGNORECASE).strip()
        )
    
    # Заменяем NaN на пустые строки для полей "Доп. Атт." и "Cрок продления"
    if 'Доп. Атт.' in df.columns:
        df['Доп. Атт.'] = df['Доп. Атт.'].fillna('')
    if 'Cрок продления' in df.columns:
        df['Cрок продления'] = df['Cрок продления'].fillna('')
    
    return df

def read_and_insert_data():
    # Путь к файлу с данными
    file_path = r"../../naks_главное.xlsx"
    db_path = r"../../../../BD_Kingisepp/M_Kran_Kingesepp.db"
    
    # Проверяем существование файлов
    if not os.path.exists(file_path):
        print(f"Ошибка: Excel файл не найден по пути: {os.path.abspath(file_path)}")
        return
    
    if not os.path.exists(db_path):
        print(f"Ошибка: База данных не найдена по пути: {os.path.abspath(db_path)}")
        return
    
    print(f"Excel файл найден: {os.path.abspath(file_path)}")
    print(f"База данных найдена: {os.path.abspath(db_path)}")
    
    try:
        # Читаем данные из Excel файла
        print("Чтение Excel файла...")
        df = pd.read_excel(file_path)
        
        # Очищаем данные
        print("Очистка данных...")
        df = clean_data(df)
        
        # Подключаемся к базе данных
        print("Подключение к базе данных...")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получаем существующие записи
        print("Проверка существующих записей...")
        cursor.execute("""
            SELECT "Номер удостоверения", "Доп. Атт.", 
                   "Окончание срока действия удостоверения", "Cрок продления"
            FROM naks_home
        """)
        existing_records = {
            (str(row[0]), str(row[1]), str(row[2]), str(row[3]))
            for row in cursor.fetchall()
        }
        
        # Подготовка данных для вставки
        new_records = []
        skipped_records = 0
        
        for _, row in df.iterrows():
            # Формируем ключ для проверки уникальности
            key = (
                str(row['Номер удостоверения']),
                str(row['Доп. Атт.']),
                str(row['Окончание срока действия удостоверения']),
                str(row['Cрок продления'])
            )
            
            if key not in existing_records:
                new_records.append(row)
            else:
                skipped_records += 1
        
        if new_records:
            # Создаем DataFrame только с новыми записями
            new_df = pd.DataFrame(new_records)
            
            # Импорт только новых данных
            print(f"Импорт {len(new_records)} новых записей...")
            new_df.to_sql('naks_home', conn, if_exists='append', index=False)
            
            # Проверка количества импортированных записей
            cursor.execute("SELECT COUNT(*) FROM naks_home")
            total_count = cursor.fetchone()[0]
            
            print(f"\nИмпорт успешно завершен!")
            print(f"Всего записей в базе: {total_count}")
            print(f"Добавлено новых записей: {len(new_records)}")
            print(f"Пропущено существующих записей: {skipped_records}")
        else:
            print("\nНет новых записей для импорта.")
            print(f"Пропущено существующих записей: {skipped_records}")
        
        conn.commit()
        
    except FileNotFoundError:
        print(f"Ошибка: Файл {file_path} не найден")
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    create_database()
    read_and_insert_data() 
    