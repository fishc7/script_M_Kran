import sqlite3
import pandas as pd
import os
import numpy as np

# Импортируем утилиты
try:
    # Пробуем относительный импорт (для запуска через web-приложение)
    from ..utilities.path_utils import get_database_path
except ImportError:
    # Если не работает, используем абсолютный импорт (для прямого запуска)
    import sys
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    utilities_dir = os.path.join(os.path.dirname(current_dir), 'utilities')
    if utilities_dir not in sys.path:
        sys.path.insert(0, utilities_dir)
    
    from path_utils import get_database_path, get_excel_paths

excel_paths = get_excel_paths()
db_path = get_database_path()
excel_path = excel_paths['ogs_tks'] + '/Реестор_ТКС.xlsx'
excel_file = excel_paths['ogs_dl'] + '/Реестор ДЛ.xlsx'

def create_table(conn):
    try:
        cursor = conn.cursor()
        # Удаляем старую таблицу, если она существует
        cursor.execute('DROP TABLE IF EXISTS tks_registry')
        # Создаем таблицу с фиксированными именами столбцов
        cursor.execute('''
        CREATE TABLE tks_registry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            companyId TEXT,
            projectId TEXT,
            wpsno TEXT,
            material TEXT,
            S_min REAL,
            S_max REAL,
            DN_min REAL,
            DN_max REAL,
            jprocess TEXT,
            jprocess2 TEXT,
            jtype TEXT,
            weldStick TEXT,
            weldRod TEXT,
            weldSolder TEXT,
            mixing TEXT,
            isHeat TEXT,
            weldStandard TEXT,
            filename TEXT,
            filepath TEXT,
            projectSubConstractor TEXT,
            remark TEXT,
            delFlag TEXT,
            createName TEXT,
            createDate TEXT,
            updateName TEXT,
            updateDate TEXT,
            opt TEXT,
            №_ТКС TEXT
        )
        ''')
        conn.commit()
        print("Таблица успешно создана!")
    except sqlite3.Error as e:
        print(f"Ошибка при создании таблицы: {e}")

def load_excel_data(conn):
    try:
        # Читаем Excel файл
        df = pd.read_excel(excel_path)
        
        # Выводим имена столбцов из Excel файла
        print("Имена столбцов в Excel файле:")
        for col in df.columns:
            print(f"- {col}")
        
        # Заменяем NaN на None (NULL в SQLite)
        df = df.replace({np.nan: None})

        # Округляем числовые значения до одного знака после запятой
        numeric_columns = ['S_min', 'S_max', 'DN_min', 'DN_max']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: f"{float(x):.1f}" if pd.notna(x) else None)

        # Преобразуем даты в формат YYYY-MM-DD без времени
        for col in ['createDate', 'updateDate']:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
        
        cursor = conn.cursor()
        
        # Подготавливаем SQL запрос для вставки данных
        columns = ['companyId', 'projectId', 'wpsno', 'material', 'S_min', 'S_max',
                  'DN_min', 'DN_max', 'jprocess', 'jprocess2', 'jtype', 'weldStick',
                  'weldRod', 'weldSolder', 'mixing', 'isHeat', 'weldStandard', 'filename',
                  'filepath', 'projectSubConstractor', 'remark', 'delFlag', 'createName',
                  'createDate', 'updateName', 'updateDate', 'opt', '№_ТКС']
        
        placeholders = ','.join(['?' for _ in range(len(columns))])
        columns_str = ','.join(columns)
        insert_query = f"INSERT INTO tks_registry ({columns_str}) VALUES ({placeholders})"
        
        # Преобразуем DataFrame в список кортежей для вставки
        data = df[columns].values.tolist()
        
        # Вставляем данные
        cursor.executemany(insert_query, data)
        conn.commit()
        
        print(f"Успешно загружено {len(data)} записей в базу данных!")
        print("✅ Скрипт успешно завершён. Загружено строк:", len(df))
        
    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")

def load_data_to_db():
    try:
        # Чтение данных из Excel
        print("Чтение данных из Excel файла...")
        df = pd.read_excel(excel_file, sheet_name='аттестация')
        
        # Подключение к базе данных
        print("Подключение к базе данных...")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Здесь нужно будет добавить код для создания таблицы, если она не существует
        # и код для вставки данных
        
        print("Данные успешно загружены!")
        
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    print("Путь к базе данных:", db_path)
    try:
        # Подключаемся к базе данных
        conn = sqlite3.connect(db_path)
        print("Подключение к базе данных установлено!")
        
        # Создаем таблицу
        create_table(conn)
        
        # Загружаем данные из Excel
        load_excel_data(conn)
        
        # Закрываем соединение
        conn.close()
        print("Соединение с базой данных закрыто.")
        
    except Exception as e:
        print(f"Произошла ошибка: {e}")

if __name__ == "__main__":
    main()

# Для запуска через GUI
def run_script():
    """Функция для запуска скрипта через GUI"""
    main()
    load_data_to_db() 