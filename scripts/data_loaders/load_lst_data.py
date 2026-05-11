import sqlite3
import pandas as pd
import os
import numpy as np

# Импортируем утилиты
try:
    # Пробуем относительный импорт (для запуска через web-приложение)
    from ..utilities.db_utils import clean_column_name
    from ..utilities.path_utils import get_database_path
except ImportError:
    # Если не работает, используем абсолютный импорт (для прямого запуска)
    import sys
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    utilities_dir = os.path.join(os.path.dirname(current_dir), 'utilities')
    if utilities_dir not in sys.path:
        sys.path.insert(0, utilities_dir)
    
    from db_utils import clean_column_name, clean_data_values, print_column_cleaning_report
    from path_utils import get_database_path, get_excel_paths

db_path = get_database_path()
excel_path = get_excel_paths()['ogs'] + '/Реестор_ТТ_категория_контроль.xlsx'

def create_table(conn):
    try:
        cursor = conn.cursor()
        # Удаляем старую таблицу, если она существует
        cursor.execute('DROP TABLE IF EXISTS lst_piping')
        
        # Читаем Excel файл для получения структуры
        df = pd.read_excel(excel_path)
        
        # Очищаем имена столбцов
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Создаем SQL запрос для создания таблицы
        columns = ['id INTEGER PRIMARY KEY AUTOINCREMENT']
        for col in df.columns:
            columns.append(f'"{col}" TEXT')
        
        # Добавляем столбец с датой загрузки
        columns.append('Дата_загрузки TEXT')
        
        create_table_sql = f'''
        CREATE TABLE lst_piping (
            {', '.join(columns)}
        )
        '''
        
        cursor.execute(create_table_sql)
        conn.commit()
        print("Таблица успешно создана!")
    except sqlite3.Error as e:
        print(f"Ошибка при создании таблицы: {e}")

def load_excel_data(conn):
    try:
        # Читаем Excel файл
        print("Чтение данных из Excel файла...")
        df = pd.read_excel(excel_path)
        
        # Сохраняем оригинальные названия столбцов для отчета
        original_columns = df.columns.tolist()
        
        # Выводим имена столбцов из Excel файла
        print("\nИмена столбцов в Excel файле:")
        for col in df.columns:
            print(f"- {col}")
        
        # Очищаем имена столбцов с использованием улучшенной функции
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Выводим отчет об очистке названий столбцов
        print_column_cleaning_report(original_columns, df.columns.tolist())
        
        # Очищаем значения в данных от переносов строк
        df = clean_data_values(df)
        
        # Заменяем NaN на None (NULL в SQLite)
        df = df.replace({np.nan: None})

        # Добавляем столбец с датой загрузки
        df['Дата_загрузки'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        
        cursor = conn.cursor()
        
        # Подготавливаем SQL запрос для вставки данных
        columns = df.columns.tolist()
        placeholders = ','.join(['?' for _ in range(len(columns))])
        columns_str = ','.join([f'"{col}"' for col in columns])
        insert_query = f'INSERT INTO lst_piping ({columns_str}) VALUES ({placeholders})'
        
        # Преобразуем DataFrame в список кортежей для вставки
        data = df[columns].values.tolist()
        
        # Вставляем данные
        print("\nЗагрузка данных в базу...")
        cursor.executemany(insert_query, data)
        conn.commit()
        
        print(f"Успешно загружено {len(data)} записей в базу данных!")
        print("✅ Скрипт успешно завершён. Загружено строк:", len(df))
        
    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")

def main():
    print("Путь к базе данных:", db_path)
    try:
        # Подключаемся к базе данных
        print("Подключение к базе данных...")
        conn = sqlite3.connect(db_path)
        print("Подключение к базе данных установлено!")
        
        # Создаем таблицу
        create_table(conn)
        
        # Загружаем данные из Excel
        load_excel_data(conn)
        
        # Закрываем соединение
        conn.close()
        print("\nСоединение с базой данных закрыто.")
        
    except Exception as e:
        print(f"Произошла ошибка: {e}")

if __name__ == "__main__":
    main()

# Для запуска через GUI
def run_script():
    """Функция для запуска скрипта через GUI"""
    main() 