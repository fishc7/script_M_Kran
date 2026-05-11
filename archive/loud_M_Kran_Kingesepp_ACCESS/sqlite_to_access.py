import os
import sqlite3
import pyodbc
import pandas as pd
from datetime import datetime
import win32com.client

# Пути к базам данных
sqlite_db_path = os.path.join('..', 'BD_Kingisepp', 'M_Kran_Kingesepp.db')  # Путь к SQLite базе с расширением
access_db_path = os.path.join('BD_Kingisepp', 'M_Kran_Kingesepp.accdb')  # Путь к Access базе

# Создаем папку для Access базы, если она не существует
os.makedirs(os.path.dirname(access_db_path), exist_ok=True)

# Создаем Access базу данных, если она не существует
if not os.path.exists(access_db_path):
    try:
        access = win32com.client.Dispatch("Access.Application")
        access.DBEngine.CreateDatabase(access_db_path, win32com.client.constants.dbLangGeneral)
        access.Quit()
        print(f"Создана новая база данных Access: {access_db_path}")
    except Exception as e:
        print(f"Ошибка при создании базы данных Access: {str(e)}")
        raise

# Строка подключения к Access
conn_str = (
    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
    f'DBQ={os.path.abspath(access_db_path)};'
)

def get_access_type(sqlite_type, value, col_name, table_name, max_len=None):
    """Определяет тип данных Access на основе типа SQLite и значения"""
    # Для logs_lnk и tks_registry все текстовые поля определяем по длине
    if table_name in ['tks_registry', 'logs_lnk'] and (sqlite_type.upper() == 'TEXT' or sqlite_type == ''):
        if max_len is not None and max_len > 255:
            return 'LONGTEXT'
        elif max_len is not None:
            return f'TEXT({max_len})'
        else:
            return 'TEXT(255)'
    # Для остальных случаев
    if value is None:
        return 'TEXT(255)'
    
    sqlite_type = sqlite_type.upper()
    if sqlite_type in ('INTEGER', 'INT'):
        return 'INTEGER'
    elif sqlite_type in ('REAL', 'FLOAT', 'DOUBLE'):
        return 'DOUBLE'
    elif sqlite_type == 'TEXT':
        if isinstance(value, str) and len(value) > 255:
            return 'TEXT(1000)'
        return 'TEXT(255)'
    elif sqlite_type == 'BLOB':
        return 'LONGBINARY'
    else:
        return 'TEXT(255)'

def clean_column_name(name):
    """Очищает имя столбца от специальных символов и ограничивает длину"""
    # Заменяем специальные символы на подчеркивание
    clean_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in name)
    # Ограничиваем длину
    if len(clean_name) > 64:
        clean_name = clean_name[:64]
    return clean_name

try:
    # Подключаемся к SQLite базе
    sqlite_conn = sqlite3.connect(sqlite_db_path)
    sqlite_cursor = sqlite_conn.cursor()

    # Получаем список всех таблиц из SQLite
    sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = sqlite_cursor.fetchall()
    print(f'Найдено таблиц в SQLite: {len(tables)}')
    print('Список таблиц:', [table[0] for table in tables])

    # Подключаемся к Access базе
    access_conn = pyodbc.connect(conn_str)
    access_cursor = access_conn.cursor()

    # Для каждой таблицы
    for table in tables:
        table_name = table[0]
        # Пропускаем временные таблицы
        if table_name.startswith('temp_'):
            print(f'Пропуск временной таблицы: {table_name}')
            continue
            
        try:
            print(f'Обработка таблицы: {table_name}')
            
            # Удаляем таблицу, если она существует
            try:
                access_cursor.execute(f'DROP TABLE [{table_name}]')
            except pyodbc.Error as e:
                if 'does not exist' not in str(e):
                    print(f'Предупреждение при удалении таблицы {table_name}: {str(e)}')
            
            # Получаем данные из SQLite
            df = pd.read_sql_query(f'SELECT * FROM {table_name}', sqlite_conn)
            print(f'Загружено {len(df)} строк из таблицы {table_name}')
            
            # Получаем информацию о структуре таблицы
            sqlite_cursor.execute(f'PRAGMA table_info({table_name})')
            columns = sqlite_cursor.fetchall()
            
            # Создаем таблицу в Access
            create_table_sql = f'CREATE TABLE [{table_name}] ('
            column_defs = []
            column_names = []
            
            for col in columns:
                col_name = clean_column_name(col[1])
                col_type = col[2]
                # Для таблиц logs_lnk и tks_registry определяем длину для всех текстовых полей
                if table_name in ['logs_lnk', 'tks_registry']:
                    if not df.empty and not df[col[1]].isna().all():
                        max_len = int(df[col[1]].dropna().astype(str).map(len).max())
                    else:
                        max_len = 255
                    col_type = get_access_type(col_type, None, col_name, table_name, max_len)
                else:
                    sample_value = df[col[1]].dropna().iloc[0] if not df[col[1]].isna().all() else None
                    col_type = get_access_type(col_type, sample_value, col_name, table_name)
                column_defs.append(f'[{col_name}] {col_type}')
                column_names.append(col_name)
            
            create_table_sql += ', '.join(column_defs) + ')'
            
            access_cursor.execute(create_table_sql)
            print(f'Создана новая таблица: {table_name}')

            # Вставляем данные
            row_count = 0
            for _, row in df.iterrows():
                try:
                    # Преобразуем значения с учетом их типов
                    row_values = []
                    for val in row:
                        if pd.isna(val):
                            row_values.append(None)
                        elif isinstance(val, (int, float)):
                            row_values.append(val)
                        else:
                            row_values.append(str(val))
                    
                    placeholders = ', '.join(['?' for _ in row_values])
                    insert_sql = f'INSERT INTO [{table_name}] VALUES ({placeholders})'
                    access_cursor.execute(insert_sql, tuple(row_values))
                    row_count += 1
                except Exception as e:
                    print(f'Ошибка при вставке строки {row_count + 1} в таблицу {table_name}: {str(e)}')
                    print(f'Проблемные данные: {row_values}')
                    raise
            
            access_conn.commit()
            print(f'Таблица {table_name} успешно перенесена (всего {row_count} строк)')

        except Exception as e:
            print(f'Ошибка при обработке таблицы {table_name}: {str(e)}')
            access_conn.rollback()

    print('\nПеренос данных завершен!')

except Exception as e:
    print(f'Произошла ошибка: {str(e)}')

finally:
    # Закрываем соединения
    if 'sqlite_conn' in locals():
        sqlite_conn.close()
    if 'access_conn' in locals():
        access_conn.close() 
        