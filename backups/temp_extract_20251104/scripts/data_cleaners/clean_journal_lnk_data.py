import sqlite3
import os
import re

def get_database_path():
    """Получает правильный путь к базе данных"""
    # Получаем текущую директорию
    current_dir = os.getcwd()
    print(f"Текущая директория: {current_dir}")
    
    # Пробуем разные варианты путей
    possible_paths = [
        # Если запускаем из исходной папки проекта
        os.path.join(current_dir, 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Если запускаем из папки SQLite_data_cleansing
        os.path.join(current_dir, '..', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Если запускаем из EXE в папке dist
        os.path.join(current_dir, '..', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Если запускаем из EXE в корневой папке проекта
        os.path.join(current_dir, 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
    ]
    
    for path in possible_paths:
        abs_path = os.path.abspath(path)
        print(f"Проверяем путь: {abs_path}")
        if os.path.exists(abs_path):
            print(f"Найдена база данных: {abs_path}")
            return abs_path
    
    # Если не нашли, возвращаем исходный путь
    print("База данных не найдена, используем исходный путь")
    return os.path.join("BD_Kingisepp", "M_Kran_Kingesepp.db")

def clean_request_number():
    """
    Очистка столбца №_заявки:
    - удаляет 'Заявка' + любые пробелы + '№'
    - если после этого осталось только '№' (с пробелами), заменяет на пустую строку
    - если значение начинается с '№' и после него идёт 'TT' + номер, удаляет '№' и пробелы
    - удаляет лишние пробелы и переносы строки в начале и в конце
    """
    # Получаем правильный путь к базе данных
    db_path = get_database_path()
    
    try:
        # Подключаемся к базе данных
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получаем список всех таблиц в базе данных
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            
            # Проверяем наличие столбца №_заявки в таблице
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            has_request_column = any('№_заявки' in col[1] for col in columns)
            
            if has_request_column:
                print(f"\nОбработка таблицы: {table_name}")
                
                # Получаем id и значения
                cursor.execute(f"SELECT rowid, №_заявки FROM {table_name} WHERE №_заявки LIKE '%№%'")
                rows = cursor.fetchall()
                updated = 0
                for rowid, value in rows:
                    if value:
                        # Удаляем 'Заявка' + любые пробелы + '№'
                        new_value = re.sub(r'Заявка\s*№\s*', '', value)
                        # Если осталось только '№' с пробелами, убираем
                        if re.fullmatch(r'\s*№\s*', new_value):
                            new_value = ''
                        else:
                            # Если значение начинается с '№' и после него идёт 'TT' + номер, удаляем '№' и пробелы
                            if re.match(r'\s*№\s*TT\s*\d+', new_value):
                                new_value = re.sub(r'\s*№\s*', '', new_value)
                            # Удаляем лишние пробелы и переносы строки в начале и в конце
                            new_value = re.sub(r'^\s+|\s+$', '', new_value)
                        if new_value != value:
                            cursor.execute(f"UPDATE {table_name} SET №_заявки = ? WHERE rowid = ?", (new_value, rowid))
                            updated += 1
                print(f"Обновлено записей: {updated}")
        
        # Сохраняем изменения
        conn.commit()
        print("\nОчистка данных завершена успешно!")
        
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
            print("Выполнен откат изменений")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Соединение с базой данных закрыто")

def run_script():
    """Функция для запуска скрипта через лаунчер"""
    clean_request_number()

if __name__ == "__main__":
    clean_request_number() 