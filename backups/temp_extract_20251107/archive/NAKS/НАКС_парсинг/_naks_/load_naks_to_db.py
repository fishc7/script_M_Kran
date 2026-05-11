import pandas as pd
import sqlite3
import os
import sys
import subprocess
import platform
import re

def open_file_in_editor(file_path):
    """
    Открывает файл в редакторе по умолчанию системы
    """
    try:
        if platform.system() == "Windows":
            os.startfile(file_path)
        elif platform.system() == "Darwin":  # macOS
            subprocess.run(["open", file_path])
        else:  # Linux
            subprocess.run(["xdg-open", file_path])
        print(f"Файл открыт в редакторе: {file_path}")
    except Exception as e:
        print(f"Не удалось открыть файл в редакторе: {str(e)}")

def show_error_help(error_message, excel_file, db_path):
    """
    Показывает подробную информацию об ошибке и предлагает варианты решения
    """
    print("\n" + "="*60)
    print("ОШИБКА ПРИ ЗАГРУЗКЕ ДАННЫХ")
    print("="*60)
    print(f"Описание ошибки: {error_message}")
    print(f"\nПути к файлам:")
    print(f"Excel файл: {excel_file}")
    print(f"База данных: {db_path}")
    
    print(f"\nВозможные причины и решения:")
    print("1. Проверьте, что файл naks_merged.xlsx существует и доступен для чтения")
    print("2. Убедитесь, что база данных M_Kran_Kingesepp.db существует")
    print("3. Проверьте права доступа к файлам")
    print("4. Убедитесь, что Excel файл не открыт в другой программе")
    print("5. Проверьте формат данных в Excel файле")
    
    print(f"\nДля ручной проверки файлов:")
    print(f"- Excel файл: {excel_file}")
    print(f"- База данных: {db_path}")
    print("="*60)

def load_naks_to_db():
    """
    Загружает данные из объединенного Excel файла naks_merged.xlsx в базу данных M_Kran_Kingesepp.db
    Инкрементальная загрузка с проверкой по полю "_УДОСТОВИРЕНИЕ_НАКС_"
    """
    try:
        # Получаем пути к файлам
        current_dir = os.path.dirname(os.path.abspath(__file__))
        excel_file = os.path.join(os.path.dirname(current_dir), 'naks_merged.xlsx')
        
        # Путь к базе данных (относительно корня проекта)
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
        db_path = os.path.join(project_root, 'BD_Kingisepp', 'M_Kran_Kingesepp.db')
        
        print(f"Читаем данные из: {excel_file}")
        print(f"Загружаем в базу: {db_path}")
        
        # Проверяем существование файлов
        if not os.path.exists(excel_file):
            raise FileNotFoundError(f"Файл {excel_file} не найден")
        
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"База данных {db_path} не найдена")
        
        # Читаем Excel файл
        df = pd.read_excel(excel_file)
        print(f"Прочитано {len(df)} записей из Excel файла")
        
        # Проверяем наличие ключевого поля
        key_field = "_УДОСТОВИРЕНИЕ_НАКС_"
        if key_field not in df.columns:
            raise ValueError(f"Ключевое поле '{key_field}' не найдено в данных")
        
        # Показываем структуру данных
        print("\nСтруктура данных:")
        print(df.info())
        print("\nПервые 5 строк:")
        print(df.head())
        
        # Подключаемся к базе данных
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Создаем таблицу для данных НАКС
        table_name = 'naks_data'
        
        # Получаем список столбцов и их типы
        columns = df.columns.tolist()
        
        # Проверяем существующую структуру таблицы
        cursor.execute(f"PRAGMA table_info({table_name})")
        existing_columns = {row[1] for row in cursor.fetchall()}
        
        # Если таблица существует, фильтруем столбцы
        if existing_columns:
            # Оставляем только те столбцы, которые уже есть в таблице
            available_columns = [col for col in columns if col in existing_columns]
            missing_columns = [col for col in columns if col not in existing_columns]
            
            if missing_columns:
                print(f"Пропускаем столбцы, которых нет в таблице: {missing_columns}")
            
            columns = available_columns
            df = df[columns]  # Оставляем только доступные столбцы
        else:
            # Создаем новую таблицу со всеми столбцами
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                {', '.join([f'"{col}" TEXT' for col in columns])}
            )
            """
            cursor.execute(create_table_sql)
            print(f"Таблица {table_name} создана со всеми столбцами")
        
        print(f"Используем {len(columns)} столбцов для загрузки")
        
        # Получаем существующие ключевые значения из базы
        cursor.execute(f'SELECT DISTINCT "{key_field}" FROM {table_name} WHERE "{key_field}" IS NOT NULL')
        existing_keys = {row[0] for row in cursor.fetchall()}
        print(f"Найдено {len(existing_keys)} существующих записей в базе")
        
        # Фильтруем только новые записи
        df_new = df[~df[key_field].isin(existing_keys)]
        print(f"Найдено {len(df_new)} новых записей для загрузки")
        
        if len(df_new) == 0:
            print("Новых записей для загрузки не найдено")
            conn.close()
            return
        
        # Подготавливаем данные для вставки
        # Заменяем NaN на None для корректной работы с SQLite
        df_clean = df_new.where(pd.notnull(df_new), None)
        
        # Создаем SQL для вставки данных
        placeholders = ', '.join(['?' for _ in columns])
        insert_sql = f"INSERT INTO {table_name} ({', '.join([f'"{col}"' for col in columns])}) VALUES ({placeholders})"
        
        # Вставляем данные
        data_to_insert = [tuple(row) for row in df_clean.values]
        
        try:
            cursor.executemany(insert_sql, data_to_insert)
        except sqlite3.OperationalError as e:
            error_msg = str(e)
            if "no column named" in error_msg:
                # Извлекаем имя проблемного столбца из ошибки
                match = re.search(r'no column named "([^"]+)"', error_msg)
                if match:
                    problematic_column = match.group(1)
                    print(f"Столбец '{problematic_column}' отсутствует в таблице. Пропускаем его...")
                    
                    # Удаляем проблемный столбец из списка
                    columns = [col for col in columns if col != problematic_column]
                    df_clean = df_clean[columns]
                    
                    # Создаем новый SQL для вставки
                    placeholders = ', '.join(['?' for _ in columns])
                    insert_sql = f"INSERT INTO {table_name} ({', '.join([f'"{col}"' for col in columns])}) VALUES ({placeholders})"
                    
                    # Повторяем вставку без проблемного столбца
                    data_to_insert = [tuple(row) for row in df_clean.values]
                    cursor.executemany(insert_sql, data_to_insert)
                    print(f"Данные успешно загружены без столбца '{problematic_column}'")
                else:
                    raise e
            else:
                raise e
        
        # Сохраняем изменения
        conn.commit()
        
        # Проверяем общее количество записей в таблице
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_count = cursor.fetchone()[0]
        print(f"Всего записей в таблице {table_name}: {total_count}")
        print(f"Добавлено {len(df_new)} новых записей")
        
        # Показываем пример новых данных из базы
        cursor.execute(f"SELECT * FROM {table_name} ORDER BY id DESC LIMIT 3")
        sample_data = cursor.fetchall()
        print("\nПоследние добавленные записи:")
        for row in sample_data:
            print(row)
        
        conn.close()
        print(f"\nИнкрементальная загрузка завершена успешно!")
        
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")
        
        # Показываем подробную информацию об ошибке
        show_error_help(str(e), excel_file, db_path)
        
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    load_naks_to_db() 