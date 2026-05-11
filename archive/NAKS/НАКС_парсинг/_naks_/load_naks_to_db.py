import pandas as pd
import sqlite3
import os
import sys
import subprocess
import platform
import re

def clean_column_name(col_name):
    """
    Очищает название столбца от переносов строк и лишних пробелов
    """
    # Заменяем переносы строк на пробелы
    col_name = str(col_name).replace('\n', ' ')
    # Заменяем множественные пробелы на один пробел
    col_name = re.sub(r'\s+', ' ', col_name)
    # Убираем пробелы в начале и конце
    col_name = col_name.strip()
    return col_name

def normalize_key_value(value):
    """
    Нормализует значение ключа для корректного сравнения
    
    Args:
        value: Значение для нормализации (может быть str, int, float, None, NaN)
        
    Returns:
        Нормализованная строка или None для пустых значений
    """
    # Обрабатываем None и NaN
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    
    # Преобразуем в строку
    str_value = str(value)
    
    # Убираем пробелы в начале и конце
    str_value = str_value.strip()
    
    # Приводим к нижнему регистру для сравнения без учета регистра
    str_value = str_value.lower()
    
    # Убираем множественные пробелы внутри строки
    str_value = re.sub(r'\s+', ' ', str_value)
    
    # Обрабатываем специальные случаи
    # Если после нормализации получилась пустая строка или "nan", "none"
    if not str_value or str_value in ['nan', 'none', 'null', '']:
        return None
    
    return str_value

def update_table_structure(cursor, table_name, columns):
    """
    Добавляет новые столбцы в существующую таблицу
    """
    # Получаем существующие столбцы
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = {row[1] for row in cursor.fetchall()}
    
    # Добавляем новые столбцы
    for col in columns:
        if col not in existing_columns:
            print(f"Добавление нового столбца: {col}")
            try:
                cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" TEXT')
            except sqlite3.OperationalError as e:
                print(f"Предупреждение: не удалось добавить столбец {col}: {e}")

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

def run_script(force_reload=False):
    """
    Основная функция для запуска загрузки НАКС в БД через script_runner
    Загружает данные из объединенного Excel файла naks_merged.xlsx в базу данных M_Kran_Kingesepp.db
    Инкрементальная загрузка с проверкой по полю "_УДОСТОВИРЕНИЕ_НАКС_"
    
    Args:
        force_reload: Если True, загружает все записи без проверки на дубликаты (по умолчанию False)
    """
    # Инициализируем переменные для обработки ошибок
    excel_file = None
    db_path = None
    conn = None
    
    try:
        # Получаем пути к файлам
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Определяем корень проекта (поднимаемся на 4 уровня вверх от _naks_)
        # _naks_ -> НАКС_парсинг -> NAKS -> archive -> корень проекта
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(current_dir))))
        
        # Проверяем возможные пути к файлу naks_merged.xlsx (приоритет: NAKS, затем archive)
        possible_excel_paths = [
            os.path.join(project_root, 'NAKS', 'НАКС_парсинг', 'naks_merged.xlsx'),
            os.path.join(project_root, 'archive', 'NAKS', 'НАКС_парсинг', 'naks_merged.xlsx'),
            os.path.join(os.path.dirname(current_dir), 'naks_merged.xlsx'),
            os.path.join(project_root, 'naks_merged.xlsx'),
        ]
        
        excel_file = None
        for path in possible_excel_paths:
            abs_path = os.path.abspath(path)
            if os.path.exists(abs_path):
                excel_file = abs_path
                break
        
        if not excel_file:
            raise FileNotFoundError(f"Файл naks_merged.xlsx не найден. Проверенные пути:\n" + '\n'.join([f'  - {os.path.abspath(p)}' for p in possible_excel_paths]))
        
        # Проверяем возможные пути к БД
        possible_db_paths = [
            os.path.join(project_root, 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
            os.path.join(project_root, 'database', 'M_Kran_Kingesepp.db'),
            os.path.join(project_root, 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        ]
        
        db_path = None
        for path in possible_db_paths:
            abs_path = os.path.abspath(path)
            if os.path.exists(abs_path):
                db_path = abs_path
                break
        
        if not db_path:
            # Используем первый путь по умолчанию, даже если файл не существует
            db_path = os.path.abspath(possible_db_paths[0])
        
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
        
        # Очищаем названия столбцов (как в naks_merged_sql.py)
        print("\nОчистка названий столбцов...")
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
            if orig != clean:
                print(f'  "{orig}"  -->  "{clean}"')
        
        # Переименовываем столбцы в DataFrame
        df = df.rename(columns=column_mapping)
        
        # Проверяем наличие ключевого поля (после очистки)
        key_field = "_УДОСТОВИРЕНИЕ_НАКС_"
        # Пробуем найти поле с разными вариантами названия
        key_field_found = None
        for col in df.columns:
            if key_field.lower() in col.lower() or col.lower() in key_field.lower():
                key_field_found = col
                break
        
        if not key_field_found:
            # Пробуем найти по части названия
            for col in df.columns:
                if 'удостоверение' in col.lower() or 'накс' in col.lower():
                    key_field_found = col
                    break
        
        if not key_field_found:
            raise ValueError(f"Ключевое поле '{key_field}' не найдено в данных. Доступные поля: {list(df.columns)}")
        
        if key_field_found != key_field:
            print(f"Используется поле '{key_field_found}' вместо '{key_field}'")
            key_field = key_field_found
        
        # Показываем структуру данных
        print("\nСтруктура данных:")
        print(f"Всего столбцов: {len(df.columns)}")
        print(f"Всего записей: {len(df)}")
        
        # Подключаемся к базе данных
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Создаем таблицу для данных НАКС
        table_name = 'naks_data'
        
        # Получаем список очищенных столбцов
        columns = list(column_mapping.values())
        
        # Проверяем существование таблицы
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            # Создаем новую таблицу со всеми столбцами
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                {', '.join([f'"{col}" TEXT' for col in columns])}
            )
            """
            cursor.execute(create_table_sql)
            conn.commit()
            print(f"Таблица {table_name} создана со всеми столбцами")
        else:
            # Обновляем структуру существующей таблицы (добавляем новые столбцы)
            print("Обновление структуры таблицы...")
            update_table_structure(cursor, table_name, columns)
            conn.commit()
        
        print(f"Используем {len(columns)} столбцов для загрузки")
        
        # Получаем существующие ключевые значения из базы с нормализацией
        if force_reload:
            print("⚠️ РЕЖИМ ПРИНУДИТЕЛЬНОЙ ЗАГРУЗКИ: все записи будут загружены без проверки на дубликаты")
            existing_keys_normalized = set()
            existing_keys_original = set()
        else:
            print("Проверка существующих записей...")
            existing_keys_normalized = set()
            existing_keys_original = set()
        
        try:
            # Проверяем, существует ли столбец в таблице
            cursor.execute(f"PRAGMA table_info({table_name})")
            table_columns = {row[1] for row in cursor.fetchall()}
            
            if key_field not in table_columns:
                print(f"Предупреждение: Столбец '{key_field}' не найден в таблице. Доступные столбцы: {list(table_columns)[:10]}...")
                # Пробуем найти похожий столбец
                for col in table_columns:
                    if key_field.lower() in col.lower() or col.lower() in key_field.lower():
                        print(f"Используется столбец '{col}' вместо '{key_field}'")
                        key_field = col
                        break
                else:
                    print("Ключевое поле не найдено в таблице. Будет выполнена полная загрузка всех записей.")
                    existing_keys_normalized = set()
                    existing_keys_original = set()
            
            if key_field in table_columns:
                cursor.execute(f'SELECT DISTINCT "{key_field}" FROM {table_name} WHERE "{key_field}" IS NOT NULL AND "{key_field}" != ""')
                rows = cursor.fetchall()
                
                for row in rows:
                    original_value = row[0]
                    normalized_value = normalize_key_value(original_value)
                    if normalized_value is not None:
                        existing_keys_normalized.add(normalized_value)
                        existing_keys_original.add(str(original_value))
                
                print(f"Найдено {len(existing_keys_normalized)} уникальных существующих записей в базе")
                
                # Логируем примеры для отладки (первые 5)
                if existing_keys_original:
                    print("Примеры существующих ключей в БД:")
                    for i, key in enumerate(list(existing_keys_original)[:5]):
                        normalized = normalize_key_value(key)
                        print(f"  {i+1}. Оригинал: '{key}' -> Нормализован: '{normalized}'")
        except sqlite3.OperationalError as e:
            print(f"Предупреждение: не удалось проверить существующие записи: {e}")
            existing_keys_normalized = set()
            existing_keys_original = set()
        
        # Фильтруем только новые записи с нормализацией
        print("\nФильтрация новых записей...")
        
        # Создаем нормализованные ключи для сравнения
        df['_cert_key_normalized'] = df[key_field].apply(normalize_key_value)
        df['_cert_key_original'] = df[key_field].astype(str)
        
        # Фильтруем: оставляем только те записи, которых нет в базе
        # Проверяем и по нормализованному значению, и по оригинальному
        mask_new = ~(
            df['_cert_key_normalized'].isin(existing_keys_normalized) | 
            df['_cert_key_original'].isin(existing_keys_original)
        )
        
        # Также исключаем записи с пустыми ключами
        mask_new = mask_new & pd.notna(df['_cert_key_normalized'])
        
        df_new = df[mask_new].copy()
        
        # Удаляем временные столбцы
        df = df.drop(['_cert_key_normalized', '_cert_key_original'], axis=1)
        df_new = df_new.drop(['_cert_key_normalized', '_cert_key_original'], axis=1)
        
        print(f"\nНайдено {len(df_new)} новых записей для загрузки")
        print(f"Всего записей в файле: {len(df)}")
        print(f"Существующих записей в БД: {len(existing_keys_normalized)}")
        
        # Логируем примеры новых записей для отладки
        if len(df_new) > 0:
            print("\nПримеры новых ключей для загрузки:")
            for i, key in enumerate(df_new[key_field].head(5)):
                normalized = normalize_key_value(key)
                print(f"  {i+1}. Оригинал: '{key}' -> Нормализован: '{normalized}'")
        else:
            print("\n" + "="*60)
            print("ДИАГНОСТИКА: Почему записи не загружаются")
            print("="*60)
            print("Все записи из файла уже существуют в базе данных.")
            print("\nПримеры записей из Excel (которые уже есть в БД):")
            for i, key in enumerate(df[key_field].head(10)):
                normalized = normalize_key_value(key)
                is_in_db_normalized = normalized is not None and normalized in existing_keys_normalized
                is_in_db_original = str(key) in existing_keys_original
                status = "✓ найден в БД" if (is_in_db_normalized or is_in_db_original) else "✗ не найден в БД"
                print(f"  {i+1}. '{key}' (нормализован: '{normalized}') - {status}")
            
            # Проверяем, может быть проблема с дубликатами в файле
            duplicates_in_file = df[key_field].duplicated().sum()
            if duplicates_in_file > 0:
                print(f"\n⚠️ В файле найдено {duplicates_in_file} дублирующихся записей по ключевому полю!")
                print("Дубликаты в файле:")
                dup_keys = df[df[key_field].duplicated(keep=False)][key_field].unique()
                for dup_key in dup_keys[:5]:
                    count = (df[key_field] == dup_key).sum()
                    print(f"  - '{dup_key}': {count} раз(а)")
            
            print("\n" + "="*60)
        
        if len(df_new) == 0:
            print("\n" + "="*60)
            print("ВНИМАНИЕ: Новых записей для загрузки не найдено!")
            print("="*60)
            print(f"Всего записей в файле: {len(df)}")
            print(f"Всего записей в базе данных: {len(existing_keys_normalized)}")
            print(f"Пропущено существующих записей: {len(df)}")
            print("\nВсе записи из файла уже присутствуют в базе данных.")
            print("Если вы хотите обновить существующие записи, используйте режим обновления.")
            print("="*60)
            conn.close()
            return
        
        # Подготавливаем данные для вставки
        # Заменяем NaN на None для корректной работы с SQLite
        df_clean = df_new.where(pd.notnull(df_new), None)
        
        # Используем pandas to_sql для более надежной вставки
        print(f"Импорт {len(df_new)} новых записей...")
        try:
            df_clean.to_sql(table_name, conn, if_exists='append', index=False)
            print("Данные успешно загружены через pandas.to_sql")
        except Exception as e:
            print(f"Ошибка при использовании pandas.to_sql: {e}")
            print("Пробуем альтернативный метод...")
            
            # Альтернативный метод через executemany
            # Фильтруем только существующие столбцы
            cursor.execute(f"PRAGMA table_info({table_name})")
            existing_columns = {row[1] for row in cursor.fetchall()}
            available_columns = [col for col in columns if col in existing_columns]
            
            if len(available_columns) != len(columns):
                print(f"Используем только {len(available_columns)} из {len(columns)} столбцов")
                df_clean = df_clean[available_columns]
                columns = available_columns
            
            # Создаем SQL для вставки данных
            placeholders = ', '.join(['?' for _ in columns])
            insert_sql = f"INSERT INTO {table_name} ({', '.join([f'"{col}"' for col in columns])}) VALUES ({placeholders})"
            
            # Вставляем данные
            data_to_insert = [tuple(row) for row in df_clean.values]
            cursor.executemany(insert_sql, data_to_insert)
            print("Данные успешно загружены через executemany")
        
        # Сохраняем изменения
        conn.commit()
        
        # Проверяем общее количество записей в таблице
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_count = cursor.fetchone()[0]
        print(f"\nВсего записей в таблице {table_name}: {total_count}")
        print(f"Добавлено новых записей: {len(df_new)}")
        print(f"Пропущено существующих записей: {len(df) - len(df_new)}")
        
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
        # Используем значения по умолчанию, если переменные не были инициализированы
        error_excel_file = excel_file if excel_file else "не определен"
        error_db_path = db_path if db_path else "не определен"
        show_error_help(str(e), error_excel_file, error_db_path)
        
        if conn:
            try:
                conn.close()
            except:
                pass

if __name__ == "__main__":
    run_script() 