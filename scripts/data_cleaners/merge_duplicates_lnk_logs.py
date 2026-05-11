import sqlite3
import os
import sys
import traceback
import time

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

def merge_duplicates():
    # Получаем правильный путь к базе данных
    db_path = get_database_path()
    
    print(f"Начинаем работу с базой данных: {db_path}")
    
    # Проверяем существование файла базы данных
    if not os.path.exists(db_path):
        print(f"Ошибка: Файл базы данных {db_path} не существует!")
        return
    
    print(f"Файл базы данных {db_path} существует")
    
    # Проверяем права доступа к файлу
    if not os.access(db_path, os.R_OK | os.W_OK):
        print(f"Ошибка: Нет прав доступа к файлу базы данных {db_path}!")
        return
    
    print("Права доступа к файлу базы данных в порядке")
    
    try:
        # Подключаемся к базе данных
        print("Подключение к базе данных...")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        print("Подключение успешно установлено")
        
        # Проверяем целостность базы данных
        print("Проверка целостности базы данных...")
        cursor.execute("PRAGMA integrity_check")
        integrity_check = cursor.fetchone()[0]
        if integrity_check != "ok":
            print(f"Ошибка: База данных повреждена! Результат проверки: {integrity_check}")
            return
        print("Целостность базы данных проверена успешно")
        
        # Проверяем существование таблицы
        print("Проверка существования таблицы logs_lnk...")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='logs_lnk'")
        if not cursor.fetchone():
            print("Ошибка: Таблица logs_lnk не найдена в базе данных!")
            return
        print("Таблица logs_lnk найдена")
        
        # Получаем структуру таблицы
        cursor.execute("PRAGMA table_info(logs_lnk)")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        
        print("\nСтруктура таблицы logs_lnk:")
        for col in columns:
            print(f"Колонка: {col[1]}, Тип: {col[2]}")
        
        # Проверяем, есть ли уже колонка Количество_дубликатов
        if "Количество_дубликатов" in column_names:
            print("\nВНИМАНИЕ: Колонка 'Количество_дубликатов' уже существует в таблице!")
            # Удаляем колонку Количество_дубликатов из списка колонок
            columns = [col for col in columns if col[1] != "Количество_дубликатов"]
            column_names = [col[1] for col in columns]
        
        # Создаем временную таблицу с уникальным именем
        temp_table_name = f"temp_logs_lnk_{int(time.time())}"
        print(f"\nСоздание временной таблицы {temp_table_name}...")
        
        create_table_sql = f"CREATE TABLE {temp_table_name} ("
        create_table_sql += ", ".join([f"{col[1]} {col[2]}" for col in columns])
        create_table_sql += ", Количество_дубликатов INTEGER DEFAULT 1)"
        
        cursor.execute(create_table_sql)
        print(f"Таблица {temp_table_name} создана успешно")
        
        # Получаем все уникальные комбинации Чертеж и Номер_стыка с количеством дубликатов
        print("\nПоиск уникальных комбинаций и подсчет дубликатов...")
        cursor.execute("""
            SELECT Чертеж, Номер_стыка, COUNT(*) as count
            FROM logs_lnk
            WHERE Чертеж IS NOT NULL AND Номер_стыка IS NOT NULL
            GROUP BY Чертеж, Номер_стыка
            HAVING COUNT(*) > 1
            ORDER BY count DESC
        """)
        duplicate_pairs = cursor.fetchall()
        
        if duplicate_pairs:
            print("\nНайдены следующие дубликаты:")
            for чертеж, номер_стыка, count in duplicate_pairs:
                print(f"Чертеж: {чертеж}, Номер_стыка: {номер_стыка}, Количество дубликатов: {count}")
        else:
            print("\nДубликатов не найдено!")
        
        # Получаем все уникальные комбинации для обработки
        cursor.execute("""
            SELECT DISTINCT Чертеж, Номер_стыка
            FROM logs_lnk
            WHERE Чертеж IS NOT NULL AND Номер_стыка IS NOT NULL
        """)
        unique_pairs = cursor.fetchall()
        print(f"\nВсего уникальных комбинаций: {len(unique_pairs)}")
        
        # Для каждой уникальной комбинации
        print("\nОбработка записей...")
        for чертеж, номер_стыка in unique_pairs:
            # Получаем количество дубликатов для этой комбинации
            cursor.execute("""
                SELECT COUNT(*) FROM logs_lnk 
                WHERE Чертеж = ? AND Номер_стыка = ?
            """, (чертеж, номер_стыка))
            duplicate_count = cursor.fetchone()[0]
            
            # Получаем все записи с этой комбинацией
            cursor.execute("""
                SELECT * FROM logs_lnk 
                WHERE Чертеж = ? AND Номер_стыка = ?
            """, (чертеж, номер_стыка))
            rows = cursor.fetchall()
            
            if len(rows) > 1:
                print(f"Объединение дубликатов для Чертеж={чертеж}, Номер_стыка={номер_стыка}")
                
                # Создаем новую запись с объединенными значениями
                merged_values = []
                for col_idx in range(len(columns)):
                    # Собираем все непустые значения для этой колонки
                    values = set()
                    for row in rows:
                        value = row[col_idx]
                        if value and str(value).strip():
                            values.add(str(value).strip())
                    
                    # Объединяем значения через *
                    if values:
                        merged_values.append('*'.join(sorted(values)))
                    else:
                        merged_values.append(None)
                
                # Добавляем количество дубликатов
                merged_values.append(duplicate_count)
                
                # Вставляем объединенную запись во временную таблицу
                placeholders = ','.join(['?' for _ in range(len(columns) + 1)])
                cursor.execute(f"""
                    INSERT INTO {temp_table_name} ({','.join(column_names)}, Количество_дубликатов)
                    VALUES ({placeholders})
                """, merged_values)
            else:
                # Если дубликатов нет, просто копируем запись с количеством 1
                cursor.execute(f"""
                    INSERT INTO {temp_table_name}
                    SELECT *, 1 as Количество_дубликатов FROM logs_lnk
                    WHERE Чертеж = ? AND Номер_стыка = ?
                """, (чертеж, номер_стыка))
        
        # Получаем количество записей
        cursor.execute("SELECT COUNT(*) FROM logs_lnk")
        count_before = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM {temp_table_name}")
        count_after = cursor.fetchone()[0]
        
        print(f"\nКоличество записей до обработки: {count_before}")
        print(f"Количество записей после обработки: {count_after}")
        print(f"Удалено дубликатов: {count_before - count_after}")
        
        # Проверяем наличие дубликатов во временной таблице
        print("\nПроверка на наличие дубликатов во временной таблице...")
        cursor.execute(f"""
            SELECT Чертеж, Номер_стыка, COUNT(*) as count
            FROM {temp_table_name}
            GROUP BY Чертеж, Номер_стыка
            HAVING COUNT(*) > 1
        """)
        duplicates = cursor.fetchall()
        
        if not duplicates:
            print("Дубликатов не найдено!")
            
            # Если все в порядке, переименовываем таблицы
            print("\nПереименование таблиц...")
            # Сначала удаляем старую таблицу logs_lnk_old, если она существует
            cursor.execute("DROP TABLE IF EXISTS logs_lnk_old")
            # Затем переименовываем текущую таблицу в logs_lnk_old
            cursor.execute("ALTER TABLE logs_lnk RENAME TO logs_lnk_old")
            # И наконец переименовываем временную таблицу в logs_lnk
            cursor.execute(f"ALTER TABLE {temp_table_name} RENAME TO logs_lnk")
            print("Таблицы успешно переименованы!")
        else:
            print("ВНИМАНИЕ: Найдены дубликаты во временной таблице!")
            for чертеж, номер_стыка, count in duplicates:
                print(f"Чертеж: {чертеж}, Номер_стыка: {номер_стыка}, Количество: {count}")
            
            # Удаляем временную таблицу
            cursor.execute(f"DROP TABLE {temp_table_name}")
            print("Временная таблица удалена из-за ошибок")
            return
        
        # Сохраняем изменения
        conn.commit()
        print("\nИзменения сохранены в базе данных")
        
        # Проверяем финальный результат
        print("\nПроверка финального результата...")
        cursor.execute("""
            SELECT Чертеж, Номер_стыка, COUNT(*) as count
            FROM logs_lnk
            GROUP BY Чертеж, Номер_стыка
            HAVING COUNT(*) > 1
        """)
        final_duplicates = cursor.fetchall()
        
        if not final_duplicates:
            print("Отлично! Дубликатов больше нет!")
        else:
            print("ВНИМАНИЕ: Все еще есть дубликаты!")
            for чертеж, номер_стыка, count in final_duplicates:
                print(f"Чертеж: {чертеж}, Номер_стыка: {номер_стыка}, Количество: {count}")
        
        print("\nОбработка завершена успешно!")
        
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")
        print(f"Полный стек ошибки:")
        traceback.print_exc()
        
        if 'conn' in locals():
            try:
                conn.rollback()
                print("Выполнен откат изменений")
            except:
                pass
    finally:
        if 'conn' in locals():
            conn.close()
            print("Соединение с базой данных закрыто")

def run_script():
    """Функция для запуска скрипта через лаунчер"""
    merge_duplicates()

if __name__ == "__main__":
    merge_duplicates() 
    