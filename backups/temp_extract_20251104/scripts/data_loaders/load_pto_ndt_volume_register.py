import sqlite3

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
    
    from db_utils import clean_column_name
    from path_utils import get_database_path
, clean_data_values, print_column_cleaning_report
import pandas as pd
import os
import numpy as np
from datetime import datetime

, get_excel_paths

excel_paths = get_excel_paths()
excel_path = excel_paths['pto'] + '/Обьем_НК_от_ПТО.xlsx'
db_path = get_database_path()

def create_or_update_table(conn, df):
    """
    Создает таблицу pto_ndt_volume_register или добавляет новые столбцы к существующей
    """
    try:
        cursor = conn.cursor()
        
        # Сохраняем оригинальные названия столбцов для отчета
        original_columns = df.columns.tolist()
        
        # Очищаем имена столбцов с использованием улучшенной функции
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Выводим отчет об очистке названий столбцов
        print_column_cleaning_report(original_columns, df.columns.tolist())
        
        # Очищаем значения в данных от переносов строк
        df = clean_data_values(df)
        
        # Добавляем столбец с датой загрузки (если его еще нет)
        if 'Дата_загрузки' not in df.columns:
            df['Дата_загрузки'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Проверяем, существует ли таблица
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pto_ndt_volume_register'")
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            # Создаем новую таблицу
            print("📋 Таблица pto_ndt_volume_register не существует. Создаем новую...")
            
            columns = ['id INTEGER PRIMARY KEY AUTOINCREMENT']
            for col in df.columns:
                columns.append(f'"{col}" TEXT')
            
            create_table_sql = f'''
            CREATE TABLE pto_ndt_volume_register (
                {', '.join(columns)}
            )
            '''
            
            cursor.execute(create_table_sql)
            conn.commit()
            print("✓ Таблица pto_ndt_volume_register успешно создана!")
            
            # Выводим информацию о созданных столбцах
            print("\nСозданные столбцы:")
            for i, col in enumerate(df.columns, 1):
                print(f"  {i}. {col}")
                
        else:
            # Таблица существует, проверяем и добавляем новые столбцы
            print("📋 Таблица pto_ndt_volume_register уже существует. Проверяем столбцы...")
            
            # Получаем существующие столбцы
            cursor.execute("PRAGMA table_info(pto_ndt_volume_register)")
            existing_columns = [row[1] for row in cursor.fetchall()]
            
            # Находим новые столбцы
            new_columns = []
            for col in df.columns:
                if col not in existing_columns and col != 'id':
                    new_columns.append(col)
            
            if new_columns:
                print(f"🔧 Найдено {len(new_columns)} новых столбцов:")
                for i, col in enumerate(new_columns, 1):
                    print(f"  {i}. {col}")
                
                # Добавляем новые столбцы
                for col in new_columns:
                    try:
                        alter_sql = f'ALTER TABLE pto_ndt_volume_register ADD COLUMN "{col}" TEXT'
                        cursor.execute(alter_sql)
                        print(f"  ✓ Добавлен столбец: {col}")
                    except sqlite3.OperationalError as e:
                        if "duplicate column name" in str(e):
                            print(f"  ⚠️  Столбец {col} уже существует")
                        else:
                            print(f"  ✗ Ошибка добавления столбца {col}: {e}")
                
                conn.commit()
                print("✓ Новые столбцы успешно добавлены!")
            else:
                print("✓ Все столбцы уже существуют в таблице")
        
    except sqlite3.Error as e:
        print(f"✗ Ошибка при работе с таблицей: {e}")
        raise

def load_excel_data(conn, excel_path):
    """
    Загружает данные из Excel файла в таблицу pto_ndt_volume_register
    """
    try:
        # Читаем Excel файл
        print(f"📖 Чтение данных из Excel файла: {excel_path}")
        df = pd.read_excel(excel_path)
        
        print(f"✓ Прочитано {len(df)} строк и {len(df.columns)} столбцов")
        
        # Выводим имена столбцов из Excel файла
        print("\n📋 Исходные имена столбцов в Excel файле:")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i}. {col}")
        
        # Очищаем имена столбцов
        original_columns = df.columns.copy()
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Выводим очищенные имена столбцов
        print("\n🔧 Очищенные имена столбцов:")
        for i, (orig, cleaned) in enumerate(zip(original_columns, df.columns), 1):
            print(f"  {i}. {orig} → {cleaned}")
        
        # Заменяем NaN на None (NULL в SQLite)
        df = df.replace({np.nan: None})
        
        # Добавляем столбец с датой загрузки (если его еще нет)
        if 'Дата_загрузки' not in df.columns:
            df['Дата_загрузки'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        cursor = conn.cursor()
        
        # Создаем или обновляем таблицу на основе данных
        create_or_update_table(conn, df)
        
        # Проверяем, есть ли уже данные в таблице
        cursor.execute("SELECT COUNT(*) FROM pto_ndt_volume_register")
        existing_count = cursor.fetchone()[0]
        
        if existing_count > 0:
            print(f"⚠️  В таблице уже есть {existing_count} записей")
            print("🔄 Обновляем данные (удаляем старые и добавляем новые)...")
            
            # Удаляем все существующие данные
            cursor.execute("DELETE FROM pto_ndt_volume_register")
            print(f"✓ Удалено {existing_count} старых записей")
        
        # Подготавливаем SQL запрос для вставки данных
        columns = df.columns.tolist()
        placeholders = ','.join(['?' for _ in range(len(columns))])
        columns_str = ','.join([f'"{col}"' for col in columns])
        insert_query = f'INSERT INTO pto_ndt_volume_register ({columns_str}) VALUES ({placeholders})'
        
        # Преобразуем DataFrame в список кортежей для вставки
        data = df[columns].values.tolist()
        
        # Вставляем данные
        print(f"\n💾 Загрузка {len(data)} записей в базу данных...")
        cursor.executemany(insert_query, data)
        conn.commit()
        
        print(f"✓ Успешно загружено {len(data)} записей в таблицу pto_ndt_volume_register!")
        
        # Проверяем загруженные данные
        cursor.execute("SELECT COUNT(*) FROM pto_ndt_volume_register")
        count = cursor.fetchone()[0]
        print(f"✓ В таблице pto_ndt_volume_register теперь {count} записей")
        
    except Exception as e:
        print(f"✗ Ошибка при загрузке данных: {e}")
        import traceback
        print("Полный стек ошибки:")
        print(traceback.format_exc())
        raise

def main():
    """
    Основная функция скрипта
    """
    print("Путь к базе данных:", db_path)
    # Проверяем существование файла
    if not os.path.exists(excel_path):
        print(f"✗ Файл не найден: {excel_path}")
        print("Пожалуйста, убедитесь, что файл 'Обьем_НК_от_ПТО.xlsx' находится в папке D:\\МК_Кран\\МК_Кран_Кингесеп\\ПТО\\")
        return
    
    try:
        # Подключаемся к базе данных
        print("🔌 Подключение к базе данных...")
        conn = get_database_connection()
        print("✓ Подключение к базе данных установлено")
        
        # Загружаем данные
        load_excel_data(conn, excel_path)
        
        print("\n🎉 Загрузка данных завершена успешно!")
        
    except Exception as e:
        print(f"✗ Произошла ошибка: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("🔌 Соединение с базой данных закрыто")

if __name__ == "__main__":
    main()

# Для запуска через GUI
def run_script():
    """Функция для запуска скрипта через GUI"""
    main() 