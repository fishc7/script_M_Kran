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
    from path_utils import get_database_path, clean_data_values, print_column_cleaning_report, get_excel_paths
import pandas as pd
import os
from pathlib import Path

excel_paths = get_excel_paths()
excel_path = excel_paths['ogs_dl'] + '/Реестор ДЛ.xlsx'
db_path = get_database_path()

def load_excel_to_sqlite():
    print("Путь к базе данных:", db_path)
    print(f"Файл базы данных существует: {os.path.exists(db_path)}")
    
    try:
        # Подключаемся к базе данных
        conn = sqlite3.connect(db_path)
        print("Успешно подключились к базе данных")
        
        # Загружаем лист 'область'
        print("\nЗагрузка листа 'область'...")
        df_region = pd.read_excel(excel_path, sheet_name='область')
        print(f"Прочитано {len(df_region)} строк из Excel")
        
        df_region.columns = [clean_column_name(col) for col in df_region.columns]
        print("Столбцы после очистки:")
        for col in df_region.columns:
            print(f"- {col}")
            
        print("\nЗагрузка в базу данных...")
        df_region.to_sql('dl_region', conn, if_exists='replace', index=False)
        print("Данные из листа 'область' успешно загружены в базу данных")
        print("✅ Скрипт успешно завершён. Загружено строк:", len(df_region))
        
        # Проверяем, что таблица создалась
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dl_region';")
        if cursor.fetchone():
            print("Таблица dl_region успешно создана")
        else:
            print("ОШИБКА: Таблица dl_region не создана!")
            
        # Загружаем лист 'аттестация'
        print("\nЗагрузка листа 'аттестация'...")
        df_attestation = pd.read_excel(excel_path, sheet_name='аттестация')
        print(f"Прочитано {len(df_attestation)} строк из Excel")
        
        df_attestation.columns = [clean_column_name(col) for col in df_attestation.columns]
        print("Столбцы после очистки:")
        for col in df_attestation.columns:
            print(f"- {col}")
            
        print("\nЗагрузка в базу данных...")
        df_attestation.to_sql('dl_attestation', conn, if_exists='replace', index=False)
        print("Данные из листа 'аттестация' успешно загружены в базу данных")
        
        # Проверяем, что таблица создалась
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dl_attestation';")
        if cursor.fetchone():
            print("Таблица dl_attestation успешно создана")
        else:
            print("ОШИБКА: Таблица dl_attestation не создана!")
        
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")
        import traceback
        print("Полный стек ошибки:")
        print(traceback.format_exc())
    finally:
        if 'conn' in locals():
            conn.close()
            print("\nСоединение с базой данных закрыто")

def main():
    """Основная функция"""
    load_excel_to_sqlite()

if __name__ == "__main__":
    main()

# Для запуска через GUI
def run_script():
    """Функция для запуска скрипта через GUI"""
    main() 