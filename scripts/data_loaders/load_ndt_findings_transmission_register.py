import os
import sys
import pandas as pd
import sqlite3
from pathlib import Path
import re
import glob
import logging
from typing import List, Optional, Dict, Any, Set, Tuple
from dataclasses import dataclass
from datetime import datetime
import hashlib
import traceback

# Импортируем утилиты
try:
    # Пробуем относительный импорт (для запуска через web-приложение)
    from ..utilities.db_utils import clean_data_values, print_column_cleaning_report
    from ..utilities.path_utils import get_database_path, get_excel_paths, get_log_path
except ImportError:
    # Если не работает, используем абсолютный импорт (для прямого запуска)
    import sys
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    utilities_dir = os.path.join(os.path.dirname(current_dir), 'utilities')
    if utilities_dir not in sys.path:
        sys.path.insert(0, utilities_dir)
    
    from db_utils import clean_data_values, print_column_cleaning_report
    from path_utils import get_database_path, get_excel_paths, get_log_path

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class Config:
    """Конфигурация для загрузки данных"""
    folder_path: str = r"D:\МК_Кран\МК_Кран_Кингесеп\НК\Реестр_передачи_заключений"
    db_path: str = get_database_path()
    table_name: str = 'NDT_Findings_Transmission_Register'
    excel_extensions: tuple = ('.xlsx', '.xls')
    max_columns: int = 12  # Максимальное количество столбцов

def get_all_excel_files(folder_path: str) -> List[str]:
    """Получает список всех Excel файлов в указанной папке"""
    excel_files = []
    try:
        folder = Path(folder_path)
        if not folder.exists():
            logger.error(f"Папка не существует: {folder_path}")
            return excel_files
        
        # Ищем все Excel файлы
        for ext in ['.xlsx', '.xls']:
            files = list(folder.glob(f"*{ext}"))
            excel_files.extend([str(f) for f in files])
        
        logger.info(f"Найдено {len(excel_files)} Excel файлов")
        return excel_files
        
    except Exception as e:
        logger.error(f"Ошибка при поиске файлов: {e}")
        return excel_files

def process_excel_file(file_path: str) -> Optional[pd.DataFrame]:
    """Обрабатывает Excel файл и возвращает DataFrame без заголовков"""
    try:
        logger.info(f"Обработка файла: {os.path.basename(file_path)}")
        
        # Читаем файл начиная с 7 строки (пропускаем первые 6) БЕЗ заголовков
        df = pd.read_excel(file_path, header=None, skiprows=6)
        
        if df.empty:
            logger.warning(f"Файл пустой: {os.path.basename(file_path)}")
            return None
        
        # Создаем правильные названия столбцов на основе схемы
        column_names = [
            'No',                    # п/п No
            'Title',                 # Титул
            'Drawing_Number',        # Номер чертежа
            'Sheet',                 # Лист
            'Diameter_mm',           # Диаметр мм
            'Thickness_mm',          # Толщина стенки мм
            'Weld_Number',           # Номер стыка
            'Welders_ID',            # № клейма сварщиков
            'Type_of_Control_NDT',   # Вид контроля
            'Report',                # № заключения
            'Date_Control',          # Дата контроля
            'Result'                 # Результат контроля
        ]
        
        max_cols = min(len(df.columns), 12)  # Максимум 12 столбцов
        actual_columns = column_names[:max_cols]
        
        # Обрезаем DataFrame до нужного количества столбцов
        df = df.iloc[:, :max_cols]
        df.columns = actual_columns
        
        # Удаляем полностью пустые строки
        df = df.dropna(how='all')
        
        # Фильтруем по столбцу Result (если он существует)
        if len(df.columns) >= 12:
            col_12 = df.columns[11]  # 12-й столбец (индекс 11) - Result
            
            # Создаем условия фильтрации
            filter_conditions = (
                df[col_12].notna() &  # Не пустые значения
                (df[col_12] != '') &  # Не пустые строки
                (df[col_12] != 12) &  # Не равно 12
                (~df[col_12].astype(str).str.contains('дата', case=False, na=False))  # Не содержит "дата" (без учета регистра)
            )
            
            # Применяем фильтр
            df = df[filter_conditions]
            logger.info(f"После фильтрации по 12 столбцу осталось строк: {len(df)}")
        
        # Добавляем информацию о файле
        df['Source_File'] = os.path.basename(file_path)
        df['Full_Path'] = file_path
        
        logger.info(f"Обработано строк: {len(df)}")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при обработке файла {file_path}: {e}")
        return None

def clear_table(conn: sqlite3.Connection):
    """Очищает таблицу от всех данных"""
    try:
        conn.execute('DELETE FROM NDT_Findings_Transmission_Register')
        conn.commit()
        logger.info("Таблица очищена от всех данных")
    except Exception as e:
        logger.error(f"Ошибка при очистке таблицы: {e}")
        raise

def create_table(conn: sqlite3.Connection):
    """Создает таблицу с фиксированной структурой"""
    try:
        # Создаем SQL для создания таблицы с правильными названиями столбцов
        columns_sql = [
            'No TEXT',                    # п/п No
            'Title TEXT',                 # Титул
            'Drawing_Number TEXT',        # Номер чертежа
            'Sheet TEXT',                 # Лист
            'Diameter_mm TEXT',           # Диаметр мм
            'Thickness_mm TEXT',          # Толщина стенки мм
            'Weld_Number TEXT',           # Номер стыка
            'Welders_ID TEXT',            # № клейма сварщиков
            'Type_of_Control_NDT TEXT',   # Вид контроля
            'Report TEXT',                # № заключения
            'Date_Control TEXT',          # Дата контроля
            'Result TEXT'                 # Результат контроля
        ]
        
        # Добавляем служебные столбцы
        columns_sql.extend([
            'Source_File TEXT',
            'Full_Path TEXT'
        ])
        
        create_sql = f'''
        CREATE TABLE IF NOT EXISTS NDT_Findings_Transmission_Register (
            rowid INTEGER PRIMARY KEY AUTOINCREMENT,
            {', '.join(columns_sql)}
        )
        '''
        
        conn.execute(create_sql)
        conn.commit()
        logger.info(f"Создана таблица с {len(columns_sql)} столбцами")
        
    except Exception as e:
        logger.error(f"Ошибка при создании таблицы: {e}")
        raise

def insert_data(conn: sqlite3.Connection, df: pd.DataFrame) -> bool:
    """Вставляет данные в таблицу"""
    try:
        logger.info(f"Вставка {len(df)} строк в базу данных")
        
        # Убеждаемся, что у нас правильные названия столбцов
        expected_columns = [
            'No', 'Title', 'Drawing_Number', 'Sheet', 'Diameter_mm', 'Thickness_mm',
            'Weld_Number', 'Welders_ID', 'Type_of_Control_NDT', 'Report', 'Date_Control', 'Result'
        ] + ['Source_File', 'Full_Path']
        
        # Создаем новый DataFrame с правильными столбцами
        new_df = pd.DataFrame(columns=expected_columns)
        
        # Копируем данные из исходного DataFrame
        for col in df.columns:
            if col in new_df.columns:
                new_df[col] = df[col]
        
        # Заполняем пустые значения
        new_df = new_df.fillna('')
        
        # Используем to_sql для вставки
        new_df.to_sql('NDT_Findings_Transmission_Register', conn, if_exists='append', index=False)
        conn.commit()
        
        logger.info("Данные успешно вставлены")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при вставке данных: {e}")
        return False

def main():
    """Главная функция"""
    try:
        logger.info("Начало обработки NDT Findings Transmission Register (без заголовков)")
        
        config = Config()
        
        # Получаем список файлов
        excel_files = get_all_excel_files(config.folder_path)
        
        if not excel_files:
            logger.error("Excel файлы не найдены")
            return
        
        # Подключаемся к базе данных
        conn = sqlite3.connect(config.db_path)
        
        try:
            # Проверяем количество строк до обработки
            try:
                cur = conn.execute('SELECT COUNT(*) FROM NDT_Findings_Transmission_Register')
                initial_count = cur.fetchone()[0]
                logger.info(f"Строк в таблице до обработки: {initial_count}")
            except:
                initial_count = 0
                logger.info("Таблица не существует, будет создана")
            
            # Создаем таблицу с фиксированной структурой
            create_table(conn)
            
            # Очищаем таблицу перед вставкой новых данных
            clear_table(conn)
            
            # Обрабатываем файлы
            success_count = 0
            total_rows = 0
            
            for idx, file_path in enumerate(excel_files, 1):
                logger.info(f"Обработка файла {idx}/{len(excel_files)}")
                
                df = process_excel_file(file_path)
                if df is not None and not df.empty:
                    # Вставляем данные сразу после обработки каждого файла
                    if insert_data(conn, df):
                        success_count += 1
                        total_rows += len(df)
                        logger.info(f"Файл {idx} успешно обработан: {len(df)} строк")
                    else:
                        logger.error(f"Ошибка при вставке данных из файла {idx}")
            
            # Проверяем количество строк после обработки
            cur = conn.execute('SELECT COUNT(*) FROM NDT_Findings_Transmission_Register')
            final_count = cur.fetchone()[0]
            logger.info(f"Строк в таблице после обработки: {final_count}")
            logger.info(f"Добавлено новых строк: {final_count - initial_count}")
            logger.info(f"Успешно обработано файлов: {success_count}/{len(excel_files)}")
            
        finally:
            conn.close()
        
        logger.info("Обработка завершена успешно")
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()

# Для запуска через GUI
def run_script():
    """Функция для запуска скрипта через GUI"""
    main()
