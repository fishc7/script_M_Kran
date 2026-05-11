import os
import pandas as pd
import sqlite3
import glob
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import re
import json
from dataclasses import dataclass, asdict

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

# Создаем директорию logs, если она не существует
os.makedirs('logs', exist_ok=True)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/staff_log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ExcelDataInfo:
    """Информация о собранных данных из Excel"""
    file_path: str
    file_name: str
    sheet_name: str
    header_row: int
    total_rows: int
    total_columns: int
    column_names: List[str]
    data_preview: List[Dict[str, Any]]

class PositionDataCollector:
    """Сборщик данных из Excel файлов с заголовком 'Должность' и загрузка в БД"""
    
    def __init__(self, folder1_path: str, folder2_path: str, db_path: Optional[str] = None, force_reload: bool = False):
        self.folder1_path = Path(folder1_path)
        self.folder2_path = Path(folder2_path)
        # Используем функцию для определения правильного пути к БД
        self.db_path = db_path if db_path else get_database_path()
        self.force_reload = force_reload
        
        # Статистика
        self.stats = {
            'total_files_found': 0,
            'files_with_position_header': 0,
            'total_rows_collected': 0,
            'rows_loaded_to_db': 0,
            'errors': 0
        }
        
        # Собранные данные
        self.collected_data = []
        self.all_dataframes = []
    
    def get_database_connection(self):
        """Получает подключение к базе данных"""
        try:
            conn = sqlite3.connect(self.db_path)
            return conn
        except Exception as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            raise
    
    def create_table_from_dataframe(self, conn: sqlite3.Connection, df: pd.DataFrame, table_name: str = "Daily_Staff_Allocation"):
        """Создает таблицу в базе данных только с нужными столбцами (если не существует)"""
        try:
            cursor = conn.cursor()
            
            # Проверяем, существует ли таблица
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            if cursor.fetchone():
                return True
            
            # Создаем таблицу только если она не существует
            columns_sql = [
                "id INTEGER PRIMARY KEY AUTOINCREMENT",
                '"ФИО_Сотрудника" TEXT',
                '"Должность" TEXT',
                '"Титул" TEXT',
                '"Дата" TEXT',
                'source_file TEXT',
                'processed_date TEXT'
            ]
            create_sql = f"""
            CREATE TABLE {table_name} (
                {', '.join(columns_sql)}
            )
            """
            cursor.execute(create_sql)
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка создания таблицы: {e}")
            return False
    
    def get_existing_records(self, conn: sqlite3.Connection, table_name: str = "Daily_Staff_Allocation") -> set:
        """Получает существующие записи для проверки дубликатов"""
        try:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT "ФИО_Сотрудника", "Должность", "Титул", "Дата", source_file 
                FROM {table_name}
            """)
            existing_records = set()
            for row in cursor.fetchall():
                # Создаем уникальный ключ из комбинации полей
                key = (str(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]))
                existing_records.add(key)
            return existing_records
        except Exception as e:
            logger.error(f"Ошибка получения записей: {e}")
            return set()
    
    def load_data_to_database(self, conn: sqlite3.Connection, df: pd.DataFrame, table_name: str = "Daily_Staff_Allocation"):
        """Загружает данные в базу данных (инкрементная загрузка или принудительная перезагрузка)"""
        try:
            cursor = conn.cursor()
            
            if self.force_reload:
                # Принудительная перезагрузка - очищаем таблицу
                cursor.execute(f"DELETE FROM {table_name}")
                logger.info(f"Таблица {table_name} очищена для принудительной перезагрузки")
                
                # Загружаем все данные
                columns = ['ФИО_Сотрудника', 'Должность', 'Титул', 'Дата', 'source_file', 'processed_date']
                df_copy = df[columns].copy()
                
                placeholders = ','.join(['?' for _ in columns])
                columns_str = ','.join([f'"{col}"' for col in columns])
                insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                
                records_to_insert = [tuple(row) for _, row in df_copy.iterrows()]
                cursor.executemany(insert_sql, records_to_insert)
                conn.commit()
                
                rows_inserted = len(records_to_insert)
                self.stats['rows_loaded_to_db'] += rows_inserted
                
                logger.info(f"Принудительно загружено в БД: {rows_inserted} записей")
                
            else:
                # Инкрементная загрузка (оригинальная логика)
                # Получаем существующие записи
                existing_records = self.get_existing_records(conn, table_name)
                
                # Подготавливаем данные для вставки
                columns = ['ФИО_Сотрудника', 'Должность', 'Титул', 'Дата', 'source_file', 'processed_date']
                df_copy = df[columns].copy()
                
                # Фильтруем только новые записи
                new_records = []
                duplicates_count = 0
                
                for _, row in df_copy.iterrows():
                    # Создаем уникальный ключ для проверки
                    key = (
                        str(row['ФИО_Сотрудника']), 
                        str(row['Должность']), 
                        str(row['Титул']), 
                        str(row['Дата']), 
                        str(row['source_file'])
                    )
                    
                    if key not in existing_records:
                        new_records.append(tuple(row))
                    else:
                        duplicates_count += 1
                
                if new_records:
                    # Вставляем только новые записи
                    placeholders = ','.join(['?' for _ in columns])
                    columns_str = ','.join([f'"{col}"' for col in columns])
                    insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                    cursor.executemany(insert_sql, new_records)
                    conn.commit()
                    
                    rows_inserted = len(new_records)
                    self.stats['rows_loaded_to_db'] += rows_inserted
                    
                    logger.info(f"Загружено в БД: {rows_inserted} записей")
                else:
                    logger.info("Все записи уже существуют в базе данных")
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка загрузки в БД: {e}")
            return False
    
    def find_header_row_with_position(self, df: pd.DataFrame, max_search_rows: int = 20) -> Optional[int]:
        """Находит строку с заголовком, содержащим слово 'Должность'"""
        for row_idx in range(min(len(df), max_search_rows)):
            row_values = df.iloc[row_idx].astype(str)
            for cell_value in row_values:
                if 'должность' in cell_value.lower():
                    return row_idx
        return None
    
    def clean_column_names(self, columns: List[str]) -> List[str]:
        """Очищает и нормализует названия столбцов"""
        cleaned_columns = []
        for col in columns:
            if pd.isna(col):
                col = f"Column_{len(cleaned_columns)}"
            else:
                # Очищаем от лишних символов
                col = str(col).strip()
                col = re.sub(r'[^\w\sа-яА-Я]', '', col)
                col = re.sub(r'\s+', '_', col)
                col = col.strip('_')
                
                if not col:
                    col = f"Column_{len(cleaned_columns)}"
            
            # Делаем уникальным
            base_col = col
            counter = 1
            while col in cleaned_columns:
                col = f"{base_col}_{counter}"
                counter += 1
            
            cleaned_columns.append(col)
        
        return cleaned_columns
    
    def extract_title_from_filename(self, filename: str) -> str:
        """Извлекает титул из имени файла (слово, начинающееся с '124')"""
        # Ищем слово, начинающееся с "124"
        title_pattern = r'124\d+'
        match = re.search(title_pattern, filename)
        if match:
            return match.group(0)  # Возвращаем найденное слово полностью
        return ""

    def extract_date_from_filename(self, filename: str) -> str:
        """Извлекает дату из имени файла в форматах DD.MM.YYYY или DD.MM.YY"""
        # Ищем паттерн даты DD.MM.YYYY или DD.MM.YY
        date_pattern = r'(\d{2}\.\d{2}\.\d{2,4})'
        match = re.search(date_pattern, filename)
        if match:
            return match.group(1)
        return ""

    def process_excel_file(self, file_path: Path) -> List[ExcelDataInfo]:
        """Обрабатывает Excel файл и извлекает данные с заголовком 'Должность'"""
        results = []
        
        try:
            # Читаем Excel файл без заголовков
            excel_file = pd.ExcelFile(file_path)
            
            for sheet_name in excel_file.sheet_names:
                try:
                    # Читаем лист без заголовков
                    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                    
                    # Ищем строку с заголовком "Должность"
                    header_row = self.find_header_row_with_position(df)
                    
                    if header_row is not None:
                        # Читаем данные с найденной строки заголовка
                        df_with_header = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row)
                        
                        # Очищаем названия столбцов
                        cleaned_columns = self.clean_column_names(df_with_header.columns.tolist())
                        df_with_header.columns = cleaned_columns
                        
                        # Оставляем только нужные столбцы, если они есть
                        required_cols = [col for col in cleaned_columns if col.lower() in ["фио_сотрудника", "должность"]]
                        if not ("фио_сотрудника" in [c.lower() for c in required_cols] and "должность" in [c.lower() for c in required_cols]):
                            continue
                        df_filtered = df_with_header[required_cols]
                        
                        # Фильтруем только строки, где в Должность есть 'сварщ' (без учета регистра)
                        col_dolzh = [c for c in required_cols if c.lower() == "должность"][0]
                        mask = df_filtered[col_dolzh].astype(str).str.lower().str.contains("сварщ", na=False)
                        df_filtered = df_filtered[mask]
                        
                        # Фильтруем строки, исключая записи с "Рыбкин" в ФИО_Сотрудника
                        col_fio = [c for c in required_cols if c.lower() == "фио_сотрудника"][0]
                        mask_no_rybkin = ~df_filtered[col_fio].astype(str).str.contains("Рыбкин", case=False, na=False)
                        df_filtered = df_filtered[mask_no_rybkin]
                        
                        # Удаляем полностью пустые строки
                        df_filtered = df_filtered.dropna(how='all')
                        # Удаляем строки, где обе ячейки пустые или содержат только пробелы
                        non_empty_mask = df_filtered.astype(str).apply(lambda x: x.str.strip() != '').any(axis=1)
                        df_filtered = df_filtered[non_empty_mask]
                        
                        if len(df_filtered) > 0:
                            # Добавляем служебные столбцы
                            df_filtered['source_file'] = file_path.name
                            df_filtered['processed_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            # Добавляем титул и дату из имени файла
                            df_filtered['Титул'] = self.extract_title_from_filename(file_path.name)
                            df_filtered['Дата'] = self.extract_date_from_filename(file_path.name)
                            
                            # Добавляем DataFrame в общий список
                            self.all_dataframes.append(df_filtered)
                            
                            # Создаем информацию о данных (только для внутренней статистики)
                            preview_data = []
                            for _, row in df_filtered.head(10).iterrows():
                                preview_data.append({str(k): str(v) for k, v in row.items()})
                            data_info = ExcelDataInfo(
                                file_path=str(file_path),
                                file_name=file_path.name,
                                sheet_name=str(sheet_name),
                                header_row=header_row,
                                total_rows=len(df_filtered),
                                total_columns=len(df_filtered.columns),
                                column_names=list(df_filtered.columns),
                                data_preview=preview_data
                            )
                            results.append(data_info)
                            
                            # Обновляем статистику
                            self.stats['files_with_position_header'] += 1
                            self.stats['total_rows_collected'] += len(df_filtered)
                        
                except Exception as e:
                    logger.error(f"Ошибка в {file_path.name}: {e}")
                    self.stats['errors'] += 1
        except Exception as e:
            logger.error(f"Ошибка файла {file_path}: {e}")
            self.stats['errors'] += 1
        
        return results
    
    def scan_and_process_folders(self) -> None:
        """Сканирует папки и обрабатывает Excel файлы"""
        all_excel_files = []
        
        # Сканируем первую папку
        if self.folder1_path.exists():
            excel_files = list(self.folder1_path.rglob("*.xlsx")) + list(self.folder1_path.rglob("*.xls"))
            all_excel_files.extend(excel_files)
        else:
            logger.warning(f"Папка {self.folder1_path} не существует")
        
        # Сканируем вторую папку
        if self.folder2_path.exists():
            excel_files = list(self.folder2_path.rglob("*.xlsx")) + list(self.folder2_path.rglob("*.xls"))
            all_excel_files.extend(excel_files)
        else:
            logger.warning(f"Папка {self.folder2_path} не существует")
        
        self.stats['total_files_found'] = len(all_excel_files)
        logger.info(f"Найдено Excel файлов: {len(all_excel_files)}")
        
        # Обрабатываем файлы
        for file_path in all_excel_files:
            results = self.process_excel_file(file_path)
            self.collected_data.extend(results)
    
    def load_to_database(self) -> None:
        """Загружает данные в базу данных"""
        if not self.all_dataframes:
            logger.warning("Нет данных для загрузки в базу данных")
            return
        
        try:
            # Получаем подключение к базе данных
            conn = self.get_database_connection()
            
            # Объединяем все данные
            combined_df = pd.concat(self.all_dataframes, ignore_index=True)
            
            # Создаем таблицу
            if self.create_table_from_dataframe(conn, combined_df, "Daily_Staff_Allocation"):
                # Загружаем данные
                self.load_data_to_database(conn, combined_df, "Daily_Staff_Allocation")
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Ошибка БД: {e}")
    
    def run_collection(self) -> None:
        """Запускает полный процесс сбора данных"""
        logger.info("=== НАЧАЛО СБОРА ДАННЫХ ===")
        try:
            # Сканируем и обрабатываем папки
            self.scan_and_process_folders()
            
            # Загружаем в базу данных (инкрементная загрузка)
            self.load_to_database()
            
            # Логируем итоговую статистику
            logger.info("=== ИТОГОВАЯ СТАТИСТИКА ===")
            logger.info(f"Найдено Excel файлов: {self.stats['total_files_found']}")
            logger.info(f"Файлов с заголовком 'Должность': {self.stats['files_with_position_header']}")
            logger.info(f"Собрано строк данных: {self.stats['total_rows_collected']}")
            logger.info(f"Загружено в БД новых строк: {self.stats['rows_loaded_to_db']}")
            logger.info(f"Ошибок: {self.stats['errors']}")
            logger.info("=== СБОР ДАННЫХ ЗАВЕРШЕН ===")
            
            # Выводим итоги
            print("\n" + "=" * 60)
            print("СБОР ДАННЫХ ЗАВЕРШЕН")
            print("=" * 60)
            print(f"Найдено Excel файлов: {self.stats['total_files_found']}")
            print(f"Файлов с заголовком 'Должность': {self.stats['files_with_position_header']}")
            print(f"Собрано строк данных: {self.stats['total_rows_collected']}")
            print(f"Загружено в БД новых строк: {self.stats['rows_loaded_to_db']}")
            print(f"Ошибок: {self.stats['errors']}")
            print(f"Данные загружены в таблицу: Daily_Staff_Allocation (инкрементная загрузка)")
            print("✅ Скрипт успешно завершён. Загружено строк:", self.stats['rows_loaded_to_db'])
            
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}")
            print(f"Произошла ошибка: {e}")

def main():
    """Основная функция"""
    excel_paths = get_excel_paths()
    folder1 = excel_paths['smr_rasstanovka_12460']
    folder2 = excel_paths['smr_rasstanovka_12470']
    
    # Создаем сборщик данных (путь к БД определится автоматически)
    # Установите force_reload=True для принудительной перезагрузки
    collector = PositionDataCollector(folder1, folder2, force_reload=False)
    
    # Запускаем сбор данных
    collector.run_collection()

def force_reload_data():
    """Функция для принудительной перезагрузки данных"""
    excel_paths = get_excel_paths()
    folder1 = excel_paths['smr_rasstanovka_12460']
    folder2 = excel_paths['smr_rasstanovka_12470']
    
    # Создаем сборщик данных с принудительной перезагрузкой
    collector = PositionDataCollector(folder1, folder2, force_reload=True)
    
    # Запускаем сбор данных
    collector.run_collection()

if __name__ == "__main__":
    main()

# Для запуска через GUI
def run_script():
    """Функция для запуска скрипта через GUI"""
    main() 