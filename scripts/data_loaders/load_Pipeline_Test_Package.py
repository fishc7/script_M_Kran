import os
import pandas as pd
import sqlite3
from pathlib import Path
import re
import glob
import logging
from typing import List, Optional, Dict, Any, Set
from dataclasses import dataclass
from datetime import datetime
import sys
import numpy as np
import warnings
from os.path import getmtime

# Импортируем утилиты
try:
    # Пробуем относительный импорт (для запуска через web-приложение)
    from ..utilities.db_utils import clean_column_name
    from ..utilities.path_utils import get_excel_paths
except ImportError:
    # Если не работает, используем абсолютный импорт (для прямого запуска)
    import sys
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    utilities_dir = os.path.join(os.path.dirname(current_dir), 'utilities')
    if utilities_dir not in sys.path:
        sys.path.insert(0, utilities_dir)
    
    from db_utils import clean_column_name, clean_data_values, print_column_cleaning_report
    from path_utils import get_excel_paths, get_database_path, get_log_path

# Подавляем предупреждения openpyxl
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(get_log_path('load_Pipeline_Test_Package'), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class Config:
    """Конфигурация для загрузки данных Pipeline Test Package"""
    folder_path: str = get_excel_paths()['ogs_tests']
    db_path: str = get_database_path()
    table_name: str = 'Pipeline_Test_Package'
    excel_extensions: tuple = ('.xlsx', '.xls')
    
    def __post_init__(self):
        """Автоматически определяем путь к базе данных"""
        if self.db_path is None:
            db_path_found = get_database_path()
            if db_path_found is None:
                raise FileNotFoundError("База данных M_Kran_Kingesepp.db не найдена")
            self.db_path = db_path_found
            logger.info(f"Найдена база данных: {self.db_path}")

class DatabaseManager:
    """Класс для управления подключением к базе данных"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection = None
    
    def __enter__(self):
        self.connection = sqlite3.connect(self.db_path)
        return self.connection
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()

class DataCleaner:
    """Класс для очистки данных"""
    
    @staticmethod
    def clean_text(text: Any, is_column_name: bool = False) -> str:
        """Очищает текст от лишних символов"""
        if not isinstance(text, str):
            return str(text) if text is not None else ""
        
        if is_column_name:
            # Заменяем переносы строк на '/'
            text = text.replace('\n', '/').replace('\r', '/')
            # Оставляем только кириллицу, английские буквы, цифры и '/'
            text = re.sub(r'[^а-яА-Яa-zA-Z0-9/]', '', text)
            # Убираем лишние пробелы и '/'
            text = re.sub(r'\s+', ' ', text)
            text = re.sub(r'/+', '/', text)
            # Убираем пробелы и '/' в начале и конце
            text = text.strip(' /')
        else:
            # Заменяем все символы, кроме кириллицы и английских букв, на пробел
            text = re.sub(r'[^а-яА-Яa-zA-Z]', ' ', text)
            # Убираем лишние пробелы
            text = re.sub(r'\s+', ' ', text)
            # Убираем пробелы в начале и конце
            text = text.strip()
        
        return text
    
    @staticmethod
    def clean_column_name(name: Any) -> str:
        """Очищает название столбца"""
        if not isinstance(name, str):
            name = str(name)
        
        name = name.replace('\n', ' ').replace('\r', ' ')
        name = re.sub(r'\s+', ' ', name)
        name = name.strip()
        
        # Разделители для короткого имени
        separators = ['/', ',', '(', '[', '\\', '|']
        for sep in separators:
            if sep in name:
                name = name.split(sep)[0].strip()
        
        name = name.replace(' ', '_')
        # Удалить иероглифы (все символы вне латиницы, кириллицы и цифр)
        name = re.sub(r'[^A-Za-zА-Яа-я0-9_]', '', name)
        
        # Если после очистки получилась пустая строка, используем значение по умолчанию
        if not name:
            name = 'Column'
        
        return name
    
    @staticmethod
    def make_unique_columns(columns: List[str]) -> List[str]:
        """Делает имена столбцов уникальными"""
        seen = {}
        result = []
        
        for col in columns:
            col_base = col
            i = 1
            while col in seen:
                col = f"{col_base}_{i}"
                i += 1
            seen[col] = True
            result.append(col)
        
        return result
    
    @staticmethod
    def remove_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Удаляет столбцы, которые содержат только пустые значения (NaN, None, пустые строки)"""
        if df.empty:
            return df
        
        # Создаем копию DataFrame
        df_cleaned = df.copy()
        
        # Список столбцов для удаления
        columns_to_remove = []
        
        for column in df_cleaned.columns:
            # Проверяем, содержит ли столбец только пустые значения
            # Учитываем NaN, None, пустые строки и строки только из пробелов
            is_empty = True
            
            for value in df_cleaned[column]:
                # Проверяем различные типы пустых значений
                if pd.isna(value) or value is None:
                    continue
                elif isinstance(value, str) and value.strip() == '':
                    continue
                else:
                    is_empty = False
                    break
            
            if is_empty:
                columns_to_remove.append(column)
        
        # Удаляем пустые столбцы
        if columns_to_remove:
            df_cleaned = df_cleaned.drop(columns=columns_to_remove)
            logger.info(f"Удалено {len(columns_to_remove)} пустых столбцов: {', '.join(columns_to_remove)}")
        
        return df_cleaned

class ExcelProcessor:
    """Класс для обработки Excel файлов"""
    
    def __init__(self, config: Config):
        self.config = config
        self.cleaner = DataCleaner()
    
    def get_all_excel_files(self) -> List[str]:
        """Получает список всех Excel файлов в указанной папке"""
        excel_files = []
        folder_path = Path(self.config.folder_path)
        
        if not folder_path.exists():
            logger.error(f"Папка {self.config.folder_path} не существует")
            return excel_files
        
        for ext in self.config.excel_extensions:
            excel_files.extend(folder_path.glob(f"*{ext}"))
        
        # Фильтруем временные файлы Excel (начинающиеся с ~$)
        filtered_files = [f for f in excel_files if not f.name.startswith('~$')]
        
        logger.info(f"Найдено {len(filtered_files)} Excel файлов (исключая временные)")
        return [str(f) for f in filtered_files]
    
    def get_file_modification_time(self, file_path: str) -> Optional[datetime]:
        """Получает время последней модификации файла"""
        try:
            mtime = getmtime(file_path)
            return datetime.fromtimestamp(mtime)
        except (OSError, ValueError) as e:
            logger.warning(f"Не удалось получить время модификации файла {file_path}: {e}")
            return None
    
    def find_header_row(self, df: pd.DataFrame) -> Optional[int]:
        """Находит строку с заголовками"""
        for i in range(min(len(df), 10)):  # Проверяем первые 10 строк
            row_values = [str(cell).lower() for cell in df.iloc[i] if pd.notna(cell)]
            # Ищем ключевые слова, которые могут быть в заголовках
            keywords = ['испытание', 'пакет', 'трубопровод', 'система', 'номер', 'дата', 'результат']
            if any(keyword in ' '.join(row_values) for keyword in keywords):
                return i
        return None
    
    def read_excel_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """Читает Excel-файл и возвращает DataFrame только с нужными столбцами и добавляет столбец 'Титул'"""
        try:
            # Сначала читаем без заголовков
            df = pd.read_excel(file_path, header=None)
            
            # Ищем строку с заголовками
            header_row = self.find_header_row(df)
            
            if header_row is None:
                logger.warning(f"Не удалось найти строку заголовков в {file_path}")
                # Пробуем читать с первой строки как заголовок
                df = pd.read_excel(file_path, header=0)
            else:
                # Читаем с найденной строкой заголовков
                df = pd.read_excel(file_path, header=header_row)
            
            # Оставляем только первые 5 столбцов из файла
            df = df.iloc[:, :5]
            
            # Формируем первый столбец — автонумерация
            df.insert(0, 'Номер_п_п', range(1, len(df) + 1))
            
            # Извлекаем титул из имени файла (полный титул, начинающийся с 124)
            filename = os.path.basename(file_path)
            filename_wo_ext = os.path.splitext(filename)[0]
            match = re.search(r'(124[^\s]*)', filename_wo_ext)
            title = match.group(1) if match else ''
            df['Титул'] = title
            
            # Очищаем названия столбцов
            cleaned_cols = [self.cleaner.clean_column_name(col) for col in df.columns]
            
            # Проверяем на пустые названия столбцов и заменяем их
            for i, col in enumerate(cleaned_cols):
                if not col or col.strip() == '':
                    cleaned_cols[i] = f'Column_{i+1}'
            
            cleaned_cols = self.cleaner.make_unique_columns(cleaned_cols)
            df.columns = cleaned_cols
            
            # Извлекаем данные типа "FLL-0600" из столбца "Тэговый_номер_ТХ_оборудования"
            if 'Тэговый_номер_ТХ_оборудования' in df.columns:
                logger.info(f"Найден столбец 'Тэговый_номер_ТХ_оборудования' в файле {file_path}")
                def extract_tag_data(value):
                    if pd.isna(value) or value is None:
                        return ''
                    value_str = str(value)
                    # Ищем третий дефис в строке
                    first_dash = value_str.find('-')
                    if first_dash == -1:
                        return ''
                    
                    second_dash = value_str.find('-', first_dash + 1)
                    if second_dash == -1:
                        return ''
                    
                    third_dash = value_str.find('-', second_dash + 1)
                    if third_dash == -1:
                        return ''
                    
                    # Извлекаем часть от начала до третьего дефиса
                    result = value_str[:third_dash]
                    if result:
                        logger.debug(f"Извлечен номер линии: {result} из значения: {value_str}")
                    return result
                
                df['_Номер_Линии_'] = df['Тэговый_номер_ТХ_оборудования'].apply(extract_tag_data)
            else:
                df['_Номер_Линии_'] = ''
            
            # Заменяем NaN на None
            df = df.replace({np.nan: None})
            
            # Добавляем информацию о файле
            df['Исходный_файл'] = filename
            df['Дата_загрузки'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            logger.info(f"Успешно прочитан файл {file_path}: {len(df)} строк, {len(df.columns)} столбцов")
            return df
            
        except Exception as e:
            logger.error(f"Ошибка при чтении файла {file_path}: {str(e)}")
            return None

class DatabaseHandler:
    """Класс для работы с базой данных"""
    
    def __init__(self, config: Config):
        self.config = config
    
    def table_exists(self, conn: sqlite3.Connection) -> bool:
        """Проверяет существование таблицы"""
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name=?
        """, (self.config.table_name,))
        return cursor.fetchone() is not None
    
    def get_existing_files(self, conn: sqlite3.Connection) -> Set[str]:
        """Получает список уже загруженных файлов"""
        if not self.table_exists(conn):
            return set()
        
        cursor = conn.cursor()
        cursor.execute(f"SELECT DISTINCT Исходный_файл FROM {self.config.table_name}")
        return {row[0] for row in cursor.fetchall()}
    
    def get_file_load_time(self, conn: sqlite3.Connection, filename: str) -> Optional[datetime]:
        """Получает время загрузки файла в базу данных"""
        if not self.table_exists(conn):
            return None
        
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT MAX(Дата_загрузки) 
            FROM {self.config.table_name} 
            WHERE Исходный_файл = ?
        """, (filename,))
        
        result = cursor.fetchone()
        if result and result[0]:
            try:
                return datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S')
            except ValueError:
                return None
        return None
    
    def delete_file_data(self, conn: sqlite3.Connection, filename: str) -> int:
        """Удаляет все данные файла из базы данных и возвращает количество удаленных записей"""
        if not self.table_exists(conn):
            return 0
        
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {self.config.table_name} WHERE Исходный_файл = ?", (filename,))
        count = cursor.fetchone()[0]
        
        cursor.execute(f"DELETE FROM {self.config.table_name} WHERE Исходный_файл = ?", (filename,))
        conn.commit()
        
        logger.info(f"Удалено {count} записей для файла {filename}")
        return count
    
    def get_table_columns(self, conn: sqlite3.Connection) -> Optional[List[str]]:
        """Получает список столбцов таблицы"""
        if not self.table_exists(conn):
            return None
        
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({self.config.table_name})")
        return [row[1] for row in cursor.fetchall()]
    
    def get_row_count(self, conn: sqlite3.Connection) -> int:
        """Получает количество строк в таблице"""
        if not self.table_exists(conn):
            return 0
        
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {self.config.table_name}")
        return cursor.fetchone()[0]
    
    def remove_duplicates(self, conn: sqlite3.Connection) -> int:
        """Удаляет дубликаты из таблицы"""
        if not self.table_exists(conn):
            return 0
        
        cursor = conn.cursor()
        
        # Получаем список столбцов
        columns = self.get_table_columns(conn)
        if not columns:
            return 0
        
        # Исключаем системные столбцы
        exclude_columns = ['id', 'Исходный_файл', 'Дата_загрузки']
        data_columns = [col for col in columns if col not in exclude_columns]
        
        if not data_columns:
            return 0
        
        # Создаем временную таблицу без дубликатов
        temp_table = f"{self.config.table_name}_temp"
        columns_str = ', '.join([f'"{col}"' for col in columns])
        
        # Создаем временную таблицу
        cursor.execute(f"""
            CREATE TABLE {temp_table} AS
            SELECT DISTINCT {columns_str}
            FROM {self.config.table_name}
        """)
        
        # Удаляем старую таблицу
        cursor.execute(f"DROP TABLE {self.config.table_name}")
        
        # Переименовываем временную таблицу
        cursor.execute(f"ALTER TABLE {temp_table} RENAME TO {self.config.table_name}")
        
        conn.commit()
        
        # Получаем количество удаленных дубликатов
        original_count = self.get_row_count(conn)
        return original_count
    
    def create_table_from_df(self, conn: sqlite3.Connection, df: pd.DataFrame) -> None:
        """Создает таблицу на основе структуры DataFrame"""
        cursor = conn.cursor()
        
        # Удаляем существующую таблицу
        cursor.execute(f"DROP TABLE IF EXISTS {self.config.table_name}")
        
        # Создаем SQL для создания таблицы
        columns_sql = ['id INTEGER PRIMARY KEY AUTOINCREMENT']
        
        for i, col in enumerate(df.columns):
            # Проверяем на пустые названия столбцов
            if not col or col.strip() == '':
                col = f'Column_{i+1}'
            
            # Определяем тип данных для столбца
            col_type = 'TEXT'  # По умолчанию TEXT для всех столбцов
            columns_sql.append(f'"{col}" {col_type}')
        
        create_sql = f"""
        CREATE TABLE {self.config.table_name} (
            {', '.join(columns_sql)}
        )
        """
        
        cursor.execute(create_sql)
        conn.commit()
        logger.info(f"Создана таблица {self.config.table_name}")
    
    def insert_data(self, conn: sqlite3.Connection, df: pd.DataFrame, table_columns: List[str]) -> bool:
        """Вставляет данные в таблицу"""
        try:
            cursor = conn.cursor()
            
            # Исключаем столбец 'id' из списка для вставки
            insert_columns = [col for col in table_columns if col != 'id']
            
            # Проверяем, есть ли все необходимые столбцы в DataFrame
            missing_columns = [col for col in insert_columns if col not in df.columns]
            if missing_columns:
                logger.warning(f"Отсутствующие столбцы в DataFrame: {missing_columns}")
                # Добавляем отсутствующие столбцы с пустыми значениями
                for col in missing_columns:
                    df[col] = None
            
            # Проверяем, есть ли лишние столбцы в DataFrame
            extra_columns = [col for col in df.columns if col not in insert_columns]
            if extra_columns:
                logger.warning(f"Лишние столбцы в DataFrame: {extra_columns}")
            
            # Подготавливаем данные для вставки
            df_to_insert = df[insert_columns].copy()
            
            # Заменяем NaN на None
            df_to_insert = df_to_insert.replace({np.nan: None})
            
            # Создаем SQL запрос для вставки
            placeholders = ', '.join(['?' for _ in insert_columns])
            columns_str = ', '.join([f'"{col}"' for col in insert_columns])
            insert_sql = f"INSERT INTO {self.config.table_name} ({columns_str}) VALUES ({placeholders})"
            
            # Вставляем данные
            data = df_to_insert.values.tolist()
            cursor.executemany(insert_sql, data)
            conn.commit()
            
            logger.info(f"Успешно вставлено {len(data)} записей в таблицу {self.config.table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при вставке данных: {str(e)}")
            return False

class PipelineTestPackageProcessor:
    """Основной класс для обработки данных Pipeline Test Package"""
    
    def __init__(self, config: Config):
        self.config = config
        self.excel_processor = ExcelProcessor(config)
        self.db_handler = DatabaseHandler(config)
    
    def get_connection(self) -> sqlite3.Connection:
        """Получает подключение к базе данных"""
        if self.config.db_path is None:
            raise ValueError("Путь к базе данных не установлен")
        return sqlite3.connect(self.config.db_path)
    
    def process_file(self, file_path: str, table_columns: List[str]) -> bool:
        """Обрабатывает один файл"""
        try:
            # Читаем Excel файл
            df = self.excel_processor.read_excel_file(file_path)
            if df is None or df.empty:
                logger.warning(f"Файл {file_path} пуст или не может быть прочитан")
                return False
            
            # Подключаемся к базе данных
            with self.get_connection() as conn:
                # Вставляем данные
                success = self.db_handler.insert_data(conn, df, table_columns)
                if success:
                    logger.info(f"Файл {file_path} успешно обработан")
                    print("✅ Скрипт успешно завершён. Загружено строк:", len(df))
                return success
                
        except Exception as e:
            logger.error(f"Ошибка при обработке файла {file_path}: {str(e)}")
            return False
    
    def check_and_process_updates(self, excel_files: List[str], table_columns: List[str]) -> int:
        """Проверяет и обрабатывает обновления файлов"""
        updated_files = 0
        
        with self.get_connection() as conn:
            for file_path in excel_files:
                filename = os.path.basename(file_path)
                
                # Получаем время модификации файла
                file_mtime = self.excel_processor.get_file_modification_time(file_path)
                if file_mtime is None:
                    continue
                
                # Получаем время загрузки файла в БД
                db_load_time = self.db_handler.get_file_load_time(conn, filename)
                
                # Если файл был изменен после загрузки в БД
                if db_load_time is None:
                    # Файл еще не загружен
                    continue
                elif file_mtime > db_load_time:
                    logger.info(f"Файл {filename} был изменен после загрузки. Обновляем данные...")
                    
                    # Удаляем старые данные файла
                    deleted_count = self.db_handler.delete_file_data(conn, filename)
                    
                    # Обрабатываем файл заново
                    if self.process_file(file_path, table_columns):
                        updated_files += 1
                        logger.info(f"Файл {filename} успешно обновлен")
                    else:
                        logger.error(f"Не удалось обновить файл {filename}")
        
        return updated_files
    
    def run(self) -> None:
        """Основной метод запуска обработки"""
        logger.info("Начинаем обработку данных Pipeline Test Package")
        
        # Получаем список Excel файлов
        excel_files = self.excel_processor.get_all_excel_files()
        if not excel_files:
            logger.error("Excel файлы не найдены")
            return
        
        logger.info(f"Найдено {len(excel_files)} Excel файлов для обработки")
        
        # Подключаемся к базе данных
        with self.get_connection() as conn:
            # Проверяем существующие файлы
            existing_files = self.db_handler.get_existing_files(conn)
            logger.info(f"Найдено {len(existing_files)} уже загруженных файлов")
            
            # Получаем или создаем структуру таблицы
            table_columns = self.db_handler.get_table_columns(conn)
            
            if table_columns is None:
                # Создаем таблицу на основе первого файла
                logger.info("Создаем новую таблицу")
                first_df = self.excel_processor.read_excel_file(excel_files[0])
                if first_df is not None:
                    self.db_handler.create_table_from_df(conn, first_df)
                    table_columns = self.db_handler.get_table_columns(conn)
                    if table_columns is None:
                        logger.error("Не удалось получить столбцы таблицы после создания")
                        return
                else:
                    logger.error("Не удалось создать таблицу")
                    return
            
            # Проверяем обновления существующих файлов
            logger.info("Проверяем обновления существующих файлов...")
            updated_files = self.check_and_process_updates(excel_files, table_columns)
            logger.info(f"Обновлено {updated_files} файлов")
            
            # Фильтруем новые файлы
            new_files = [f for f in excel_files if os.path.basename(f) not in existing_files]
            logger.info(f"Найдено {len(new_files)} новых файлов для загрузки")
            
            # Обрабатываем новые файлы
            successful_files = 0
            for file_path in new_files:
                logger.info(f"Обрабатываем новый файл: {os.path.basename(file_path)}")
                if self.process_file(file_path, table_columns):
                    successful_files += 1
            
            # Удаляем дубликаты
            logger.info("Удаляем дубликаты...")
            removed_count = self.db_handler.remove_duplicates(conn)
            if removed_count > 0:
                logger.info(f"Удалено {removed_count} дубликатов")
            
            # Выводим итоговую статистику
            total_rows = self.db_handler.get_row_count(conn)
            logger.info(f"Обработка завершена. Успешно обработано {successful_files} новых файлов, обновлено {updated_files} файлов")
            logger.info(f"Всего записей в таблице {self.config.table_name}: {total_rows}")

def main():
    """Главная функция"""
    config = Config()
    processor = PipelineTestPackageProcessor(config)
    processor.run()

if __name__ == "__main__":
    main()

# Для запуска через GUI
def run_script():
    """Функция для запуска скрипта через GUI"""
    main() 