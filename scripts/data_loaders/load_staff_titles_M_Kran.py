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
                # Таблица существует, проверяем наличие новых столбцов и добавляем их при необходимости
                cursor.execute(f"PRAGMA table_info({table_name})")
                existing_columns = [row[1] for row in cursor.fetchall()]

                # Добавляем отсутствующие столбцы
                if "Ответств" not in existing_columns:
                    try:
                        cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN "Ответств" TEXT')
                        logger.info(f"Добавлен столбец 'Ответств' в таблицу {table_name}")
                    except Exception as e:
                        logger.warning(f"Не удалось добавить столбец 'Ответств': {e}")

                if "Зона" not in existing_columns:
                    try:
                        cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN "Зона" TEXT')
                        logger.info(f"Добавлен столбец 'Зона' в таблицу {table_name}")
                    except Exception as e:
                        logger.warning(f"Не удалось добавить столбец 'Зона': {e}")

                if "ФИО_Сотрудника_нормализованное" not in existing_columns:
                    try:
                        # Столбец может хранить как INTEGER (id_fio), так и TEXT (старые значения ФИО)
                        cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN "ФИО_Сотрудника_нормализованное" TEXT')
                        logger.info(f"Добавлен столбец 'ФИО_Сотрудника_нормализованное' в таблицу {table_name}")
                    except Exception as e:
                        logger.warning(f"Не удалось добавить столбец 'ФИО_Сотрудника_нормализованное': {e}")

                conn.commit()
                return True

            # Создаем таблицу только если она не существует
            columns_sql = [
                "id INTEGER PRIMARY KEY AUTOINCREMENT",
                '"ФИО_Сотрудника" TEXT',
                '"ФИО_Сотрудника_нормализованное" TEXT',
                '"Должность" TEXT',
                '"Ответств" TEXT',
                '"Зона" TEXT',
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
                SELECT "ФИО_Сотрудника", "Должность", "Ответств", "Зона", "Титул", "Дата", source_file
                FROM {table_name}
            """)
            existing_records = set()
            for row in cursor.fetchall():
                # Создаем уникальный ключ из комбинации полей
                key = (str(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]), str(row[5]), str(row[6]))
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
                columns = ['ФИО_Сотрудника', 'ФИО_Сотрудника_нормализованное', 'Должность', 'Ответств', 'Зона', 'Титул', 'Дата', 'source_file', 'processed_date']
                # Проверяем наличие столбцов в DataFrame
                available_columns = [col for col in columns if col in df.columns]
                df_copy = df[available_columns].copy()
                # Заполняем отсутствующие столбцы пустыми значениями
                for col in columns:
                    if col not in df_copy.columns:
                        df_copy[col] = ''

                # Нормализуем ФИО для последующего сопоставления с ФИО_свар
                if 'ФИО_Сотрудника_нормализованное' not in df_copy.columns or df_copy['ФИО_Сотрудника_нормализованное'].isna().all():
                    if 'ФИО_Сотрудника' in df_copy.columns:
                        df_copy['ФИО_Сотрудника_нормализованное'] = df_copy['ФИО_Сотрудника'].apply(
                            lambda x: self.normalize_fio(x) if pd.notna(x) else ''
                        )
                    else:
                        df_copy['ФИО_Сотрудника_нормализованное'] = ''

                placeholders = ','.join(['?' for _ in columns])
                columns_str = ','.join([f'"{col}"' for col in columns])
                insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

                records_to_insert = [tuple(row) for _, row in df_copy.iterrows()]
                cursor.executemany(insert_sql, records_to_insert)
                conn.commit()

                rows_inserted = len(records_to_insert)
                self.stats['rows_loaded_to_db'] += rows_inserted

                logger.info(f"Принудительно загружено в БД: {rows_inserted} записей")

                # Обновляем нормализованное ФИО из ФИО_свар
                self.update_normalized_fio_from_summ_table(conn, table_name)

            else:
                # Инкрементная загрузка (оригинальная логика)
                # Получаем существующие записи
                existing_records = self.get_existing_records(conn, table_name)

                # Подготавливаем данные для вставки
                columns = ['ФИО_Сотрудника', 'ФИО_Сотрудника_нормализованное', 'Должность', 'Ответств', 'Зона', 'Титул', 'Дата', 'source_file', 'processed_date']
                # Проверяем наличие столбцов в DataFrame
                available_columns = [col for col in columns if col in df.columns]
                df_copy = df[available_columns].copy()
                # Заполняем отсутствующие столбцы пустыми значениями
                for col in columns:
                    if col not in df_copy.columns:
                        df_copy[col] = ''

                # Нормализуем ФИО для последующего сопоставления с ФИО_свар
                if 'ФИО_Сотрудника_нормализованное' not in df_copy.columns or df_copy['ФИО_Сотрудника_нормализованное'].isna().all():
                    if 'ФИО_Сотрудника' in df_copy.columns:
                        df_copy['ФИО_Сотрудника_нормализованное'] = df_copy['ФИО_Сотрудника'].apply(
                            lambda x: self.normalize_fio(x) if pd.notna(x) else ''
                        )
                    else:
                        df_copy['ФИО_Сотрудника_нормализованное'] = ''

                # Фильтруем только новые записи
                new_records = []
                duplicates_count = 0

                for _, row in df_copy.iterrows():
                    # Создаем уникальный ключ для проверки
                    key = (
                        str(row['ФИО_Сотрудника']) if 'ФИО_Сотрудника' in row else '',
                        str(row['Должность']) if 'Должность' in row else '',
                        str(row['Ответств']) if 'Ответств' in row else '',
                        str(row['Зона']) if 'Зона' in row else '',
                        str(row['Титул']) if 'Титул' in row else '',
                        str(row['Дата']) if 'Дата' in row else '',
                        str(row['source_file']) if 'source_file' in row else ''
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

                    # Обновляем нормализованное ФИО из ФИО_свар
                    self.update_normalized_fio_from_summ_table(conn, table_name)
                else:
                    logger.info("Все записи уже существуют в базе данных")

            return True

        except Exception as e:
            logger.error(f"Ошибка загрузки в БД: {e}")
            return False

    def find_header_row_with_position(self, df: pd.DataFrame, max_search_rows: int = 20) -> Optional[int]:
        """Находит строку с заголовком, содержащим слово 'Должность'"""
        for row_idx in range(min(len(df), max_search_rows)):
            # Важно: у Series итерация идет по индексам, а не по значениям.
            # Берем именно значения ячеек, иначе возможен float.lower() для имен колонок.
            row_values = df.iloc[row_idx].astype(str).values
            for cell_value in row_values:
                if 'должность' in str(cell_value).lower():
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

    def update_normalized_fio_from_summ_table(self, conn: sqlite3.Connection, table_name: str = "Daily_Staff_Allocation") -> None:
        """
        Обновляет нормализованное ФИО в Daily_Staff_Allocation из таблицы ФИО_свар
        используя Python-логику для более гибкого и поддерживаемого сопоставления.
        
        В столбец ФИО_Сотрудника_нормализованное записывается id_fio из таблицы ФИО_свар
        вместо полного ФИО для правильной нормализации данных.

        Args:
            conn: Подключение к базе данных
            table_name: Имя таблицы для обновления
        """
        try:
            import sys
            import os
            # Добавляем путь к модулям для импорта
            current_dir = os.path.dirname(os.path.abspath(__file__))
            if current_dir not in sys.path:
                sys.path.insert(0, current_dir)
            from fio_matcher import create_fio_matcher_from_db
            
            # Создаем сопоставитель ФИО
            matcher = create_fio_matcher_from_db(conn, "ФИО_свар", "ФИО", "id_fio")
            
            cursor = conn.cursor()
            
            # Получаем все записи, которые нужно обновить
            # Сначала записи с заполненным нормализованным ФИО (проверяем, является ли оно id_fio или ФИО)
            cursor.execute(f"""
                SELECT id, "ФИО_Сотрудника_нормализованное", "ФИО_Сотрудника"
                FROM "{table_name}"
                WHERE "ФИО_Сотрудника" IS NOT NULL
                  AND TRIM("ФИО_Сотрудника") != ''
                  AND "ФИО_Сотрудника_нормализованное" IS NOT NULL
                  AND TRIM("ФИО_Сотрудника_нормализованное") != ''
            """)
            records_normalized = cursor.fetchall()
            
            # Затем записи с пустым нормализованным ФИО
            cursor.execute(f"""
                SELECT id, "ФИО_Сотрудника"
                FROM "{table_name}"
                WHERE "ФИО_Сотрудника" IS NOT NULL
                  AND TRIM("ФИО_Сотрудника") != ''
                  AND (
                      "ФИО_Сотрудника_нормализованное" IS NULL
                      OR "ФИО_Сотрудника_нормализованное" = ''
                      OR TRIM("ФИО_Сотрудника_нормализованное") = ''
                  )
            """)
            records_original = cursor.fetchall()
            
            # Обновляем записи с заполненным нормализованным ФИО
            updated_count_normalized = 0
            for record_id, normalized_value, original_fio in records_normalized:
                # Проверяем, является ли значение числом (id_fio) или строкой (ФИО)
                try:
                    # Если это число, пропускаем (уже обновлено)
                    int(normalized_value)
                    continue
                except (ValueError, TypeError):
                    # Если это строка (старое значение), ищем id_fio
                    matched_id = matcher.match(normalized_value)
                    if not matched_id:
                        # Если не нашли по нормализованному, пробуем по оригинальному
                        matched_id = matcher.match(original_fio)
                    if matched_id:
                        cursor.execute(f"""
                            UPDATE "{table_name}"
                            SET "ФИО_Сотрудника_нормализованное" = ?
                            WHERE id = ?
                        """, (matched_id, record_id))
                        updated_count_normalized += 1
            
            # Обновляем записи с пустым нормализованным ФИО
            updated_count_original = 0
            for record_id, original_fio in records_original:
                matched_id = matcher.match(original_fio)
                if matched_id:
                    cursor.execute(f"""
                        UPDATE "{table_name}"
                        SET "ФИО_Сотрудника_нормализованное" = ?
                        WHERE id = ?
                    """, (matched_id, record_id))
                    updated_count_original += 1
            
            conn.commit()
            
            total_updated = updated_count_normalized + updated_count_original
            if total_updated > 0:
                logger.info(f"Обновлено нормализованных ФИО из ФИО_свар: {total_updated} записей (по нормализованному: {updated_count_normalized}, по оригинальному: {updated_count_original})")
            else:
                logger.info("Нет записей для обновления нормализованного ФИО")

        except Exception as e:
            logger.error(f"Ошибка обновления нормализованного ФИО из ФИО_свар: {e}")
            if conn:
                conn.rollback()

    def normalize_fio(self, fio: str) -> str:
        """
        Нормализует ФИО к единому формату: "Фамилия Имя Отчество"

        Обрабатывает различные варианты написания:
        - "Иванов И.И." -> "Иванов И И"
        - "Иванов Иван Иванович" -> "Иванов Иван Иванович"
        - "ИВАНОВ иван иванович" -> "Иванов Иван Иванович"
        - "Иванов  И.  И." -> "Иванов И И" (убирает лишние пробелы)
        - "Иванов, И.И." -> "Иванов И И" (убирает запятые)
        - "Иванов И.И." -> "Иванов И И" (обрабатывает инициалы с точками)

        Args:
            fio: Исходное ФИО

        Returns:
            str: Нормализованное ФИО в формате "Фамилия Имя Отчество"
        """
        if pd.isna(fio) or not fio or str(fio).strip() == '':
            return ''

        # Преобразуем в строку и убираем лишние пробелы
        fio_str = str(fio).strip()

        # Удаляем запятые и другие разделители
        fio_str = re.sub(r'[,;:]+', ' ', fio_str)

        # Превращаем слитные формы через точку в разделенные токены:
        # "Власов.Ю" -> "Власов Ю", "Иванов.И.И" -> "Иванов И И"
        fio_str = re.sub(r'([А-Яа-яA-Za-z])\.([А-Яа-яA-Za-z])', r'\1 \2', fio_str)

        # Обрабатываем инициалы с точками: "И.И." -> "И И"
        # Заменяем паттерн "Буква.Буква." на "Буква Буква"
        fio_str = re.sub(r'([А-Яа-яA-Za-z])\.([А-Яа-яA-Za-z])\.', r'\1 \2', fio_str)
        # Заменяем одиночные инициалы с точкой: "И." -> "И"
        fio_str = re.sub(r'([А-Яа-яA-Za-z])\.', r'\1', fio_str)

        # Заменяем множественные пробелы на один
        fio_str = re.sub(r'\s+', ' ', fio_str)

        # Разбиваем на части
        parts = fio_str.split()

        if not parts:
            return ''

        # Нормализуем каждую часть
        normalized_parts = []
        for part in parts:
            part = part.strip()
            if not part:
                continue

            # Убираем оставшиеся точки в конце
            part = part.rstrip('.')

            # Приводим к формату: первая буква заглавная, остальные строчные
            if len(part) > 0:
                # Для кириллицы и латиницы используем capitalize
                part = part.capitalize()

            if part:
                normalized_parts.append(part)

        # Объединяем части пробелами
        normalized_fio = ' '.join(normalized_parts)

        return normalized_fio

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

                        # Ищем обязательные столбцы
                        required_cols = [col for col in cleaned_columns if str(col).lower() in ["фио_сотрудника", "должность"]]
                        if not ("фио_сотрудника" in [str(c).lower() for c in required_cols] and "должность" in [str(c).lower() for c in required_cols]):
                            continue

                        # Ищем дополнительные столбцы "Ответств" и "Зона" (по частичному совпадению)
                        otvetstv_col = None
                        zona_col = None
                        for col in cleaned_columns:
                            col_lower = str(col).lower()
                            if "ответств" in col_lower and otvetstv_col is None:
                                otvetstv_col = col
                            if "зона" in col_lower and zona_col is None:
                                zona_col = col

                        # Формируем список всех нужных столбцов
                        all_cols = required_cols.copy()
                        if otvetstv_col:
                            all_cols.append(otvetstv_col)
                        if zona_col:
                            all_cols.append(zona_col)

                        df_filtered = df_with_header[all_cols].copy()

                        # Переименовываем столбцы для единообразия
                        col_mapping = {}
                        for col in df_filtered.columns:
                            col_lower = str(col).lower()
                            if "фио_сотрудника" in col_lower or "фио" in col_lower:
                                col_mapping[col] = "ФИО_Сотрудника"
                            elif "должность" in col_lower:
                                col_mapping[col] = "Должность"
                            elif otvetstv_col and col == otvetstv_col:
                                col_mapping[col] = "Ответств"
                            elif zona_col and col == zona_col:
                                col_mapping[col] = "Зона"

                        df_filtered = df_filtered.rename(columns=col_mapping)

                        # Применяем forward fill для столбцов "Ответств" и "Зона" перед фильтрацией
                        if "Ответств" in df_filtered.columns:
                            df_filtered["Ответств"] = df_filtered["Ответств"].ffill()
                        if "Зона" in df_filtered.columns:
                            df_filtered["Зона"] = df_filtered["Зона"].ffill()

                        # Фильтруем только строки, где в Должность есть 'сварщ' (без учета регистра)
                        mask = df_filtered["Должность"].astype(str).str.lower().str.contains("сварщ", na=False)
                        df_filtered = df_filtered[mask]

                        # Фильтруем строки, исключая записи с "Рыбкин" в ФИО_Сотрудника
                        mask_no_rybkin = ~df_filtered["ФИО_Сотрудника"].astype(str).str.contains("Рыбкин", case=False, na=False)
                        df_filtered = df_filtered[mask_no_rybkin]

                        # Удаляем полностью пустые строки
                        df_filtered = df_filtered.dropna(how='all')
                        # Удаляем строки, где обе ячейки пустые или содержат только пробелы
                        non_empty_mask = df_filtered.astype(str).apply(lambda x: x.str.strip() != '').any(axis=1)
                        df_filtered = df_filtered[non_empty_mask]

                        if len(df_filtered) > 0:
                            # Убеждаемся, что столбцы "Ответств" и "Зона" существуют (заполняем пустыми строками, если их нет)
                            if "Ответств" not in df_filtered.columns:
                                df_filtered["Ответств"] = ''
                            if "Зона" not in df_filtered.columns:
                                df_filtered["Зона"] = ''

                            # Заменяем NaN на пустые строки для новых столбцов
                            df_filtered["Ответств"] = df_filtered["Ответств"].fillna('')
                            df_filtered["Зона"] = df_filtered["Зона"].fillna('')

                            # Добавляем служебные столбцы
                            df_filtered['source_file'] = file_path.name
                            df_filtered['processed_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            # Добавляем титул и дату из имени файла
                            df_filtered['Титул'] = self.extract_title_from_filename(file_path.name)
                            df_filtered['Дата'] = self.extract_date_from_filename(file_path.name)

                            # Нормализуем ФИО для последующего сопоставления с ФИО_свар
                            if 'ФИО_Сотрудника' in df_filtered.columns:
                                df_filtered['ФИО_Сотрудника_нормализованное'] = df_filtered['ФИО_Сотрудника'].apply(
                                    lambda x: self.normalize_fio(x) if pd.notna(x) else ''
                                )
                            else:
                                df_filtered['ФИО_Сотрудника_нормализованное'] = ''

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

    def update_existing_normalized_fio(self, conn: sqlite3.Connection, table_name: str = "Daily_Staff_Allocation") -> None:
        """Обновляет нормализованное ФИО для существующих записей из ФИО_свар, где оно пустое"""
        # Используем тот же метод, что и для новых записей
        self.update_normalized_fio_from_summ_table(conn, table_name)

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

                # Обновляем нормализованное ФИО для существующих записей (если были добавлены новые)
                self.update_existing_normalized_fio(conn, "Daily_Staff_Allocation")

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
            print("Скрипт успешно завершен. Загружено строк:", self.stats['rows_loaded_to_db'])

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
