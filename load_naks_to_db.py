#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для загрузки данных НАКС из файла naks_merged.xlsx в базу данных M_Kran_Kingesepp.db

Загружает данные из объединенного Excel файла naks_merged.xlsx в таблицу naks_data.
Поддерживает инкрементальную загрузку с проверкой по полю "_УДОСТОВИРЕНИЕ_НАКС_".

Использование:
    python load_naks_to_db.py
"""

import os
import sys
import pandas as pd
import re
import logging
from pathlib import Path
from typing import Optional, Dict, List

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Добавляем путь к scripts для импорта модулей
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

try:
    from scripts.core.database import get_database_connection, DatabaseConnection
except ImportError:
    logger.error("Не удалось импортировать модуль database из scripts.core")
    logger.error("Убедитесь, что скрипт запускается из корня проекта")
    sys.exit(1)


def clean_column_name(col_name: str) -> str:
    """
    Очищает название столбца от переносов строк и лишних пробелов
    
    Args:
        col_name: Оригинальное название столбца
        
    Returns:
        Очищенное название столбца
    """
    if pd.isna(col_name):
        return 'unnamed_column'
    
    # Преобразуем в строку
    col_name = str(col_name)
    
    # Заменяем переносы строк на пробелы
    col_name = col_name.replace('\n', ' ').replace('\r', ' ')
    
    # Заменяем множественные пробелы на один пробел
    col_name = re.sub(r'\s+', ' ', col_name)
    
    # Убираем пробелы в начале и конце
    col_name = col_name.strip()
    
    # Если название пустое, возвращаем дефолтное
    if not col_name:
        return 'unnamed_column'
    
    return col_name


def find_naks_merged_file() -> Optional[Path]:
    """
    Ищет файл naks_merged.xlsx в возможных местах
    
    Returns:
        Path к файлу или None, если файл не найден
    """
    possible_paths = [
        project_root / 'archive' / 'NAKS' / 'НАКС_парсинг' / 'naks_merged.xlsx',
        project_root / 'NAKS' / 'НАКС_парсинг' / 'naks_merged.xlsx',
        project_root / 'naks_merged.xlsx',
    ]
    
    for path in possible_paths:
        if path.exists():
            logger.info(f"Файл найден: {path}")
            return path
    
    logger.error(f"Файл naks_merged.xlsx не найден. Проверенные пути:")
    for path in possible_paths:
        logger.error(f"  - {path}")
    return None


def update_table_structure(cursor, table_name: str, columns: List[str]) -> None:
    """
    Добавляет новые столбцы в существующую таблицу
    
    Args:
        cursor: Курсор базы данных
        table_name: Название таблицы
        columns: Список столбцов для проверки
    """
    # Получаем существующие столбцы
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = {row[1] for row in cursor.fetchall()}
    
    # Добавляем новые столбцы
    new_columns_added = 0
    for col in columns:
        if col not in existing_columns:
            logger.info(f"Добавление нового столбца: {col}")
            try:
                cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" TEXT')
                new_columns_added += 1
            except Exception as e:
                logger.warning(f"Не удалось добавить столбец {col}: {e}")
    
    if new_columns_added > 0:
        logger.info(f"Добавлено новых столбцов: {new_columns_added}")


def load_naks_to_db() -> bool:
    """
    Загружает данные из naks_merged.xlsx в базу данных
    
    Returns:
        True если загрузка успешна, False иначе
    """
    try:
        # Ищем файл naks_merged.xlsx
        excel_file = find_naks_merged_file()
        if not excel_file:
            return False
        
        logger.info(f"Читаем данные из: {excel_file}")
        
        # Читаем Excel файл
        try:
            df = pd.read_excel(excel_file)
            logger.info(f"Прочитано {len(df)} записей из Excel файла")
        except Exception as e:
            logger.error(f"Ошибка при чтении Excel файла: {e}")
            return False
        
        if len(df) == 0:
            logger.warning("Файл не содержит данных")
            return False
        
        # Очищаем названия столбцов
        logger.info("Очистка названий столбцов...")
        column_mapping: Dict[str, str] = {}
        seen_cleaned_names: Dict[str, int] = {}
        
        for col in df.columns:
            clean_name = clean_column_name(col)
            
            # Обрабатываем дубликаты очищенных названий
            if clean_name in seen_cleaned_names:
                seen_cleaned_names[clean_name] += 1
                clean_name = f"{clean_name}_{seen_cleaned_names[clean_name]}"
            else:
                seen_cleaned_names[clean_name] = 1
            
            column_mapping[col] = clean_name
        
        # Выводим сопоставление оригинальных и очищенных названий (только если изменились)
        changed_names = {orig: clean for orig, clean in column_mapping.items() if orig != clean}
        if changed_names:
            logger.info(f"Переименовано столбцов: {len(changed_names)}")
            for orig, clean in list(changed_names.items())[:10]:  # Показываем первые 10
                logger.debug(f'  "{orig}"  -->  "{clean}"')
        
        # Переименовываем столбцы в DataFrame
        df = df.rename(columns=column_mapping)
        
        # Проверяем наличие ключевого поля для проверки дубликатов
        key_field = "_УДОСТОВИРЕНИЕ_НАКС_"
        key_field_found = None
        
        # Пробуем найти поле с разными вариантами названия
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
            logger.warning(f"Ключевое поле '{key_field}' не найдено в данных")
            logger.info(f"Доступные поля: {list(df.columns)[:20]}...")  # Показываем первые 20
            # Продолжаем без проверки дубликатов
            key_field_found = None
        else:
            if key_field_found != key_field:
                logger.info(f"Используется поле '{key_field_found}' вместо '{key_field}'")
                key_field = key_field_found
        
        # Подключаемся к базе данных
        logger.info("Подключение к базе данных...")
        with DatabaseConnection() as conn:
            cursor = conn.cursor()
            
            # Создаем таблицу для данных НАКС
            table_name = 'naks_data'
            
            # Получаем список очищенных столбцов
            columns = list(df.columns)
            
            # Проверяем существование таблицы
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            table_exists = cursor.fetchone() is not None
            
            if not table_exists:
                # Создаем новую таблицу со всеми столбцами
                logger.info(f"Создание таблицы {table_name}...")
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    {', '.join([f'"{col}" TEXT' for col in columns])}
                )
                """
                cursor.execute(create_table_sql)
                conn.commit()
                logger.info(f"Таблица {table_name} создана со {len(columns)} столбцами")
            else:
                # Обновляем структуру существующей таблицы (добавляем новые столбцы)
                logger.info("Обновление структуры таблицы...")
                update_table_structure(cursor, table_name, columns)
                conn.commit()
                
                # Получаем только существующие столбцы для загрузки
                cursor.execute(f"PRAGMA table_info({table_name})")
                existing_columns = {row[1] for row in cursor.fetchall()}
                available_columns = [col for col in columns if col in existing_columns]
                
                if len(available_columns) < len(columns):
                    missing = set(columns) - set(available_columns)
                    logger.warning(f"Пропущено столбцов, которых нет в таблице: {len(missing)}")
                    logger.debug(f"Пропущенные столбцы: {list(missing)[:10]}")
                
                columns = available_columns
                df = df[columns]  # Оставляем только доступные столбцы
            
            logger.info(f"Используем {len(columns)} столбцов для загрузки")
            
            # Получаем существующие ключевые значения из базы (если есть ключевое поле)
            existing_keys = set()
            if key_field and key_field in columns:
                logger.info("Проверка существующих записей...")
                try:
                    cursor.execute(f'SELECT DISTINCT "{key_field}" FROM {table_name} WHERE "{key_field}" IS NOT NULL')
                    existing_keys = {str(row[0]) for row in cursor.fetchall()}
                    logger.info(f"Найдено {len(existing_keys)} существующих записей в базе")
                except Exception as e:
                    logger.warning(f"Не удалось проверить существующие записи: {e}")
                    existing_keys = set()
            
            # Фильтруем только новые записи
            if key_field and key_field in df.columns:
                df['_cert_key'] = df[key_field].astype(str)
                df_new = df[~df['_cert_key'].isin(existing_keys)]
                df = df.drop('_cert_key', axis=1)
                if '_cert_key' in df_new.columns:
                    df_new = df_new.drop('_cert_key', axis=1)
            else:
                # Если нет ключевого поля, загружаем все записи
                df_new = df.copy()
                logger.warning("Ключевое поле не найдено, будут загружены все записи (возможны дубликаты)")
            
            logger.info(f"Найдено {len(df_new)} новых записей для загрузки")
            
            if len(df_new) == 0:
                logger.info("Новых записей для загрузки не найдено")
                if key_field:
                    logger.info(f"Пропущено существующих записей: {len(df) - len(df_new)}")
                return True
            
            # Подготавливаем данные для вставки
            # Заменяем NaN на None для корректной работы с SQLite
            df_clean = df_new.where(pd.notnull(df_new), None)
            
            # Используем pandas to_sql для более надежной вставки
            logger.info(f"Импорт {len(df_new)} новых записей...")
            try:
                df_clean.to_sql(table_name, conn, if_exists='append', index=False)
                logger.info("Данные успешно загружены через pandas.to_sql")
            except Exception as e:
                logger.warning(f"Ошибка при использовании pandas.to_sql: {e}")
                logger.info("Пробуем альтернативный метод...")
                
                # Альтернативный метод через executemany
                # Фильтруем только существующие столбцы
                cursor.execute(f"PRAGMA table_info({table_name})")
                existing_columns = {row[1] for row in cursor.fetchall()}
                available_columns = [col for col in columns if col in existing_columns]
                
                if len(available_columns) != len(columns):
                    logger.info(f"Используем только {len(available_columns)} из {len(columns)} столбцов")
                    df_clean = df_clean[available_columns]
                    columns = available_columns
                
                # Создаем SQL для вставки данных
                placeholders = ', '.join(['?' for _ in columns])
                insert_sql = f"INSERT INTO {table_name} ({', '.join([f'\"{col}\"' for col in columns])}) VALUES ({placeholders})"
                
                # Вставляем данные батчами для лучшей производительности
                batch_size = 1000
                total_rows = len(df_clean)
                inserted_rows = 0
                
                for i in range(0, total_rows, batch_size):
                    batch = df_clean.iloc[i:i+batch_size]
                    data_to_insert = [tuple(row) for row in batch.values]
                    cursor.executemany(insert_sql, data_to_insert)
                    inserted_rows += len(batch)
                    logger.info(f"Загружено {inserted_rows} из {total_rows} записей...")
                
                logger.info("Данные успешно загружены через executemany")
            
            # Сохраняем изменения
            conn.commit()
            
            # Проверяем общее количество записей в таблице
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_count = cursor.fetchone()[0]
            logger.info(f"\nВсего записей в таблице {table_name}: {total_count}")
            logger.info(f"Добавлено новых записей: {len(df_new)}")
            if key_field:
                logger.info(f"Пропущено существующих записей: {len(df) - len(df_new)}")
            
            # Показываем пример новых данных из базы
            cursor.execute(f"SELECT * FROM {table_name} ORDER BY id DESC LIMIT 3")
            sample_data = cursor.fetchall()
            if sample_data:
                logger.info("\nПоследние добавленные записи (первые 3 столбца):")
                for row in sample_data:
                    cols = [row.keys()[i] if hasattr(row, 'keys') else i for i in range(min(3, len(row)))]
                    logger.info(f"  ID: {row[0] if len(row) > 0 else 'N/A'}, ...")
        
        logger.info("\n✅ Инкрементальная загрузка завершена успешно!")
        return True
        
    except Exception as e:
        logger.error(f"Произошла ошибка: {str(e)}", exc_info=True)
        return False


def main():
    """Основная функция"""
    logger.info("=" * 60)
    logger.info("Загрузка данных НАКС из naks_merged.xlsx в базу данных")
    logger.info("=" * 60)
    
    success = load_naks_to_db()
    
    if success:
        logger.info("\n✅ Скрипт выполнен успешно")
        sys.exit(0)
    else:
        logger.error("\n❌ Скрипт завершился с ошибками")
        sys.exit(1)


if __name__ == "__main__":
    main()

