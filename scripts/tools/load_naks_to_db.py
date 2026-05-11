#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для загрузки данных НАКС из файла naks_merged.xlsx в базу данных M_Kran_Kingesepp.db.
"""

import sys
import pandas as pd
import re
import logging
from pathlib import Path
from typing import Optional, Dict, List

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

try:
    from scripts.core.database import DatabaseConnection
except ImportError:
    logger.error("Не удалось импортировать модуль database из scripts.core")
    logger.error("Убедитесь, что скрипт запускается из корня проекта")
    sys.exit(1)


def clean_column_name(col_name: str) -> str:
    if pd.isna(col_name):
        return "unnamed_column"
    col_name = str(col_name).replace("\n", " ").replace("\r", " ")
    col_name = re.sub(r"\s+", " ", col_name).strip()
    return col_name or "unnamed_column"


def find_naks_merged_file() -> Optional[Path]:
    possible_paths = [
        project_root / "archive" / "NAKS" / "НАКС_парсинг" / "naks_merged.xlsx",
        project_root / "NAKS" / "НАКС_парсинг" / "naks_merged.xlsx",
        project_root / "naks_merged.xlsx",
    ]
    for path in possible_paths:
        if path.exists():
            logger.info(f"Файл найден: {path}")
            return path
    logger.error("Файл naks_merged.xlsx не найден. Проверенные пути:")
    for path in possible_paths:
        logger.error(f"  - {path}")
    return None


def update_table_structure(cursor, table_name: str, columns: List[str]) -> None:
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = {row[1] for row in cursor.fetchall()}
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
    try:
        excel_file = find_naks_merged_file()
        if not excel_file:
            return False

        logger.info(f"Читаем данные из: {excel_file}")
        try:
            df = pd.read_excel(excel_file)
            logger.info(f"Прочитано {len(df)} записей из Excel файла")
        except Exception as e:
            logger.error(f"Ошибка при чтении Excel файла: {e}")
            return False

        if len(df) == 0:
            logger.warning("Файл не содержит данных")
            return False

        logger.info("Очистка названий столбцов...")
        column_mapping: Dict[str, str] = {}
        seen_cleaned_names: Dict[str, int] = {}
        for col in df.columns:
            clean_name = clean_column_name(col)
            if clean_name in seen_cleaned_names:
                seen_cleaned_names[clean_name] += 1
                clean_name = f"{clean_name}_{seen_cleaned_names[clean_name]}"
            else:
                seen_cleaned_names[clean_name] = 1
            column_mapping[col] = clean_name

        changed_names = {orig: clean for orig, clean in column_mapping.items() if orig != clean}
        if changed_names:
            logger.info(f"Переименовано столбцов: {len(changed_names)}")
        df = df.rename(columns=column_mapping)

        key_field = "_УДОСТОВИРЕНИЕ_НАКС_"
        key_field_found = None
        for col in df.columns:
            if key_field.lower() in col.lower() or col.lower() in key_field.lower():
                key_field_found = col
                break
        if not key_field_found:
            for col in df.columns:
                if "удостоверение" in col.lower() or "накс" in col.lower():
                    key_field_found = col
                    break
        if not key_field_found:
            logger.warning(f"Ключевое поле '{key_field}' не найдено в данных")
            logger.info(f"Доступные поля: {list(df.columns)[:20]}...")
            key_field_found = None
        elif key_field_found != key_field:
            logger.info(f"Используется поле '{key_field_found}' вместо '{key_field}'")
            key_field = key_field_found

        logger.info("Подключение к базе данных...")
        with DatabaseConnection() as conn:
            cursor = conn.cursor()
            table_name = "naks_data"
            columns = list(df.columns)

            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            table_exists = cursor.fetchone() is not None
            if not table_exists:
                logger.info(f"Создание таблицы {table_name}...")
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    {', '.join([f'"{col}" TEXT' for col in columns])}
                )
                """
                cursor.execute(create_table_sql)
                conn.commit()
            else:
                logger.info("Обновление структуры таблицы...")
                update_table_structure(cursor, table_name, columns)
                conn.commit()
                cursor.execute(f"PRAGMA table_info({table_name})")
                existing_columns = {row[1] for row in cursor.fetchall()}
                columns = [col for col in columns if col in existing_columns]
                df = df[columns]

            existing_keys = set()
            if key_field and key_field in columns:
                try:
                    cursor.execute(
                        f'SELECT DISTINCT "{key_field}" FROM {table_name} WHERE "{key_field}" IS NOT NULL'
                    )
                    existing_keys = {str(row[0]) for row in cursor.fetchall()}
                except Exception as e:
                    logger.warning(f"Не удалось проверить существующие записи: {e}")
                    existing_keys = set()

            if key_field and key_field in df.columns:
                df["_cert_key"] = df[key_field].astype(str)
                df_new = df[~df["_cert_key"].isin(existing_keys)]
                df = df.drop("_cert_key", axis=1)
                if "_cert_key" in df_new.columns:
                    df_new = df_new.drop("_cert_key", axis=1)
            else:
                df_new = df.copy()
                logger.warning("Ключевое поле не найдено, будут загружены все записи")

            logger.info(f"Найдено {len(df_new)} новых записей для загрузки")
            if len(df_new) == 0:
                logger.info("Новых записей для загрузки не найдено")
                return True

            df_clean = df_new.where(pd.notnull(df_new), None)
            try:
                df_clean.to_sql(table_name, conn, if_exists="append", index=False)
            except Exception as e:
                logger.warning(f"Ошибка при использовании pandas.to_sql: {e}")
                cursor.execute(f"PRAGMA table_info({table_name})")
                existing_columns = {row[1] for row in cursor.fetchall()}
                columns = [col for col in columns if col in existing_columns]
                df_clean = df_clean[columns]
                placeholders = ", ".join(["?" for _ in columns])
                insert_sql = (
                    f"INSERT INTO {table_name} ({', '.join([f'\"{col}\"' for col in columns])}) VALUES ({placeholders})"
                )
                batch_size = 1000
                total_rows = len(df_clean)
                for i in range(0, total_rows, batch_size):
                    batch = df_clean.iloc[i : i + batch_size]
                    data_to_insert = [tuple(row) for row in batch.values]
                    cursor.executemany(insert_sql, data_to_insert)

            conn.commit()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_count = cursor.fetchone()[0]
            logger.info(f"Всего записей в таблице {table_name}: {total_count}")
            logger.info(f"Добавлено новых записей: {len(df_new)}")

        logger.info("✅ Инкрементальная загрузка завершена успешно!")
        return True
    except Exception as e:
        logger.error(f"Произошла ошибка: {str(e)}", exc_info=True)
        return False


def main():
    logger.info("=" * 60)
    logger.info("Загрузка данных НАКС из naks_merged.xlsx в базу данных")
    logger.info("=" * 60)
    success = load_naks_to_db()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
