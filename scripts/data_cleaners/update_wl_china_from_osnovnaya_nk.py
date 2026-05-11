#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Обновляет wl_china данными из таблицы основнаяНК.

Ключи сопоставления:
- wl_china."Установка" = основнаяНК."титул / title"
- wl_china."N_Линии_по_старой_ревизии_РД" = основнаяНК."Номер линии / Line No"

Обновляемые поля:
- wl_china."Проектный_контроля"
- wl_china."проектный_объем_РК"

Источник значения:
- основнаяНК."РК (Радиографический контроль) / RT"
"""

import sqlite3
from pathlib import Path


SOURCE_TABLE = "основнаяНК"
TARGET_TABLE = "wl_china"
SOURCE_VALUE_COLUMN = "РК (Радиографический контроль) / RT"

TARGET_MATCH_TITLE = "Установка"
TARGET_MATCH_LINE = "N_Линии_по_старой_ревизии_РД"
SOURCE_MATCH_TITLE = "титул / title"
SOURCE_MATCH_LINE = "Номер линии / Line No"

TARGET_COLUMNS = (
    "Проектный_контроля",
    "проектный_объем_РК",
)


def get_db_path() -> Path:
    return Path(__file__).resolve().parents[2] / "database" / "BD_Kingisepp" / "M_Kran_Kingesepp.db"


def get_table_columns(cursor: sqlite3.Cursor, table_name: str) -> set[str]:
    cursor.execute(f'PRAGMA table_info("{table_name}")')
    return {row[1] for row in cursor.fetchall()}


def validate_schema(cursor: sqlite3.Cursor) -> None:
    target_columns = get_table_columns(cursor, TARGET_TABLE)
    source_columns = get_table_columns(cursor, SOURCE_TABLE)

    if not target_columns:
        raise RuntimeError(f'Таблица "{TARGET_TABLE}" не найдена')
    if not source_columns:
        raise RuntimeError(f'Таблица "{SOURCE_TABLE}" не найдена')

    missing_target = {
        TARGET_MATCH_TITLE,
        TARGET_MATCH_LINE,
        *TARGET_COLUMNS,
    } - target_columns
    missing_source = {
        SOURCE_MATCH_TITLE,
        SOURCE_MATCH_LINE,
        SOURCE_VALUE_COLUMN,
    } - source_columns

    if missing_target:
        raise RuntimeError(
            f'В таблице "{TARGET_TABLE}" отсутствуют столбцы: {", ".join(sorted(missing_target))}'
        )
    if missing_source:
        raise RuntimeError(
            f'В таблице "{SOURCE_TABLE}" отсутствуют столбцы: {", ".join(sorted(missing_source))}'
        )


def build_match_condition(target_alias: str = "wc", source_alias: str = "onk") -> str:
    return f'''
        TRIM(COALESCE({target_alias}."{TARGET_MATCH_TITLE}", "")) = TRIM(COALESCE({source_alias}."{SOURCE_MATCH_TITLE}", ""))
        AND TRIM(COALESCE({target_alias}."{TARGET_MATCH_LINE}", "")) = TRIM(COALESCE({source_alias}."{SOURCE_MATCH_LINE}", ""))
        AND TRIM(COALESCE({source_alias}."{SOURCE_VALUE_COLUMN}", "")) <> ""
    '''


def get_candidate_count(cursor: sqlite3.Cursor) -> int:
    cursor.execute(
        f'''
        SELECT COUNT(*)
        FROM "{TARGET_TABLE}" AS wc
        WHERE EXISTS (
            SELECT 1
            FROM "{SOURCE_TABLE}" AS onk
            WHERE {build_match_condition()}
        )
        '''
    )
    return cursor.fetchone()[0]


def get_filled_count(cursor: sqlite3.Cursor, column_name: str) -> int:
    cursor.execute(
        f'''
        SELECT COUNT(*)
        FROM "{TARGET_TABLE}"
        WHERE TRIM(COALESCE("{column_name}", "")) <> ""
        '''
    )
    return cursor.fetchone()[0]


def update_wl_china_from_osnovnaya_nk() -> None:
    db_path = get_db_path()
    if not db_path.exists():
        raise FileNotFoundError(f"База данных не найдена: {db_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.cursor()
        validate_schema(cursor)

        candidate_count = get_candidate_count(cursor)
        before_counts = {column: get_filled_count(cursor, column) for column in TARGET_COLUMNS}

        print(f"База данных: {db_path}")
        print(f"Совпадений по ключам найдено: {candidate_count}")
        for column_name, count in before_counts.items():
            print(f'До обновления заполнено "{column_name}": {count}')

        update_sql = f'''
        UPDATE "{TARGET_TABLE}" AS wc
        SET
            "{TARGET_COLUMNS[0]}" = (
                SELECT onk."{SOURCE_VALUE_COLUMN}"
                FROM "{SOURCE_TABLE}" AS onk
                WHERE {build_match_condition()}
                LIMIT 1
            ),
            "{TARGET_COLUMNS[1]}" = (
                SELECT onk."{SOURCE_VALUE_COLUMN}"
                FROM "{SOURCE_TABLE}" AS onk
                WHERE {build_match_condition()}
                LIMIT 1
            )
        WHERE EXISTS (
            SELECT 1
            FROM "{SOURCE_TABLE}" AS onk
            WHERE {build_match_condition()}
        )
        '''

        before_changes = conn.total_changes
        cursor.execute(update_sql)
        changed_rows = conn.total_changes - before_changes
        conn.commit()

        after_counts = {column: get_filled_count(cursor, column) for column in TARGET_COLUMNS}

        print(f"Изменено строк: {changed_rows}")
        for column_name in TARGET_COLUMNS:
            print(
                f'После обновления заполнено "{column_name}": {after_counts[column_name]} '
                f'(было {before_counts[column_name]})'
            )

        print("✅ Обновление wl_china из основнаяНК завершено")
    finally:
        conn.close()


def main() -> int:
    try:
        update_wl_china_from_osnovnaya_nk()
    except Exception as exc:
        print(f"❌ Ошибка обновления: {exc}")
        return 1
    return 0


def run_script():
    """Функция для запуска через веб-интерфейс."""
    return main()


if __name__ == "__main__":
    raise SystemExit(main())
