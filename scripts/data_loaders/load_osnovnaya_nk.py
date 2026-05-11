import os
import sqlite3
import sys

import pandas as pd

try:
    from ..utilities.path_utils import get_database_path, get_excel_paths
except ImportError:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    utilities_dir = os.path.join(os.path.dirname(current_dir), "utilities")
    if utilities_dir not in sys.path:
        sys.path.insert(0, utilities_dir)
    from path_utils import get_database_path, get_excel_paths


TABLE_NAME = "основнаяНК"
SHEET_NAME = "основнаяНК"
DEFAULT_FILE_NAME = "LST-NK.xlsx"

# Сопоставление Excel -> SQLite по фактической схеме таблицы.
COLUMN_RENAMES = {
    "Давление, МПа / Pressure MPa .1": "Давление, МПа / Pressure MPa _1",
}

# Поля, которые есть в Excel, но отсутствуют в текущей таблице SQLite.
IGNORED_SOURCE_COLUMNS = {
    "код_среды",
}

EXPECTED_COLUMN_TYPES = {
    "титул / title": "TEXT",
}


def _configure_stdout():
    if sys.platform == "win32" and hasattr(sys.stdout, "buffer"):
        import io

        try:
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass


def resolve_excel_path():
    override = (os.environ.get("OSNOVNAYA_NK_FILE") or "").strip()
    if override:
        return os.path.abspath(override)
    return os.path.join(get_excel_paths()["ogs"], DEFAULT_FILE_NAME)


def get_table_columns(conn):
    cursor = conn.cursor()
    cursor.execute(f'PRAGMA table_info("{TABLE_NAME}")')
    columns = [row[1] for row in cursor.fetchall()]
    if not columns:
        raise RuntimeError(f'Таблица "{TABLE_NAME}" не найдена в базе данных')
    return columns


def get_table_schema(conn):
    cursor = conn.cursor()
    cursor.execute(f'PRAGMA table_info("{TABLE_NAME}")')
    rows = cursor.fetchall()
    if not rows:
        raise RuntimeError(f'Таблица "{TABLE_NAME}" не найдена в базе данных')
    return rows


def canonicalize_column_name(name):
    return str(name).replace("\xa0", " ").strip()


def coerce_expected_text_columns(df):
    prepared = df.copy()
    for column, target_type in EXPECTED_COLUMN_TYPES.items():
        if column not in prepared.columns or target_type.upper() != "TEXT":
            continue
        prepared[column] = prepared[column].apply(
            lambda value: None if pd.isna(value) else str(value)
        )
    return prepared


def ensure_table_schema(conn):
    schema = get_table_schema(conn)
    needs_rebuild = False
    for _, name, col_type, _, _, _ in schema:
        expected_type = EXPECTED_COLUMN_TYPES.get(name)
        if expected_type and (col_type or "").upper() != expected_type.upper():
            needs_rebuild = True
            break

    if not needs_rebuild:
        return

    print("\nИсправление схемы таблицы под ожидаемые типы...")
    temp_table = f"{TABLE_NAME}__schema_fix"
    create_parts = []
    column_names = []

    for _, name, col_type, notnull, default_value, pk in schema:
        final_type = EXPECTED_COLUMN_TYPES.get(name, col_type or "TEXT")
        part = f'"{name}" {final_type}'
        if notnull:
            part += " NOT NULL"
        if default_value is not None:
            part += f" DEFAULT {default_value}"
        if pk:
            part += " PRIMARY KEY"
        create_parts.append(part)
        column_names.append(name)

    insert_columns_sql = ", ".join(f'"{name}"' for name in column_names)
    select_columns_sql = ", ".join(
        f'CAST("{name}" AS TEXT) AS "{name}"'
        if EXPECTED_COLUMN_TYPES.get(name, "").upper() == "TEXT"
        else f'"{name}"'
        for name in column_names
    )

    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
    cursor.execute(f'CREATE TABLE "{temp_table}" ({", ".join(create_parts)})')
    cursor.execute(
        f'INSERT INTO "{temp_table}" ({insert_columns_sql}) '
        f'SELECT {select_columns_sql} FROM "{TABLE_NAME}"'
    )
    cursor.execute(f'DROP TABLE "{TABLE_NAME}"')
    cursor.execute(f'ALTER TABLE "{temp_table}" RENAME TO "{TABLE_NAME}"')
    conn.commit()


def normalize_dataframe(df):
    df = df.copy()
    df.columns = [str(col) for col in df.columns]
    df = df.rename(columns=COLUMN_RENAMES)
    df = df.drop(columns=[col for col in IGNORED_SOURCE_COLUMNS if col in df.columns], errors="ignore")
    df = df.dropna(how="all").reset_index(drop=True)
    df = coerce_expected_text_columns(df)
    return df


def build_column_mapping(df_columns, db_columns):
    matched = []
    missing_in_excel = []
    extra_in_excel = []

    df_lookup = {}
    for column in df_columns:
        df_lookup.setdefault(canonicalize_column_name(column), column)

    db_lookup = {}
    for column in db_columns:
        db_lookup.setdefault(canonicalize_column_name(column), column)

    rename_to_db = {}

    for db_column in db_columns:
        key = canonicalize_column_name(db_column)
        source_column = df_lookup.get(key)
        if source_column is None:
            missing_in_excel.append(db_column)
            continue
        matched.append((source_column, db_column))
        rename_to_db[source_column] = db_column

    for column in df_columns:
        if canonicalize_column_name(column) not in db_lookup:
            extra_in_excel.append(column)

    return matched, missing_in_excel, extra_in_excel, rename_to_db


def prepare_rows(df, db_columns, rename_to_db):
    prepared = df.copy()
    prepared = prepared.rename(columns=rename_to_db)
    prepared = prepared.where(pd.notna(prepared), None)
    return prepared[db_columns].values.tolist()


def load_osnovnaya_nk(excel_path=None, db_path=None, dry_run=False):
    excel_path = excel_path or resolve_excel_path()
    db_path = db_path or get_database_path()

    print(f"Excel файл: {excel_path}")
    print(f"База данных: {db_path}")

    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"Excel файл не найден: {excel_path}")
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"База данных не найдена: {db_path}")

    df = pd.read_excel(excel_path, sheet_name=SHEET_NAME)
    df = normalize_dataframe(df)

    conn = sqlite3.connect(db_path)
    try:
        ensure_table_schema(conn)
        db_columns = get_table_columns(conn)
        matched, missing_in_excel, extra_in_excel, rename_to_db = build_column_mapping(
            df.columns.tolist(), db_columns
        )

        print("\nСопоставление столбцов Excel -> SQLite:")
        for source_name, target_name in matched:
            print(f"  {source_name} -> {target_name}")

        if COLUMN_RENAMES:
            print("\nПереименованные столбцы:")
            for source_name, target_name in COLUMN_RENAMES.items():
                print(f"  {source_name} -> {target_name}")

        if extra_in_excel:
            print("\nЛишние столбцы Excel, которые будут пропущены:")
            for column in extra_in_excel:
                print(f"  {column}")

        if missing_in_excel:
            raise RuntimeError(
                "В Excel отсутствуют столбцы, обязательные для загрузки в SQLite: "
                + ", ".join(missing_in_excel)
            )

        rows = prepare_rows(df, db_columns, rename_to_db)
        if dry_run:
            print(f"\nDRY RUN: таблица \"{TABLE_NAME}\" не изменялась")
            print(f"Строк будет загружено: {len(rows)}")
            return True

        cursor = conn.cursor()
        print(f'\nОчистка таблицы "{TABLE_NAME}"...')
        cursor.execute(f'DELETE FROM "{TABLE_NAME}"')

        placeholders = ", ".join("?" for _ in db_columns)
        columns_sql = ", ".join(f'"{column}"' for column in db_columns)
        insert_sql = f'INSERT INTO "{TABLE_NAME}" ({columns_sql}) VALUES ({placeholders})'

        print(f"Загрузка строк: {len(rows)}")
        cursor.executemany(insert_sql, rows)
        conn.commit()

        print(f'✅ Таблица "{TABLE_NAME}" успешно обновлена. Загружено строк: {len(rows)}')
        return True
    finally:
        conn.close()


def run_script():
    _configure_stdout()
    load_osnovnaya_nk()


def main():
    _configure_stdout()
    try:
        dry_run = "--dry-run" in sys.argv
        ok = load_osnovnaya_nk(dry_run=dry_run)
    except Exception as exc:
        print(f"❌ Ошибка загрузки: {exc}")
        return 1
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
