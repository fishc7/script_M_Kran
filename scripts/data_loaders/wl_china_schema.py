# -*- coding: utf-8 -*-
"""
Фиксированная схема wl_china и сопоставление имён столбцов Excel → БД.
Таблица не пересоздаётся из заголовков файла: при загрузке только DELETE + INSERT,
при необходимости ALTER TABLE ADD COLUMN для недостающих канонических полей.
"""

from __future__ import annotations

import sqlite3

import pandas as pd

# Все поля данных (без id и без Дата_загрузки). Порядок = порядок INSERT.
_RAW_ORDER = (
    "Секция_титул",
    "Установка",
    "блок_N",
    "Номер_рабочей_зоны",
    "Номер_чертежа",
    "N_Линии",
    "N_Линии_по_старой_ревизии_РД",
    "Категория_трубопровода",
    "Группа_среды",
    "N_трубного_узла_спула",
    "Номер_сварного_шва",
    "Монтаж_F_Цех_S",
    "Проектный_контроля",
    "Вид_контроля",
    "Результат_контроля",
    "проектный_объем_РК",
    "UT",
    "проектный_объем_ПВК",
    "Базовый_материал_1",
    "Базовый_материал_2",
    "размер",
    "Наружный_диаметр_1",
    "Толщина_1",
    "Наружный_диаметр_2",
    "Толщина_2",
    "Тип_соединения_российский_стандарт",
    "Тип_сварного_шва",
    "Номер_WPS",
    "Метод_сварки_корневой_слой",
    "Метод_сварки_заполнение_облицовка",
    "Дата_сварки",
    "Температура_окружающей_среды_при_сварке",
    "Деталь_1",
    "Деталь_2",
    "Клеймо_сварщика_корневой_слой",
    "Клеймо_сварщика_заполнение_облицовка",
    "Проверка_сварщика",
    "Необходимость_ТО",
    "Дата_ТО",
    "N_протокола_ТО",
    "Результаты_ТО",
    "Дата_АКТ_ВИК",
    "АКТ_ВИК_N",
    "Результаты_АКТ_ВИК",
    "Дата_Заключения_РК",
    "Заключение_РК_N",
    "Результаты_Заключения_РК",
    "Результаты_Заключения_PT",
    "Результаты_Заключения_Стилоскопирование",
    "Результаты_Заключения_МПД",
    "Дефект",
    "Дата_Заключения_УЗК",
    "Заключение_УЗК_N",
    "Результаты_Заключения_УЗК",
    "Сварочный_материал_корневой_слой",
    "Сварочный_материал_Заполнение_облицовка",
    "Состояние_сварного_шва",
    "Кол_во_РГК_стыка",
    "Расширенные_сварные_швы",
    "Заводское_изготовление",
    "Ремонтный_сварщик",
    "Дата_контроля",
    "Статус",
    "Заводское_изготовление_2",
    "Ремонтный_сварщик_2",
    "Дата_контроля_2",
    "Статус_2",
    "Заводское_изготовление_3",
    "Ремонтный_сварщик_3",
    "Дата_контроля_3",
    "Статус_3",
    "Заводское_изготовление_4",
    "Замечание",
    "_Номер_заключения_ВИК",
    "_Номер_заключения_РК",
    "_Номер_сварного_шва_без_S_F_",
)

CANONICAL_WL_CHINA_DATA_COLUMNS: tuple[str, ...] = tuple(dict.fromkeys(_RAW_ORDER))
CANONICAL_SET = frozenset(CANONICAL_WL_CHINA_DATA_COLUMNS)

# Имя столбца в Excel (после clean_column_name / как в DataFrame) → имя в БД
EXCEL_ALIAS_TO_CANONICAL: dict[str, str] = {
    # Старые двухстрочные заголовки WELDLOG после clean превращаются в префикс VT_/RT_
    "VT_АКТ_ВИК_": "АКТ_ВИК_N",
    "VT_АКТ_ВИК": "АКТ_ВИК_N",
    "RT_Заключение_РК_": "Заключение_РК_N",
    "RT_Заключение_РК": "Заключение_РК_N",
    # Короткие варианты
    "АКТ_ВИК": "АКТ_ВИК_N",
    "Заключение_РК": "Заключение_РК_N",
}


def _resolve_excel_column(excel_col: str) -> str | None:
    if excel_col in CANONICAL_SET:
        return excel_col
    return EXCEL_ALIAS_TO_CANONICAL.get(excel_col)


def _cell_empty(val) -> bool:
    if val is None:
        return True
    try:
        if pd.isna(val):
            return True
    except TypeError:
        pass
    s = str(val).strip()
    return s in ("", "nan", "None")


def project_dataframe_to_canonical(df_wide: pd.DataFrame, log_print=print) -> pd.DataFrame:
    """
    Строит DataFrame только с каноническими столбцами: сопоставление по имени и по ALIAS.
    Несколько столбцов Excel, попадающих в одно поле БД: заполняется первое непустое.
    """
    out = pd.DataFrame({c: pd.Series([None] * len(df_wide), dtype=object) for c in CANONICAL_WL_CHINA_DATA_COLUMNS}, index=df_wide.index)
    unknown: list[str] = []

    for excel_col in df_wide.columns:
        target = _resolve_excel_column(str(excel_col))
        if target is None:
            unknown.append(str(excel_col))
            continue
        src = df_wide[excel_col]
        cur = out[target]
        fill_mask = cur.map(_cell_empty) & ~src.map(_cell_empty)
        out.loc[fill_mask, target] = src[fill_mask]

    if unknown and log_print:
        sample = unknown[:25]
        log_print(
            f"ℹ️  Столбцы Excel без сопоставления с wl_china (игнорируются), всего {len(unknown)}: "
            f"{sample}{'…' if len(unknown) > len(sample) else ''}"
        )

    return out


def fill_legacy_document_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Если в файле не было Заключение_РК_N / АКТ_ВИК_N, подставляем извлечённые _Номер_*."""
    if "Заключение_РК_N" in df.columns and "_Номер_заключения_РК" in df.columns:
        m = df["Заключение_РК_N"].map(_cell_empty) & ~df["_Номер_заключения_РК"].map(_cell_empty)
        df.loc[m, "Заключение_РК_N"] = df.loc[m, "_Номер_заключения_РК"]
    if "АКТ_ВИК_N" in df.columns and "_Номер_заключения_ВИК" in df.columns:
        m = df["АКТ_ВИК_N"].map(_cell_empty) & ~df["_Номер_заключения_ВИК"].map(_cell_empty)
        df.loc[m, "АКТ_ВИК_N"] = df.loc[m, "_Номер_заключения_ВИК"]
    return df


def ensure_wl_china_table(cursor, log_print=print) -> None:
    """
    Гарантирует наличие таблицы wl_china с каноническими столбцами, очищает данные (DELETE).
    Лишние столбцы, которых нет в каноне, в таблице не трогаем (остаются пустыми для новых строк).
    """
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='wl_china'")
    exists = cursor.fetchone() is not None

    if not exists:
        cols_sql = ", ".join(f'"{c}" TEXT' for c in CANONICAL_WL_CHINA_DATA_COLUMNS)
        cursor.execute(
            f"""
            CREATE TABLE wl_china (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                {cols_sql},
                "Дата_загрузки" TEXT
            )
            """
        )
        if log_print:
            log_print("✅ Создана таблица wl_china с фиксированной схемой")
    else:
        cursor.execute("PRAGMA table_info(wl_china)")
        have = {row[1] for row in cursor.fetchall()}
        for c in CANONICAL_WL_CHINA_DATA_COLUMNS:
            if c not in have:
                cursor.execute(f'ALTER TABLE wl_china ADD COLUMN "{c}" TEXT')
                if log_print:
                    log_print(f'✅ Добавлен отсутствующий столбец wl_china."{c}"')
        if "Дата_загрузки" not in have:
            cursor.execute('ALTER TABLE wl_china ADD COLUMN "Дата_загрузки" TEXT')
            if log_print:
                log_print('✅ Добавлен столбец wl_china."Дата_загрузки"')

        if "_Номер_сварного_шва" in have:
            try:
                cursor.execute('ALTER TABLE wl_china DROP COLUMN "_Номер_сварного_шва"')
                if log_print:
                    log_print('🗑️ Удалён столбец wl_china."_Номер_сварного_шва" (не используется)')
            except sqlite3.OperationalError as e:
                if log_print:
                    log_print(
                        f'⚠️ Не удалось DROP COLUMN "_Номер_сварного_шва" '
                        f'(нужен SQLite 3.35+): {e}'
                    )

    cursor.execute("DELETE FROM wl_china")
    if log_print:
        log_print("🗑️  Данные wl_china очищены (DELETE), структура таблицы сохранена")
