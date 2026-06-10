#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Очистка опечаток в столбцах O, P и Q до нормализации имён (лист «ЖСР»).

Справочник: config/wl_report_smr_удалить_опечатки.xlsx
  — столбец A: ФИО для очистки ячеек в столбце Q;
  — столбец B: значения для очистки ячеек в столбцах O и P;
  — строка 1 в каждом столбце: пояснение (пропускается).

В O и P дополнительно: серийные номера дат Excel (35000–65000), например 45847.
Строки отчёта не удаляются — только значения в найденных ячейках.
"""

import os
import re
import sys

import pandas as pd

# Столбцы Excel: O=14, P=15, Q=16 (A=0)
COLUMN_O_INDEX = 14
COLUMN_P_INDEX = 15
COLUMN_Q_INDEX = 16

CONFIG_FILENAME = "wl_report_smr_удалить_опечатки.xlsx"

EXCEL_SERIAL_MIN = 35000
EXCEL_SERIAL_MAX = 65000


def _get_project_root():
    try:
        from ..utilities.path_utils import get_project_root
        return get_project_root()
    except ImportError:
        utilities_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "utilities"
        )
        if utilities_dir not in sys.path:
            sys.path.insert(0, utilities_dir)
        from path_utils import get_project_root
        return get_project_root()


def resolve_typos_removal_file_path(project_root=None):
    env_path = os.environ.get("WL_REPORT_SMR_TYPOS_REMOVAL_FILE")
    if env_path:
        return os.path.abspath(env_path)

    root = project_root or _get_project_root()
    return os.path.join(root, "config", CONFIG_FILENAME)


def _is_instruction_row(value, column_letter):
    if pd.isna(value):
        return True
    text = str(value).strip().lower()
    if not text:
        return True
    col = column_letter.lower()
    if col == "q":
        return ("столбце" in text and "q" in text) or ("удалить" in text and "фио" in text)
    if col in ("p", "o", "op"):
        return ("столбце" in text and col in text) or (
            "удалить" in text and col in text
        ) or ("столбец" in text and col in text)
    return False


def _normalize_cell_token(value):
    if pd.isna(value):
        return None
    if isinstance(value, bool):
        return str(value).strip()
    num = pd.to_numeric(value, errors="coerce")
    if pd.notna(num) and float(num) == int(float(num)):
        return str(int(float(num)))
    text = str(value).strip()
    if re.fullmatch(r"\d+\.0+", text):
        return text.split(".")[0]
    return text


def _is_excel_date_serial(value):
    num = pd.to_numeric(value, errors="coerce")
    if pd.isna(num):
        return False
    n = float(num)
    if n != int(n):
        return False
    return EXCEL_SERIAL_MIN <= n <= EXCEL_SERIAL_MAX


def load_removal_lists(file_path, logger=None):
    """
    Returns:
        tuple[set[str], set[str]]: (fio_for_q, values_for_o_and_p)
    """
    empty = (set(), set())

    if not os.path.exists(file_path):
        msg = (
            f"Справочник исключений не найден: {file_path}. "
            f"Добавьте {CONFIG_FILENAME} в папку config проекта."
        )
        if logger:
            logger.warning(msg)
        else:
            print(f"ПРЕДУПРЕЖДЕНИЕ: {msg}")
        return empty

    df = pd.read_excel(file_path, header=None)
    if df.empty:
        if logger:
            logger.warning("Справочник исключений пуст")
        return empty

    fio_for_q = set()
    values_for_op = set()

    if df.shape[1] >= 1:
        for raw in df.iloc[:, 0]:
            if _is_instruction_row(raw, "q"):
                continue
            name = str(raw).strip()
            if name:
                fio_for_q.add(name)

    if df.shape[1] >= 2:
        for raw in df.iloc[:, 1]:
            if _is_instruction_row(raw, "p"):
                continue
            token = _normalize_cell_token(raw)
            if token:
                values_for_op.add(token)

    if logger:
        logger.info(f"Справочник: {file_path}")
        logger.info(f"  Столбец Q: {len(fio_for_q)} ФИО для очистки")
        for i, name in enumerate(sorted(fio_for_q), 1):
            logger.info(f"    {i}. {name}")
        logger.info(f"  Столбцы O и P: {len(values_for_op)} значений для очистки")
        for i, val in enumerate(sorted(values_for_op), 1):
            logger.info(f"    {i}. {val}")

    return fio_for_q, values_for_op


def load_fio_removal_list(file_path, logger=None):
    fio_for_q, _ = load_removal_lists(file_path, logger)
    return fio_for_q


def _log_cleared_values(series, mask_clear, column_label, col_name, logger):
    cleared = int(mask_clear.sum())
    if logger:
        logger.info(
            f"Столбец {column_label} ({col_name!r}): очищено {cleared} ячеек"
        )
    if cleared and logger:
        tokens = series.map(_normalize_cell_token)
        counts = tokens[mask_clear].value_counts()
        for val, cnt in counts.items():
            logger.info(f"  — {val}: {cnt}")


def clear_column_q_values(df, fio_to_clear, logger=None):
    """Очищает ячейки столбца Q, где значение совпадает с ФИО из справочника."""
    if not fio_to_clear:
        return df
    if len(df.columns) <= COLUMN_Q_INDEX:
        if logger:
            logger.warning(
                f"В отчёте меньше {COLUMN_Q_INDEX + 1} столбцов — очистка Q пропущена"
            )
        return df

    df = df.copy()
    col_name = df.columns[COLUMN_Q_INDEX]
    series = df.iloc[:, COLUMN_Q_INDEX]
    normalized = series.map(
        lambda v: str(v).strip()
        if pd.notna(v) and str(v).strip() not in ("", "nan")
        else None
    )
    mask_clear = normalized.isin(fio_to_clear)
    if mask_clear.any():
        df.iloc[mask_clear.values, COLUMN_Q_INDEX] = None
    _log_cleared_values(series, mask_clear, "Q", col_name, logger)
    return df


def clear_column_op_values(
    df,
    column_index,
    column_label,
    values_to_clear,
    clear_excel_serials=True,
    logger=None,
):
    """
    Очищает ячейки столбца O или P:
      — значения из справочника (столбец B);
      — серийные номера дат Excel (если clear_excel_serials=True).
    """
    if len(df.columns) <= column_index:
        if logger:
            logger.warning(
                f"В отчёте меньше {column_index + 1} столбцов — "
                f"очистка {column_label} пропущена"
            )
        return df

    df = df.copy()
    col_name = df.columns[column_index]
    series = df.iloc[:, column_index]
    tokens = series.map(_normalize_cell_token)

    mask_clear = pd.Series(False, index=df.index)
    if values_to_clear:
        mask_clear |= tokens.isin(values_to_clear)
    if clear_excel_serials:
        mask_clear |= series.map(_is_excel_date_serial)

    if not mask_clear.any():
        if logger:
            logger.info(
                f"Столбец {column_label} ({col_name!r}): ячеек для очистки не найдено"
            )
        return df

    df.iloc[mask_clear.values, column_index] = None
    _log_cleared_values(series, mask_clear, column_label, col_name, logger)
    return df


def clear_column_o_values(df, values_to_clear, clear_excel_serials=True, logger=None):
    return clear_column_op_values(
        df,
        COLUMN_O_INDEX,
        "O",
        values_to_clear,
        clear_excel_serials=clear_excel_serials,
        logger=logger,
    )


def clear_column_p_values(df, values_to_clear, clear_excel_serials=True, logger=None):
    return clear_column_op_values(
        df,
        COLUMN_P_INDEX,
        "P",
        values_to_clear,
        clear_excel_serials=clear_excel_serials,
        logger=logger,
    )


def filter_rows_by_column_q(df, fio_to_remove, logger=None):
    return clear_column_q_values(df, fio_to_remove, logger)


def filter_rows_by_column_p(df, values_to_remove, remove_excel_serials=True, logger=None):
    return clear_column_p_values(
        df, values_to_remove, clear_excel_serials=remove_excel_serials, logger=logger
    )


def apply_pre_normalization_filters(df, project_root=None, logger=None):
    """Очистка опечаток в столбцах O, P и Q перед clean_column_name."""
    config_path = resolve_typos_removal_file_path(project_root)
    fio_for_q, values_for_op = load_removal_lists(config_path, logger)

    if logger:
        logger.info("Очистка опечаток в столбцах O, P и Q (строки сохраняются)...")

    if fio_for_q:
        df = clear_column_q_values(df, fio_for_q, logger)

    df = clear_column_o_values(
        df, values_for_op, clear_excel_serials=True, logger=logger
    )
    df = clear_column_p_values(
        df, values_for_op, clear_excel_serials=True, logger=logger
    )

    return df
