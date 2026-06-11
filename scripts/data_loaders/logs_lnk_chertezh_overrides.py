#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Подстановка столбца «Чертеж» по app_row_id перед загрузкой журнала НГС в logs_lnk.

Справочник: config/logs_lnk_чертеж_по_app_row_id.xlsx
  — столбец A: app_row_id;
  — столбец B: целевое значение «Чертеж»;
  — строка 1: пояснение (пропускается).
"""

import os
import re
import sys

import pandas as pd

CONFIG_FILENAME = "logs_lnk_чертеж_по_app_row_id.xlsx"


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


def resolve_chertezh_overrides_file_path(project_root=None):
    env_path = os.environ.get("LOGS_LNK_CHERTEZH_OVERRIDES_FILE")
    if env_path:
        return os.path.abspath(env_path)
    root = project_root or _get_project_root()
    return os.path.join(root, "config", CONFIG_FILENAME)


def _is_instruction_row(app_row_id_value):
    if pd.isna(app_row_id_value):
        return True
    text = str(app_row_id_value).strip().lower()
    if not text:
        return True
    return (
        "app_row_id" in text
        or "столбец" in text
        or "чертеж" in text
        or text in ("id", "app row id")
    )


def load_chertezh_overrides(file_path):
    """
    Читает справочник app_row_id → Чертеж.

    Returns:
        dict[str, str]: нормализованный app_row_id → целевой Чертеж
    """
    if not os.path.exists(file_path):
        return {}

    df = pd.read_excel(file_path, header=None)
    if df.empty or df.shape[1] < 2:
        return {}

    mapping = {}
    for _, row in df.iterrows():
        raw_id = row.iloc[0]
        raw_chertezh = row.iloc[1]
        if _is_instruction_row(raw_id):
            continue
        if pd.isna(raw_chertezh):
            continue
        chertezh = str(raw_chertezh).strip()
        if not chertezh:
            continue
        key = _normalize_app_row_id_for_lookup(raw_id)
        if key is None:
            continue
        mapping[key] = chertezh
    return mapping


def _normalize_app_row_id_for_lookup(val):
    """Упрощённая нормализация ключа (совместима с load_lnk_data._norm_app_row_id_key)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, bool):
        return None
    if isinstance(val, int):
        return str(val)
    if isinstance(val, float):
        if val == int(val) and abs(val) < 2**53:
            return str(int(val))
        s = str(val).strip()
        return s if s else None
    s = str(val).strip()
    if not s:
        return None
    if re.fullmatch(r"-?\d+", s):
        try:
            return str(int(s))
        except ValueError:
            return s
    m = re.fullmatch(r"(-?\d+)\.(0+)", s)
    if m:
        return m.group(1)
    return s


def apply_chertezh_overrides_by_app_row_id(df, norm_key=None, file_path=None):
    """
    Заменяет «Чертеж» для строк, чей app_row_id есть в справочнике.

    Args:
        df: DataFrame журнала НГС до вставки в logs_lnk
        norm_key: функция нормализации app_row_id (по умолчанию — встроенная)
        file_path: путь к xlsx; если None — config/logs_lnk_чертеж_по_app_row_id.xlsx

    Returns:
        int: число заменённых строк
    """
    if "app_row_id" not in df.columns or "Чертеж" not in df.columns:
        print(
            "⚠️ Справочник чертежей: столбцы app_row_id или Чертеж отсутствуют — пропуск."
        )
        return 0

    path = file_path or resolve_chertezh_overrides_file_path()
    mapping = load_chertezh_overrides(path)
    if not mapping:
        if not os.path.exists(path):
            print(
                f"ℹ️ Справочник чертежей не найден ({path}). "
                f"Добавьте {CONFIG_FILENAME} в config при необходимости."
            )
        else:
            print(f"ℹ️ Справочник чертежей пуст: {path}")
        return 0

    key_fn = norm_key or _normalize_app_row_id_for_lookup
    df_keys = df["app_row_id"].map(key_fn)
    replaced = 0
    not_found_ids = set(mapping.keys())

    for idx, key in df_keys.items():
        if key is None or key not in mapping:
            continue
        new_val = mapping[key]
        old_val = df.at[idx, "Чертеж"]
        if str(old_val or "") != new_val:
            df.at[idx, "Чертеж"] = new_val
            replaced += 1
        not_found_ids.discard(key)

    print(
        f"9️⃣ Справочник чертежей по app_row_id: {path} — "
        f"заменено {replaced} строк (в справочнике {len(mapping)} id)."
    )
    if not_found_ids:
        sample = ", ".join(sorted(not_found_ids)[:5])
        extra = f" и ещё {len(not_found_ids) - 5}" if len(not_found_ids) > 5 else ""
        print(
            f"   ℹ️ Id из справочника не найдены в выгрузке НГС ({len(not_found_ids)}): "
            f"{sample}{extra}"
        )
    return replaced
