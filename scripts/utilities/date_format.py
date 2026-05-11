# -*- coding: utf-8 -*-
"""
Единый формат дат для logs_lnk и смежных загрузок: ГГГГ-ММ-ДД (например 2024-11-21).
"""

import re
from datetime import datetime, timedelta

import pandas as pd


def normalize_logs_lnk_date(val):
    """
    Приводит значение ячейки к строке даты ГГГГ-ММ-ДД или None.
    Поддерживает datetime/Timestamp, серийный номер Excel, ДД.ММ.ГГГГ, ГГГГ-ММ-ДД.
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, pd.Timestamp):
        return val.strftime('%Y-%m-%d')
    if hasattr(val, 'strftime') and callable(getattr(val, 'strftime')):
        try:
            return val.strftime('%Y-%m-%d')
        except Exception:
            pass
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        if 2000 <= val <= 80000:
            try:
                base = datetime(1899, 12, 30)
                dt = base + timedelta(days=float(val))
                return dt.strftime('%Y-%m-%d')
            except (OverflowError, ValueError):
                pass
    s = str(val).strip()
    if not s or s.lower() in ('nan', 'none', 'nat'):
        return None
    if ' ' in s:
        s = s.split()[0]
    m = re.fullmatch(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', s)
    if m:
        d, mo, y = m.groups()
        return f'{y}-{int(mo):02d}-{int(d):02d}'
    m = re.fullmatch(r'(\d{4})-(\d{2})-(\d{2})', s)
    if m:
        y, mo, d = m.groups()
        return f'{y}-{mo}-{d}'
    m = re.fullmatch(r'(\d{4})/(\d{1,2})/(\d{1,2})', s)
    if m:
        y, mo, d = m.groups()
        return f'{y}-{int(mo):02d}-{int(d):02d}'
    return s
