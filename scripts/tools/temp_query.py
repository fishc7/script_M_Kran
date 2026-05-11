#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
from pathlib import Path


project_root = Path(__file__).resolve().parents[2]
db_path = project_root / "database" / "BD_Kingisepp" / "M_Kran_Kingesepp.db"

conn = sqlite3.connect(str(db_path))
conn.row_factory = sqlite3.Row
cur = conn.cursor()
row = cur.execute(
    """
    SELECT 
        COUNT(DISTINCT CASE WHEN TRIM(COALESCE(wc.Результаты_АКТ_ВИК, '')) = 'Заказ отправлен' THEN wc.Номер_сварного_шва END) as заявлен_вик,
        COUNT(DISTINCT CASE WHEN TRIM(COALESCE(wc.Результаты_АКТ_ВИК, '')) = 'годен' THEN wc.Номер_сварного_шва END) as годен_вик,
        COUNT(DISTINCT CASE WHEN TRIM(COALESCE(wc.Результаты_АКТ_ВИК, '')) = 'Не годен' THEN wc.Номер_сварного_шва END) as не_годен_вик,
        COUNT(DISTINCT CASE WHEN wc.Результаты_АКТ_ВИК IS NULL OR TRIM(COALESCE(wc.Результаты_АКТ_ВИК, '')) = '' OR LOWER(TRIM(COALESCE(wc.Результаты_АКТ_ВИК, ''))) = 'none' THEN wc.Номер_сварного_шва END) as не_подан_вик
    FROM wl_china wc
    WHERE wc.Номер_чертежа = 'GCC-NAG-DDD-12460-12-1500-TK-ISO-00052'
      AND wc.Дата_сварки IS NOT NULL AND TRIM(wc.Дата_сварки) <> ''
      AND ((wc.Клеймо_сварщика_корневой_слой IS NOT NULL AND wc.Клеймо_сварщика_корневой_слой <> '')
           OR (wc.Клеймо_сварщика_заполнение_облицовка IS NOT NULL AND wc.Клеймо_сварщика_заполнение_облицовка <> ''))
      AND wc.Номер_сварного_шва IS NOT NULL
      AND (wc.Клеймо_сварщика_корневой_слой = '2Z08' OR wc.Клеймо_сварщика_заполнение_облицовка = '2Z08')
      AND (wc.Метод_сварки_корневой_слой = 'SMAW' OR wc.Метод_сварки_заполнение_облицовка = 'SMAW')
      AND wc.Тип_соединения_российский_стандарт LIKE '%У17%'
"""
).fetchone()
print(dict(row))
