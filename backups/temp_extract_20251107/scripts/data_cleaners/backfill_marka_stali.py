#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Заполняет поле "Марка стали" в weld_repair_log из wl_china.Базовый_материал_1
по ключам: Чертеж = Номер_чертежа и № стыка = _Номер_сварного_шва_без_S_F_
"""

import sqlite3
from pathlib import Path

def main():
    db_path = Path(__file__).resolve().parents[2] / 'database' / 'BD_Kingisepp' / 'M_Kran_Kingesepp.db'
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    cur.execute('SELECT COUNT(*) FROM weld_repair_log WHERE "Марка стали" IS NULL OR TRIM("Марка стали") = ""')
    before = cur.fetchone()[0]

    update_sql = '''
    UPDATE weld_repair_log
    SET "Марка стали" = (
      SELECT wc."Базовый_материал_1"
      FROM logs_lnk l
      JOIN wl_china wc 
        ON wc."Номер_чертежа" = l."Чертеж"
       AND CAST(wc."_Номер_сварного_шва_без_S_F_" AS TEXT) = CAST(l."_Номер_сварного_шва_без_S_F_" AS TEXT)
      WHERE l.app_row_id = weld_repair_log.app_row_id
      LIMIT 1
    )
    WHERE ("Марка стали" IS NULL OR TRIM("Марка стали") = "");
    '''
    cur.execute(update_sql)
    changed = conn.total_changes
    conn.commit()

    cur.execute('SELECT COUNT(*) FROM weld_repair_log WHERE "Марка стали" IS NULL OR TRIM("Марка стали") = ""')
    after = cur.fetchone()[0]

    print(f"До обновления пустых: {before}")
    print(f"Изменено строк: {changed}")
    print(f"После обновления пустых: {after}")

    conn.close()

if __name__ == '__main__':
    main()


