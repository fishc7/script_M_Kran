import sqlite3
import os

DB_PATH = r"D:\МК_Кран\script_M_Kran\database\BD_Kingisepp\M_Kran_Kingesepp.db"

def main():
    if not os.path.exists(DB_PATH):
        print("DB not found:", DB_PATH)
        return 1
    con = sqlite3.connect(DB_PATH)
    c = con.cursor()

    # 1) Reference fields presence
    ref_cols = [
        'No TEST-PACK',
        '№ ИЧ',
        'Категория и группа трубопровода по ГОСТ 32569-2013 / Pipeline Group and category according to GOST 32569-2013) ',
        'Материал / Material  ',
        'РК (Радиографический контроль) / RT',
        'СТ (Стилоскоп) Стилоскопирование / PMI',
        'МПК (Магнитопорошковый контроль) / MT',
        'ПВК (Контроль проникающими веществами) / PT',
    ]
    c.execute('SELECT COUNT(*) FROM сварено_сварщиком')
    total = c.fetchone()[0]
    print('Total rows:', total)
    for col in ref_cols:
        c.execute(f'SELECT COUNT(*) FROM сварено_сварщиком WHERE [{col}] IS NOT NULL AND TRIM([{col}]) <> ""')
        print(f'{col}:', c.fetchone()[0])

    # 2) PT formula check (plan minus approved, clamped to 0)
    q_pt = (
        "SELECT COUNT(*) FROM сварено_сварщиком ss WHERE "
        "COALESCE(ПВК,0) != MAX(CAST((((CASE WHEN (ss.Всего_сваренно_сварщиком - ss.не_годен_рк) > 0 "
        "THEN (ss.Всего_сваренно_сварщиком - ss.не_годен_рк) ELSE 0 END) * COALESCE(("
        "SELECT CAST(REPLACE(REPLACE(он.'ПВК (Контроль проникающими веществами) / PT', '%', ''), ',', '.') AS REAL) "
        "FROM основнаяНК он WHERE он.'Номер линии / Line No' = ss._Линия LIMIT 1), 0)) + 99) / 100 AS INT) - COALESCE(ss.годен_pt,0), 0)"
    )
    c.execute(q_pt)
    print('PT mismatches:', c.fetchone()[0])

    # 3) MT formula check (plan minus approved, clamped to 0)
    q_mt = (
        "SELECT COUNT(*) FROM сварено_сварщиком ss WHERE "
        "COALESCE(МПД,0) != MAX(CAST((((CASE WHEN (ss.Всего_сваренно_сварщиком - ss.не_годен_рк) > 0 "
        "THEN (ss.Всего_сваренно_сварщиком - ss.не_годен_рк) ELSE 0 END) * COALESCE(("
        "SELECT он.'МПК (Магнитопорошковый контроль) / MT' FROM основнаяНК он "
        "WHERE он.'Номер линии / Line No' = ss._Линия LIMIT 1), 0)) + 99) / 100 AS INT) - COALESCE(ss.годен_мпд,0), 0)"
    )
    c.execute(q_mt)
    print('MT mismatches:', c.fetchone()[0])

    # 4) PMI formula check (plan or * then minus approved, clamped to 0)
    q_pmi = (
        "SELECT COUNT(*) FROM сварено_сварщиком ss WHERE COALESCE([Стило(PMI)],0) != "
        "MAX((CASE WHEN EXISTS (SELECT 1 FROM основнаяНК он WHERE он.'Номер линии / Line No' = ss._Линия "
        "AND он.'СТ (Стилоскоп) Стилоскопирование / PMI' LIKE '%*%') THEN 2 ELSE "
        "CAST((((CASE WHEN (ss.Всего_сваренно_сварщиком - ss.не_годен_рк) > 0 THEN (ss.Всего_сваренно_сварщиком - ss.не_годен_рк) ELSE 0 END) "
        "* COALESCE((SELECT CAST(REPLACE(REPLACE(он.'СТ (Стилоскоп) Стилоскопирование / PMI', '%', ''), ',', '.') AS REAL) FROM основнаяНК он "
        "WHERE он.'Номер линии / Line No' = ss._Линия LIMIT 1), 0)) + 99) / 100 AS INT) END) - COALESCE(ss.годен_стилоскопирование,0), 0)"
    )
    try:
        c.execute(q_pmi)
        print('PMI mismatches:', c.fetchone()[0])
    except Exception as e:
        print('PMI check error:', e)

    # 5) not_submitted checks
    q_not_pt = (
        "SELECT COUNT(*) FROM сварено_сварщиком WHERE COALESCE(не_подан_pt,0) != "
        "CASE WHEN (COALESCE(ПВК,0)-COALESCE(заявлен_pt,0)) < 0 THEN 0 ELSE (COALESCE(ПВК,0)-COALESCE(заявлен_pt,0)) END"
    )
    c.execute(q_not_pt)
    print('not_submitted_pt mismatches:', c.fetchone()[0])

    # not_submitted_rk check
    q_not_rk = (
        "SELECT COUNT(*) FROM сварено_сварщиком WHERE COALESCE(не_подан_рк,0) != "
        "CASE WHEN (COALESCE(РК,0)-COALESCE(заявлен_рк,0)) < 0 THEN 0 ELSE (COALESCE(РК,0)-COALESCE(заявлен_рк,0)) END"
    )
    c.execute(q_not_rk)
    print('not_submitted_rk mismatches:', c.fetchone()[0])

    q_not_pmi = (
        "SELECT COUNT(*) FROM сварено_сварщиком WHERE COALESCE(не_подан_стилоскопирование,0) != "
        "CASE WHEN (COALESCE([Стило(PMI)],0)-COALESCE(заявлен_стилоскопирование,0)) < 0 THEN 0 ELSE (COALESCE([Стило(PMI)],0)-COALESCE(заявлен_стилоскопирование,0)) END"
    )

    q_not_mpd = (
        "SELECT COUNT(*) FROM сварено_сварщиком WHERE COALESCE(не_подан_мпд,0) != "
        "CASE WHEN (COALESCE(МПД,0)-COALESCE(заявлен_мпд,0)) < 0 THEN 0 ELSE (COALESCE(МПД,0)-COALESCE(заявлен_мпд,0)) END"
    )
    c.execute(q_not_pmi)
    print('not_submitted_pmi mismatches:', c.fetchone()[0])
    c.execute(q_not_mpd)
    print('not_submitted_mpd mismatches:', c.fetchone()[0])

    con.close()
    return 0

if __name__ == '__main__':
    raise SystemExit(main())


