"""
Диагностический скрипт для проверки проблем с заполнением condition_weld из wl_china
Проверяет конкретный чертеж и номер шва
"""
import sqlite3
import sys
import os

# Добавляем путь к корню проекта
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

try:
    from scripts.utilities.db_utils import get_database_connection
except ImportError:
    # Альтернативный способ получения подключения
    def get_database_connection():
        db_path = os.path.join(os.path.dirname(__file__), '..', '..', 'database', 'M_Kran_Kingesepp.db')
        if not os.path.exists(db_path):
            db_path = os.path.join(os.path.dirname(__file__), '..', '..', 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db')
        return sqlite3.connect(db_path, check_same_thread=False)

def check_drawing_and_weld(drawing_number, weld_number):
    """
    Проверяет наличие данных для конкретного чертежа и номера шва
    """
    conn = get_database_connection()
    cursor = conn.cursor()
    
    print("=" * 80)
    print(f"ДИАГНОСТИКА: Чертеж = {drawing_number}, Номер шва = {weld_number}")
    print("=" * 80)
    
    # 1. Проверяем наличие в pipeline_weld_joint_iso
    print("\n1. Проверка в таблице pipeline_weld_joint_iso:")
    print("-" * 80)
    query_pwji = """
        SELECT id, ISO, стык, Линия, Титул, Код_удаления
        FROM pipeline_weld_joint_iso
        WHERE ISO = ? AND стык = ?
    """
    cursor.execute(query_pwji, (drawing_number, weld_number))
    pwji_row = cursor.fetchone()
    
    if pwji_row:
        print(f"[OK] НАЙДЕНО в pipeline_weld_joint_iso:")
        print(f"   ID: {pwji_row[0]}")
        print(f"   ISO: {pwji_row[1]}")
        print(f"   стык: {pwji_row[2]}")
        print(f"   Линия: {pwji_row[3]}")
        print(f"   Титул: {pwji_row[4]}")
        print(f"   Код_удаления: {pwji_row[5]}")
    else:
        print(f"[ERROR] НЕ НАЙДЕНО в pipeline_weld_joint_iso")
        # Проверяем похожие записи
        query_similar_iso = """
            SELECT DISTINCT ISO, стык
            FROM pipeline_weld_joint_iso
            WHERE ISO LIKE ? OR стык = ?
            LIMIT 10
        """
        cursor.execute(query_similar_iso, (f'%{drawing_number[-20:]}%', weld_number))
        similar = cursor.fetchall()
        if similar:
            print(f"   Похожие записи:")
            for row in similar:
                print(f"     ISO: {row[0]}, стык: {row[1]}")
    
    # 2. Проверяем наличие в wl_china
    print("\n2. Проверка в таблице wl_china:")
    print("-" * 80)
    query_wl_china = """
        SELECT id, Номер_чертежа, "_Номер_сварного_шва_без_S_F_", 
               Заключение_РК_N, Результаты_Заключения_РК, Дата_Заключения_РК,
               АКТ_ВИК_N, Дата_АКТ_ВИК, Результаты_АКТ_ВИК, Дата_сварки
        FROM wl_china
        WHERE Номер_чертежа = ? AND "_Номер_сварного_шва_без_S_F_" = ?
    """
    cursor.execute(query_wl_china, (drawing_number, weld_number))
    wl_china_row = cursor.fetchone()
    
    if wl_china_row:
        print(f"[OK] НАЙДЕНО в wl_china:")
        print(f"   ID: {wl_china_row[0]}")
        print(f"   Номер_чертежа: {wl_china_row[1]}")
        print(f"   _Номер_сварного_шва_без_S_F_: {wl_china_row[2]}")
        print(f"   Заключение_РК_N: {wl_china_row[3]}")
        print(f"   Результаты_Заключения_РК: {wl_china_row[4]}")
        print(f"   Дата_Заключения_РК: {wl_china_row[5]}")
        print(f"   АКТ_ВИК_N: {wl_china_row[6]}")
        print(f"   Дата_АКТ_ВИК: {wl_china_row[7]}")
        print(f"   Результаты_АКТ_ВИК: {wl_china_row[8]}")
        print(f"   Дата_сварки: {wl_china_row[9]}")
    else:
        print(f"[ERROR] НЕ НАЙДЕНО в wl_china")
        # Проверяем похожие записи
        query_similar_wc = """
            SELECT DISTINCT Номер_чертежа, "_Номер_сварного_шва_без_S_F_"
            FROM wl_china
            WHERE Номер_чертежа LIKE ? OR "_Номер_сварного_шва_без_S_F_" = ?
            LIMIT 10
        """
        cursor.execute(query_similar_wc, (f'%{drawing_number[-20:]}%', weld_number))
        similar = cursor.fetchall()
        if similar:
            print(f"   Похожие записи:")
            for row in similar:
                print(f"     Номер_чертежа: {row[0]}, _Номер_сварного_шва_без_S_F_: {row[1]}")
    
    # 3. Проверяем наличие в condition_weld
    print("\n3. Проверка в таблице condition_weld:")
    print("-" * 80)
    query_cw = """
        SELECT id, ISO, стык, ID_WC, Заключение_РК_N, Результаты_Заключения_РК, 
               Дата_Заключения_РК, АКТ_ВИК_N, Дата_АКТ_ВИК, Результаты_АКТ_ВИК, Дата_сварки
        FROM condition_weld
        WHERE ISO = ? AND стык = ?
    """
    cursor.execute(query_cw, (drawing_number, weld_number))
    cw_row = cursor.fetchone()
    
    if cw_row:
        print(f"[OK] НАЙДЕНО в condition_weld:")
        print(f"   ID: {cw_row[0]}")
        print(f"   ISO: {cw_row[1]}")
        print(f"   стык: {cw_row[2]}")
        print(f"   ID_WC: {cw_row[3]}")
        print(f"   Заключение_РК_N: {cw_row[4]}")
        print(f"   Результаты_Заключения_РК: {cw_row[5]}")
        print(f"   Дата_Заключения_РК: {cw_row[6]}")
        print(f"   АКТ_ВИК_N: {cw_row[7]}")
        print(f"   Дата_АКТ_ВИК: {cw_row[8]}")
        print(f"   Результаты_АКТ_ВИК: {cw_row[9]}")
        print(f"   Дата_сварки: {cw_row[10]}")
        
        if cw_row[3] is None:
            print(f"\n[WARNING] ПРОБЛЕМА: ID_WC = NULL, значит данные из wl_china не попали!")
        else:
            print(f"\n[OK] Данные из wl_china присутствуют (ID_WC = {cw_row[3]})")
    else:
        print(f"[ERROR] НЕ НАЙДЕНО в condition_weld")
    
    # 4. Проверяем точное совпадение значений (с учетом пробелов и регистра)
    print("\n4. Проверка точного совпадения значений:")
    print("-" * 80)
    
    if pwji_row and wl_china_row:
        pwji_iso = pwji_row[1]
        pwji_styk = pwji_row[2]
        wc_iso = wl_china_row[1]
        wc_styk = wl_china_row[2]
        
        print(f"pipeline_weld_joint_iso.ISO: '{pwji_iso}' (len={len(pwji_iso) if pwji_iso else 0})")
        print(f"wl_china.Номер_чертежа: '{wc_iso}' (len={len(wc_iso) if wc_iso else 0})")
        print(f"Совпадение ISO: {pwji_iso == wc_iso}")
        
        print(f"\npipeline_weld_joint_iso.стык: '{pwji_styk}' (len={len(pwji_styk) if pwji_styk else 0})")
        print(f"wl_china._Номер_сварного_шва_без_S_F_: '{wc_styk}' (len={len(wc_styk) if wc_styk else 0})")
        print(f"Совпадение стык: {pwji_styk == wc_styk}")
        
        # Проверяем пробелы
        if pwji_iso != wc_iso:
            print(f"\n[WARNING] ISO не совпадают! Разница в символах:")
            print(f"   pwji: {repr(pwji_iso)}")
            print(f"   wc:   {repr(wc_iso)}")
        
        if pwji_styk != wc_styk:
            print(f"\n[WARNING] Стык не совпадают! Разница в символах:")
            print(f"   pwji: {repr(pwji_styk)}")
            print(f"   wc:   {repr(wc_styk)}")
    
    # 5. Проверяем все записи для этого чертежа
    print("\n5. Все записи для этого чертежа:")
    print("-" * 80)
    
    # В pipeline_weld_joint_iso
    query_all_pwji = """
        SELECT COUNT(*), COUNT(DISTINCT стык)
        FROM pipeline_weld_joint_iso
        WHERE ISO = ?
    """
    cursor.execute(query_all_pwji, (drawing_number,))
    all_pwji = cursor.fetchone()
    print(f"В pipeline_weld_joint_iso: {all_pwji[0]} записей, {all_pwji[1]} уникальных стыков")
    
    # В wl_china
    query_all_wc = """
        SELECT COUNT(*), COUNT(DISTINCT "_Номер_сварного_шва_без_S_F_")
        FROM wl_china
        WHERE Номер_чертежа = ?
    """
    cursor.execute(query_all_wc, (drawing_number,))
    all_wc = cursor.fetchone()
    print(f"В wl_china: {all_wc[0]} записей, {all_wc[1]} уникальных номеров швов")
    
    # В condition_weld
    query_all_cw = """
        SELECT COUNT(*), COUNT(DISTINCT стык), COUNT(ID_WC)
        FROM condition_weld
        WHERE ISO = ?
    """
    cursor.execute(query_all_cw, (drawing_number,))
    all_cw = cursor.fetchone()
    print(f"В condition_weld: {all_cw[0]} записей, {all_cw[1]} уникальных стыков, {all_cw[2]} с ID_WC")
    
    # 6. Проверяем JOIN вручную
    print("\n6. Ручная проверка JOIN:")
    print("-" * 80)
    query_manual_join = """
        SELECT 
            pwji.id AS pwji_id,
            pwji.ISO AS pwji_iso,
            pwji.стык AS pwji_styk,
            wc.id AS wc_id,
            wc.Номер_чертежа AS wc_iso,
            wc."_Номер_сварного_шва_без_S_F_" AS wc_styk
        FROM pipeline_weld_joint_iso pwji
        LEFT JOIN wl_china wc ON pwji.ISO = wc.Номер_чертежа AND pwji.стык = wc."_Номер_сварного_шва_без_S_F_"
        WHERE pwji.ISO = ? AND pwji.стык = ?
    """
    cursor.execute(query_manual_join, (drawing_number, weld_number))
    join_result = cursor.fetchone()
    
    if join_result:
        print(f"Результат JOIN:")
        print(f"   pwji_id: {join_result[0]}")
        print(f"   pwji_ISO: '{join_result[1]}'")
        print(f"   pwji_стык: '{join_result[2]}'")
        print(f"   wc_id: {join_result[3]}")
        print(f"   wc_Номер_чертежа: '{join_result[4]}'")
        print(f"   wc_стык: '{join_result[5]}'")
        
        if join_result[3] is None:
            print(f"\n[ERROR] JOIN не сработал - wc_id = NULL")
        else:
            print(f"\n[OK] JOIN сработал - wc_id = {join_result[3]}")
    else:
        print(f"[ERROR] Запись не найдена в pipeline_weld_joint_iso для JOIN")
    
    conn.close()
    print("\n" + "=" * 80)

if __name__ == "__main__":
    drawing_number = "GCC-NAG-DDD-12460-12-1500-TK-ISO-00002"
    weld_number = "15R"
    
    if len(sys.argv) > 1:
        drawing_number = sys.argv[1]
    if len(sys.argv) > 2:
        weld_number = sys.argv[2]
    
    check_drawing_and_weld(drawing_number, weld_number)

