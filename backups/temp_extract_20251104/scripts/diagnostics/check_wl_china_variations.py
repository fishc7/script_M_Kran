"""
Проверяет все варианты номеров швов в wl_china для конкретного чертежа
"""
import sqlite3
import sys
import os

# Добавляем путь к корню проекта
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

try:
    from scripts.utilities.db_utils import get_database_connection
except ImportError:
    def get_database_connection():
        db_path = os.path.join(os.path.dirname(__file__), '..', '..', 'database', 'M_Kran_Kingesepp.db')
        if not os.path.exists(db_path):
            db_path = os.path.join(os.path.dirname(__file__), '..', '..', 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db')
        return sqlite3.connect(db_path, check_same_thread=False)

def check_wl_china_variations(drawing_number, target_weld="15RW"):
    """
    Проверяет все варианты номеров швов в wl_china для поиска возможного соответствия
    """
    conn = get_database_connection()
    cursor = conn.cursor()
    
    print("=" * 80)
    print(f"Проверка вариантов номеров швов для чертежа: {drawing_number}")
    print(f"Ищем соответствие для: {target_weld}")
    print("=" * 80)
    
    # Получаем все записи из wl_china для этого чертежа
    query_all = """
        SELECT 
            id,
            Номер_чертежа,
            "_Номер_сварного_шва_без_S_F_",
            Номер_сварного_шва,
            Дата_сварки,
            Заключение_РК_N,
            АКТ_ВИК_N
        FROM wl_china
        WHERE Номер_чертежа = ?
        ORDER BY CAST("_Номер_сварного_шва_без_S_F_" AS INTEGER),
                 "_Номер_сварного_шва_без_S_F_"
    """
    
    cursor.execute(query_all, (drawing_number,))
    all_records = cursor.fetchall()
    
    print(f"\nВсего записей в wl_china для этого чертежа: {len(all_records)}")
    print("\nВсе номера швов из wl_china:")
    print("-" * 80)
    
    # Ищем возможные совпадения
    target_clean = target_weld.replace('W', '').replace('R', '').strip()
    possible_matches = []
    
    for record in all_records:
        wc_id = record[0]
        wc_iso = record[1]
        wc_styk_clean = record[2]  # _Номер_сварного_шва_без_S_F_
        wc_styk_original = record[3]  # Номер_сварного_шва
        wc_date = record[4]
        wc_rk = record[5]
        wc_vik = record[6]
        
        print(f"ID: {wc_id:5d} | _Номер_сварного_шва_без_S_F_: '{wc_styk_clean}' | Номер_сварного_шва: '{wc_styk_original}' | Дата: {wc_date}")
        
        # Проверяем возможные совпадения
        if wc_styk_clean:
            wc_clean = str(wc_styk_clean).replace('W', '').replace('R', '').strip()
            if wc_clean == target_clean or wc_styk_clean == target_weld:
                possible_matches.append(record)
    
    # Проверяем в pipeline_weld_joint_iso
    print("\n" + "=" * 80)
    print("Проверка в pipeline_weld_joint_iso:")
    print("-" * 80)
    
    query_pwji = """
        SELECT id, ISO, стык, Линия
        FROM pipeline_weld_joint_iso
        WHERE ISO = ? AND стык = ?
    """
    cursor.execute(query_pwji, (drawing_number, target_weld))
    pwji_record = cursor.fetchone()
    
    if pwji_record:
        print(f"[OK] Найдено в pipeline_weld_joint_iso:")
        print(f"   ID: {pwji_record[0]}")
        print(f"   ISO: {pwji_record[1]}")
        print(f"   стык: '{pwji_record[2]}'")
        print(f"   Линия: {pwji_record[3]}")
    else:
        print(f"[ERROR] Не найдено в pipeline_weld_joint_iso")
    
    # Проверяем в condition_weld
    print("\n" + "=" * 80)
    print("Проверка в condition_weld:")
    print("-" * 80)
    
    query_cw = """
        SELECT id, ISO, стык, ID_WC
        FROM condition_weld
        WHERE ISO = ? AND стык = ?
    """
    cursor.execute(query_cw, (drawing_number, target_weld))
    cw_record = cursor.fetchone()
    
    if cw_record:
        print(f"[OK] Найдено в condition_weld:")
        print(f"   ID: {cw_record[0]}")
        print(f"   ISO: {cw_record[1]}")
        print(f"   стык: '{cw_record[2]}'")
        print(f"   ID_WC: {cw_record[3]}")
        
        if cw_record[3] is None:
            print(f"\n[WARNING] ID_WC = NULL - данные из wl_china не попали!")
    else:
        print(f"[ERROR] Не найдено в condition_weld")
    
    # Выводим возможные совпадения
    if possible_matches:
        print("\n" + "=" * 80)
        print("Возможные совпадения:")
        print("-" * 80)
        for match in possible_matches:
            print(f"ID: {match[0]} | _Номер_сварного_шва_без_S_F_: '{match[2]}' | Номер_сварного_шва: '{match[3]}'")
    else:
        print("\n" + "=" * 80)
        print("Возможные совпадения не найдены")
        print("-" * 80)
        
        # Проверяем, есть ли просто "15" в wl_china
        query_15 = """
            SELECT id, "_Номер_сварного_шва_без_S_F_", Номер_сварного_шва
            FROM wl_china
            WHERE Номер_чертежа = ? AND "_Номер_сварного_шва_без_S_F_" LIKE '15%'
        """
        cursor.execute(query_15, (drawing_number,))
        records_15 = cursor.fetchall()
        
        if records_15:
            print("\nНайдены записи с номерами, начинающимися с '15':")
            for rec in records_15:
                print(f"   ID: {rec[0]} | _Номер_сварного_шва_без_S_F_: '{rec[1]}' | Номер_сварного_шва: '{rec[2]}'")
    
    conn.close()
    print("\n" + "=" * 80)

if __name__ == "__main__":
    drawing_number = "GCC-NAG-DDD-12460-12-1500-TK-ISO-00002"
    target_weld = "15RW"
    
    if len(sys.argv) > 1:
        drawing_number = sys.argv[1]
    if len(sys.argv) > 2:
        target_weld = sys.argv[2]
    
    check_wl_china_variations(drawing_number, target_weld)

