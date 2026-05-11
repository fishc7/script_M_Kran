"""
Тестовый скрипт для проверки работы JOIN с вариантами номеров швов
"""
import sqlite3
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

try:
    from scripts.utilities.db_utils import get_database_connection
except ImportError:
    def get_database_connection():
        db_path = os.path.join(os.path.dirname(__file__), '..', '..', 'database', 'M_Kran_Kingesepp.db')
        if not os.path.exists(db_path):
            db_path = os.path.join(os.path.dirname(__file__), '..', '..', 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db')
        return sqlite3.connect(db_path, check_same_thread=False)

def test_weld_join(drawing_number, weld_number):
    """
    Тестирует JOIN для конкретного номера шва
    """
    conn = get_database_connection()
    cursor = conn.cursor()
    
    print("=" * 80)
    print(f"Тест JOIN для: ISO={drawing_number}, стык={weld_number}")
    print("=" * 80)
    
    # Тестируем новый JOIN
    query = """
        SELECT 
            pwji.id,
            pwji.ISO,
            pwji.стык,
            wc.id AS wc_id,
            wc."_Номер_сварного_шва_без_S_F_" AS wc_weld,
            wc.Заключение_РК_N,
            wc.Дата_сварки
        FROM pipeline_weld_joint_iso pwji
        LEFT JOIN (
            SELECT DISTINCT
                wc.id,
                wc.Номер_чертежа,
                wc."_Номер_сварного_шва_без_S_F_",
                wc.Заключение_РК_N,
                wc.Результаты_Заключения_РК,
                wc.Дата_Заключения_РК,
                wc.АКТ_ВИК_N,
                wc.Дата_АКТ_ВИК,
                wc.Результаты_АКТ_ВИК,
                wc.Дата_сварки,
                pwji_match.ISO AS match_iso,
                pwji_match.стык AS match_styk,
                ROW_NUMBER() OVER (
                    PARTITION BY pwji_match.ISO, pwji_match.стык
                    ORDER BY 
                        CASE WHEN pwji_match.стык = wc."_Номер_сварного_шва_без_S_F_" THEN 1 ELSE 2 END,
                        CASE WHEN pwji_match.стык LIKE '%W' AND SUBSTR(pwji_match.стык, 1, LENGTH(pwji_match.стык) - 1) = wc."_Номер_сварного_шва_без_S_F_" THEN 2 ELSE 3 END,
                        CASE WHEN pwji_match.стык LIKE '%RW' AND SUBSTR(pwji_match.стык, 1, LENGTH(pwji_match.стык) - 2) = wc."_Номер_сварного_шва_без_S_F_" THEN 3 ELSE 4 END,
                        wc.id DESC
                ) as rn
            FROM pipeline_weld_joint_iso pwji_match
            INNER JOIN wl_china wc ON pwji_match.ISO = wc.Номер_чертежа
            WHERE (
                    pwji_match.стык = wc."_Номер_сварного_шва_без_S_F_"
                    OR (pwji_match.стык LIKE '%W' AND SUBSTR(pwji_match.стык, 1, LENGTH(pwji_match.стык) - 1) = wc."_Номер_сварного_шва_без_S_F_")
                    OR (pwji_match.стык LIKE '%RW' AND SUBSTR(pwji_match.стык, 1, LENGTH(pwji_match.стык) - 2) = wc."_Номер_сварного_шва_без_S_F_")
                )
        ) wc ON pwji.ISO = wc.match_iso AND pwji.стык = wc.match_styk AND wc.rn = 1
        WHERE pwji.ISO = ? AND pwji.стык = ?
    """
    
    try:
        cursor.execute(query, (drawing_number, weld_number))
        result = cursor.fetchone()
        
        if result:
            print(f"\n[OK] Результат JOIN:")
            print(f"   pwji.id: {result[0]}")
            print(f"   pwji.ISO: {result[1]}")
            print(f"   pwji.стык: {result[2]}")
            print(f"   wc.id: {result[3]}")
            print(f"   wc._Номер_сварного_шва_без_S_F_: {result[4]}")
            print(f"   Заключение_РК_N: {result[5]}")
            print(f"   Дата_сварки: {result[6]}")
            
            if result[3] is None:
                print(f"\n[WARNING] JOIN не сработал - wc.id = NULL")
                return False
            else:
                print(f"\n[OK] JOIN успешно нашел совпадение!")
                return True
        else:
            print(f"\n[ERROR] Запись не найдена в pipeline_weld_joint_iso")
            return False
    except Exception as e:
        print(f"\n[ERROR] Ошибка при выполнении запроса: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        conn.close()

if __name__ == "__main__":
    drawing_number = "GCC-NAG-DDD-12460-12-1500-TK-ISO-00002"
    weld_number = "15RW"
    
    if len(sys.argv) > 1:
        drawing_number = sys.argv[1]
    if len(sys.argv) > 2:
        weld_number = sys.argv[2]
    
    test_weld_join(drawing_number, weld_number)




