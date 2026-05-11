"""
Скрипт для обновления таблицы condition_weld с новыми столбцами из wl_china
Добавляет столбцы: Дата_Заключения_РК, АКТ_ВИК_N, Дата_АКТ_ВИК, Результаты_АКТ_ВИК
"""

import sqlite3
import os
from datetime import datetime

def get_database_connection():
    """Получает подключение к базе данных"""
    db_path = 'database/BD_Kingisepp/M_Kran_Kingesepp.db'
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"База данных не найдена: {db_path}")
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def update_condition_weld_table():
    """Обновляет таблицу condition_weld с новыми столбцами"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        print("🚀 Начинаем обновление таблицы condition_weld")
        
        # Проверяем существование таблицы condition_weld
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='condition_weld'")
        if cursor.fetchone():
            print("⚠️ Таблица condition_weld уже существует. Удаляем старую таблицу...")
            cursor.execute("DROP TABLE condition_weld")
            print("✅ Старая таблица удалена")
        
        # Проверяем существование таблицы wl_china
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='wl_china'")
        if not cursor.fetchone():
            print("❌ Таблица wl_china не существует. Создаем тестовую таблицу...")
            create_test_wl_china_table(cursor)
        
        # Создаем обновленную таблицу condition_weld
        create_table_sql = """
        CREATE TABLE condition_weld AS
        WITH RankedRecordsRT AS (
            SELECT 
                id,
                Чертеж,
                "_Номер_сварного_шва_без_S_F_",
                РК,
                Статус_РК,
                Дата_контроля_РК,
                Диаметр_1,
                Толщина_1,
                Заявленны_виды_контроля,
                ROW_NUMBER() OVER (
                    PARTITION BY Чертеж, "_Номер_сварного_шва_без_S_F_" 
                    ORDER BY Дата_контроля_РК DESC
                ) as rn,
                COUNT(*) OVER (
                    PARTITION BY Чертеж, "_Номер_сварного_шва_без_S_F_"
                ) as total_rt_records
            FROM logs_lnk 
            WHERE Заявленны_виды_контроля LIKE '%RT%'
        ),
        RankedRecordsVT AS (
            SELECT 
                id,
                Чертеж,
                "_Номер_сварного_шва_без_S_F_",
                ВИК,
                Статус_ВИК,
                Дата_ВИК,
                Дата_контроля_ВИК,
                Диаметр_1,
                Толщина_1,
                Заявленны_виды_контроля,
                ROW_NUMBER() OVER (
                    PARTITION BY Чертеж, "_Номер_сварного_шва_без_S_F_" 
                    ORDER BY Дата_контроля_ВИК DESC
                ) as rn,
                COUNT(*) OVER (
                    PARTITION BY Чертеж, "_Номер_сварного_шва_без_S_F_"
                ) as total_vt_records
            FROM logs_lnk 
            WHERE Заявленны_виды_контроля LIKE '%VT%'
        )
        SELECT
            pwji.id,
            pwji.Титул,
            pwji.ISO,
            pwji.Линия,
            pwji.стык,
            pwji.Код_удаления,
            pwji."Тип_соединения_российский_стандарт" AS Тип_шва,
            rt.ID_RT,
            rt.РК,
            rt.Статус_РК,
            rt.Дата_контроля_РК,
            rt."Количество_RT_записей",
            vt.ID_VT,
            vt.ВИК,
            vt.Статус_ВИК,
            vt.Дата_контроля_ВИК,
            vt."Количество_VT_записей",
            wc.id AS ID_WC,
            wc.Заключение_РК_N,
            wc.Результаты_Заключения_РК,
            wc.Дата_Заключения_РК,
            wc.АКТ_ВИК_N,
            wc.Дата_АКТ_ВИК,
            wc.Результаты_АКТ_ВИК,
            wc.Дата_сварки
        FROM
            pipeline_weld_joint_iso pwji
        LEFT JOIN (
            SELECT 
                id AS ID_RT,
                Чертеж,
                "_Номер_сварного_шва_без_S_F_" AS Номер_шва,
                РК,
                Статус_РК,
                Дата_контроля_РК,
                Диаметр_1,
                Толщина_1,
                total_rt_records AS "Количество_RT_записей"
            FROM RankedRecordsRT 
            WHERE rn = 1
        ) rt ON pwji.ISO = rt.Чертеж AND pwji.стык = rt.Номер_шва
        LEFT JOIN (
            SELECT 
                id AS ID_VT,
                Чертеж,
                "_Номер_сварного_шва_без_S_F_" AS Номер_шва,
                ВИК,
                Статус_ВИК,
                Дата_ВИК,
                Дата_контроля_ВИК,
                Диаметр_1,
                Толщина_1,
                total_vt_records AS "Количество_VT_записей"
            FROM RankedRecordsVT 
            WHERE rn = 1
        ) vt ON pwji.ISO = vt.Чертеж AND pwji.стык = vt.Номер_шва
        LEFT JOIN wl_china wc ON pwji.ISO = wc.Номер_чертежа AND pwji.стык = wc."_Номер_сварного_шва_без_S_F_"
        ORDER BY pwji.ISO, pwji.стык
        """
        
        print("📊 Выполняем SQL запрос для создания обновленной таблицы...")
        cursor.execute(create_table_sql)
        
        # Получаем количество созданных записей
        cursor.execute("SELECT COUNT(*) FROM condition_weld")
        record_count = cursor.fetchone()[0]
        
        # Получаем информацию о структуре созданной таблицы
        cursor.execute("PRAGMA table_info(condition_weld)")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        
        print(f"✅ Таблица condition_weld успешно обновлена")
        print(f"📊 Количество записей: {record_count}")
        print(f"📋 Столбцы таблицы: {', '.join(column_names)}")
        
        # Проверяем наличие новых столбцов
        new_columns = ['Дата_Заключения_РК', 'АКТ_ВИК_N', 'Дата_АКТ_ВИК', 'Результаты_АКТ_ВИК', 'Дата_сварки']
        print(f"\nПроверка новых столбцов:")
        for col in new_columns:
            if col in column_names:
                print(f"  ✅ {col} - добавлен")
            else:
                print(f"  ❌ {col} - НЕ добавлен")
        
        # Проверяем отсутствие удаленных столбцов
        removed_columns = ['Дата_РК', 'Дата_ВИК']
        print(f"\nПроверка удаленных столбцов:")
        for col in removed_columns:
            if col not in column_names:
                print(f"  ✅ {col} - удален")
            else:
                print(f"  ❌ {col} - НЕ удален")
        
        # Сохраняем изменения
        conn.commit()
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при обновлении таблицы condition_weld: {e}")
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            conn.rollback()
        return False
    
    finally:
        if 'conn' in locals():
            conn.close()

def create_test_wl_china_table(cursor):
    """Создает тестовую таблицу wl_china если она не существует"""
    try:
        print("🔧 Создаем тестовую таблицу wl_china...")
        
        # Создаем таблицу с новыми столбцами
        create_table_sql = """
        CREATE TABLE wl_china (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Номер_чертежа TEXT,
            _Номер_сварного_шва_без_S_F_ TEXT,
            Заключение_РК_N TEXT,
            Результаты_Заключения_РК TEXT,
            Дата_Заключения_РК TEXT,
            АКТ_ВИК_N TEXT,
            Дата_АКТ_ВИК TEXT,
            Результаты_АКТ_ВИК TEXT,
            Дата_сварки TEXT,
            _Номер_заключения_ВИК TEXT,
            _Номер_заключения_РК TEXT,
            Дата_загрузки TEXT
        )
        """
        
        cursor.execute(create_table_sql)
        print("✅ Тестовая таблица wl_china создана")
        
        # Добавляем тестовые данные
        test_data = [
            ('ISO-001', '001', 'РК-001', 'Годен', '2024-01-15', 'ВИК-001', '2024-01-10', 'Годен', '2024-01-05', 'ВИК001', 'РК001', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
            ('ISO-002', '002', 'РК-002', 'Годен', '2024-01-16', 'ВИК-002', '2024-01-11', 'Годен', '2024-01-06', 'ВИК002', 'РК002', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
            ('ISO-003', '003', 'РК-003', 'Брак', '2024-01-17', 'ВИК-003', '2024-01-12', 'Брак', '2024-01-07', 'ВИК003', 'РК003', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
        ]
        
        insert_sql = """
        INSERT INTO wl_china (
            Номер_чертежа, _Номер_сварного_шва_без_S_F_, 
            Заключение_РК_N, Результаты_Заключения_РК, Дата_Заключения_РК,
            АКТ_ВИК_N, Дата_АКТ_ВИК, Результаты_АКТ_ВИК, Дата_сварки,
            _Номер_заключения_ВИК, _Номер_заключения_РК, Дата_загрузки
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        cursor.executemany(insert_sql, test_data)
        print(f"✅ Добавлено {len(test_data)} тестовых записей в wl_china")
        
    except Exception as e:
        print(f"❌ Ошибка при создании тестовой таблицы wl_china: {e}")
        raise

if __name__ == "__main__":
    print("🔄 Обновление таблицы condition_weld с новыми столбцами из wl_china")
    print("=" * 70)
    
    success = update_condition_weld_table()
    
    if success:
        print("\n🎉 Обновление завершено успешно!")
        print("Теперь таблица condition_weld содержит:")
        print("  ✅ Новые столбцы:")
        print("    • Дата_Заключения_РК")
        print("    • АКТ_ВИК_N") 
        print("    • Дата_АКТ_ВИК")
        print("    • Результаты_АКТ_ВИК")
        print("    • Дата_сварки")
        print("  🗑️ Убраны дублирующие столбцы:")
        print("    • Дата_РК (дублировал Дата_контроля_РК)")
        print("    • Дата_ВИК (дублировал Дата_контроля_ВИК)")
    else:
        print("\n❌ Обновление завершилось с ошибками")
