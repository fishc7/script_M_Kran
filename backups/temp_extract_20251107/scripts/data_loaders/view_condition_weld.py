import sqlite3
import os
import sys
import pandas as pd
from datetime import datetime

# Импортируем утилиты
try:
    # Пробуем относительный импорт (для запуска через web-приложение)
    from ..utilities.db_utils import get_database_connection
    from ..utilities.path_utils import get_log_path
except ImportError:
    # Если не работает, используем абсолютный импорт (для прямого запуска)
    import sys
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    utilities_dir = os.path.join(os.path.dirname(current_dir), 'utilities')
    if utilities_dir not in sys.path:
        sys.path.insert(0, utilities_dir)
    
    try:
        from db_utils import get_database_connection
        from path_utils import get_log_path
    except ImportError:
        # Если и это не работает, используем прямой путь
        sys.path.insert(0, os.path.join(current_dir, '..', 'utilities'))
        from db_utils import get_database_connection
        from path_utils import get_log_path

def view_condition_weld_data(limit=10, iso_filter=None, joint_filter=None):
    """
    Просматривает данные из таблицы condition_weld
    
    Args:
        limit: Количество записей для отображения (по умолчанию 10)
        iso_filter: Фильтр по ISO (опционально)
        joint_filter: Фильтр по номеру стыка (опционально)
    """
    try:
        # Подключение к базе данных
        conn = get_database_connection()
        cursor = conn.cursor()
        
        print("=" * 80)
        print("📊 ПРОСМОТР ДАННЫХ ТАБЛИЦЫ CONDITION_WELD")
        print("=" * 80)
        
        # Проверяем наличие таблицы
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='condition_weld'")
        if not cursor.fetchone():
            print("❌ Таблица condition_weld не найдена")
            return
        
        # Формируем запрос с фильтрами
        query = """
        SELECT 
            id,
            Титул,
            ISO,
            Линия,
            стык,
            ID_RT,
            РК,
            Статус_РК,
            Дата_Заключения_РК,
            "Количество_RT_записей",
            ID_VT,
            ВИК,
            Статус_ВИК,
            Дата_ВИК,
            "Количество_VT_записей",
            ID_WC,
            Заключение_РК_N,
            Результаты_Заключения_РК
        FROM condition_weld
        """
        
        conditions = []
        params = []
        
        if iso_filter:
            conditions.append("ISO LIKE ?")
            params.append(f"%{iso_filter}%")
        
        if joint_filter:
            conditions.append("стык LIKE ?")
            params.append(f"%{joint_filter}%")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY ISO, стык LIMIT ?"
        params.append(limit)
        
        # Выполняем запрос
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        if not rows:
            print("❌ Данные не найдены")
            return
        
        # Получаем названия столбцов
        columns = [description[0] for description in cursor.description]
        
        # Создаем DataFrame для красивого отображения
        df = pd.DataFrame(rows, columns=columns)
        
        # Отображаем данные
        print(f"📋 Найдено записей: {len(df)}")
        if iso_filter:
            print(f"🔍 Фильтр по ISO: {iso_filter}")
        if joint_filter:
            print(f"🔍 Фильтр по стыку: {joint_filter}")
        print()
        
        # Отображаем основные столбцы
        display_columns = ['id', 'ISO', 'стык', 'Код_удаления', 'Тип_шва', 'Статус_РК', 'Статус_ВИК', 'Заключение_РК_N']
        display_df = df[display_columns].copy()
        
        # Заменяем None на пустые строки для лучшего отображения
        display_df = display_df.fillna('')
        
        print("📊 Основные данные:")
        print(display_df.to_string(index=False))
        print()
        
        # Статистика по статусам
        print("📈 Статистика по статусам:")
        
        # RT статусы
        rt_status_counts = df['Статус_РК'].value_counts()
        if not rt_status_counts.empty:
            print("   RT статусы:")
            for status, count in rt_status_counts.items():
                if pd.notna(status):
                    print(f"     {status}: {count}")
        
        # VT статусы
        vt_status_counts = df['Статус_ВИК'].value_counts()
        if not vt_status_counts.empty:
            print("   VT статусы:")
            for status, count in vt_status_counts.items():
                if pd.notna(status):
                    print(f"     {status}: {count}")
        
        # Заключения РК
        conclusion_counts = df['Заключение_РК_N'].value_counts()
        if not conclusion_counts.empty:
            print("   Заключения РК:")
            for conclusion, count in conclusion_counts.items():
                if pd.notna(conclusion):
                    print(f"     {conclusion}: {count}")
        
        print()
        
        # Показываем полные данные для первой записи
        if len(df) > 0:
            print("📋 Полные данные первой записи:")
            first_record = df.iloc[0]
            for col in df.columns:
                value = first_record[col]
                if pd.notna(value):
                    print(f"   {col}: {value}")
        
    except Exception as e:
        print(f"❌ Ошибка при просмотре данных: {e}")
    
    finally:
        if 'conn' in locals():
            conn.close()

def show_table_info():
    """
    Показывает информацию о таблице condition_weld
    """
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        print("📊 ИНФОРМАЦИЯ О ТАБЛИЦЕ CONDITION_WELD")
        print("=" * 50)
        
        # Количество записей
        cursor.execute("SELECT COUNT(*) FROM condition_weld")
        total_records = cursor.fetchone()[0]
        print(f"Всего записей: {total_records}")
        
        # Структура таблицы
        cursor.execute("PRAGMA table_info(condition_weld)")
        columns = cursor.fetchall()
        print(f"Количество столбцов: {len(columns)}")
        print("\nСтолбцы:")
        for col in columns:
            print(f"  {col[1]} ({col[2]})")
        
        # Статистика по типам данных
        print("\n📈 Статистика:")
        
        # RT данные
        cursor.execute("SELECT COUNT(*) FROM condition_weld WHERE ID_RT IS NOT NULL")
        rt_count = cursor.fetchone()[0]
        print(f"Записей с RT данными: {rt_count} ({rt_count/total_records*100:.1f}%)")
        
        # VT данные
        cursor.execute("SELECT COUNT(*) FROM condition_weld WHERE ID_VT IS NOT NULL")
        vt_count = cursor.fetchone()[0]
        print(f"Записей с VT данными: {vt_count} ({vt_count/total_records*100:.1f}%)")
        
        # WL_China данные
        cursor.execute("SELECT COUNT(*) FROM condition_weld WHERE ID_WC IS NOT NULL")
        wc_count = cursor.fetchone()[0]
        print(f"Записей с WL_China данными: {wc_count} ({wc_count/total_records*100:.1f}%)")
        
        # Уникальные ISO
        cursor.execute("SELECT COUNT(DISTINCT ISO) FROM condition_weld")
        unique_iso = cursor.fetchone()[0]
        print(f"Уникальных ISO: {unique_iso}")
        
        # Уникальные стыки
        cursor.execute("SELECT COUNT(DISTINCT стык) FROM condition_weld")
        unique_joints = cursor.fetchone()[0]
        print(f"Уникальных стыков: {unique_joints}")
        
    except Exception as e:
        print(f"❌ Ошибка при получении информации о таблице: {e}")
    
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    """
    Основная функция
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Просмотр данных таблицы condition_weld')
    parser.add_argument('--limit', type=int, default=10, help='Количество записей для отображения (по умолчанию 10)')
    parser.add_argument('--iso', type=str, help='Фильтр по ISO')
    parser.add_argument('--joint', type=str, help='Фильтр по номеру стыка')
    parser.add_argument('--info', action='store_true', help='Показать информацию о таблице')
    
    args = parser.parse_args()
    
    if args.info:
        show_table_info()
    else:
        view_condition_weld_data(
            limit=args.limit,
            iso_filter=args.iso,
            joint_filter=args.joint
        )

if __name__ == "__main__":
    main()
