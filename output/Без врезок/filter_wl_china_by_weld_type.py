"""
Скрипт для группировки данных из таблицы wl_china:
- Группирует по столбцу Номер_чертежа
- Объединяет все значения столбца Тип_соединения_российский_стандарт через запятую
- Выводит результат в Excel файл
"""

import sys
import os
import sqlite3
import pandas as pd
from datetime import datetime

# Добавляем путь к utilities для импорта модулей
current_dir = os.path.dirname(os.path.abspath(__file__))
utilities_dir = os.path.join(current_dir, '..', 'utilities')
if utilities_dir not in sys.path:
    sys.path.insert(0, utilities_dir)

from db_utils import get_database_path

# Настройка кодировки для вывода в консоль Windows
if sys.platform == 'win32' and hasattr(sys.stdout, 'buffer'):
    import io
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    except (AttributeError, ValueError):
        pass


def filter_wl_china_data():
    """
    Группирует данные из таблицы wl_china:
    - Группирует по Номер_чертежа
    - Объединяет все значения Тип_соединения_российский_стандарт через запятую
    - Выводит результат в Excel
    """
    print("=" * 60)
    print("ГРУППИРОВКА ДАННЫХ ИЗ ТАБЛИЦЫ wl_china")
    print("=" * 60)
    
    # Получаем путь к базе данных
    db_path = get_database_path()
    if db_path is None:
        print("❌ Ошибка: База данных не найдена!")
        return
    
    print(f"📁 Путь к базе данных: {db_path}")
    
    try:
        # Подключаемся к базе данных
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Проверяем существование таблицы wl_china
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='wl_china'")
        if not cursor.fetchone():
            print("❌ Ошибка: Таблица wl_china не найдена в базе данных!")
            conn.close()
            return
        
        # Проверяем наличие необходимых столбцов
        cursor.execute("PRAGMA table_info(wl_china)")
        columns_info = cursor.fetchall()
        column_names = [col[1] for col in columns_info]
        
        if 'Номер_чертежа' not in column_names:
            print("❌ Ошибка: Столбец 'Номер_чертежа' не найден в таблице wl_china!")
            print(f"   Доступные столбцы: {', '.join(column_names[:10])}...")
            conn.close()
            return
        
        if 'Тип_соединения_российский_стандарт' not in column_names:
            print("❌ Ошибка: Столбец 'Тип_соединения_российский_стандарт' не найден в таблице wl_china!")
            print(f"   Доступные столбцы: {', '.join(column_names[:10])}...")
            conn.close()
            return
        
        print("✅ Таблица wl_china найдена")
        print(f"✅ Найдено столбцов: {len(column_names)}")
        
        # Сначала получаем все столбцы для группировки
        # Берем первое значение для остальных столбцов, кроме Тип_соединения_российский_стандарт
        # Для Тип_соединения_российский_стандарт используем GROUP_CONCAT
        
        # Формируем список столбцов для SELECT (кроме Тип_соединения_российский_стандарт)
        other_columns = [col for col in column_names if col != 'Тип_соединения_российский_стандарт' and col != 'id']
        
        # Создаем SELECT часть с агрегацией
        select_parts = []
        for col in other_columns:
            select_parts.append(f'MIN("{col}") AS "{col}"')
        
        # Добавляем GROUP_CONCAT для Тип_соединения_российский_стандарт
        # GROUP_CONCAT автоматически пропускает NULL значения
        # Используем COALESCE для замены пустых строк на NULL, чтобы они не попадали в результат
        select_parts.append('GROUP_CONCAT(NULLIF("Тип_соединения_российский_стандарт", ""), ", ") AS "Тип_соединения_российский_стандарт_объединенный"')
        
        # Добавляем COUNT для количества записей
        select_parts.append('COUNT(*) AS "Количество_записей"')
        
        select_clause = ', '.join(select_parts)
        
        # SQL запрос для группировки:
        # Группируем по Номер_чертежа, объединяем Тип_соединения_российский_стандарт через запятую
        query = f"""
        SELECT 
            {select_clause}
        FROM wl_china
        GROUP BY "Номер_чертежа"
        HAVING "Тип_соединения_российский_стандарт_объединенный" IS NOT NULL 
           AND "Тип_соединения_российский_стандарт_объединенный" != ''
        ORDER BY "Номер_чертежа"
        """
        
        print("\n📊 Выполняем SQL запрос...")
        print("   Группировка: по Номер_чертежа")
        print("   Объединение: Тип_соединения_российский_стандарт через запятую")
        
        # Выполняем запрос
        df = pd.read_sql_query(query, conn)
        
        print(f"✅ Найдено групп (Номер_чертежа): {len(df)}")
        
        if len(df) == 0:
            print("⚠️  Результат пуст.")
            conn.close()
            return
        
        # Подсчитываем общее количество записей в исходной таблице для этих чертежей
        total_records = df['Количество_записей'].sum()
        print(f"✅ Всего записей в группах: {total_records}")
        
        # Создаем имя файла с датой и временем
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        # Определяем путь к папке output в корне проекта
        project_root = os.path.join(os.path.dirname(current_dir), '..')
        output_dir = os.path.join(project_root, 'output')
        os.makedirs(output_dir, exist_ok=True)
        excel_filename = f'wl_china_filtered_{timestamp}.xlsx'
        excel_path = os.path.join(output_dir, excel_filename)
        excel_path = os.path.abspath(excel_path)  # Абсолютный путь для ясности
        
        print(f"\n💾 Сохраняем результат в Excel: {excel_path}")
        
        # Переименовываем столбец для удобства
        if 'Тип_соединения_российский_стандарт_объединенный' in df.columns:
            df = df.rename(columns={'Тип_соединения_российский_стандарт_объединенный': 'Тип_соединения_российский_стандарт'})
        
        # Сохраняем в Excel с несколькими листами
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            # Лист 1: Сгруппированные данные
            df.to_excel(writer, sheet_name='Сгруппированные_данные', index=False)
            
            # Лист 2: Сводная информация
            summary_data = {
                'Параметр': [
                    'Дата создания отчета',
                    'Уникальных Номер_чертежа',
                    'Всего записей в группах',
                    'Условие фильтрации'
                ],
                'Значение': [
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    len(df),
                    total_records,
                    'Группировка по Номер_чертежа с объединением значений Тип_соединения_российский_стандарт'
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Сводка', index=False)
        
        print(f"✅ Файл успешно сохранен: {excel_path}")
        print(f"\n📋 Содержимое файла:")
        print(f"   - Лист 'Сгруппированные_данные': {len(df)} групп (Номер_чертежа)")
        print(f"   - Лист 'Сводка': информация об отчете")
        
        # Закрываем соединение
        conn.close()
        
        print("\n" + "=" * 60)
        print("ОТЧЕТ УСПЕШНО СОЗДАН")
        print("=" * 60)
        
    except sqlite3.Error as e:
        print(f"❌ Ошибка базы данных: {e}")
        if 'conn' in locals():
            conn.close()
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
        import traceback
        print(traceback.format_exc())
        if 'conn' in locals():
            conn.close()


def main():
    """Основная функция"""
    filter_wl_china_data()


if __name__ == "__main__":
    main()
