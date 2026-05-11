import sqlite3
import re
import os

import pandas as pd

# --- Функция для обработки значения ---
def clean_joint(val):
    if not isinstance(val, str):
        return val
    # Удаляем S или F в начале, пробелы, затем все ведущие нули
    cleaned = re.sub(r'^[SF]\s*-?\s*', '', val, flags=re.IGNORECASE)
    cleaned = cleaned.replace(' ', '')  # Убираем все пробелы
    # Удаляем все ведущие нули, но оставляем хотя бы одну цифру
    cleaned = re.sub(r'^0+', '', cleaned)
    # Если после удаления нулей ничего не осталось, возвращаем '0'
    if not cleaned:
        cleaned = '0'
    return cleaned

# --- Основная функция ---
def process_db(db_path, table_name, column_name):
    # Подключение к базе
    conn = sqlite3.connect(db_path)
    
    # Проверяем существование столбца
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info(`{table_name}`)")
    columns = [row[1] for row in cursor.fetchall()]
    
    if column_name not in columns:
        print(f'Столбец {column_name} не найден!')
        conn.close()
        return
    
    # Получаем общее количество записей
    cursor.execute(f'SELECT COUNT(*) FROM `{table_name}`')
    total_records = cursor.fetchone()[0]
    print(f'Всего записей в таблице {table_name}: {total_records}')
    
    # Обрабатываем данные по частям для больших таблиц
    batch_size = 1000
    processed = 0
    
    # Создаем новый столбец если его нет
    try:
        cursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN СТЫКИ_БЕЗ_S_и_F TEXT")
        print(f'Добавлен новый столбец СТЫКИ_БЕЗ_S_и_F в таблицу {table_name}')
    except sqlite3.OperationalError:
        print(f'Столбец СТЫКИ_БЕЗ_S_и_F уже существует в таблице {table_name}')
    
    # Обрабатываем данные по частям
    for offset in range(0, total_records, batch_size):
        # Загружаем часть данных
        query = f'SELECT id, `{column_name}` FROM `{table_name}` LIMIT {batch_size} OFFSET {offset}'
        df_batch = pd.read_sql_query(query, conn)
        
        if df_batch.empty:
            break
            
        # Применяем функцию очистки
        df_batch['СТЫКИ_БЕЗ_S_и_F'] = df_batch[column_name].apply(clean_joint)
        
        # Обновляем данные в базе
        for _, row in df_batch.iterrows():
            cursor.execute(
                f'UPDATE `{table_name}` SET СТЫКИ_БЕЗ_S_и_F = ? WHERE id = ?',
                (row['СТЫКИ_БЕЗ_S_и_F'], row['id'])
            )
        
        processed += len(df_batch)
        progress = (processed / total_records) * 100
        print(f'Обработано: {processed}/{total_records} записей ({progress:.1f}%)')
    
    # Сохраняем изменения
    conn.commit()
    print(f'Готово! В таблицу {table_name} добавлен столбец СТЫКИ_БЕЗ_S_и_F (без S/F и ведущих нулей).')
    print(f'Обработано записей: {processed}')
    conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Извлечение номеров стыков без S/F и всех ведущих нулей.")
    parser.add_argument('--db', required=True, help='Путь к базе данных SQLite')
    parser.add_argument('--table', required=True, help='Имя таблицы')
    parser.add_argument('--column', required=True, help='Имя столбца с номерами стыков')
    args = parser.parse_args()
    if not os.path.exists(args.db):
        print(f'Файл базы данных {args.db} не найден!')
    else:
        process_db(args.db, args.table, args.column) 