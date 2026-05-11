#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для удаления префиксов S/F из номеров стыков
Автор: M_Kran
Версия: 1.0
"""

import sqlite3
import re
import os
import sys
from pathlib import Path

def get_db_connection():
    """Получение подключения к базе данных"""
    try:
        # Путь к базе данных (адаптируйте под вашу структуру)
        db_path = Path(__file__).parent / "database" / "main.db"
        
        if not db_path.exists():
            # Попробуем найти базу данных в других местах
            possible_paths = [
                Path(__file__).parent / "web" / "database" / "main.db",
                Path(__file__).parent / "main.db",
                Path.cwd() / "main.db",
                Path.cwd() / "database" / "main.db"
            ]
            
            for path in possible_paths:
                if path.exists():
                    db_path = path
                    break
            else:
                print("❌ База данных не найдена!")
                print("Искал в следующих местах:")
                for path in [db_path] + possible_paths:
                    print(f"  - {path}")
                return None
        
        print(f"✅ База данных найдена: {db_path}")
        return sqlite3.connect(str(db_path))
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        return None

def extract_joint_number(joint_text):
    """
    Извлекает номер стыка из текста
    Убирает ведущие нули из извлеченных чисел.
    """
    if not joint_text:
        return None
    
    try:
        # Безопасное преобразование в строку
        if isinstance(joint_text, bytes):
            joint_text = joint_text.decode('utf-8', errors='ignore')
        else:
            joint_text = str(joint_text)
        
        # Дополнительная очистка от проблемных символов
        joint_text = ''.join(char for char in joint_text if ord(char) < 128 or char.isdigit())
        joint_text = joint_text.strip()
        
        match = re.search(r'\d+', joint_text)
        if match:
            number = match.group()
            # Убираем ведущие нули
            return str(int(number))
    except Exception as e:
        # Игнорируем все ошибки
        pass
    
    return None

def clean_joint_number(joint_text):
    """
    Улучшенная функция очистки номера стыка
    Удаляет префиксы S/F и ведущие нули
    """
    if not joint_text:
        return None
    
    try:
        # Безопасное преобразование в строку
        if isinstance(joint_text, bytes):
            joint_text = joint_text.decode('utf-8', errors='ignore')
        else:
            joint_text = str(joint_text)
        
        # Дополнительная очистка от проблемных символов
        joint_text = ''.join(char for char in joint_text if ord(char) < 128 or char.isdigit())
        joint_text = joint_text.strip()
        
        # Удаляем S или F в начале, пробелы, дефисы
        cleaned = re.sub(r'^[SF]\s*-?\s*', '', joint_text, flags=re.IGNORECASE)
        cleaned = cleaned.replace(' ', '')  # Убираем все пробелы
        
        # Удаляем все ведущие нули, но оставляем хотя бы одну цифру
        cleaned = re.sub(r'^0+', '', cleaned)
        
        # Если после удаления нулей ничего не осталось, возвращаем '0'
        if not cleaned:
            cleaned = '0'
        
        return cleaned
    except Exception as e:
        # Игнорируем все ошибки
        pass
    
    return None

def extract_and_clean_joint_number(joint_text):
    """
    Комбинированная функция: извлекает число и очищает его от префиксов S/F
    """
    if not joint_text:
        return None
    
    try:
        # Безопасное преобразование в строку
        if isinstance(joint_text, bytes):
            joint_text = joint_text.decode('utf-8', errors='ignore')
        else:
            joint_text = str(joint_text)
        
        # Дополнительная очистка от проблемных символов
        joint_text = ''.join(char for char in joint_text if ord(char) < 128 or char.isdigit())
        joint_text = joint_text.strip()
        
        # Сначала пытаемся извлечь число с префиксом S/F
        match = re.search(r'[SF]\s*-?\s*(\d+)', joint_text, flags=re.IGNORECASE)
        if match:
            number = match.group(1)
            # Убираем ведущие нули
            return str(int(number))
        
        # Если нет префикса S/F, ищем любые цифры
        match = re.search(r'(\d+)', joint_text)
        if match:
            number = match.group(1)
            # Убираем ведущие нули
            return str(int(number))
    except Exception as e:
        # Игнорируем все ошибки
        pass
    
    return None

def get_tables(conn):
    """Получение списка всех таблиц"""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    return [row[0] for row in cursor.fetchall()]

def get_columns(conn, table_name):
    """Получение списка столбцов таблицы"""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info(`{table_name}`)")
    return [row[1] for row in cursor.fetchall()]

def process_table_numbers(table_name, source_column, target_column=None, mode='combined'):
    """
    Обработка номеров в таблице с удалением префиксов S/F
    
    Args:
        table_name (str): Имя таблицы
        source_column (str): Исходный столбец с номерами
        target_column (str): Целевой столбец для результатов (если None, перезаписывает исходный)
        mode (str): Режим обработки ('simple', 'clean', 'combined')
    """
    conn = get_db_connection()
    if not conn:
        return False, "Ошибка подключения к БД"
    
    try:
        cursor = conn.cursor()
        
        # Проверяем существование таблицы
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if not cursor.fetchone():
            return False, f"Таблица {table_name} не найдена"
        
        # Проверяем существование исходного столбца
        columns = get_columns(conn, table_name)
        if source_column not in columns:
            return False, f"Столбец {source_column} не найден в таблице {table_name}"
        
        # Определяем целевой столбец
        if target_column is None:
            target_column = f"{source_column}_cleaned"
        
        # Добавляем целевой столбец, если его нет
        if target_column not in columns:
            try:
                cursor.execute(f'ALTER TABLE `{table_name}` ADD COLUMN `{target_column}` TEXT')
                print(f"✅ Добавлен новый столбец: {target_column}")
            except Exception as e:
                if "duplicate column name" not in str(e).lower():
                    return False, f"Ошибка при добавлении столбца: {e}"
        
        # Получаем данные для обработки
        cursor.execute(f'SELECT rowid, `{source_column}` FROM `{table_name}` WHERE `{source_column}` IS NOT NULL')
        records = cursor.fetchall()
        
        if not records:
            return False, f"Нет данных для обработки в столбце {source_column}"
        
        # Выбираем функцию в зависимости от режима
        if mode == 'simple':
            process_function = extract_joint_number
        elif mode == 'clean':
            process_function = clean_joint_number
        else:  # combined
            process_function = extract_and_clean_joint_number
        
        # Обрабатываем записи
        updated_count = 0
        total_records = len(records)
        
        print(f"🔄 Обработка {total_records} записей в режиме '{mode}'...")
        
        for i, record in enumerate(records, 1):
            rowid, source_text = record
            
            try:
                # Безопасное преобразование в строку
                if source_text is not None:
                    try:
                        source_text_str = str(source_text)
                    except UnicodeEncodeError:
                        source_text_str = str(source_text).encode('utf-8', errors='ignore').decode('utf-8')
                else:
                    source_text_str = ""
                
                processed_number = process_function(source_text_str)
                if processed_number is not None:
                    cursor.execute(f'UPDATE `{table_name}` SET `{target_column}` = ? WHERE rowid = ?', (processed_number, rowid))
                    updated_count += 1
                
                # Показываем прогресс каждые 100 записей
                if i % 100 == 0:
                    progress = (i / total_records) * 100
                    print(f"📊 Прогресс: {progress:.1f}% ({i}/{total_records})")
                    
            except Exception as e:
                # Игнорируем ошибки и продолжаем обработку
                continue
        
        # Сохраняем изменения
        conn.commit()
        conn.close()
        
        message = f"✅ Обработано записей: {updated_count} из {total_records}"
        print(message)
        return True, message
        
    except Exception as e:
        return False, f"Ошибка обработки: {e}"

def interactive_mode():
    """Интерактивный режим работы"""
    print("🔧 Скрипт удаления префиксов S/F из номеров стыков")
    print("=" * 50)
    
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        # Получаем список таблиц
        tables = get_tables(conn)
        if not tables:
            print("❌ В базе данных нет таблиц")
            return
        
        print(f"📋 Найдено таблиц: {len(tables)}")
        print("\nДоступные таблицы:")
        for i, table in enumerate(tables, 1):
            print(f"  {i}. {table}")
        
        # Выбор таблицы
        while True:
            try:
                choice = input(f"\nВыберите номер таблицы (1-{len(tables)}): ").strip()
                table_index = int(choice) - 1
                if 0 <= table_index < len(tables):
                    table_name = tables[table_index]
                    break
                else:
                    print("❌ Неверный номер таблицы")
            except ValueError:
                print("❌ Введите число")
        
        print(f"✅ Выбрана таблица: {table_name}")
        
        # Получаем столбцы таблицы
        columns = get_columns(conn, table_name)
        print(f"\n📊 Столбцы в таблице '{table_name}':")
        for i, column in enumerate(columns, 1):
            print(f"  {i}. {column}")
        
        # Выбор столбца
        while True:
            try:
                choice = input(f"\nВыберите номер столбца с номерами (1-{len(columns)}): ").strip()
                column_index = int(choice) - 1
                if 0 <= column_index < len(columns):
                    source_column = columns[column_index]
                    break
                else:
                    print("❌ Неверный номер столбца")
            except ValueError:
                print("❌ Введите число")
        
        print(f"✅ Выбран столбец: {source_column}")
        
        # Выбор режима
        modes = {
            '1': ('combined', 'Комбинированный (рекомендуется)'),
            '2': ('clean', 'Только очистка префиксов'),
            '3': ('simple', 'Простое извлечение чисел')
        }
        
        print("\n🔧 Режимы обработки:")
        for key, (mode, description) in modes.items():
            print(f"  {key}. {description}")
        
        while True:
            choice = input("\nВыберите режим обработки (1-3): ").strip()
            if choice in modes:
                mode = modes[choice][0]
                break
            else:
                print("❌ Неверный выбор")
        
        print(f"✅ Выбран режим: {modes[choice][1]}")
        
        # Подтверждение
        print(f"\n📋 Параметры обработки:")
        print(f"  Таблица: {table_name}")
        print(f"  Столбец: {source_column}")
        print(f"  Режим: {modes[choice][1]}")
        print(f"  Целевой столбец: {source_column}_cleaned")
        
        confirm = input("\nПродолжить? (y/N): ").strip().lower()
        if confirm not in ['y', 'yes', 'да', 'д']:
            print("❌ Операция отменена")
            return
        
        # Выполнение обработки
        success, message = process_table_numbers(table_name, source_column, mode=mode)
        if success:
            print(f"\n🎉 {message}")
        else:
            print(f"\n❌ {message}")
    
    finally:
        conn.close()

def main():
    """Главная функция"""
    if len(sys.argv) > 1:
        # Командная строка
        if sys.argv[1] == '--help' or sys.argv[1] == '-h':
            print("""
🔧 Скрипт удаления префиксов S/F из номеров стыков

Использование:
  python prefix_remover.py                    # Интерактивный режим
  python prefix_remover.py --help            # Показать справку
  python prefix_remover.py <table> <column>  # Прямое выполнение

Примеры:
  python prefix_remover.py Log_Piping_PTO "Номер стыка"
  python prefix_remover.py defects "Joint Number"
            """)
            return
        
        if len(sys.argv) >= 3:
            table_name = sys.argv[1]
            source_column = sys.argv[2]
            target_column = sys.argv[3] if len(sys.argv) > 3 else None
            mode = sys.argv[4] if len(sys.argv) > 4 else 'combined'
            
            print(f"🔧 Обработка таблицы '{table_name}', столбец '{source_column}'")
            success, message = process_table_numbers(table_name, source_column, target_column, mode)
            if success:
                print(f"🎉 {message}")
            else:
                print(f"❌ {message}")
                sys.exit(1)
        else:
            print("❌ Недостаточно аргументов. Используйте --help для справки.")
            sys.exit(1)
    else:
        # Интерактивный режим
        interactive_mode()

if __name__ == "__main__":
    main()
