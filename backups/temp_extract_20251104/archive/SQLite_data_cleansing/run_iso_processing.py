import sqlite3
import os
import re
import logging
from datetime import datetime
from typing import List, Tuple, Optional, Dict, Any
import pandas as pd

# Настройка логирования
def setup_logging():
    """Настройка системы логирования"""
    log_filename = f"logs/iso_processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def get_database_path():
    """Получает правильный путь к базе данных"""
    current_dir = os.getcwd()
    logger = logging.getLogger(__name__)
    logger.info(f"Текущая директория: {current_dir}")
    
    possible_paths = [
        os.path.join(current_dir, 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        os.path.join(current_dir, '..', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        os.path.join(current_dir, '..', '..', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        os.path.join(current_dir, 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
    ]
    
    for path in possible_paths:
        abs_path = os.path.abspath(path)
        logger.info(f"Проверяем путь: {abs_path}")
        if os.path.exists(abs_path):
            logger.info(f"Найдена база данных: {abs_path}")
            return abs_path
    
    logger.error("База данных не найдена, используем исходный путь")
    return 'BD_Kingisepp/M_Kran_Kingesepp.db'

def transform_drawing_number(drawing: str) -> str:
    """Преобразует номер чертежа для создания ключа"""
    if not drawing:
        return drawing
    
    # Сначала удаляем всё в скобках, включая сами скобки
    drawing = re.sub(r'\([^)]*\)', '', drawing)
    
    # Разделяем по дефисам
    parts = drawing.split('-')
    if len(parts) == 3:
        # Удаляем ведущие нули из всех частей
        parts = [part.lstrip('0') or '0' for part in parts]
        # Соединяем обратно с дефисами
        drawing = '-'.join(parts)
    
    # Удаляем все пробелы и переносы строк
    return re.sub(r'\s+', '', drawing)

def clean_spaces(text: str) -> str:
    """Очищает пробелы из текста"""
    if not text:
        return text
    # Удаляем все типы пробелов, включая неразрывные пробелы и переносы строк
    return re.sub(r'\s+', '', text)

def replace_russian_to_english(text: str) -> str:
    """
    Заменяет русские буквы на похожие английские в столбце ЛИНИЯ
    
    Args:
        text: Исходный текст
        
    Returns:
        str: Текст с замененными буквами
    """
    if not text or not isinstance(text, str):
        return text
    
    # Словарь замен русских букв на английские
    russian_to_english = {
        'А': 'A', 'а': 'a',
        'В': 'B', 'в': 'b', 
        'Е': 'E', 'е': 'e',
        'К': 'K', 'к': 'k',
        'М': 'M', 'м': 'm',
        'Н': 'H', 'н': 'h',
        'О': 'O', 'о': 'o',
        'Р': 'P', 'р': 'p',
        'С': 'C', 'с': 'c',
        'Т': 'T', 'т': 't',
        'У': 'Y', 'у': 'y',
        'Х': 'X', 'х': 'x'
    }
    
    # Заменяем русские буквы на английские
    result = text
    for russian, english in russian_to_english.items():
        result = result.replace(russian, english)
    
    return result

def remove_brackets_numbers(text: str) -> str:
    """
    Удаляет номера в скобках из значений в столбце ЛИНИЯ
    
    Args:
        text: Исходный текст
        
    Returns:
        str: Текст без номеров в скобках
    """
    if not text or not isinstance(text, str):
        return text
    
    # Удаляем номера в скобках в конце строки
    # Паттерн: (число) в конце строки
    result = re.sub(r'\(\d+\)$', '', text.strip())
    
    return result

def process_line_column(text) -> str:
    """
    Полная обработка столбца ЛИНИЯ: очистка пробелов + замена русских букв + удаление номеров в скобках
    
    Args:
        text: Исходный текст
        
    Returns:
        str: Обработанный текст
    """
    if not text or not isinstance(text, str):
        return str(text) if text is not None else ""
    
    # Шаг 1: Очистка пробелов
    result = clean_spaces(text)
    
    # Шаг 2: Замена русских букв на английские
    result = replace_russian_to_english(result)
    
    # Шаг 3: Удаление номеров в скобках в конце строки
    result = remove_brackets_numbers(result)
    
    return result

def validate_table_structure(cursor, table_name: str, required_columns: List[str]) -> Tuple[bool, List[str]]:
    """Проверяет структуру таблицы и возвращает недостающие столбцы"""
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = [col[1] for col in cursor.fetchall()]
    missing_columns = [col for col in required_columns if col not in existing_columns]
    return len(missing_columns) == 0, missing_columns

def create_backup_table(cursor, table_name: str) -> str:
    """Создает резервную копию таблицы"""
    backup_table_name = f"{table_name}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    cursor.execute(f"CREATE TABLE {backup_table_name} AS SELECT * FROM {table_name}")
    return backup_table_name

def get_table_statistics(cursor, table_name: str) -> Dict[str, Any]:
    """Получает статистику по таблице"""
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    total_rows = cursor.fetchone()[0]
    
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [col[1] for col in cursor.fetchall()]
    
    return {
        'total_rows': total_rows,
        'columns': columns,
        'column_count': len(columns)
    }

def step1_generate_keys(cursor) -> bool:
    """Шаг 1: Генерация ключей для отчета от мастеров"""
    logger = logging.getLogger(__name__)
    logger.info("\n" + "="*60)
    logger.info("ШАГ 1: Генератор ключей отчет от мастеров")
    logger.info("🔄 Обработка столбца ЛИНИЯ: очистка пробелов + замена русских букв + удаление номеров в скобках")
    logger.info("="*60)
    
    # Проверяем существование таблицы
    cursor.execute('SELECT name FROM sqlite_master WHERE type="table" AND name="wl_report_smr"')
    if not cursor.fetchone():
        logger.error("❌ Таблица 'wl_report_smr' не существует!")
        return False
    
    # Создаем резервную копию
    backup_table = create_backup_table(cursor, 'wl_report_smr')
    logger.info(f"✅ Создана резервная копия: {backup_table}")
    
    # Получаем статистику таблицы
    stats = get_table_statistics(cursor, 'wl_report_smr')
    logger.info(f"📊 Статистика таблицы wl_report_smr:")
    logger.info(f"   - Всего записей: {stats['total_rows']}")
    logger.info(f"   - Столбцов: {stats['column_count']}")
    
    # Добавляем столбец ключь_жср_смр, если его нет
    try:
        cursor.execute('ALTER TABLE wl_report_smr ADD COLUMN ключь_жср_смр TEXT')
        logger.info("✅ Добавлен новый столбец 'ключь_жср_смр'")
    except sqlite3.OperationalError:
        logger.info("ℹ️ Столбец 'ключь_жср_смр' уже существует")
    
    # Получаем все записи из таблицы
    cursor.execute('SELECT id_smr, Чертеж, ЛИНИЯ FROM wl_report_smr')
    records = cursor.fetchall()
    logger.info(f"📊 Найдено записей для обработки: {len(records)}")
    
    # Статистика обработки
    processed_count = 0
    drawing_processed = 0
    line_processed = 0
    errors_count = 0
    
    # Обновляем каждую запись с преобразованным номером чертежа и очищенной ЛИНИЕЙ
    for i, record in enumerate(records, 1):
        try:
            number, drawing, line = record
            updates = []
            params = []
            
            if drawing:
                transformed_drawing = transform_drawing_number(drawing)
                updates.append('ключь_жср_смр = ?')
                params.append(transformed_drawing)
                drawing_processed += 1
            
            if line:
                processed_line = process_line_column(line)
                updates.append('ЛИНИЯ = ?')
                params.append(processed_line)
                line_processed += 1
            
            if updates:
                query = f'UPDATE wl_report_smr SET {", ".join(updates)} WHERE id_smr = ?'
                params.append(number)
                cursor.execute(query, params)
                processed_count += 1
            
            # Логируем прогресс каждые 1000 записей
            if i % 1000 == 0:
                logger.info(f"   Обработано записей: {i}/{len(records)} ({i/len(records)*100:.1f}%)")
                
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке записи {i}: {e}")
            errors_count += 1
    
    logger.info(f"✅ Обработка завершена:")
    logger.info(f"   - Обработано записей: {processed_count}")
    logger.info(f"   - Обработано чертежей: {drawing_processed}")
    logger.info(f"   - Обработано линий: {line_processed}")
    logger.info(f"   - Ошибок: {errors_count}")
    
    return True

def step2_extract_iso_data(cursor) -> bool:
    """Шаг 2: Извлечение данных ISO из Log_Piping_PTO"""
    logger = logging.getLogger(__name__)
    logger.info("\n" + "="*60)
    logger.info("ШАГ 2: Извлечение данных ISO из Log_Piping_PTO")
    logger.info("="*60)
    
    # Проверяем существование таблиц
    cursor.execute('SELECT name FROM sqlite_master WHERE type="table" AND name="wl_report_smr"')
    if not cursor.fetchone():
        logger.error("❌ Таблица 'wl_report_smr' не существует!")
        return False
    
    cursor.execute('SELECT name FROM sqlite_master WHERE type="table" AND name="Log_Piping_PTO"')
    if not cursor.fetchone():
        logger.error("❌ Таблица 'Log_Piping_PTO' не существует!")
        return False
    
    # Получаем статистику таблиц
    wl_stats = get_table_statistics(cursor, 'wl_report_smr')
    log_stats = get_table_statistics(cursor, 'Log_Piping_PTO')
    
    logger.info(f"📊 Статистика таблиц:")
    logger.info(f"   - wl_report_smr: {wl_stats['total_rows']} записей, {wl_stats['column_count']} столбцов")
    logger.info(f"   - Log_Piping_PTO: {log_stats['total_rows']} записей, {log_stats['column_count']} столбцов")
    
    # Добавляем столбец _ISO, если его нет
    try:
        cursor.execute('ALTER TABLE wl_report_smr ADD COLUMN _ISO TEXT')
        logger.info("✅ Добавлен новый столбец '_ISO' в wl_report_smr")
    except sqlite3.OperationalError:
        logger.info("ℹ️ Столбец '_ISO' уже существует в wl_report_smr")
    
    # Проверяем наличие необходимых столбцов
    required_wl_columns = ['ключь_жср_смр', 'ЛИНИЯ']
    required_log_columns = ['ключь_жср_смр', 'Линия', 'ISO']
    
    wl_valid, missing_wl = validate_table_structure(cursor, 'wl_report_smr', required_wl_columns)
    log_valid, missing_log = validate_table_structure(cursor, 'Log_Piping_PTO', required_log_columns)
    
    if not wl_valid:
        logger.error(f"❌ В таблице wl_report_smr отсутствуют столбцы: {missing_wl}")
        return False
        
    if not log_valid:
        logger.error(f"❌ В таблице Log_Piping_PTO отсутствуют столбцы: {missing_log}")
        return False
    
    logger.info("✅ Все необходимые столбцы найдены")
    
    # Анализируем данные перед обновлением
    logger.info("🔍 Анализ данных перед обновлением...")
    
    # Проверяем количество записей с ключами в обеих таблицах
    cursor.execute("SELECT COUNT(*) FROM wl_report_smr WHERE ключь_жср_смр IS NOT NULL AND ключь_жср_смр != ''")
    wl_with_keys = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM Log_Piping_PTO WHERE ключь_жср_смр IS NOT NULL AND ключь_жср_смр != ''")
    log_with_keys = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM Log_Piping_PTO WHERE ISO IS NOT NULL AND ISO != ''")
    log_with_iso = cursor.fetchone()[0]
    
    logger.info(f"📊 Анализ данных:")
    logger.info(f"   - wl_report_smr с ключами: {wl_with_keys}")
    logger.info(f"   - Log_Piping_PTO с ключами: {log_with_keys}")
    logger.info(f"   - Log_Piping_PTO с ISO: {log_with_iso}")
    
    # Получаем количество записей для обновления
    cursor.execute("""
        SELECT COUNT(*) 
        FROM wl_report_smr wl
        INNER JOIN Log_Piping_PTO log 
        ON wl.ключь_жср_смр = log.ключь_жср_смр 
        AND wl.ЛИНИЯ = log.Линия
        WHERE log.ISO IS NOT NULL AND log.ISO != ''
    """)
    count_to_update = cursor.fetchone()[0]
    logger.info(f"📊 Найдено записей для обновления: {count_to_update}")
    
    if count_to_update == 0:
        logger.warning("⚠️ Нет записей для обновления")
        return True
    
    # Показываем примеры сопоставлений
    cursor.execute("""
        SELECT wl.ключь_жср_смр, wl.ЛИНИЯ, log.ISO
        FROM wl_report_smr wl
        INNER JOIN Log_Piping_PTO log 
        ON wl.ключь_жср_смр = log.ключь_жср_смр 
        AND wl.ЛИНИЯ = log.Линия
        WHERE log.ISO IS NOT NULL AND log.ISO != ''
        LIMIT 5
    """)
    
    examples = cursor.fetchall()
    if examples:
        logger.info("\n📋 Примеры сопоставлений:")
        for i, example in enumerate(examples, 1):
            logger.info(f"  {i}. ключь: {example[0]}, линия: {example[1]}, ISO: {example[2]}")
    
    # Выполняем обновление
    logger.info("🔄 Начинаем обновление данных...")
    cursor.execute("""
        UPDATE wl_report_smr 
        SET _ISO = (
            SELECT log.ISO 
            FROM Log_Piping_PTO log 
            WHERE wl_report_smr.ключь_жср_смр = log.ключь_жср_смр 
            AND wl_report_smr.ЛИНИЯ = log.Линия
            AND log.ISO IS NOT NULL 
            AND log.ISO != ''
        )
        WHERE EXISTS (
            SELECT 1 
            FROM Log_Piping_PTO log 
            WHERE wl_report_smr.ключь_жср_смр = log.ключь_жср_смр 
            AND wl_report_smr.ЛИНИЯ = log.Линия
            AND log.ISO IS NOT NULL 
            AND log.ISO != ''
        )
    """)
    
    updated_rows = cursor.rowcount
    logger.info(f"✅ Обновлено записей: {updated_rows}")
    
    # Проверяем результат
    cursor.execute("SELECT COUNT(*) FROM wl_report_smr WHERE _ISO IS NOT NULL AND _ISO != ''")
    final_count = cursor.fetchone()[0]
    logger.info(f"📊 Итоговое количество записей с ISO: {final_count}")
    
    # Показываем примеры обновленных записей
    cursor.execute("""
        SELECT wl.ключь_жср_смр, wl.ЛИНИЯ, wl._ISO
        FROM wl_report_smr wl
        WHERE wl._ISO IS NOT NULL AND wl._ISO != ''
        LIMIT 3
    """)
    
    examples = cursor.fetchall()
    if examples:
        logger.info("\n📋 Примеры обновленных записей:")
        for i, example in enumerate(examples, 1):
            logger.info(f"  {i}. ключь_жср_смр: {example[0]}, ЛИНИЯ: {example[1]}, _ISO: {example[2]}")
    
    return True

def step3_fill_iso_from_excel(cursor):
    """Шаг 3: Заполнение _ISO из Excel, если оно пустое"""
    logger = logging.getLogger(__name__)
    logger.info("\n" + "="*60)
    logger.info("ШАГ 3: Заполнение _ISO из Excel для пустых значений")
    logger.info("="*60)

    excel_path = r'D:\МК_Кран\МК_Кран_Кингесеп\ОГС\Черновики\ISO_для_СМР.xlsx'
    if not os.path.exists(excel_path):
        logger.error(f"❌ Excel-файл не найден: {excel_path}")
        return False

    # Читаем Excel-файл
    try:
        df = pd.read_excel(excel_path, dtype=str)
        logger.info(f"📄 Прочитано строк из Excel: {len(df)}")
    except Exception as e:
        logger.error(f"❌ Ошибка при чтении Excel: {e}")
        return False

    # Очищаем пробелы в ключевых столбцах
    for col in ['ЛИНИЯ', 'ключь_жср_смр', 'Титул']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    if 'ISO' not in df.columns:
        logger.error("❌ В Excel-файле нет столбца 'ISO'")
        return False

    # Получаем записи из базы, где _ISO пустое
    cursor.execute("""
        SELECT rowid, ЛИНИЯ, ключь_жср_смр, Титул FROM wl_report_smr
        WHERE (_ISO IS NULL OR _ISO = '')
    """)
    records = cursor.fetchall()
    logger.info(f"🔍 Найдено строк с пустым _ISO: {len(records)}")

    # Индексируем Excel по ключу для быстрого поиска
    excel_index = df.set_index(['ЛИНИЯ', 'ключь_жср_смр', 'Титул'])
    updated = 0
    for rowid, line, key, title in records:
        try:
            excel_iso = excel_index.loc[(str(line).strip(), str(key).strip(), str(title).strip()), 'ISO']
            if isinstance(excel_iso, pd.Series):
                excel_iso = excel_iso.iloc[0]
            if pd.notna(excel_iso) and str(excel_iso).strip():
                cursor.execute('UPDATE wl_report_smr SET _ISO = ? WHERE rowid = ?', (str(excel_iso).strip(), rowid))
                updated += 1
        except KeyError:
            continue  # Нет совпадения — пропускаем
        except Exception as e:
            logger.error(f"Ошибка при обновлении rowid={rowid}: {e}")
    logger.info(f"✅ Обновлено строк из Excel: {updated}")
    return True

def step3_validation_and_cleanup(cursor) -> bool:
    """Шаг 3: Валидация и очистка данных"""
    logger = logging.getLogger(__name__)
    logger.info("\n" + "="*60)
    logger.info("ШАГ 3: Валидация и очистка данных")
    logger.info("="*60)
    
    # Проверяем целостность данных
    logger.info("🔍 Проверка целостности данных...")
    
    # Проверяем дубликаты ключей
    cursor.execute("""
        SELECT ключь_жср_смр, ЛИНИЯ, COUNT(*) as count
        FROM wl_report_smr 
        WHERE ключь_жср_смр IS NOT NULL AND ключь_жср_смр != ''
        GROUP BY ключь_жср_смр, ЛИНИЯ
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 10
    """)
    
    duplicates = cursor.fetchall()
    if duplicates:
        logger.warning(f"⚠️ Найдено {len(duplicates)} групп дубликатов ключей:")
        for dup in duplicates:
            logger.warning(f"   - ключь: {dup[0]}, линия: {dup[1]}, количество: {dup[2]}")
    else:
        logger.info("✅ Дубликаты ключей не найдены")
    
    # Проверяем записи без ISO
    cursor.execute("""
        SELECT COUNT(*) 
        FROM wl_report_smr 
        WHERE ключь_жср_смр IS NOT NULL AND ключь_жср_смр != ''
        AND (_ISO IS NULL OR _ISO = '')
    """)
    
    missing_iso = cursor.fetchone()[0]
    logger.info(f"📊 Записей без ISO: {missing_iso}")
    
    # Показываем примеры записей без ISO
    if missing_iso > 0:
        cursor.execute("""
            SELECT ключь_жср_смр, ЛИНИЯ
            FROM wl_report_smr 
            WHERE ключь_жср_смр IS NOT NULL AND ключь_жср_смр != ''
            AND (_ISO IS NULL OR _ISO = '')
            LIMIT 5
        """)
        
        examples = cursor.fetchall()
        logger.info("📋 Примеры записей без ISO:")
        for i, example in enumerate(examples, 1):
            logger.info(f"  {i}. ключь: {example[0]}, линия: {example[1]}")
    
    return True

def generate_final_report(cursor) -> Dict[str, Any]:
    """Генерирует итоговый отчет"""
    logger = logging.getLogger(__name__)
    
    # Получаем финальную статистику
    cursor.execute("SELECT COUNT(*) FROM wl_report_smr")
    total_records = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM wl_report_smr WHERE ключь_жср_смр IS NOT NULL AND ключь_жср_смр != ''")
    records_with_keys = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM wl_report_smr WHERE _ISO IS NOT NULL AND _ISO != ''")
    records_with_iso = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT _ISO) FROM wl_report_smr WHERE _ISO IS NOT NULL AND _ISO != ''")
    unique_iso_count = cursor.fetchone()[0]
    
    report = {
        'total_records': total_records,
        'records_with_keys': records_with_keys,
        'records_with_iso': records_with_iso,
        'unique_iso_count': unique_iso_count,
        'coverage_percentage': (records_with_iso / records_with_keys * 100) if records_with_keys > 0 else 0
    }
    
    logger.info("\n" + "="*60)
    logger.info("ИТОГОВЫЙ ОТЧЕТ")
    logger.info("="*60)
    logger.info(f"📊 Общая статистика:")
    logger.info(f"   - Всего записей: {report['total_records']}")
    logger.info(f"   - Записей с ключами: {report['records_with_keys']}")
    logger.info(f"   - Записей с ISO: {report['records_with_iso']}")
    logger.info(f"   - Уникальных ISO: {report['unique_iso_count']}")
    logger.info(f"   - Покрытие ISO: {report['coverage_percentage']:.1f}%")
    
    return report

def main():
    """Главная функция для обработки ISO данных"""
    # Настройка логирования
    logger = setup_logging()
    
    logger.info("🚀 ЗАПУСК ПОСЛЕДОВАТЕЛЬНОЙ ОБРАБОТКИ ISO ДАННЫХ")
    logger.info("="*60)
    
    # Получаем правильный путь к базе данных
    db_path = get_database_path()
    
    # Подключаемся к базе данных
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Включаем внешние ключи и оптимизируем
        cursor.execute("PRAGMA foreign_keys = ON")
        cursor.execute("PRAGMA journal_mode = WAL")
        cursor.execute("PRAGMA synchronous = NORMAL")
        cursor.execute("PRAGMA cache_size = 10000")
        cursor.execute("PRAGMA temp_store = MEMORY")
        
        # Выполняем все шаги
        step1_success = step1_generate_keys(cursor)
        step2_success = step2_extract_iso_data(cursor)
        step3_excel_success = step3_fill_iso_from_excel(cursor)
        step4_success = step3_validation_and_cleanup(cursor)
        
        # Сохраняем изменения
        conn.commit()
        
        # Генерируем итоговый отчет
        final_report = generate_final_report(cursor)
        
        # Итоговый статус
        logger.info(f"\n{'='*60}")
        logger.info("СТАТУС ВЫПОЛНЕНИЯ")
        logger.info(f"{'='*60}")
        
        if step1_success and step2_success and step3_excel_success and step4_success:
            logger.info("🎉 ВСЕ ОПЕРАЦИИ ВЫПОЛНЕНЫ УСПЕШНО!")
            logger.info("✅ Столбец 'ключь_жср_смр' создан и заполнен")
            logger.info("✅ Столбец '_ISO' создан и заполнен данными из Log_Piping_PTO и Excel")
            logger.info("✅ Валидация и очистка данных выполнены")
        else:
            logger.warning("⚠️ НЕ ВСЕ ОПЕРАЦИИ ВЫПОЛНЕНЫ УСПЕШНО")
            if not step1_success:
                logger.error("❌ Ошибка в шаге 1: Генерация ключей")
            if not step2_success:
                logger.error("❌ Ошибка в шаге 2: Извлечение ISO данных")
            if not step3_excel_success:
                logger.error("❌ Ошибка в шаге 3: Заполнение из Excel")
            if not step4_success:
                logger.error("❌ Ошибка в шаге 4: Валидация и очистка")
        
        logger.info(f"{'='*60}")

    except Exception as e:
        logger.error(f"❌ Произошла критическая ошибка: {e}")
        import traceback
        logger.error("Полный стек ошибки:")
        logger.error(traceback.format_exc())
        # Откатываем изменения при ошибке
        conn.rollback()
        logger.info("🔄 Изменения откачены")
    finally:
        conn.close()
        logger.info("🔒 Соединение с базой данных закрыто")

def run_script():
    """Функция для запуска скрипта через лаунчер"""
    main()

if __name__ == '__main__':
    main() 