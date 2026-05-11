import sqlite3
import os
import re
import logging
from datetime import datetime
import pandas as pd

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/update_titul_from_iso_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_database_connection():
    """Получает подключение к базе данных"""
    try:
        # Пробуем разные пути к базе данных
        possible_paths = [
            'database/BD_Kingisepp/M_Kran_Kingesepp.db',
            '../database/BD_Kingisepp/M_Kran_Kingesepp.db',
            'BD_Kingisepp/M_Kran_Kingesepp.db',
            '../BD_Kingisepp/M_Kran_Kingesepp.db'
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                logger.info(f"Найдена база данных: {path}")
                return sqlite3.connect(path)
        
        logger.error("База данных не найдена!")
        return None
        
    except Exception as e:
        logger.error(f"Ошибка подключения к базе данных: {e}")
        return None

def extract_titul_from_iso(iso_filename):
    """
    Извлекает данные титула из имени ISO файла
    Пример: GCC-NAG-DDD-12470-13-1400-TK-ISO-00008 -> 12470-13
    """
    if not iso_filename:
        return None
    
    # Паттерн для извлечения номера титула
    # Ищем паттерн: число-число (например, 12470-13)
    pattern = r'(\d{5}-\d{2})'
    match = re.search(pattern, iso_filename)
    
    if match:
        return match.group(1)
    
    # Альтернативный паттерн для других форматов
    pattern2 = r'(\d{4,5}-\d{1,2})'
    match2 = re.search(pattern2, iso_filename)
    
    if match2:
        return match2.group(1)
    
    return None

def get_iso_files_from_directory(directory_path):
    """
    Получает список ISO файлов из директории
    """
    iso_files = []
    
    if not os.path.exists(directory_path):
        logger.warning(f"Директория не найдена: {directory_path}")
        return iso_files
    
    try:
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                # Ищем файлы с расширениями ISO, PDF или другие технические файлы
                if any(ext in file.upper() for ext in ['.ISO', '.PDF', '.DWG', '.DXF']):
                    # Проверяем, содержит ли имя файла паттерн титула
                    titul = extract_titul_from_iso(file)
                    if titul:
                        iso_files.append({
                            'filename': file,
                            'filepath': os.path.join(root, file),
                            'titul': titul
                        })
        
        logger.info(f"Найдено {len(iso_files)} ISO файлов с данными титула")
        return iso_files
        
    except Exception as e:
        logger.error(f"Ошибка при поиске ISO файлов: {e}")
        return []

def update_titul_in_database():
    """
    Обновляет столбец "Титул" в таблице pipeline_weld_joint_iso
    """
    try:
        # Подключаемся к базе данных
        conn = get_database_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Проверяем существование таблицы
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pipeline_weld_joint_iso'")
        if not cursor.fetchone():
            logger.error("Таблица pipeline_weld_joint_iso не существует!")
            return False
        
        # Получаем текущие данные из таблицы
        cursor.execute('SELECT id, "ISO", "Титул" FROM pipeline_weld_joint_iso WHERE "Титул" IS NULL OR "Титул" = ""')
        records_to_update = cursor.fetchall()
        
        logger.info(f"Найдено {len(records_to_update)} записей для обновления столбца 'Титул'")
        
        if not records_to_update:
            logger.info("Нет записей для обновления")
            return True
        
        # Получаем ISO файлы
        iso_directory = r"D:\МК_Кран\МК_Кран_Кингесеп\ПТО\номерация стыков по iso"
        iso_files = get_iso_files_from_directory(iso_directory)
        
        # Создаем словарь соответствия ISO -> Титул
        iso_to_titul = {}
        for iso_file in iso_files:
            iso_to_titul[iso_file['filename']] = iso_file['titul']
        
        logger.info(f"Создан словарь соответствия с {len(iso_to_titul)} записями")
        
        # Обновляем записи
        updated_count = 0
        not_found_count = 0
        
        for record_id, iso_value, current_titul in records_to_update:
            if not iso_value:
                continue
            
            # Ищем соответствие в словаре ISO файлов
            titul_value = None
            
            # Пробуем найти точное совпадение
            if iso_value in iso_to_titul:
                titul_value = iso_to_titul[iso_value]
            else:
                # Пробуем найти частичное совпадение
                for iso_filename, titul in iso_to_titul.items():
                    if iso_value in iso_filename or iso_filename in iso_value:
                        titul_value = titul
                        break
            
            if titul_value:
                # Обновляем запись
                cursor.execute(
                    'UPDATE pipeline_weld_joint_iso SET "Титул" = ? WHERE id = ?',
                    (titul_value, record_id)
                )
                updated_count += 1
                
                if updated_count <= 10:  # Показываем первые 10 обновлений
                    logger.info(f"✅ ID {record_id}: ISO '{iso_value}' -> Титул '{titul_value}'")
            else:
                not_found_count += 1
                if not_found_count <= 10:  # Показываем первые 10 ненайденных
                    logger.info(f"❌ ID {record_id}: ISO '{iso_value}' - титул не найден")
        
        # Сохраняем изменения
        conn.commit()
        
        logger.info(f"📊 Результаты обновления:")
        logger.info(f"   - Обновлено записей: {updated_count}")
        logger.info(f"   - Не найдено соответствий: {not_found_count}")
        logger.info(f"   - Всего обработано: {len(records_to_update)}")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при обновлении данных: {e}")
        import traceback
        logger.error("Полный стек ошибки:")
        logger.error(traceback.format_exc())
        return False

def show_current_titul_data():
    """
    Показывает текущие данные столбца "Титул" в таблице
    """
    try:
        conn = get_database_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Получаем статистику по столбцу "Титул"
        cursor.execute('SELECT COUNT(*) FROM pipeline_weld_joint_iso')
        total_records = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM pipeline_weld_joint_iso WHERE "Титул" IS NOT NULL AND "Титул" != ""')
        filled_records = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM pipeline_weld_joint_iso WHERE "Титул" IS NULL OR "Титул" = ""')
        empty_records = cursor.fetchone()[0]
        
        logger.info(f"📊 Статистика столбца 'Титул':")
        logger.info(f"   - Всего записей: {total_records}")
        logger.info(f"   - Заполнено: {filled_records}")
        logger.info(f"   - Пустых: {empty_records}")
        
        # Показываем примеры заполненных записей
        cursor.execute('SELECT "ISO", "Титул" FROM pipeline_weld_joint_iso WHERE "Титул" IS NOT NULL AND "Титул" != "" LIMIT 5')
        filled_examples = cursor.fetchall()
        
        if filled_examples:
            logger.info("📋 Примеры заполненных записей:")
            for iso, titul in filled_examples:
                logger.info(f"   ISO: '{iso}' -> Титул: '{titul}'")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при получении статистики: {e}")
        return False

if __name__ == "__main__":
    logger.info("🚀 Начинаем обновление столбца 'Титул' в таблице pipeline_weld_joint_iso")
    
    # Показываем текущую статистику
    logger.info("📊 Текущее состояние данных:")
    show_current_titul_data()
    
    # Обновляем данные
    logger.info("\n🔄 Начинаем обновление...")
    success = update_titul_in_database()
    
    if success:
        logger.info("\n✅ Обновление завершено успешно!")
        
        # Показываем итоговую статистику
        logger.info("\n📊 Итоговое состояние данных:")
        show_current_titul_data()
    else:
        logger.error("\n❌ Обновление завершено с ошибками!")

