import sqlite3
import os
import re
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/update_titul_from_iso_direct_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
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

def extract_titul_from_iso_string(iso_string):
    """
    Извлекает данные титула из строки ISO
    Пример: GCC-NAG-DDD-12470-13-1400-TK-ISO-00008 -> 12470-13
    """
    if not iso_string:
        return None
    
    # Паттерн для извлечения номера титула
    # Ищем паттерн: число-число (например, 12470-13)
    pattern = r'(\d{5}-\d{2})'
    match = re.search(pattern, iso_string)
    
    if match:
        return match.group(1)
    
    # Альтернативный паттерн для других форматов
    pattern2 = r'(\d{4,5}-\d{1,2})'
    match2 = re.search(pattern2, iso_string)
    
    if match2:
        return match2.group(1)
    
    return None

def update_titul_in_database():
    """
    Обновляет столбец "Титул" в таблице pipeline_weld_joint_iso
    извлекая данные прямо из значений ISO
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
        
        # Получаем записи с пустым столбцом "Титул", но с заполненным "ISO"
        cursor.execute('SELECT id, "ISO", "Титул" FROM pipeline_weld_joint_iso WHERE ("Титул" IS NULL OR "Титул" = "") AND ("ISO" IS NOT NULL AND "ISO" != "")')
        records_to_update = cursor.fetchall()
        
        logger.info(f"Найдено {len(records_to_update)} записей для обновления столбца 'Титул'")
        
        if not records_to_update:
            logger.info("Нет записей для обновления")
            return True
        
        # Обновляем записи
        updated_count = 0
        not_found_count = 0
        
        for record_id, iso_value, current_titul in records_to_update:
            if not iso_value:
                continue
            
            # Извлекаем титул из строки ISO
            titul_value = extract_titul_from_iso_string(iso_value)
            
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
        
        # Показываем примеры записей для обновления
        cursor.execute('SELECT "ISO", "Титул" FROM pipeline_weld_joint_iso WHERE ("Титул" IS NULL OR "Титул" = "") AND ("ISO" IS NOT NULL AND "ISO" != "") LIMIT 5')
        empty_examples = cursor.fetchall()
        
        if empty_examples:
            logger.info("📋 Примеры записей для обновления:")
            for iso, titul in empty_examples:
                extracted_titul = extract_titul_from_iso_string(iso)
                logger.info(f"   ISO: '{iso}' -> Извлеченный титул: '{extracted_titul}'")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при получении статистики: {e}")
        return False

def test_extraction():
    """
    Тестирует функцию извлечения титула на примерах
    """
    test_cases = [
        "GCC-NAG-DDD-12470-13-1400-TK-ISO-00008",
        "GCC-NAG-DDD-12470-13-1400-TK-ISO-00010",
        "GCC-NAG-DDD-12460-12-1500-TK-ISO-00001",
        "GCC-NAG-DDD-12460-12-1500-TK-ISO-00002",
        "GCC-NAG-DDD-12470-13-1400-TK-ISO-00015"
    ]
    
    logger.info("🧪 Тестирование извлечения титула:")
    for test_case in test_cases:
        titul = extract_titul_from_iso_string(test_case)
        logger.info(f"   '{test_case}' -> '{titul}'")

if __name__ == "__main__":
    logger.info("🚀 Начинаем обновление столбца 'Титул' в таблице pipeline_weld_joint_iso")
    
    # Тестируем извлечение титула
    test_extraction()
    
    # Показываем текущую статистику
    logger.info("\n📊 Текущее состояние данных:")
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

