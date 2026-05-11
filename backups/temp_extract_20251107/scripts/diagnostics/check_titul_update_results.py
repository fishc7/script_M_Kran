import sqlite3
import os
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_database_connection():
    """Получает подключение к базе данных"""
    try:
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

def check_titul_update_results():
    """
    Проверяет результаты обновления столбца "Титул"
    """
    try:
        conn = get_database_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Общая статистика
        cursor.execute('SELECT COUNT(*) FROM pipeline_weld_joint_iso')
        total_records = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM pipeline_weld_joint_iso WHERE "Титул" IS NOT NULL AND "Титул" != ""')
        filled_records = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM pipeline_weld_joint_iso WHERE "Титул" IS NULL OR "Титул" = ""')
        empty_records = cursor.fetchone()[0]
        
        logger.info("="*60)
        logger.info("[STATS] РЕЗУЛЬТАТЫ ОБНОВЛЕНИЯ СТОЛБЦА 'ТИТУЛ'")
        logger.info("="*60)
        logger.info(f"   - Всего записей: {total_records}")
        logger.info(f"   - Заполнено: {filled_records}")
        logger.info(f"   - Пустых: {empty_records}")
        logger.info(f"   - Процент заполнения: {(filled_records/total_records)*100:.1f}%")
        
        # Статистика по титулам
        cursor.execute('SELECT "Титул", COUNT(*) as count FROM pipeline_weld_joint_iso WHERE "Титул" IS NOT NULL AND "Титул" != "" GROUP BY "Титул" ORDER BY count DESC')
        titul_stats = cursor.fetchall()
        
        logger.info("\n[STATS] Статистика по титулам:")
        for titul, count in titul_stats:
            logger.info(f"   - {titul}: {count} записей")
        
        # Примеры обновленных записей
        logger.info("\n[EXAMPLES] Примеры обновленных записей:")
        cursor.execute('SELECT "ISO", "Титул", "стык" FROM pipeline_weld_joint_iso WHERE "Титул" IS NOT NULL AND "Титул" != "" ORDER BY "Титул", "стык" LIMIT 10')
        examples = cursor.fetchall()
        
        for iso, titul, styuk in examples:
            logger.info(f"   ISO: '{iso}' -> Титул: '{titul}' -> Стык: '{styuk}'")
        
        # Проверяем конкретные примеры из вашего запроса
        logger.info("\n[CHECK] Проверка конкретных примеров:")
        test_cases = [
            "GCC-NAG-DDD-12470-13-1400-TK-ISO-00008",
            "GCC-NAG-DDD-12470-13-1400-TK-ISO-00010",
            "GCC-NAG-DDD-12460-12-1500-TK-ISO-00001"
        ]
        
        for test_iso in test_cases:
            cursor.execute('SELECT "Титул", "стык" FROM pipeline_weld_joint_iso WHERE "ISO" = ?', (test_iso,))
            result = cursor.fetchone()
            if result:
                titul, styuk = result
                logger.info(f"   [OK] '{test_iso}' -> Титул: '{titul}', Стык: '{styuk}'")
            else:
                logger.info(f"   [ERROR] '{test_iso}' -> не найден в базе")
        
        # Проверяем записи с титулом 12470-13
        logger.info("\n[CHECK] Записи с титулом '12470-13':")
        cursor.execute('SELECT "ISO", "стык" FROM pipeline_weld_joint_iso WHERE "Титул" = "12470-13" ORDER BY "стык" LIMIT 10')
        records_12470_13 = cursor.fetchall()
        
        for iso, styuk in records_12470_13:
            logger.info(f"   ISO: '{iso}' -> Стык: '{styuk}'")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при проверке результатов: {e}")
        import traceback
        logger.error("Полный стек ошибки:")
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    logger.info("[CHECK] Проверяем результаты обновления столбца 'Титул'")
    success = check_titul_update_results()
    
    if success:
        logger.info("\n[OK] Проверка завершена успешно!")
    else:
        logger.error("\n[ERROR] Проверка завершена с ошибками!")

