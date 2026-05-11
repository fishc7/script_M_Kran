
# Импортируем утилиты
try:
    # Пробуем относительный импорт (для запуска через web-приложение)
    from ..utilities.db_utils import get_database_path
    from ..utilities.path_utils import get_mk_kran_kingesepp_path
except ImportError:
    # Если не работает, используем абсолютный импорт (для прямого запуска)
    import sys
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    utilities_dir = os.path.join(os.path.dirname(current_dir), 'utilities')
    if utilities_dir not in sys.path:
        sys.path.insert(0, utilities_dir)
    
    from db_utils import get_database_path
    from path_utils import get_mk_kran_kingesepp_path
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для создания таблицы folder_NDT_Report с файлами из папки заключений НК
"""

import os
import sqlite3
import logging
from pathlib import Path
from datetime import datetime
import sys

# Добавляем путь к модулям проекта
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
utilities_dir = os.path.join(project_root, 'scripts', 'utilities')
if utilities_dir not in sys.path:
    sys.path.insert(0, utilities_dir)

try:
    from db_utils import get_database_path
    from path_utils import get_mk_kran_kingesepp_path
except ImportError:
    # Альтернативный импорт
    from scripts.utilities.db_utils import get_database_path
    from scripts.utilities.path_utils import get_mk_kran_kingesepp_path


def setup_logging():
    """Настройка логирования"""
    log_filename = f"logs/create_ndt_reports_table_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def get_ndt_reports_folder():
    """Получает путь к папке с заключениями НК"""
    mk_kran_path = get_mk_kran_kingesepp_path()
    ndt_folder = os.path.join(mk_kran_path, "НК", "Заключения")
    
    if not os.path.exists(ndt_folder):
        # Пробуем альтернативные пути
        alternative_paths = [
            os.path.join(mk_kran_path, "НК", "Заключения"),
            os.path.join(mk_kran_path, "НК", "Заключения НК"),
            os.path.join(mk_kran_path, "НК", "Отчеты"),
            os.path.join(mk_kran_path, "НК", "Reports"),
            os.path.join(mk_kran_path, "Заключения"),
            os.path.join(mk_kran_path, "Отчеты НК"),
        ]
        
        for path in alternative_paths:
            if os.path.exists(path):
                ndt_folder = path
                break
    
    return ndt_folder

def extract_conclusion_number(file_name):
    """Извлекает все числа из имени файла и объединяет их"""
    try:
        # Убираем расширение файла
        name_without_ext = os.path.splitext(file_name)[0]
        
        # Извлекаем все числа из имени файла и объединяем их
        import re
        numbers = re.findall(r'\d+', name_without_ext)
        if numbers:
            conclusion_number = ''.join(numbers)
            return conclusion_number
        else:
            return None
    except Exception:
        return None

def create_ndt_reports_table(db_path):
    """Создает таблицу folder_NDT_Report в базе данных"""
    logger = logging.getLogger(__name__)
    
    try:
        # Подключаемся к базе данных
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Создаем таблицу, если она не существует
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS folder_NDT_Report (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT NOT NULL,
            full_path TEXT NOT NULL,
            _Номер_заключений TEXT,
            created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_date DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        # Создаем индексы для быстрого поиска
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_file_name ON folder_NDT_Report(file_name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_full_path ON folder_NDT_Report(full_path)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_conclusion_number ON folder_NDT_Report(_Номер_заключений)")
        
        conn.commit()
        logger.info("✅ Таблица folder_NDT_Report создана/обновлена успешно")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка при создании таблицы: {e}")
        return False
    finally:
        if conn:
            conn.close()

def scan_ndt_reports_folder(folder_path):
    """Сканирует папку с заключениями и возвращает список файлов"""
    logger = logging.getLogger(__name__)
    
    if not os.path.exists(folder_path):
        logger.error(f"❌ Папка не найдена: {folder_path}")
        return []
    
    files_info = []
    
    try:
        # Поддерживаемые расширения файлов
        supported_extensions = ('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.txt', '.rtf')
        
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                file_ext = os.path.splitext(file)[1].lower()
                
                # Проверяем, что файл имеет поддерживаемое расширение
                if file_ext in supported_extensions:
                    try:
                        # Извлекаем номер заключения из имени файла
                        conclusion_number = extract_conclusion_number(file)
                        
                        files_info.append({
                            'file_name': file,
                            'full_path': file_path,
                            '_Номер_заключений': conclusion_number
                        })
                        
                    except Exception as e:
                        logger.warning(f"⚠️ Не удалось обработать файл {file_path}: {e}")
        
        logger.info(f"✅ Найдено {len(files_info)} файлов в папке {folder_path}")
        return files_info
        
    except Exception as e:
        logger.error(f"❌ Ошибка при сканировании папки: {e}")
        return []

def update_ndt_reports_table(db_path, files_info):
    """Обновляет таблицу folder_NDT_Report данными о файлах"""
    logger = logging.getLogger(__name__)
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Очищаем таблицу перед обновлением
        cursor.execute("DELETE FROM folder_NDT_Report")
        logger.info("🗑️ Таблица очищена")
        
        # Вставляем новые данные
        insert_sql = """
        INSERT INTO folder_NDT_Report 
        (file_name, full_path, _Номер_заключений, created_date, updated_date)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """
        
        inserted_count = 0
        for file_info in files_info:
            try:
                cursor.execute(insert_sql, (
                    file_info['file_name'],
                    file_info['full_path'],
                    file_info['_Номер_заключений']
                ))
                inserted_count += 1
                
            except Exception as e:
                logger.warning(f"⚠️ Не удалось вставить файл {file_info['file_name']}: {e}")
        
        conn.commit()
        logger.info(f"✅ Вставлено {inserted_count} записей в таблицу")
        
        return inserted_count
        
    except Exception as e:
        logger.error(f"❌ Ошибка при обновлении таблицы: {e}")
        return 0
    finally:
        if conn:
            conn.close()

def get_table_info(db_path):
    """Получает информацию о таблице folder_NDT_Report"""
    logger = logging.getLogger(__name__)
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получаем количество записей
        cursor.execute("SELECT COUNT(*) FROM folder_NDT_Report")
        count = cursor.fetchone()[0]
        
        # Получаем статистику по номерам заключений
        cursor.execute("""
            SELECT _Номер_заключений, COUNT(*) as count 
            FROM folder_NDT_Report 
            WHERE _Номер_заключений IS NOT NULL
            GROUP BY _Номер_заключений 
            ORDER BY count DESC
            LIMIT 10
        """)
        conclusion_stats = cursor.fetchall()
        
        # Получаем количество файлов без номера заключения
        cursor.execute("SELECT COUNT(*) FROM folder_NDT_Report WHERE _Номер_заключений IS NULL")
        no_number_count = cursor.fetchone()[0]
        
        conn.close()
        
        logger.info(f"📊 Статистика таблицы folder_NDT_Report:")
        logger.info(f"   Всего файлов: {count}")
        logger.info(f"   Файлов с номером заключения: {count - no_number_count}")
        logger.info(f"   Файлов без номера заключения: {no_number_count}")
        logger.info(f"   Топ-10 номеров заключений:")
        for number, number_count in conclusion_stats:
            logger.info(f"     {number}: {number_count} файлов")
        
        return {
            'total_files': count,
            'files_with_number': count - no_number_count,
            'files_without_number': no_number_count,
            'conclusion_stats': conclusion_stats
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка при получении информации о таблице: {e}")
        return None

def run_script():
    """Функция для запуска скрипта из главной формы"""
    return main()

def main():
    """Основная функция"""
    logger = setup_logging()
    
    logger.info("🚀 Начинаем создание таблицы folder_NDT_Report")
    
    try:
        # Получаем путь к базе данных
        db_path = get_database_path()
        if not db_path:
            logger.error("❌ Не удалось найти базу данных")
            return False
        
        logger.info(f"📁 База данных: {db_path}")
        
        # Создаем таблицу
        if not create_ndt_reports_table(db_path):
            return False
        
        # Получаем путь к папке с заключениями
        ndt_folder = get_ndt_reports_folder()
        if not ndt_folder or not os.path.exists(ndt_folder):
            logger.error(f"❌ Папка с заключениями не найдена: {ndt_folder}")
            return False
        
        logger.info(f"📁 Папка с заключениями: {ndt_folder}")
        
        # Сканируем папку
        files_info = scan_ndt_reports_folder(ndt_folder)
        if not files_info:
            logger.warning("⚠️ Файлы не найдены в папке заключений")
            return True
        
        # Обновляем таблицу
        inserted_count = update_ndt_reports_table(db_path, files_info)
        
        # Получаем статистику
        stats = get_table_info(db_path)
        
        logger.info("✅ Таблица folder_NDT_Report успешно создана и заполнена!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("✅ Скрипт выполнен успешно!")
    else:
        print("❌ Скрипт завершился с ошибкой!")
        sys.exit(1) 