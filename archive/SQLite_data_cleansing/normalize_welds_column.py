import subprocess
import sys
import os
import sqlite3
import re
import logging

# Настройка кодировки для правильной работы с кириллическими символами
if sys.platform.startswith('win'):
    import locale
    try:
        # Устанавливаем кодировку для Windows
        locale.setlocale(locale.LC_ALL, 'Russian_Russia.1251')
    except:
        try:
            locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')
        except:
            pass
    
    # Настройка переменных окружения для правильной кодировки
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    os.environ['PYTHONLEGACYWINDOWSSTDIO'] = 'utf-8'

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def safe_print_path(path):
    """Безопасно выводит путь с правильной кодировкой"""
    try:
        # Пытаемся вывести путь как есть
        return str(path)
    except UnicodeEncodeError:
        try:
            # Если не получается, пробуем с кодировкой utf-8
            return path.encode('utf-8', errors='replace').decode('utf-8')
        except:
            # В крайнем случае, показываем только имя файла
            return os.path.basename(path)

def extract_joint_number(joint_text):
    """
    Извлекает номер стыка из текста
    Поддерживает форматы: 'S01', 'S02', '123', 'Joint-123', 'Стык 456' и т.д.
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
        
        # Дополнительная очистка от проблемных символов (оставляем ASCII и цифры)
        joint_text = ''.join(char for char in joint_text if ord(char) < 128 or char.isdigit())
        joint_text = joint_text.strip()
        
        # Убираем название столбца, если оно попало в данные
        if joint_text in ['Номер_стыка_Welded_joint_No_', 'Welded_joint_No', 'Joint_No', 'Номер стыка']:
            return None
        
        # Паттерн 1: S01, S02, S123 (буква S + цифры)
        match = re.search(r'S(\d+)', joint_text, re.IGNORECASE)
        if match:
            # Убираем ведущие нули
            number = match.group(1)
            return str(int(number))
        
        # Паттерн 2: любые цифры в строке
        match = re.search(r'(\d+)', joint_text)
        if match:
            # Убираем ведущие нули
            number = match.group(1)
            return str(int(number))
        
        return None
    except Exception as e:
        # Игнорируем все ошибки
        return None

def get_database_path():
    """Получает правильный путь к базе данных с исправленной кодировкой"""
    try:
        current_dir = os.getcwd()
        logger.info(f"Текущая директория: {safe_print_path(current_dir)}")
        
        # Получаем базовый путь проекта
        base_path = os.path.dirname(os.path.dirname(current_dir)) if 'SQLite_data_cleansing' in current_dir else os.path.dirname(current_dir)
        
        possible_paths = [
            os.path.join(current_dir, 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
            os.path.join(current_dir, '..', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
            os.path.join(base_path, 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
            os.path.join(current_dir, 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        ]
        
        for path in possible_paths:
            try:
                abs_path = os.path.abspath(path)
                logger.info(f"Проверяем путь: {safe_print_path(abs_path)}")
                if os.path.exists(abs_path):
                    logger.info(f"[OK] Найдена база данных: {safe_print_path(abs_path)}")
                    return abs_path
            except Exception as e:
                logger.error(f"Ошибка при проверке пути {safe_print_path(path)}: {e}")
                continue
        
        logger.error("[ERROR] База данных не найдена, используем исходный путь")
        return 'BD_Kingisepp/M_Kran_Kingesepp.db'
        
    except Exception as e:
        logger.error(f"[ERROR] Ошибка в get_database_path: {e}")
        return 'BD_Kingisepp/M_Kran_Kingesepp.db'

def process_table_wl_china(cursor):
    """Обработка таблицы wl_china"""
    logger.info("\n1. [ОБРАБОТКА] Обработка таблицы wl_china...")
    
    try:
        # Проверяем существование таблицы
        cursor.execute('SELECT name FROM sqlite_master WHERE type="table" AND name="wl_china"')
        if not cursor.fetchone():
            logger.warning("[WARNING] Таблица 'wl_china' не существует.")
            return
        
        # Добавляем столбец, если его нет
        try:
            cursor.execute('ALTER TABLE wl_china ADD COLUMN _Номер_сварного_шва TEXT')
            logger.info("[OK] Добавлен новый столбец '_Номер_сварного_шва' в wl_china")
        except Exception as e:
            if "duplicate column name" in str(e).lower():
                logger.info("[INFO] Столбец '_Номер_сварного_шва' уже существует в wl_china")
            else:
                logger.warning(f"[WARNING] Ошибка при добавлении столбца: {e}")
        
        # Получаем данные для обработки
        cursor.execute('SELECT rowid, "Номер_сварного_шва" FROM wl_china WHERE "Номер_сварного_шва" IS NOT NULL')
        records = cursor.fetchall()
        logger.info(f"[INFO] Найдено записей для обработки в wl_china: {len(records)}")
        
        # Обрабатываем записи
        updated_count = 0
        for record in records:
            try:
                rowid, joint_text = record
                joint_number = extract_joint_number(joint_text)
                if joint_number is not None:
                    cursor.execute('UPDATE wl_china SET _Номер_сварного_шва = ? WHERE rowid = ?', (joint_number, rowid))
                    updated_count += 1
            except Exception as e:
                # Игнорируем ошибки и продолжаем обработку
                continue
        
        logger.info(f"[OK] Обработано записей в wl_china: {updated_count}")
        
    except Exception as e:
        logger.error(f"[ERROR] Ошибка при обработке таблицы wl_china: {e}")

def process_table_wl_report_smr(cursor):
    """Обработка таблицы wl_report_smr"""
    logger.info("\n2. [ОБРАБОТКА] Обработка таблицы wl_report_smr...")
    
    try:
        # Проверяем существование таблицы
        cursor.execute('SELECT name FROM sqlite_master WHERE type="table" AND name="wl_report_smr"')
        if not cursor.fetchone():
            logger.warning("[WARNING] Таблица 'wl_report_smr' не существует.")
            return
        
        # Добавляем столбец, если его нет
        try:
            cursor.execute('ALTER TABLE wl_report_smr ADD COLUMN _Номер_стыка TEXT')
            logger.info("[OK] Добавлен новый столбец '_Номер_стыка' в wl_report_smr")
        except Exception as e:
            if "duplicate column name" in str(e).lower():
                logger.info("[INFO] Столбец '_Номер_стыка' уже существует в wl_report_smr")
            else:
                logger.warning(f"[WARNING] Ошибка при добавлении столбца: {e}")
        
        # Получаем данные для обработки
        cursor.execute('SELECT rowid, "_Стыка" FROM wl_report_smr WHERE "_Стыка" IS NOT NULL')
        records = cursor.fetchall()
        logger.info(f"[INFO] Найдено записей для обработки в wl_report_smr: {len(records)}")
        
        # Обрабатываем записи
        updated_count = 0
        for record in records:
            try:
                rowid, joint_text = record
                joint_number = extract_joint_number(joint_text)
                if joint_number is not None:
                    cursor.execute('UPDATE wl_report_smr SET _Номер_стыка = ? WHERE rowid = ?', (joint_number, rowid))
                    updated_count += 1
            except Exception as e:
                # Игнорируем ошибки и продолжаем обработку
                continue
        
        logger.info(f"[OK] Обработано записей в wl_report_smr: {updated_count}")
        
    except Exception as e:
        logger.error(f"[ERROR] Ошибка при обработке таблицы wl_report_smr: {e}")

def process_table_work_order_log_ndt(cursor):
    """Обработка таблицы work_order_log_NDT"""
    logger.info("\n3. [ОБРАБОТКА] Обработка таблицы work_order_log_NDT...")
    
    try:
        # Проверяем существование таблицы
        cursor.execute('SELECT name FROM sqlite_master WHERE type="table" AND name="work_order_log_NDT"')
        if not cursor.fetchone():
            logger.warning("[WARNING] Таблица 'work_order_log_NDT' не существует.")
            return
        
        # Добавляем столбец, если его нет
        try:
            cursor.execute('ALTER TABLE work_order_log_NDT ADD COLUMN _Номер_стыка TEXT')
            logger.info("[OK] Добавлен новый столбец '_Номер_стыка' в work_order_log_NDT")
        except Exception as e:
            if "duplicate column name" in str(e).lower():
                logger.info("[INFO] Столбец '_Номер_стыка' уже существует в work_order_log_NDT")
            else:
                logger.warning(f"[WARNING] Ошибка при добавлении столбца: {e}")
        
        # Получаем данные для обработки
        cursor.execute('SELECT rowid, "Номер_стыка" FROM work_order_log_NDT WHERE "Номер_стыка" IS NOT NULL')
        records = cursor.fetchall()
        logger.info(f"[INFO] Найдено записей для обработки в work_order_log_NDT: {len(records)}")
        
        # Обрабатываем записи
        updated_count = 0
        for record in records:
            try:
                rowid, joint_text = record
                joint_number = extract_joint_number(joint_text)
                if joint_number is not None:
                    cursor.execute('UPDATE work_order_log_NDT SET _Номер_стыка = ? WHERE rowid = ?', (joint_number, rowid))
                    updated_count += 1
            except Exception as e:
                # Игнорируем ошибки и продолжаем обработку
                continue
        
        logger.info(f"[OK] Обработано записей в work_order_log_NDT: {updated_count}")
        
    except Exception as e:
        logger.error(f"[ERROR] Ошибка при обработке таблицы work_order_log_NDT: {e}")

def process_table_logs_lnk(cursor):
    """Обработка таблицы logs_lnk"""
    logger.info("\n4. [ОБРАБОТКА] Обработка таблицы logs_lnk...")
    
    try:
        # Проверяем существование таблицы
        cursor.execute('SELECT name FROM sqlite_master WHERE type="table" AND name="logs_lnk"')
        if not cursor.fetchone():
            logger.warning("[WARNING] Таблица 'logs_lnk' не существует.")
            return
        
        # Добавляем столбец, если его нет
        try:
            cursor.execute('ALTER TABLE logs_lnk ADD COLUMN _Номер_стыка TEXT')
            logger.info("[OK] Добавлен новый столбец '_Номер_стыка' в logs_lnk")
        except Exception as e:
            if "duplicate column name" in str(e).lower():
                logger.info("[INFO] Столбец '_Номер_стыка' уже существует в logs_lnk")
            else:
                logger.warning(f"[WARNING] Ошибка при добавлении столбца: {e}")
        
        # Получаем данные для обработки
        cursor.execute('SELECT rowid, "Номер_стыка" FROM logs_lnk WHERE "Номер_стыка" IS NOT NULL')
        records = cursor.fetchall()
        logger.info(f"[INFO] Найдено записей для обработки в logs_lnk: {len(records)}")
        
        # Обрабатываем записи
        updated_count = 0
        for record in records:
            try:
                rowid, joint_text = record
                joint_number = extract_joint_number(joint_text)
                if joint_number is not None:
                    cursor.execute('UPDATE logs_lnk SET _Номер_стыка = ? WHERE rowid = ?', (joint_number, rowid))
                    updated_count += 1
            except Exception as e:
                # Игнорируем ошибки и продолжаем обработку
                continue
        
        logger.info(f"[OK] Обработано записей в logs_lnk: {updated_count}")
        
    except Exception as e:
        logger.error(f"[ERROR] Ошибка при обработке таблицы logs_lnk: {e}")

def main():
    """Главная функция для нормализации номеров стыков/швов"""
    try:
        logger.info("=== Извлечение числовых номеров из обозначений швов ===")
        
        # Получаем правильный путь к базе данных
        db_path = get_database_path()
        
        # Проверяем существование файла базы данных
        if not os.path.exists(db_path):
            logger.error(f"[ERROR] Файл базы данных не найден: {safe_print_path(db_path)}")
            return
        
        logger.info(f"[INFO] Подключаемся к базе данных: {os.path.basename(db_path)}")
        
        # Подключаемся к базе данных
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        try:
            # Обрабатываем все таблицы
            process_table_wl_china(cursor)
            process_table_wl_report_smr(cursor)
            process_table_work_order_log_ndt(cursor)
            process_table_logs_lnk(cursor)
            
            # Сохраняем изменения
            conn.commit()
            logger.info("\n[OK] Нормализация завершена успешно!")

        except Exception as e:
            logger.error(f"[ERROR] Произошла ошибка при обработке данных: {e}")
            import traceback
            logger.error(f"Полный стек ошибки:\n{traceback.format_exc()}")
            conn.rollback()
        finally:
            conn.close()
            logger.info("[INFO] Соединение с базой данных закрыто")

    except Exception as e:
        logger.error(f"[ERROR] Критическая ошибка: {e}")
        import traceback
        logger.error(f"Полный стек ошибки:\n{traceback.format_exc()}")

def run_script():
    """Функция для запуска скрипта через лаунчер (как в десктопном приложении)"""
    try:
        logger.info("[START] Запуск скрипта normalize_welds_column.py через лаунчер...")
        main()
        logger.info("[OK] Скрипт normalize_welds_column.py завершен успешно!")
    except Exception as e:
        logger.error(f"[ERROR] Ошибка при запуске скрипта: {e}")
        import traceback
        logger.error(f"Полный стек ошибки:\n{traceback.format_exc()}")

if __name__ == '__main__':
    run_script() 