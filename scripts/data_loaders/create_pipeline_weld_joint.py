import sqlite3
import os
import pandas as pd
from datetime import datetime
import logging
import sys

# Импортируем утилиты
try:
    # Пробуем относительный импорт (для запуска через web-приложение)
    from ..utilities.path_utils import get_log_path
except ImportError:
    # Если не работает, используем абсолютный импорт (для прямого запуска)
    import sys
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    utilities_dir = os.path.join(os.path.dirname(current_dir), 'utilities')
    if utilities_dir not in sys.path:
        sys.path.insert(0, utilities_dir)
    
    from path_utils import get_log_path



# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(get_log_path('create_pipeline_weld_joint'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def create_pipeline_weld_joint_table():
    """
    Создает таблицу pipeline_weld_joint на основе данных из wl_volume.
    Дублирует строки согласно значению V_стыков и добавляет нумерацию в новый столбец Номер_стыка.
    """
    db_path = os.path.join("..", "BD_Kingisepp", "M_Kran_Kingesepp.db")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        logger.info("Подключение к базе данных успешно!")
        
        # Проверяем существование таблицы wl_volume
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='wl_volume'")
        if not cursor.fetchone():
            logger.error("Ошибка: Таблица wl_volume не существует!")
            return
        
        # Получаем структуру таблицы wl_volume
        cursor.execute('PRAGMA table_info(wl_volume)')
        columns_info = cursor.fetchall()
        column_names = [col[1] for col in columns_info]
        
        logger.info(f"Найдены столбцы в wl_volume: {column_names}")
        
        # Ищем нужные столбцы
        designation_col = None
        name_col = None
        v_stykov_col = None
        
        # Ищем столбцы с нужными названиями (возможные варианты)
        for col in column_names:
            col_lower = col.lower()
            if 'обозначение' in col_lower or 'designation' in col_lower:
                designation_col = col
            elif 'наименование' in col_lower or 'name' in col_lower:
                name_col = col
            elif 'v_стыков' in col_lower or 'стыков' in col_lower:
                v_stykov_col = col
        
        logger.info(f"Найденные столбцы:")
        logger.info(f"  Обозначение: {designation_col}")
        logger.info(f"  Наименование: {name_col}")
        logger.info(f"  V_стыков: {v_stykov_col}")
        
        if not all([designation_col, name_col, v_stykov_col]):
            logger.error("Ошибка: Не найдены все необходимые столбцы!")
            return
        
        # Получаем данные из таблицы wl_volume
        query = f"SELECT \"{designation_col}\", \"{name_col}\", \"{v_stykov_col}\" FROM wl_volume"
        cursor.execute(query)
        rows = cursor.fetchall()
        
        logger.info(f"Найдено {len(rows)} записей в wl_volume")
        
        # Создаем новую таблицу pipeline_weld_joint
        create_table_sql = '''
        CREATE TABLE IF NOT EXISTS pipeline_weld_joint (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            "Обозначение/_Designation" TEXT,
            "Наименование/Name" TEXT,
            "V_стыков" INTEGER,
            "Номер_стыка" INTEGER,
            "Дата_создания" TEXT
        )
        '''
        
        # Удаляем существующую таблицу, если она есть
        cursor.execute("DROP TABLE IF EXISTS pipeline_weld_joint")
        logger.info("Старая таблица pipeline_weld_joint удалена")
        
        cursor.execute(create_table_sql)
        logger.info("Таблица pipeline_weld_joint создана!")
        
        # Очищаем таблицу перед вставкой новых данных (больше не нужно, так как таблица новая)
        # cursor.execute("DELETE FROM pipeline_weld_joint")
        # logger.info("Старые данные удалены из таблицы pipeline_weld_joint")
        
        # Подготавливаем данные для вставки
        insert_data = []
        total_rows = 0
        processed_records = 0
        
        for row in rows:
            designation, name, v_stykov = row
            
            # Пропускаем строки с пустыми значениями
            if not designation or not name or not v_stykov:
                continue
            
            # Преобразуем V_стыков в число
            try:
                v_stykov_int = int(float(v_stykov))
            except (ValueError, TypeError):
                logger.warning(f"Пропускаем строку с некорректным V_стыков: {v_stykov}")
                continue
            
            # Создаем нужное количество копий строки
            for i in range(1, v_stykov_int + 1):
                insert_data.append((
                    designation,
                    name,
                    v_stykov_int,  # Оставляем исходное значение V_стыков
                    i,  # Нумерация от 1 до V_стыков в столбце Номер_стыка
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ))
                total_rows += 1
            
            processed_records += 1
            
            # Логируем прогресс каждые 100 записей
            if processed_records % 100 == 0:
                logger.info(f"Обработано {processed_records} записей из {len(rows)}")
        
        # Вставляем данные
        insert_query = '''
        INSERT INTO pipeline_weld_joint ("Обозначение/_Designation", "Наименование/Name", "V_стыков", "Номер_стыка", "Дата_создания")
        VALUES (?, ?, ?, ?, ?)
        '''
        
        logger.info("Начинаем вставку данных...")
        cursor.executemany(insert_query, insert_data)
        conn.commit()
        
        logger.info(f"Успешно создано {total_rows} записей в таблице pipeline_weld_joint!")
        
        # Показываем примеры созданных записей
        cursor.execute("SELECT * FROM pipeline_weld_joint LIMIT 5")
        sample_rows = cursor.fetchall()
        
        logger.info("Примеры созданных записей:")
        for i, row in enumerate(sample_rows, 1):
            logger.info(f"  {i}. ID: {row[0]}, Обозначение: {row[1]}, Наименование: {row[2]}, V_стыков: {row[3]}, Номер_стыка: {row[4]}")
        
        # Показываем статистику
        cursor.execute("SELECT COUNT(*) FROM pipeline_weld_joint")
        total_count = cursor.fetchone()[0]
        logger.info(f"Всего записей в таблице pipeline_weld_joint: {total_count}")
        
        # Статистика по уникальным обозначениям
        cursor.execute('''
            SELECT COUNT(DISTINCT "Обозначение/_Designation") as unique_designations
            FROM pipeline_weld_joint
        ''')
        unique_designations = cursor.fetchone()[0]
        logger.info(f"Уникальных обозначений: {unique_designations}")
        
        conn.close()
        logger.info("Работа завершена успешно!")
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        import traceback
        logger.error("Полный стек ошибки:")
        logger.error(traceback.format_exc())

def main():
    """
    Основная функция для запуска скрипта
    """
    logger.info("=" * 60)
    logger.info("Начало создания таблицы pipeline_weld_joint")
    logger.info("=" * 60)
    
    create_pipeline_weld_joint_table()
    
    logger.info("=" * 60)
    logger.info("Завершение работы скрипта")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()

# Для запуска через GUI
def run_script():
    """Функция для запуска скрипта через GUI"""
    main() 