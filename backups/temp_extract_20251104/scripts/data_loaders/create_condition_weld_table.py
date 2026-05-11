import sqlite3
import os
import sys
import pandas as pd
from datetime import datetime
import logging

# Импортируем утилиты
try:
    # Пробуем относительный импорт (для запуска через web-приложение)
    from ..utilities.db_utils import get_database_connection
    from ..utilities.path_utils import get_log_path
except ImportError:
    # Если не работает, используем абсолютный импорт (для прямого запуска)
    import sys
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    utilities_dir = os.path.join(os.path.dirname(current_dir), 'utilities')
    if utilities_dir not in sys.path:
        sys.path.insert(0, utilities_dir)
    
    try:
        from db_utils import get_database_connection
        from path_utils import get_log_path
    except ImportError:
        # Если и это не работает, используем прямой путь
        sys.path.insert(0, os.path.join(current_dir, '..', 'utilities'))
        from db_utils import get_database_connection
        from path_utils import get_log_path

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(get_log_path('create_condition_weld_table'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def check_required_tables(cursor):
    """
    Проверяет наличие необходимых таблиц для создания condition_weld
    """
    required_tables = ['pipeline_weld_joint_iso', 'logs_lnk', 'wl_china']
    missing_tables = []
    
    for table in required_tables:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
        if not cursor.fetchone():
            missing_tables.append(table)
    
    if missing_tables:
        logger.error(f"❌ Отсутствуют необходимые таблицы: {', '.join(missing_tables)}")
        return False
    
    logger.info("✅ Все необходимые таблицы найдены")
    return True

def get_table_info(cursor, table_name):
    """
    Получает информацию о структуре таблицы
    """
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    return [col[1] for col in columns]

def create_condition_weld_table():
    """
    Создает таблицу condition_weld на основе SQL запроса с включением столбцов заявок
    Включает номера заявок, даты заявок и app_row_id для РК и ВИК из таблицы logs_lnk
    """
    try:
        # Подключение к базе данных
        conn = get_database_connection()
        cursor = conn.cursor()
        
        logger.info("🚀 Начинаем создание таблицы condition_weld")
        
        # Проверяем наличие необходимых таблиц
        if not check_required_tables(cursor):
            return False
        
        # Проверяем, существует ли уже таблица condition_weld
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='condition_weld'")
        if cursor.fetchone():
            logger.warning("⚠️ Таблица condition_weld уже существует. Удаляем старую таблицу...")
            cursor.execute("DROP TABLE condition_weld")
            logger.info("✅ Старая таблица удалена")
        
        # SQL запрос для создания таблицы condition_weld с новыми столбцами заявок
        create_table_sql = """
        CREATE TABLE condition_weld AS
        WITH RankedRecordsRT AS (
            SELECT 
                id,
                Чертеж,
                "_Номер_сварного_шва_без_S_F_",
                РК,
                Статус_РК,
                Дата_контроля_РК,
                Диаметр_1,
                Толщина_1,
                Заявленны_виды_контроля,
                "№_заявки" AS "№_заявки_РК",
                "Дата_заявки" AS "Дата_заявки_РК",
                app_row_id AS "app_row_id_РК",
                ROW_NUMBER() OVER (
                    PARTITION BY Чертеж, "_Номер_сварного_шва_без_S_F_" 
                    ORDER BY 
                        /* 1) Приоритет статуса "Заказ отправлен" без даты */
                        CASE 
                            WHEN Статус_РК = 'Заказ отправлен' AND Дата_контроля_РК IS NULL THEN 1
                            WHEN Дата_контроля_РК IS NOT NULL THEN 2
                            ELSE 3
                        END,
                        /* 2) Затем самая новая дата контроля */
                        DATE(Дата_контроля_РК) DESC,
                        /* 3) При равенстве дат — приоритет остальных статусов */
                        CASE 
                            WHEN Статус_РК = 'Н/П' THEN 1
                            ELSE 2
                        END,
                        /* 4) Финальный стабилизатор */
                        id DESC
                ) as rn,
                COUNT(*) OVER (
                    PARTITION BY Чертеж, "_Номер_сварного_шва_без_S_F_"
                ) as total_rt_records
            FROM logs_lnk 
            WHERE Заявленны_виды_контроля LIKE '%RT%'
        ),
        RankedRecordsVT AS (
            SELECT 
                id,
                Чертеж,
                "_Номер_сварного_шва_без_S_F_",
                ВИК,
                Статус_ВИК,
                Дата_ВИК,
                Дата_контроля_ВИК,
                Диаметр_1,
                Толщина_1,
                Заявленны_виды_контроля,
                "№_заявки" AS "№_заявки_ВИК",
                "Дата_заявки" AS "Дата_заявки_ВИК",
                app_row_id AS "app_row_id_ВИК",
                ROW_NUMBER() OVER (
                    PARTITION BY Чертеж, "_Номер_сварного_шва_без_S_F_" 
                    ORDER BY Дата_контроля_ВИК DESC
                ) as rn,
                COUNT(*) OVER (
                    PARTITION BY Чертеж, "_Номер_сварного_шва_без_S_F_"
                ) as total_vt_records
            FROM logs_lnk 
            WHERE Заявленны_виды_контроля LIKE '%VT%'
        )
        SELECT
            pwji.id,
            pwji.Титул,
            pwji.ISO,
            pwji.Линия,
            pwji.стык,
            pwji.Код_удаления,
            pwji."Тип_соединения_российский_стандарт" AS Тип_шва,
            rt.ID_RT,
            rt.РК,
            rt.Статус_РК,
            rt.Дата_контроля_РК,
            rt."Количество_RT_записей",
            rt."№_заявки_РК",
            rt."Дата_заявки_РК",
            rt."app_row_id_РК",
            vt.ID_VT,
            vt.ВИК,
            vt.Статус_ВИК,
            vt.Дата_контроля_ВИК,
            vt."Количество_VT_записей",
            vt."№_заявки_ВИК",
            vt."Дата_заявки_ВИК",
            vt."app_row_id_ВИК",
            wc.id AS ID_WC,
            wc.Заключение_РК_N,
            wc.Результаты_Заключения_РК,
            wc.Дата_Заключения_РК,
            wc.АКТ_ВИК_N,
            wc.Дата_АКТ_ВИК,
            wc.Результаты_АКТ_ВИК,
            wc.Дата_сварки
        FROM
            pipeline_weld_joint_iso pwji
        LEFT JOIN (
            SELECT 
                id AS ID_RT,
                Чертеж,
                "_Номер_сварного_шва_без_S_F_" AS Номер_шва,
                РК,
                Статус_РК,
                Дата_контроля_РК,
                Диаметр_1,
                Толщина_1,
                total_rt_records AS "Количество_RT_записей",
                "№_заявки_РК",
                "Дата_заявки_РК",
                "app_row_id_РК"
            FROM RankedRecordsRT 
            WHERE rn = 1
        ) rt ON pwji.ISO = rt.Чертеж AND pwji.стык = rt.Номер_шва
        LEFT JOIN (
            SELECT 
                id AS ID_VT,
                Чертеж,
                "_Номер_сварного_шва_без_S_F_" AS Номер_шва,
                ВИК,
                Статус_ВИК,
                Дата_ВИК,
                Дата_контроля_ВИК,
                Диаметр_1,
                Толщина_1,
                total_vt_records AS "Количество_VT_записей",
                "№_заявки_ВИК",
                "Дата_заявки_ВИК",
                "app_row_id_ВИК"
            FROM RankedRecordsVT 
            WHERE rn = 1
        ) vt ON pwji.ISO = vt.Чертеж AND pwji.стык = vt.Номер_шва
        LEFT JOIN wl_china wc ON pwji.ISO = wc.Номер_чертежа AND pwji.стык = wc."_Номер_сварного_шва_без_S_F_"
        ORDER BY pwji.ISO, pwji.стык
        """
        
        logger.info("📊 Выполняем SQL запрос для создания таблицы...")
        cursor.execute(create_table_sql)
        
        # Получаем количество созданных записей
        cursor.execute("SELECT COUNT(*) FROM condition_weld")
        record_count = cursor.fetchone()[0]
        
        # Получаем информацию о структуре созданной таблицы
        cursor.execute("PRAGMA table_info(condition_weld)")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        
        logger.info(f"✅ Таблица condition_weld успешно создана")
        logger.info(f"📊 Количество записей: {record_count}")
        logger.info(f"📋 Столбцы таблицы: {', '.join(column_names)}")
        
        # Обрабатываем столбец Код_удаления: пустые и цифры -> NULL, буквы оставляем
        logger.info("🔧 Обработка столбца Код_удаления...")
        update_code_sql = """
        UPDATE condition_weld 
        SET Код_удаления = CASE 
            WHEN Код_удаления IS NULL OR TRIM(Код_удаления) = '' THEN NULL
            WHEN REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Код_удаления, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', '') = '' THEN NULL
            ELSE Код_удаления
        END
        """
        cursor.execute(update_code_sql)
        logger.info("✅ Столбец Код_удаления обработан")
        
        # Сохраняем изменения
        conn.commit()
        
        # Выводим статистику по данным
        print_statistics(cursor)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка при создании таблицы condition_weld: {e}")
        if 'conn' in locals():
            conn.rollback()
        return False
    
    finally:
        if 'conn' in locals():
            conn.close()

def print_statistics(cursor):
    """
    Выводит статистику по созданной таблице
    """
    try:
        logger.info("📈 Статистика по таблице condition_weld:")
        
        # Общее количество записей
        cursor.execute("SELECT COUNT(*) FROM condition_weld")
        total_records = cursor.fetchone()[0]
        logger.info(f"   Всего записей: {total_records}")
        
        # Количество записей с RT данными
        cursor.execute("SELECT COUNT(*) FROM condition_weld WHERE ID_RT IS NOT NULL")
        rt_records = cursor.fetchone()[0]
        logger.info(f"   Записей с RT данными: {rt_records}")
        
        # Количество записей с VT данными
        cursor.execute("SELECT COUNT(*) FROM condition_weld WHERE ID_VT IS NOT NULL")
        vt_records = cursor.fetchone()[0]
        logger.info(f"   Записей с VT данными: {vt_records}")
        
        # Количество записей с данными wl_china
        cursor.execute("SELECT COUNT(*) FROM condition_weld WHERE ID_WC IS NOT NULL")
        wc_records = cursor.fetchone()[0]
        logger.info(f"   Записей с данными wl_china: {wc_records}")
        
        # Количество записей с РК заявками
        cursor.execute('SELECT COUNT(*) FROM condition_weld WHERE "№_заявки_РК" IS NOT NULL')
        rt_requests = cursor.fetchone()[0]
        logger.info(f"   Записей с РК заявками: {rt_requests}")
        
        # Количество записей с ВИК заявками
        cursor.execute('SELECT COUNT(*) FROM condition_weld WHERE "№_заявки_ВИК" IS NOT NULL')
        vt_requests = cursor.fetchone()[0]
        logger.info(f"   Записей с ВИК заявками: {vt_requests}")
        
        # Количество уникальных ISO
        cursor.execute("SELECT COUNT(DISTINCT ISO) FROM condition_weld")
        unique_iso = cursor.fetchone()[0]
        logger.info(f"   Уникальных ISO: {unique_iso}")
        
        # Количество уникальных стыков
        cursor.execute("SELECT COUNT(DISTINCT стык) FROM condition_weld")
        unique_joints = cursor.fetchone()[0]
        logger.info(f"   Уникальных стыков: {unique_joints}")
        
        # Показываем примеры записей с заявками
        cursor.execute("""
            SELECT ISO, стык, "№_заявки_РК", "Дата_заявки_РК", "app_row_id_РК", 
                   "№_заявки_ВИК", "Дата_заявки_ВИК", "app_row_id_ВИК"
            FROM condition_weld 
            WHERE "№_заявки_РК" IS NOT NULL OR "№_заявки_ВИК" IS NOT NULL
            LIMIT 3
        """)
        
        examples = cursor.fetchall()
        if examples:
            logger.info("📋 Примеры записей с заявками:")
            for row in examples:
                logger.info(f"   ISO: {row[0]}, Стык: {row[1]}")
                logger.info(f"     РК заявка: {row[2]} ({row[3]}) - app_row_id: {row[4]}")
                logger.info(f"     ВИК заявка: {row[5]} ({row[6]}) - app_row_id: {row[7]}")
        
    except Exception as e:
        logger.error(f"Проблема при получении статистики: {e}")

def run_script():
    """
    Функция для запуска скрипта через веб-интерфейс
    """
    return main()

def main():
    """
    Основная функция для запуска скрипта
    """
    logger.info("=" * 60)
    logger.info("🏗️ СКРИПТ СОЗДАНИЯ ТАБЛИЦЫ CONDITION_WELD (с заявками)")
    logger.info("=" * 60)
    
    start_time = datetime.now()
    
    try:
        success = create_condition_weld_table()
        
        if success:
            logger.info("✅ Скрипт выполнен успешно")
        else:
            logger.error("❌ Скрипт завершился с ошибками")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        return 1
    
    finally:
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"⏱️ Время выполнения: {duration}")
        logger.info("=" * 60)
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
