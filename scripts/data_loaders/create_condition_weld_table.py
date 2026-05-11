import sqlite3
import os
import sys
import pandas as pd
from datetime import datetime
import logging

# Проверяем, нужно ли использовать PostgreSQL
USE_POSTGRESQL = os.environ.get('USE_POSTGRESQL', 'false').lower() == 'true'

# Импортируем RealDictCursor для PostgreSQL
if USE_POSTGRESQL:
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
    except ImportError:
        RealDictCursor = None
else:
    RealDictCursor = None

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
        if USE_POSTGRESQL:
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = %s
            """, (table,))
        else:
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
    if USE_POSTGRESQL:
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))
        columns = cursor.fetchall()
        return [col['column_name'] for col in columns]
    else:
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
        # Для PostgreSQL используем RealDictCursor для получения результатов в виде словаря
        if USE_POSTGRESQL and RealDictCursor:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
        else:
            cursor = conn.cursor()
        
        logger.info("🚀 Начинаем создание таблицы condition_weld")
        
        # Проверяем наличие необходимых таблиц
        if not check_required_tables(cursor):
            return False
        
        # Для PostgreSQL проверяем реальные имена столбцов в wl_china
        if USE_POSTGRESQL:
            logger.info("🔍 Проверяем имена столбцов в таблице wl_china...")
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = 'wl_china'
                ORDER BY ordinal_position
            """)
            wl_china_columns = [row['column_name'] for row in cursor.fetchall()]
            logger.info(f"📋 Столбцы в wl_china: {', '.join(wl_china_columns[:10])}...")  # Первые 10 столбцов
            # Проверяем наличие нужных столбцов
            required_wl_columns = ['Заключение_РК_N', 'Результаты_Заключения_РК', 'Дата_Заключения_РК',
                                   'АКТ_ВИК_N', 'Дата_АКТ_ВИК', 'Результаты_АКТ_ВИК', 'Дата_сварки',
                                   'Номер_чертежа', 'Номер_сварного_шва']
            missing_columns = [col for col in required_wl_columns if col not in wl_china_columns]
            if missing_columns:
                logger.warning(f"⚠️ Отсутствуют столбцы в wl_china: {missing_columns}")
                logger.warning(f"   Доступные столбцы: {wl_china_columns}")
        
        # Проверяем, существует ли уже таблица condition_weld
        if USE_POSTGRESQL:
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = %s
            """, ('condition_weld',))
        else:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='condition_weld'")
        if cursor.fetchone():
            logger.warning("⚠️ Таблица condition_weld уже существует. Удаляем старую таблицу...")
            cursor.execute("DROP TABLE condition_weld")
            conn.commit()
            logger.info("✅ Старая таблица удалена")
        
        # SQL запрос для создания таблицы condition_weld с новыми столбцами заявок
        # Адаптируем синтаксис для PostgreSQL и SQLite
        if USE_POSTGRESQL:
            rt_like_pattern = "'%RT%'"
            vt_like_pattern = "'%VT%'"
        else:
            rt_like_pattern = "'%RT%'"
            vt_like_pattern = "'%VT%'"
        
        create_table_sql = f"""
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
                "Номер_стыка",
                Заявленны_виды_контроля,
                Источник,
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
            WHERE Заявленны_виды_контроля LIKE {rt_like_pattern}
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
                "Номер_стыка",
                Заявленны_виды_контроля,
                Источник,
                "№_заявки" AS "№_заявки_ВИК",
                "Дата_заявки" AS "Дата_заявки_ВИК",
                app_row_id AS "app_row_id_ВИК",
                ROW_NUMBER() OVER (
                    PARTITION BY Чертеж, "_Номер_сварного_шва_без_S_F_" 
                    ORDER BY 
                        /* 1) Основной критерий: последнее состояние по app_row_id */
                        app_row_id DESC,
                        /* 2) Вторичный критерий: дата контроля ВИК (самая новая) */
                        DATE(Дата_контроля_ВИК) DESC
                ) as rn,
                COUNT(*) OVER (
                    PARTITION BY Чертеж, "_Номер_сварного_шва_без_S_F_"
                ) as total_vt_records
            FROM logs_lnk 
            WHERE Заявленны_виды_контроля LIKE {vt_like_pattern}
        )
        SELECT
            pwji.id,
            pwji."Титул",
            pwji."ISO",
            pwji."Линия",
            pwji."стык",
            pwji."Код_удаления",
            pwji."Тип_соединения_российский_стандарт" AS Тип_шва,
            COALESCE(rt."Номер_стыка_LNK", vt."Номер_стыка_LNK") AS "Номер_стыка_LNK",
            rt.ID_RT,
            rt.РК,
            rt.Статус_РК,
            rt.Дата_контроля_РК,
            rt."Количество_RT_записей",
            rt."№_заявки_РК",
            rt."Дата_заявки_РК",
            rt."app_row_id_РК",
            rt.Источник AS "Источник_РК",
            vt.ID_VT,
            vt.ВИК,
            vt.Статус_ВИК,
            vt.Дата_контроля_ВИК,
            vt."Количество_VT_записей",
            vt."№_заявки_ВИК",
            vt."Дата_заявки_ВИК",
            vt."app_row_id_ВИК",
            vt.Источник AS "Источник_ВИК",
            wc.id AS ID_WC,
            wc."Заключение_РК_N",
            wc."Результаты_Заключения_РК",
            wc."Дата_Заключения_РК",
            wc."АКТ_ВИК_N",
            wc."Дата_АКТ_ВИК",
            wc."Результаты_АКТ_ВИК",
            wc."Дата_сварки",
            wc."Номер_сварного_шва" AS "Номер_стыка_WL"
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
                "Номер_стыка" AS "Номер_стыка_LNK",
                total_rt_records AS "Количество_RT_записей",
                "№_заявки_РК",
                "Дата_заявки_РК",
                "app_row_id_РК",
                Источник
            FROM RankedRecordsRT 
            WHERE rn = 1
        ) rt ON pwji."ISO" = rt.Чертеж AND pwji."стык" = rt.Номер_шва
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
                "Номер_стыка" AS "Номер_стыка_LNK",
                total_vt_records AS "Количество_VT_записей",
                "№_заявки_ВИК",
                "Дата_заявки_ВИК",
                "app_row_id_ВИК",
                Источник
            FROM RankedRecordsVT 
            WHERE rn = 1
        ) vt ON pwji."ISO" = vt.Чертеж AND pwji."стык" = vt.Номер_шва
        LEFT JOIN wl_china wc ON pwji."ISO" = wc."Номер_чертежа" AND pwji."стык" = wc."_Номер_сварного_шва_без_S_F_"
        ORDER BY pwji."ISO", pwji."стык"
        """
        
        logger.info("📊 Выполняем SQL запрос для создания таблицы...")
        # Логируем часть SQL запроса для отладки (первые 500 символов)
        logger.debug(f"SQL запрос (первые 500 символов): {create_table_sql[:500]}")
        if USE_POSTGRESQL:
            # В PostgreSQL нужно выполнить запрос как есть
            try:
                cursor.execute(create_table_sql)
            except Exception as e:
                # Логируем часть SQL запроса вокруг ошибки для отладки
                logger.error(f"Ошибка выполнения SQL. Проверьте имена столбцов в таблицах.")
                logger.error(f"SQL запрос (строки 230-250): {create_table_sql[2000:3000] if len(create_table_sql) > 3000 else create_table_sql[2000:]}")
                raise
        else:
            cursor.execute(create_table_sql)
        conn.commit()
        
        # Получаем количество созданных записей
        cursor.execute("SELECT COUNT(*) as count FROM condition_weld")
        result = cursor.fetchone()
        if USE_POSTGRESQL and isinstance(result, dict):
            record_count = result.get('count', list(result.values())[0] if result else 0)
        else:
            record_count = result[0] if isinstance(result, (list, tuple)) else result
        
        # Получаем информацию о структуре созданной таблицы
        columns = get_table_info(cursor, 'condition_weld')
        column_names = columns
        
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
        import traceback
        logger.error(traceback.format_exc())
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
        # Функция для получения значения из результата COUNT(*)
        def get_count(result):
            if USE_POSTGRESQL:
                # С RealDictCursor результат будет словарем
                if isinstance(result, dict):
                    return result.get('count', result.get('COUNT(*)', list(result.values())[0] if result else 0))
                return result[0] if isinstance(result, (list, tuple)) else result
            else:
                return result[0] if isinstance(result, (list, tuple)) else result
        
        logger.info("📈 Статистика по таблице condition_weld:")
        
        # Общее количество записей
        cursor.execute("SELECT COUNT(*) as count FROM condition_weld")
        total_records = get_count(cursor.fetchone())
        logger.info(f"   Всего записей: {total_records}")
        
        # Количество записей с RT данными
        cursor.execute("SELECT COUNT(*) as count FROM condition_weld WHERE ID_RT IS NOT NULL")
        rt_records = get_count(cursor.fetchone())
        logger.info(f"   Записей с RT данными: {rt_records}")
        
        # Количество записей с VT данными
        cursor.execute("SELECT COUNT(*) as count FROM condition_weld WHERE ID_VT IS NOT NULL")
        vt_records = get_count(cursor.fetchone())
        logger.info(f"   Записей с VT данными: {vt_records}")
        
        # Количество записей с данными wl_china
        cursor.execute("SELECT COUNT(*) as count FROM condition_weld WHERE ID_WC IS NOT NULL")
        wc_records = get_count(cursor.fetchone())
        logger.info(f"   Записей с данными wl_china: {wc_records}")
        
        # Количество записей с РК заявками
        cursor.execute('SELECT COUNT(*) as count FROM condition_weld WHERE "№_заявки_РК" IS NOT NULL')
        rt_requests = get_count(cursor.fetchone())
        logger.info(f"   Записей с РК заявками: {rt_requests}")
        
        # Количество записей с ВИК заявками
        cursor.execute('SELECT COUNT(*) as count FROM condition_weld WHERE "№_заявки_ВИК" IS NOT NULL')
        vt_requests = get_count(cursor.fetchone())
        logger.info(f"   Записей с ВИК заявками: {vt_requests}")
        
        # Количество уникальных ISO
        cursor.execute('SELECT COUNT(DISTINCT "ISO") as count FROM condition_weld')
        unique_iso = get_count(cursor.fetchone())
        logger.info(f"   Уникальных ISO: {unique_iso}")
        
        # Количество уникальных стыков
        cursor.execute('SELECT COUNT(DISTINCT "стык") as count FROM condition_weld')
        unique_joints = get_count(cursor.fetchone())
        logger.info(f"   Уникальных стыков: {unique_joints}")
        
        # Показываем примеры записей с заявками
        cursor.execute("""
            SELECT "ISO", "стык", "№_заявки_РК", "Дата_заявки_РК", "app_row_id_РК", 
                   "№_заявки_ВИК", "Дата_заявки_ВИК", "app_row_id_ВИК"
            FROM condition_weld 
            WHERE "№_заявки_РК" IS NOT NULL OR "№_заявки_ВИК" IS NOT NULL
            LIMIT 3
        """)
        
        examples = cursor.fetchall()
        if examples:
            logger.info("📋 Примеры записей с заявками:")
            for row in examples:
                if USE_POSTGRESQL and isinstance(row, dict):
                    iso_val = row.get('ISO')
                    styk_val = row.get('стык')
                    rk_num = row.get('№_заявки_РК')
                    rk_date = row.get('Дата_заявки_РК')
                    rk_app_id = row.get('app_row_id_РК')
                    vik_num = row.get('№_заявки_ВИК')
                    vik_date = row.get('Дата_заявки_ВИК')
                    vik_app_id = row.get('app_row_id_ВИК')
                else:
                    iso_val = row[0]
                    styk_val = row[1]
                    rk_num = row[2]
                    rk_date = row[3]
                    rk_app_id = row[4]
                    vik_num = row[5]
                    vik_date = row[6]
                    vik_app_id = row[7]
                logger.info(f"   ISO: {iso_val}, Стык: {styk_val}")
                logger.info(f"     РК заявка: {rk_num} ({rk_date}) - app_row_id: {rk_app_id}")
                logger.info(f"     ВИК заявка: {vik_num} ({vik_date}) - app_row_id: {vik_app_id}")
        
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
