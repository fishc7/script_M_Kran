#!/usr/bin/env python3

# -*- coding: utf-8 -*-



"""

Скрипт для создания таблицы сварено_сварщиком на основе данных из таблицы wl_china



Выполняет следующие операции:

1. Удаляет старую таблицу сварено_сварщиком (если существует)

2. Создает новую таблицу с автоинкрементным первичным ключом

3. Заполняет таблицу данными, группируя по N_Линии и Номер_чертежа

4. Объединяет клейма сварщиков из корневого слоя и заполнения/облицовки

"""



import sqlite3

import os

import sys

import logging

from datetime import datetime



# Настройка путей для импорта модулей

current_dir = os.path.dirname(os.path.abspath(__file__))

utilities_dir = os.path.join(current_dir, 'scripts', 'utilities')

project_root = os.path.dirname(current_dir)



# Добавляем пути в sys.path

for path in [current_dir, utilities_dir, project_root]:

    if path not in sys.path:

        sys.path.insert(0, path)



# Импортируем утилиты

try:

    from scripts.utilities.db_utils import get_database_connection

    from scripts.utilities.path_utils import get_log_path

except ImportError:

    # Если не работает, используем абсолютный импорт

    def get_database_connection():

        # Используем абсолютный путь для избежания проблем с кодировкой

        db_path = r"D:\МК_Кран\script_M_Kran\database\BD_Kingisepp\M_Kran_Kingesepp.db"

        if not os.path.exists(db_path):

            raise FileNotFoundError(f"База данных не найдена: {db_path}")

        return sqlite3.connect(db_path)

    

    def get_log_path(script_name):

        return os.path.join(project_root, 'logs', f'{script_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')



# Настройка логирования

def setup_logging():

    """Настройка логирования с проверкой существования директории"""

    try:

        log_path = get_log_path('create_svarenno_svarshchikom_table')

        log_dir = os.path.dirname(log_path)

        if not os.path.exists(log_dir):

            os.makedirs(log_dir, exist_ok=True)

        

        logging.basicConfig(

            level=logging.INFO,

            format='%(asctime)s - %(levelname)s - %(message)s',

            handlers=[

                logging.FileHandler(log_path, encoding='utf-8'),

                logging.StreamHandler()

            ]

        )

        return logging.getLogger(__name__)

    except Exception as e:

        # Если не удается создать файловый логгер, используем только консольный

        logging.basicConfig(

            level=logging.INFO,

            format='%(asctime)s - %(levelname)s - %(message)s',

            handlers=[logging.StreamHandler()]

        )

        return logging.getLogger(__name__)



logger = setup_logging()



class SvarennoSvarshchikomCreator:

    """Класс для создания таблицы сварено_сварщиком"""

    

    def __init__(self):

        self.conn: sqlite3.Connection | None = None

        self.cursor: sqlite3.Cursor | None = None

        self.stats = {

            'total_wl_china_records': 0,

            'processed_records': 0,

            'created_groups': 0,

            'errors': 0

        }

        self.has_bazovyy_material = False  # Будет установлено в check_wl_china_structure

    
    def _ensure_connection(self) -> bool:
        """Проверка и обеспечение подключения к базе данных"""
        if self.conn is None or self.cursor is None:
            return self.connect_to_database()
        return True

    def connect_to_database(self):

        """Подключение к базе данных"""

        try:

            self.conn = get_database_connection()

            self.cursor = self.conn.cursor()

            logger.info("✅ Подключение к базе данных успешно")

            return True

        except Exception as e:

            logger.error(f"❌ Ошибка подключения к базе данных: {e}")

            return False

    

    def get_table_stats(self):

        """Получение статистики по таблице wl_china"""

        if self.cursor is None:

            return

        try:

            # Статистика wl_china

            self.cursor.execute("SELECT COUNT(*) FROM wl_china")

            self.stats['total_wl_china_records'] = self.cursor.fetchone()[0]

            

            logger.info(f"📊 Статистика таблицы wl_china:")

            logger.info(f"   - Всего записей: {self.stats['total_wl_china_records']}")

            

        except Exception as e:

            logger.error(f"❌ Ошибка получения статистики: {e}")

            self.stats['errors'] += 1



    def preprocess_welding_data(self):

        """Если дата сварки пустая, то клейма тоже обнуляем (NULL)."""

        if self.cursor is None or self.conn is None:

            return

        logger.info("🔧 Предварительная обработка: обнуляем клейма при пустой дате сварки...")

        try:

            # Обнуляем клеймо корневого слоя при пустой дате

            self.cursor.execute(

                """

                UPDATE wl_china 

                SET Клеймо_сварщика_корневой_слой = NULL

                WHERE Дата_сварки IS NULL OR Дата_сварки = '' OR TRIM(Дата_сварки) = ''

                """

            )

            # Обнуляем клеймо заполнения/облицовки при пустой дате

            self.cursor.execute(

                """

                UPDATE wl_china 

                SET Клеймо_сварщика_заполнение_облицовка = NULL

                WHERE Дата_сварки IS NULL OR Дата_сварки = '' OR TRIM(Дата_сварки) = ''

                """

            )

            self.conn.commit()

            logger.info("✅ Предварительная обработка завершена")

        except Exception as e:

            logger.error(f"❌ Ошибка предварительной обработки: {e}")

            self.conn.rollback()

    

    def check_wl_china_structure(self):

        """Проверка структуры таблицы wl_china"""

        if self.cursor is None:

            return False

        try:

            logger.info("🔍 Проверка структуры таблицы wl_china...")

            

            # Получаем информацию о колонках

            self.cursor.execute("PRAGMA table_info(wl_china)")

            columns = self.cursor.fetchall()

            

            required_columns = [

                'N_Линии',

                'Номер_чертежа', 

                'Номер_сварного_шва',

                'Клеймо_сварщика_корневой_слой',

                'Клеймо_сварщика_заполнение_облицовка',

                'Метод_сварки_корневой_слой',

                'Метод_сварки_заполнение_облицовка',

                'Тип_соединения_российский_стандарт',

                'Результаты_АКТ_ВИК',

                'Результаты_Заключения_РК',

                'Результаты_Заключения_PT',

                'Результаты_Заключения_Стилоскопирование',

                'Результаты_Заключения_МПД'

            ]

            

            existing_columns = [col[1] for col in columns]

            missing_columns = [col for col in required_columns if col not in existing_columns]

            

            if missing_columns:

                logger.error(f"❌ Отсутствуют необходимые колонки: {missing_columns}")

                return False

            

            logger.info("✅ Все необходимые колонки найдены")

            

            # Проверяем наличие опционального столбца Базовый_материал_1 для исключения из стилоскопирования

            self.has_bazovyy_material = 'Базовый_материал_1' in existing_columns

            if self.has_bazovyy_material:

                logger.info("✅ Найден столбец Базовый_материал_1 - будет применяться исключение для стилоскопирования")

            else:

                logger.info("ℹ️ Столбец Базовый_материал_1 не найден - исключение для стилоскопирования не применяется")

            

            # Проверяем наличие данных

            self.cursor.execute("""

                SELECT COUNT(*) 

                FROM wl_china 

                WHERE (Клеймо_сварщика_корневой_слой != '' 

                       OR Клеймо_сварщика_заполнение_облицовка != '')

                  AND Номер_сварного_шва IS NOT NULL

            """)

            valid_records = self.cursor.fetchone()[0]

            

            logger.info(f"📊 Записей с клеймами сварщиков: {valid_records}")

            

            return valid_records > 0

            

        except Exception as e:

            logger.error(f"❌ Ошибка проверки структуры: {e}")

            self.stats['errors'] += 1

            return False

    

    def drop_existing_table(self):

        """Удаление существующей таблицы сварено_сварщиком"""

        if self.cursor is None or self.conn is None:

            return

        try:

            logger.info("🗑️ Удаление существующей таблицы сварено_сварщиком...")

            

            self.cursor.execute("DROP TABLE IF EXISTS сварено_сварщиком")

            self.conn.commit()

            

            logger.info("✅ Таблица сварено_сварщиком удалена (если существовала)")

            

        except Exception as e:

            logger.error(f"❌ Ошибка удаления таблицы: {e}")

            self.stats['errors'] += 1

    

    def create_new_table(self):

        """Создание новой таблицы сварено_сварщиком"""

        if self.cursor is None or self.conn is None:

            return

        try:

            logger.info("🏗️ Создание новой таблицы сварено_сварщиком...")

            

            create_table_sql = """

            CREATE TABLE сварено_сварщиком (

                id INTEGER PRIMARY KEY AUTOINCREMENT,

                N_Линии INTEGER,

                _Линия TEXT,

                Номер_чертежа TEXT,

                Всего_сваренно_сварщиком INTEGER,

                РК INTEGER DEFAULT 0,

                "Стило(PMI)" INTEGER DEFAULT 0,

                ПВК INTEGER DEFAULT 0,

                МПД INTEGER DEFAULT 0,

                ВИК INTEGER DEFAULT 0,

                "No TEST-PACK" TEXT,

                "№ ИЧ" TEXT,

                "Категория и группа трубопровода по ГОСТ 32569-2013 / Pipeline Group and category according to GOST 32569-2013) " TEXT,

                "Материал / Material  " TEXT,

                "РК (Радиографический контроль) / RT" TEXT,

                "СТ (Стилоскоп) Стилоскопирование / PMI" TEXT,

                "МПК (Магнитопорошковый контроль) / MT" TEXT,

                "ПВК (Контроль проникающими веществами) / PT" TEXT,

                клейма_сварщиков TEXT,

                методы_сварки TEXT,

                типы_сварных_швов TEXT,

                исключено_по_типу INTEGER DEFAULT 0,

                исключено_по_материалу_стило INTEGER DEFAULT 0,

                заявлен_вик INTEGER DEFAULT 0,

                годен_вик INTEGER DEFAULT 0,

                не_годен_вик INTEGER DEFAULT 0,

                не_подан_вик INTEGER DEFAULT 0,

                заявлен_рк INTEGER DEFAULT 0,

                годен_рк INTEGER DEFAULT 0,

                не_годен_рк INTEGER DEFAULT 0,

                не_подан_рк INTEGER DEFAULT 0,

                заявлен_pt INTEGER DEFAULT 0,

                годен_pt INTEGER DEFAULT 0,

                не_годен_pt INTEGER DEFAULT 0,

                не_подан_pt INTEGER DEFAULT 0,

                заявлен_стилоскопирование INTEGER DEFAULT 0,

                годен_стилоскопирование INTEGER DEFAULT 0,

                не_годен_стилоскопирование INTEGER DEFAULT 0,

                не_подан_стилоскопирование INTEGER DEFAULT 0,

                заявлен_мпд INTEGER DEFAULT 0,

                годен_мпд INTEGER DEFAULT 0,

                не_годен_мпд INTEGER DEFAULT 0,

                не_подан_мпд INTEGER DEFAULT 0,

                "Не_Внесенно(не_сваренно)" INTEGER DEFAULT 0,

                дата_обновления DATETIME DEFAULT CURRENT_TIMESTAMP

            )

            """

            

            self.cursor.execute(create_table_sql)

            self.conn.commit()

            

            logger.info("✅ Таблица сварено_сварщиком создана успешно")

            

        except Exception as e:

            logger.error(f"❌ Ошибка создания таблицы: {e}")

            self.stats['errors'] += 1

    

    def populate_table(self):

        """Заполнение таблицы данными"""

        if self.cursor is None or self.conn is None:

            return

        try:

            logger.info("📝 Заполнение таблицы данными...")

            

            # Формируем условие исключения для стилоскопирования
            # Исключаем записи с материалом '09Г2С'/'09Г2C' И записи где Результаты_Заключения_РК = 'Не годен'

            if self.has_bazovyy_material:

                # Условие для проверки материала и статуса РК - используется внутри CASE WHEN
                # Исключаем если: материал содержит '09Г2С'/'09Г2C' ИЛИ Результаты_Заключения_РК = 'Не годен'

                material_check = "(COALESCE(wc.Базовый_материал_1, '') NOT LIKE '%09Г2C%' AND COALESCE(wc.Базовый_материал_1, '') NOT LIKE '%09Г2С%' AND TRIM(COALESCE(wc.Результаты_Заключения_РК, '')) != 'Не годен')"

                logger.info("🔧 Применяется исключение для стилоскопирования: исключаются записи с Базовый_материал_1 содержащим '09Г2C' или '09Г2С', а также записи где Результаты_Заключения_РК = 'Не годен'")

            else:

                # Если столбца материала нет, исключаем только записи где Результаты_Заключения_РК = 'Не годен'

                material_check = "(TRIM(COALESCE(wc.Результаты_Заключения_РК, '')) != 'Не годен')"

                logger.info("🔧 Применяется исключение для стилоскопирования: исключаются записи где Результаты_Заключения_РК = 'Не годен'")

            

            # SQL запрос для заполнения таблицы

            insert_sql = f"""

            INSERT INTO сварено_сварщиком (N_Линии, _Линия, Номер_чертежа, Всего_сваренно_сварщиком, "Не_Внесенно(не_сваренно)", клейма_сварщиков, методы_сварки, типы_сварных_швов, исключено_по_типу, исключено_по_материалу_стило, заявлен_вик, годен_вик, не_годен_вик, не_подан_вик, заявлен_рк, годен_рк, не_годен_рк, не_подан_рк, заявлен_pt, годен_pt, не_годен_pt, не_подан_pt, заявлен_стилоскопирование, годен_стилоскопирование, не_годен_стилоскопирование, не_подан_стилоскопирование, заявлен_мпд, годен_мпд, не_годен_мпд, не_подан_мпд)

            SELECT

                wc.N_Линии,

                CASE 

                    WHEN wc.N_Линии IS NOT NULL THEN

                        CASE 

                            WHEN INSTR(CAST(wc.N_Линии AS TEXT), ' ') > 0 THEN 

                                SUBSTR(CAST(wc.N_Линии AS TEXT), 1, INSTR(CAST(wc.N_Линии AS TEXT), ' ') - 1)

                            WHEN INSTR(CAST(wc.N_Линии AS TEXT), '(') > 0 THEN 

                                SUBSTR(CAST(wc.N_Линии AS TEXT), 1, INSTR(CAST(wc.N_Линии AS TEXT), '(') - 1)

                            ELSE CAST(wc.N_Линии AS TEXT)

                        END

                    ELSE NULL

                END as _Линия,

                wc.Номер_чертежа,

                COUNT(wc.Номер_сварного_шва) as Всего_сваренно_сварщиком,

                0 as "Не_Внесенно(не_сваренно)",

                (

                    SELECT group_concat(part, '/')

                    FROM (

                        SELECT DISTINCT part

                        FROM (

                            SELECT trim(wc.Клеймо_сварщика_корневой_слой) as part 

                            WHERE wc.Клеймо_сварщика_корневой_слой != ''

                            UNION ALL

                            SELECT trim(wc.Клеймо_сварщика_заполнение_облицовка) as part 

                            WHERE wc.Клеймо_сварщика_заполнение_облицовка != ''

                        )

                        WHERE part IS NOT NULL AND part != ''

                    )

                ) as клейма_сварщиков,

                (

                    SELECT group_concat(part, '/')

                    FROM (

                        SELECT DISTINCT part

                        FROM (

                            SELECT trim(wc.Метод_сварки_корневой_слой) as part 

                            WHERE wc.Метод_сварки_корневой_слой IS NOT NULL AND wc.Метод_сварки_корневой_слой != ''

                            UNION ALL

                            SELECT trim(wc.Метод_сварки_заполнение_облицовка) as part 

                            WHERE wc.Метод_сварки_заполнение_облицовка IS NOT NULL AND wc.Метод_сварки_заполнение_облицовка != ''

                        )

                        WHERE part IS NOT NULL AND part != ''

                    )

                ) as методы_сварки,

                (

                    SELECT group_concat(part, '/')

                    FROM (

                        SELECT DISTINCT part

                        FROM (

                            SELECT trim(wc.Тип_соединения_российский_стандарт) as part 

                            WHERE wc.Тип_соединения_российский_стандарт IS NOT NULL AND wc.Тип_соединения_российский_стандарт != ''

                        )

                        WHERE part IS NOT NULL AND part != ''

                    )

                ) as типы_сварных_швов,

                SUM(CASE WHEN wc.Тип_соединения_российский_стандарт LIKE '%У17%' OR wc.Тип_соединения_российский_стандарт LIKE '%У19%' THEN 1 ELSE 0 END) as исключено_по_типу,

                {f"SUM(CASE WHEN ((COALESCE(wc.Базовый_материал_1, '') LIKE '%09Г2C%' OR COALESCE(wc.Базовый_материал_1, '') LIKE '%09Г2С%') OR TRIM(COALESCE(wc.Результаты_Заключения_РК, '')) = 'Не годен') THEN 1 ELSE 0 END)" if self.has_bazovyy_material else f"SUM(CASE WHEN TRIM(COALESCE(wc.Результаты_Заключения_РК, '')) = 'Не годен' THEN 1 ELSE 0 END)"} as исключено_по_материалу_стило,

                SUM(CASE WHEN TRIM(COALESCE(wc.Результаты_АКТ_ВИК, '')) = 'Заказ отправлен' THEN 1 ELSE 0 END) as заявлен_вик,

                SUM(CASE WHEN TRIM(COALESCE(wc.Результаты_АКТ_ВИК, '')) = 'годен' THEN 1 ELSE 0 END) as годен_вик,

                SUM(CASE WHEN TRIM(COALESCE(wc.Результаты_АКТ_ВИК, '')) = 'Не годен' THEN 1 ELSE 0 END) as не_годен_вик,

                SUM(CASE WHEN wc.Результаты_АКТ_ВИК IS NULL OR TRIM(COALESCE(wc.Результаты_АКТ_ВИК, '')) = '' OR LOWER(TRIM(COALESCE(wc.Результаты_АКТ_ВИК, ''))) = 'none' THEN 1 ELSE 0 END) as не_подан_вик,

                SUM(CASE WHEN TRIM(COALESCE(wc.Результаты_Заключения_РК, '')) = 'Заказ отправлен' THEN 1 ELSE 0 END) as заявлен_рк,

                SUM(CASE WHEN TRIM(COALESCE(wc.Результаты_Заключения_РК, '')) = 'годен' THEN 1 ELSE 0 END) as годен_рк,

                SUM(CASE WHEN TRIM(COALESCE(wc.Результаты_Заключения_РК, '')) = 'Не годен' THEN 1 ELSE 0 END) as не_годен_рк,

                0 as не_подан_рк,

                SUM(CASE WHEN TRIM(COALESCE(wc.Результаты_Заключения_PT, '')) = 'Заказ отправлен' THEN 1 ELSE 0 END) as заявлен_pt,

                SUM(CASE WHEN TRIM(COALESCE(wc.Результаты_Заключения_PT, '')) = 'годен' THEN 1 ELSE 0 END) as годен_pt,

                SUM(CASE WHEN TRIM(COALESCE(wc.Результаты_Заключения_PT, '')) = 'не годен' THEN 1 ELSE 0 END) as не_годен_pt,

                SUM(CASE WHEN wc.Результаты_Заключения_PT IS NULL OR TRIM(wc.Результаты_Заключения_PT) = '' OR LOWER(TRIM(wc.Результаты_Заключения_PT)) = 'none' THEN 1 ELSE 0 END) as не_подан_pt,

                SUM(CASE WHEN TRIM(COALESCE(wc.Результаты_Заключения_Стилоскопирование, '')) = 'Заказ отправлен' AND {material_check} THEN 1 ELSE 0 END) as заявлен_стилоскопирование,

                SUM(CASE WHEN TRIM(COALESCE(wc.Результаты_Заключения_Стилоскопирование, '')) = 'годен' AND {material_check} THEN 1 ELSE 0 END) as годен_стилоскопирование,

                SUM(CASE WHEN TRIM(COALESCE(wc.Результаты_Заключения_Стилоскопирование, '')) = 'Не годен' AND {material_check} THEN 1 ELSE 0 END) as не_годен_стилоскопирование,

                SUM(CASE WHEN (wc.Результаты_Заключения_Стилоскопирование IS NULL OR TRIM(COALESCE(wc.Результаты_Заключения_Стилоскопирование, '')) = '' OR LOWER(TRIM(COALESCE(wc.Результаты_Заключения_Стилоскопирование, ''))) = 'none') AND {material_check} THEN 1 ELSE 0 END) as не_подан_стилоскопирование,

                SUM(CASE WHEN TRIM(COALESCE(wc.Результаты_Заключения_МПД, '')) = 'Заказ отправлен' THEN 1 ELSE 0 END) as заявлен_мпд,

                SUM(CASE WHEN TRIM(COALESCE(wc.Результаты_Заключения_МПД, '')) = 'годен' THEN 1 ELSE 0 END) as годен_мпд,

                SUM(CASE WHEN TRIM(COALESCE(wc.Результаты_Заключения_МПД, '')) = 'Не годен' THEN 1 ELSE 0 END) as не_годен_мпд,

                SUM(CASE WHEN wc.Результаты_Заключения_МПД IS NULL OR TRIM(COALESCE(wc.Результаты_Заключения_МПД, '')) = '' OR LOWER(TRIM(COALESCE(wc.Результаты_Заключения_МПД, ''))) = 'none' THEN 1 ELSE 0 END) as не_подан_мпд

            FROM

                wl_china wc

            WHERE

                -- учитываем только сваренные строки (непустая дата)

                wc.Дата_сварки IS NOT NULL AND TRIM(wc.Дата_сварки) <> ''

                AND (

                    wc.Клеймо_сварщика_корневой_слой IS NOT NULL AND wc.Клеймо_сварщика_корневой_слой <> ''

                    OR wc.Клеймо_сварщика_заполнение_облицовка IS NOT NULL AND wc.Клеймо_сварщика_заполнение_облицовка <> ''

                )

                AND wc.Номер_сварного_шва IS NOT NULL

            GROUP BY 

                wc.N_Линии,

                _Линия,

                wc.Номер_чертежа,

                клейма_сварщиков,

                методы_сварки,

                типы_сварных_швов

            HAVING клейма_сварщиков IS NOT NULL

            """

            self.cursor.execute(insert_sql)

            self.conn.commit()



            # Добавляем НЕ СВАРЕННЫЕ (пустая дата и пустые клейма)

            logger.info("🔧 Заполнение не сваренных записей...")

            insert_unwelded_sql = """

            INSERT INTO сварено_сварщиком (N_Линии, _Линия, Номер_чертежа, Всего_сваренно_сварщиком, "Не_Внесенно(не_сваренно)", клейма_сварщиков, методы_сварки, типы_сварных_швов, исключено_по_типу, исключено_по_материалу_стило, заявлен_вик, годен_вик, не_годен_вик, не_подан_вик, заявлен_рк, годен_рк, не_годен_рк, не_подан_рк, заявлен_pt, годен_pt, не_годен_pt, не_подан_pt, заявлен_стилоскопирование, годен_стилоскопирование, не_годен_стилоскопирование, не_подан_стилоскопирование, заявлен_мпд, годен_мпд, не_годен_мпд, не_подан_мпд)

            SELECT

                wc.N_Линии,

                CASE 

                    WHEN wc.N_Линии IS NOT NULL THEN

                        CASE 

                            WHEN INSTR(CAST(wc.N_Линии AS TEXT), ' ') > 0 THEN 

                                SUBSTR(CAST(wc.N_Линии AS TEXT), 1, INSTR(CAST(wc.N_Линии AS TEXT), ' ') - 1)

                            WHEN INSTR(CAST(wc.N_Линии AS TEXT), '(') > 0 THEN 

                                SUBSTR(CAST(wc.N_Линии AS TEXT), 1, INSTR(CAST(wc.N_Линии AS TEXT), '(') - 1)

                            ELSE CAST(wc.N_Линии AS TEXT)

                        END

                    ELSE NULL

                END as _Линия,

                wc.Номер_чертежа,

                0 as Всего_сваренно_сварщиком,

                COUNT(wc.Номер_сварного_шва) as "Не_Внесенно(не_сваренно)",

                NULL as клейма_сварщиков,

                NULL as методы_сварки,

                NULL as типы_сварных_швов,

                0 as исключено_по_типу,

                0 as исключено_по_материалу_стило,

                0 as заявлен_вик,

                0 as годен_вик,

                0 as не_годен_вик,

                0 as не_подан_вик,

                0 as заявлен_рк,

                0 as годен_рк,

                0 as не_годен_рк,

                0 as не_подан_рк,

                0 as заявлен_pt,

                0 as годен_pt,

                0 as не_годен_pt,

                0 as не_подан_pt,

                0 as заявлен_стилоскопирование,

                0 as годен_стилоскопирование,

                0 as не_годен_стилоскопирование,

                0 as не_подан_стилоскопирование,

                0 as заявлен_мпд,

                0 as годен_мпд,

                0 as не_годен_мпд,

                0 as не_подан_мпд

            FROM

                wl_china wc

            WHERE

                (wc.Дата_сварки IS NULL OR wc.Дата_сварки = '' OR TRIM(wc.Дата_сварки) = '')

                AND (wc.Клеймо_сварщика_корневой_слой IS NULL OR wc.Клеймо_сварщика_корневой_слой = '')

                AND (wc.Клеймо_сварщика_заполнение_облицовка IS NULL OR wc.Клеймо_сварщика_заполнение_облицовка = '')

                AND wc.Номер_сварного_шва IS NOT NULL

            GROUP BY 

                wc.N_Линии,

                _Линия,

                wc.Номер_чертежа

            HAVING COUNT(wc.Номер_сварного_шва) > 0

            """

            self.cursor.execute(insert_unwelded_sql)

            self.conn.commit()



            # Расчет планового РК = ceil((Всего_сваренно_сварщиком - не_годен_рк) * RT% / 100) - годен_рк, не ниже 0

            logger.info("🧮 Расчет столбца РК на основе RT из 'основнаяНК'...")

            update_rk_sql = """

            UPDATE сварено_сварщиком AS ss

            SET РК = MAX(

                CAST(((CASE WHEN (ss.Всего_сваренно_сварщиком - ss.не_годен_рк - COALESCE(ss.исключено_по_типу,0)) > 0 THEN (ss.Всего_сваренно_сварщиком - ss.не_годен_рк - COALESCE(ss.исключено_по_типу,0)) ELSE 0 END) * COALESCE((

                    SELECT он."РК (Радиографический контроль) / RT"

                    FROM основнаяНК он

                    WHERE он."Номер линии / Line No" = ss._Линия

                    LIMIT 1

                ), 0) + 99) / 100 AS INT) - ss.годен_рк,

                0

            )

            """

            self.cursor.execute(update_rk_sql)

            self.conn.commit()



            # Пересчет не_подан_рк = max(РК - заявлен_рк, 0)

            logger.info("🧮 Пересчет не_подан_рк как РК - заявлен_рк...")

            update_not_submitted_rk_sql = """

            UPDATE сварено_сварщиком

            SET не_подан_рк = MAX(РК - COALESCE(заявлен_рк, 0), 0)

            """

            self.cursor.execute(update_not_submitted_rk_sql)

            self.conn.commit()



            # Стило(PMI):

            # - если по линии в основнойНК PMI содержит '*', ставим 2

            # - иначе, если есть число (процент), рассчитываем как ceil((Всего_сваренно_сварщиком - не_годен_рк - исключено_по_типу - исключено_по_материалу_стило) * (процент) / 100), не ниже 0

            logger.info("🧮 Расчет столбца Стило(PMI) с учетом процентов в основнойНК...")

            update_pmi_sql = """

            UPDATE сварено_сварщиком AS ss

            SET "Стило(PMI)" = MAX(

                CASE 

                    WHEN EXISTS (

                        SELECT 1 FROM основнаяНК он

                        WHERE он."Номер линии / Line No" = ss._Линия

                          AND он."СТ (Стилоскоп) Стилоскопирование / PMI" LIKE '%*%'

                    ) THEN 2

                    ELSE (

                        -- ceil(база * процент / 100)

                        CASE 

                            WHEN (

                                ((MAX(ss.Всего_сваренно_сварщиком - ss.не_годен_рк - COALESCE(ss.исключено_по_типу,0) - COALESCE(ss.исключено_по_материалу_стило, 0), 0)) * COALESCE((

                                    SELECT CAST(REPLACE(REPLACE(он."СТ (Стилоскоп) Стилоскопирование / PMI", '%', ''), ',', '.') AS REAL)

                                    FROM основнаяНК он

                                    WHERE он."Номер линии / Line No" = ss._Линия

                                    LIMIT 1

                                ), 0)) / 100.0

                            ) > CAST((

                                ((MAX(ss.Всего_сваренно_сварщиком - ss.не_годен_рк - COALESCE(ss.исключено_по_материалу_стило, 0), 0)) * COALESCE((

                                    SELECT CAST(REPLACE(REPLACE(он."СТ (Стилоскоп) Стилоскопирование / PMI", '%', ''), ',', '.') AS REAL)

                                    FROM основнаяНК он

                                    WHERE он."Номер линии / Line No" = ss._Линия

                                    LIMIT 1

                                ), 0)) / 100.0

                            ) AS INT)

                            THEN CAST((

                                ((MAX(ss.Всего_сваренно_сварщиком - ss.не_годен_рк - COALESCE(ss.исключено_по_материалу_стило, 0), 0)) * COALESCE((

                                    SELECT CAST(REPLACE(REPLACE(он."СТ (Стилоскоп) Стилоскопирование / PMI", '%', ''), ',', '.') AS REAL)

                                    FROM основнаяНК он

                                    WHERE он."Номер линии / Line No" = ss._Линия

                                    LIMIT 1

                                ), 0)) / 100.0

                            ) AS INT) + 1

                            ELSE CAST((

                                ((MAX(ss.Всего_сваренно_сварщиком - ss.не_годен_рк - COALESCE(ss.исключено_по_материалу_стило, 0), 0)) * COALESCE((

                                    SELECT CAST(REPLACE(REPLACE(он."СТ (Стилоскоп) Стилоскопирование / PMI", '%', ''), ',', '.') AS REAL)

                                    FROM основнаяНК он

                                    WHERE он."Номер линии / Line No" = ss._Линия

                                    LIMIT 1

                                ), 0)) / 100.0

                            ) AS INT)

                        END

                    )

                END - COALESCE(ss.годен_стилоскопирование, 0),

                0

            )

            """

            self.cursor.execute(update_pmi_sql)

            self.conn.commit()



            # Пересчет не_подан_стилоскопирование = Стило(PMI) - заявлен_стилоскопирование - годен_стилоскопирование

            logger.info("🧮 Пересчет не_подан_стилоскопирование как 'Стило(PMI)' - заявлен_стилоскопирование - годен_стилоскопирование...")

            update_not_submitted_pmi_sql = """

            UPDATE сварено_сварщиком

            SET не_подан_стилоскопирование = MAX("Стило(PMI)" - COALESCE(заявлен_стилоскопирование, 0) - COALESCE(годен_стилоскопирование, 0), 0)

            """

            self.cursor.execute(update_not_submitted_pmi_sql)

            self.conn.commit()



            # ПВК = ceil((Всего_сваренно_сварщиком - не_годен_рк - исключено_по_типу) * PT% / 100), не ниже 0

            logger.info("🧮 Расчет столбца ПВК с учетом процента и исключением 'Не годен' по РК и типов У17/У19...")

            update_pt_sql = """

            UPDATE сварено_сварщиком AS ss

            SET ПВК = MAX(

                CAST((((CASE WHEN (ss.Всего_сваренно_сварщиком - ss.не_годен_рк - COALESCE(ss.исключено_по_типу,0)) > 0 THEN (ss.Всего_сваренно_сварщиком - ss.не_годен_рк - COALESCE(ss.исключено_по_типу,0)) ELSE 0 END) * COALESCE((

                    SELECT CAST(REPLACE(REPLACE(он."ПВК (Контроль проникающими веществами) / PT", '%', ''), ',', '.') AS REAL)

                    FROM основнаяНК он

                    WHERE он."Номер линии / Line No" = ss._Линия

                    LIMIT 1

                ), 0)) + 99) / 100 AS INT) - COALESCE(ss.годен_pt, 0),

                0

            )

            """

            self.cursor.execute(update_pt_sql)

            self.conn.commit()



            # Пересчет не_подан_pt по формуле: max(ПВК - заявлен_pt - годен_pt, 0)

            logger.info("🧮 Пересчет не_подан_pt как ПВК - заявлен_pt - годен_pt...")

            update_not_submitted_pt_sql = """

            UPDATE сварено_сварщиком

            SET не_подан_pt = MAX(ПВК - COALESCE(заявлен_pt, 0), 0)

            """

            self.cursor.execute(update_not_submitted_pt_sql)

            self.conn.commit()



            # МПД = ceil((Всего_сваренно_сварщиком - не_годен_рк - исключено_по_типу) * MT% / 100)

            logger.info("🧮 Расчет столбца МПД с исключением 'Не годен' по РК и типов У17/У19...")

            update_mt_sql = """

            UPDATE сварено_сварщиком AS ss

            SET МПД = MAX(

                CAST((((CASE WHEN (ss.Всего_сваренно_сварщиком - ss.не_годен_рк - COALESCE(ss.исключено_по_типу,0)) > 0 THEN (ss.Всего_сваренно_сварщиком - ss.не_годен_рк - COALESCE(ss.исключено_по_типу,0)) ELSE 0 END) * COALESCE((

                    SELECT он."МПК (Магнитопорошковый контроль) / MT"

                    FROM основнаяНК он

                    WHERE он."Номер линии / Line No" = ss._Линия

                    LIMIT 1

                ), 0)) + 99) / 100 AS INT) - COALESCE(ss.годен_мпд, 0),

                0

            )

            """

            self.cursor.execute(update_mt_sql)

            self.conn.commit()



            # Пересчет не_подан_мпд = max(МПД - заявлен_мпд, 0)

            logger.info("🧮 Пересчет не_подан_мпд как МПД - заявлен_мпд...")

            update_not_submitted_mt_sql = """

            UPDATE сварено_сварщиком

            SET не_подан_мпд = MAX(МПД - COALESCE(заявлен_мпд, 0), 0)

            """

            self.cursor.execute(update_not_submitted_mt_sql)

            self.conn.commit()



            # ВИК = заявлен_вик (NULL -> 0)

            logger.info("🧮 Заполнение ВИК значением заявлен_вик...")

            self.cursor.execute("UPDATE сварено_сварщиком SET ВИК = COALESCE(заявлен_вик, 0)")

            self.conn.commit()



            # "No TEST-PACK" и "№ ИЧ" из LstTp_12460 по соответствию Линия -> N_Линии

            logger.info("🔗 Заполнение 'No TEST-PACK' и '№ ИЧ' из LstTp_12460...")

            update_lsttp_sql = """

            UPDATE сварено_сварщиком AS ss

            SET 

                "No TEST-PACK" = COALESCE(

                    (

                        SELECT lt."No TEST-PACK" 

                        FROM LstTp_12460 lt 

                        WHERE lt.Линия = ss.N_Линии

                        LIMIT 1

                    ),

                    (

                        SELECT lt."No TEST-PACK" 

                        FROM LstTp_12460 lt 

                        WHERE (

                            CASE 

                                WHEN lt.Линия IS NOT NULL THEN

                                    CASE 

                                        WHEN INSTR(CAST(lt.Линия AS TEXT), ' ') > 0 THEN 

                                            SUBSTR(CAST(lt.Линия AS TEXT), 1, INSTR(CAST(lt.Линия AS TEXT), ' ') - 1)

                                        WHEN INSTR(CAST(lt.Линия AS TEXT), '(') > 0 THEN 

                                            SUBSTR(CAST(lt.Линия AS TEXT), 1, INSTR(CAST(lt.Линия AS TEXT), '(') - 1)

                                        ELSE CAST(lt.Линия AS TEXT)

                                    END

                                ELSE NULL

                            END

                        ) = ss._Линия

                        LIMIT 1

                    )

                ),

                "№ ИЧ" = COALESCE(

                    (

                        SELECT lt."№ ИЧ" 

                        FROM LstTp_12460 lt 

                        WHERE lt.Линия = ss.N_Линии

                        LIMIT 1

                    ),

                    (

                        SELECT lt."№ ИЧ" 

                        FROM LstTp_12460 lt 

                        WHERE (

                            CASE 

                                WHEN lt.Линия IS NOT NULL THEN

                                    CASE 

                                        WHEN INSTR(CAST(lt.Линия AS TEXT), ' ') > 0 THEN 

                                            SUBSTR(CAST(lt.Линия AS TEXT), 1, INSTR(CAST(lt.Линия AS TEXT), ' ') - 1)

                                        WHEN INSTR(CAST(lt.Линия AS TEXT), '(') > 0 THEN 

                                            SUBSTR(CAST(lt.Линия AS TEXT), 1, INSTR(CAST(lt.Линия AS TEXT), '(') - 1)

                                        ELSE CAST(lt.Линия AS TEXT)

                                    END

                                ELSE NULL

                            END

                        ) = ss._Линия

                        LIMIT 1

                    )

                )

            """

            self.cursor.execute(update_lsttp_sql)

            self.conn.commit()



            # Копируем справочные поля из основнойНК по ключу _Линия -> "Номер линии / Line No"

            logger.info("🔗 Копирование справочных полей из основнойНК...")

            update_mainnk_sql = """

            UPDATE сварено_сварщиком AS ss

            SET 

                "Категория и группа трубопровода по ГОСТ 32569-2013 / Pipeline Group and category according to GOST 32569-2013) " = (

                    SELECT он."Категория и группа трубопровода по ГОСТ 32569-2013 / Pipeline Group and category according to GOST 32569-2013) "

                    FROM основнаяНК он WHERE он."Номер линии / Line No" = ss._Линия LIMIT 1

                ),

                "Материал / Material  " = (

                    SELECT он."Материал / Material  "

                    FROM основнаяНК он WHERE он."Номер линии / Line No" = ss._Линия LIMIT 1

                ),

                "РК (Радиографический контроль) / RT" = (

                    SELECT он."РК (Радиографический контроль) / RT"

                    FROM основнаяНК он WHERE он."Номер линии / Line No" = ss._Линия LIMIT 1

                ),

                "СТ (Стилоскоп) Стилоскопирование / PMI" = (

                    SELECT он."СТ (Стилоскоп) Стилоскопирование / PMI"

                    FROM основнаяНК он WHERE он."Номер линии / Line No" = ss._Линия LIMIT 1

                ),

                "МПК (Магнитопорошковый контроль) / MT" = (

                    SELECT он."МПК (Магнитопорошковый контроль) / MT"

                    FROM основнаяНК он WHERE он."Номер линии / Line No" = ss._Линия LIMIT 1

                ),

                "ПВК (Контроль проникающими веществами) / PT" = (

                    SELECT он."ПВК (Контроль проникающими веществами) / PT"

                    FROM основнаяНК он WHERE он."Номер линии / Line No" = ss._Линия LIMIT 1

                )

            """

            self.cursor.execute(update_mainnk_sql)

            self.conn.commit()

            

            # Получаем количество вставленных записей

            self.cursor.execute("SELECT COUNT(*) FROM сварено_сварщиком")

            inserted_count = self.cursor.fetchone()[0]

            self.stats['created_groups'] = inserted_count

            

            logger.info(f"✅ Вставлено {inserted_count} записей в таблицу сварено_сварщиком")

            

        except Exception as e:

            logger.error(f"❌ Ошибка заполнения таблицы: {e}")

            self.stats['errors'] += 1

            if self.conn:

                self.conn.rollback()

    

    def verify_results(self):

        """Проверка результатов создания таблицы"""

        if self.cursor is None:

            return False

        try:

            logger.info("🔍 Проверка результатов...")

            

            # Получаем статистику по созданной таблице

            self.cursor.execute("SELECT COUNT(*) FROM сварено_сварщиком")

            total_records = self.cursor.fetchone()[0]

            

            # Получаем примеры записей

            self.cursor.execute("""

                SELECT N_Линии, _Линия, Номер_чертежа, Всего_сваренно_сварщиком, клейма_сварщиков, методы_сварки, типы_сварных_швов, 

                       заявлен_вик, годен_вик, не_годен_вик, не_подан_вик, заявлен_рк, годен_рк, не_годен_рк

                FROM сварено_сварщиком 

                ORDER BY Всего_сваренно_сварщиком DESC 

                LIMIT 5

            """)

            sample_records = self.cursor.fetchall()

            

            logger.info(f"📊 Результаты создания таблицы:")

            logger.info(f"   - Всего записей: {total_records}")

            logger.info(f"   - Примеры записей (топ-5 по количеству сварных швов):")

            

            for i, record in enumerate(sample_records, 1):

                n_линии, _линия, номер_чертежа, всего_сваренно, клейма, методы, типы, заявлен_вик, годен_вик, не_годен_вик, не_подан_вик, заявлен_рк, годен_рк, не_годен_рк = record

                logger.info(f"     {i}. Линия {n_линии} (_Линия: {_линия}), Чертеж {номер_чертежа}: {всего_сваренно} швов")

                logger.info(f"        Клейма: {клейма}")

                logger.info(f"        Методы сварки: {методы}")

                logger.info(f"        Типы сварных швов: {типы}")

                logger.info(f"        АКТ ВИК - Заявлен: {заявлен_вик}, Годен: {годен_вик}, Не годен: {не_годен_вик}, Не подан: {не_подан_вик}")

                logger.info(f"        Заключения РК - Заявлен: {заявлен_рк}, Годен: {годен_рк}, Не годен: {не_годен_рк}")

            

            # Статистика по линиям

            self.cursor.execute("""

                SELECT N_Линии, _Линия, COUNT(*) as групп, SUM(Всего_сваренно_сварщиком) as всего_швов

                FROM сварено_сварщиком 

                GROUP BY N_Линии, _Линия

                ORDER BY всего_швов DESC

            """)

            line_stats = self.cursor.fetchall()

            

            logger.info(f"📊 Статистика по линиям:")

            for n_линии, _линия, групп, всего_швов in line_stats:

                logger.info(f"   - Линия {n_линии} (_Линия: {_линия}): {групп} групп, {всего_швов} швов")

            

            # Статистика по _Линия

            self.cursor.execute("""

                SELECT _Линия, COUNT(*) as групп, SUM(Всего_сваренно_сварщиком) as всего_швов

                FROM сварено_сварщиком 

                WHERE _Линия IS NOT NULL

                GROUP BY _Линия

                ORDER BY всего_швов DESC

            """)

            _line_stats = self.cursor.fetchall()

            

            logger.info(f"📊 Статистика по _Линия:")

            for _линия, групп, всего_швов in _line_stats:

                logger.info(f"   - _Линия {_линия}: {групп} групп, {всего_швов} швов")

            

            return total_records > 0

            

        except Exception as e:

            logger.error(f"❌ Ошибка проверки результатов: {e}")

            self.stats['errors'] += 1

            return False

    

    def print_final_report(self):

        """Печать итогового отчета"""

        logger.info("=" * 60)

        logger.info("ИТОГОВЫЙ ОТЧЕТ СОЗДАНИЯ ТАБЛИЦЫ СВАРЕНО_СВАРЩИКОМ")

        logger.info("=" * 60)

        logger.info(f"📊 Статистика:")

        logger.info(f"   - Всего записей в wl_china: {self.stats['total_wl_china_records']}")

        logger.info(f"   - Создано групп: {self.stats['created_groups']}")

        logger.info(f"   - Ошибок: {self.stats['errors']}")

        logger.info("=" * 60)

        

        if self.stats['errors'] == 0:

            logger.info("✅ Таблица сварено_сварщиком создана успешно!")

        else:

            logger.warning(f"⚠️ Создание таблицы завершено с {self.stats['errors']} ошибками")

    

    def run_creation(self):

        """Основной метод создания таблицы"""

        logger.info("=" * 60)

        logger.info("НАЧАЛО СОЗДАНИЯ ТАБЛИЦЫ СВАРЕНО_СВАРЩИКОМ")

        logger.info("=" * 60)

        

        # Подключение к базе данных

        if not self.connect_to_database():

            return False

        

        try:

            # Предобработка пустых дат: обнуление клейм

            self.preprocess_welding_data()

            # Получение статистики

            self.get_table_stats()

            

            # Проверка структуры таблицы wl_china

            if not self.check_wl_china_structure():

                logger.error("❌ Таблица wl_china не содержит необходимых данных")

                return False

            

            # Удаление существующей таблицы

            self.drop_existing_table()

            

            # Создание новой таблицы

            self.create_new_table()

            

            # Заполнение таблицы данными

            self.populate_table()

            

            # Проверка результатов

            if not self.verify_results():

                logger.error("❌ Ошибка при проверке результатов")

                return False

            

            # Итоговый отчет

            self.print_final_report()

            

            return True

            

        except Exception as e:

            logger.error(f"❌ Критическая ошибка создания таблицы: {e}")

            import traceback

            logger.error("Полный стек ошибки:")

            logger.error(traceback.format_exc())

            return False

        

        finally:

            if self.conn:
                # Убеждаемся, что все изменения закоммичены перед закрытием
                try:
                    self.conn.commit()
                    logger.info("✅ Все изменения закоммичены в базу данных")
                except Exception as e:
                    logger.warning(f"⚠️ Предупреждение при финальном коммите: {e}")
                
                self.conn.close()

                logger.info("🔌 Соединение с базой данных закрыто")



def main():

    """Основная функция"""

    import argparse

    

    parser = argparse.ArgumentParser(description='Создание таблицы сварено_сварщиком на основе данных wl_china')

    parser.add_argument('--check-only', action='store_true',

                       help='Только проверка структуры без создания таблицы')

    parser.add_argument('--preview', action='store_true',

                       help='Предварительный просмотр данных')

    

    args = parser.parse_args()

    

    creator = SvarennoSvarshchikomCreator()

    

    if args.check_only:

        # Только проверка

        if creator.connect_to_database():

            creator.get_table_stats()

            if creator.check_wl_china_structure():

                logger.info("✅ Структура таблицы wl_china корректна")

            else:

                logger.error("❌ Структура таблицы wl_china некорректна")

            creator.conn.close()

    elif args.preview:

        # Предварительный просмотр

        if creator.connect_to_database():

            creator.get_table_stats()

            creator.check_wl_china_structure()

            

            # Показываем примеры данных

            logger.info("📋 Примеры данных из wl_china:")

            creator.cursor.execute("""

                SELECT N_Линии, Номер_чертежа, Клеймо_сварщика_корневой_слой, 

                       Клеймо_сварщика_заполнение_облицовка, Метод_сварки_корневой_слой,

                       Метод_сварки_заполнение_облицовка, Тип_сварного_шва,

                       Результаты_АКТ_ВИК, Результаты_Заключения_РК

                FROM wl_china 

                WHERE (Клеймо_сварщика_корневой_слой != '' 

                       OR Клеймо_сварщика_заполнение_облицовка != '')

                  AND Номер_сварного_шва IS NOT NULL

                LIMIT 10

            """)

            sample_data = creator.cursor.fetchall()

            

            for i, record in enumerate(sample_data, 1):

                n_линии, номер_чертежа, клеймо_корень, клеймо_заполнение, метод_корень, метод_заполнение, тип_шва, акт_вик, заключения_рк = record

                logger.info(f"   {i}. Линия {n_линии}, Чертеж {номер_чертежа}")

                logger.info(f"      Клейма - Корневой слой: {клеймо_корень}, Заполнение/облицовка: {клеймо_заполнение}")

                logger.info(f"      Методы - Корневой слой: {метод_корень}, Заполнение/облицовка: {метод_заполнение}")

                logger.info(f"      Тип сварного шва: {тип_шва}")

                logger.info(f"      Результаты АКТ ВИК: {акт_вик}")

                logger.info(f"      Результаты Заключения РК: {заключения_рк}")

            

            creator.conn.close()

    else:

        # Полное создание таблицы

        creator.run_creation()



if __name__ == "__main__":

    main()



# Для запуска через GUI

def run_script():

    """Функция для запуска скрипта через GUI"""

    print(f"🔧 run_script() вызвана с аргументами: {sys.argv}")

    

    # Проверяем аргументы командной строки

    if len(sys.argv) > 1:

        print(f"📋 Найдены аргументы командной строки: {sys.argv[1:]}")

        if '--preview' in sys.argv:

            print("👁️ Запускаем preview...")

            return run_script_preview()

        elif '--check-only' in sys.argv:

            print("🔍 Запускаем check-only...")

            return run_script_check()

    

    # По умолчанию запускаем полное создание таблицы

    print("🚀 Запускаем создание таблицы...")

    creator = SvarennoSvarshchikomCreator()

    return creator.run_creation()



def run_script_preview():

    """Функция для предварительного просмотра через GUI"""

    creator = SvarennoSvarshchikomCreator()

    if creator.connect_to_database():

        creator.get_table_stats()

        creator.check_wl_china_structure()

        

        # Показываем примеры данных

        logger.info("📋 Примеры данных из wl_china:")

        creator.cursor.execute("""

            SELECT N_Линии, Номер_чертежа, Клеймо_сварщика_корневой_слой, 

                   Клеймо_сварщика_заполнение_облицовка, Метод_сварки_корневой_слой,

                   Метод_сварки_заполнение_облицовка, Тип_сварного_шва

            FROM wl_china 

            WHERE (Клеймо_сварщика_корневой_слой != '' 

                   OR Клеймо_сварщика_заполнение_облицовка != '')

              AND Номер_сварного_шва IS NOT NULL

            LIMIT 10

        """)

        sample_data = creator.cursor.fetchall()

        

        for i, record in enumerate(sample_data, 1):

            n_линии, номер_чертежа, клеймо_корень, клеймо_заполнение, метод_корень, метод_заполнение, тип_шва = record

            logger.info(f"   {i}. Линия {n_линии}, Чертеж {номер_чертежа}")

            logger.info(f"      Клейма - Корневой слой: {клеймо_корень}, Заполнение/облицовка: {клеймо_заполнение}")

            logger.info(f"      Методы - Корневой слой: {метод_корень}, Заполнение/облицовка: {метод_заполнение}")

            logger.info(f"      Тип сварного шва: {тип_шва}")

        

        creator.conn.close()

        return True

    return False



def run_script_check():

    """Функция для проверки структуры через GUI"""

    creator = SvarennoSvarshchikomCreator()

    if creator.connect_to_database():

        creator.get_table_stats()

        result = creator.check_wl_china_structure()

        creator.conn.close()

        return result

    return False

