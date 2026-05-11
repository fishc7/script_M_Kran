import sqlite3
import sys
import os
import subprocess
import builtins
from numbers import Integral


def safe_print(*args, **kwargs):
    """Безопасный вывод: не падает, если stdout/stderr закрыт."""
    stream = kwargs.get('file', sys.stdout)
    try:
        if stream is None or getattr(stream, 'closed', False):
            fallback = getattr(sys, '__stdout__', None)
            if fallback is None or getattr(fallback, 'closed', False):
                return
            kwargs['file'] = fallback
        builtins.print(*args, **kwargs)
    except Exception:
        # Не прерываем выполнение скрипта из-за проблем с выводом
        pass


# Переопределяем print в рамках этого модуля на безопасный
print = safe_print

# Настройка кодировки для вывода в консоль Windows
# Проверяем, есть ли атрибут buffer (его нет при запуске через веб-приложение)
if sys.platform == 'win32' and hasattr(sys.stdout, 'buffer'):
    import io
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    except (AttributeError, ValueError):
        # Если не удалось настроить (например, при запуске через веб-приложение), пропускаем
        pass

# Импортируем утилиты
try:
    # Пробуем относительный импорт (для запуска через web-приложение)
    from ..utilities.db_utils import clean_column_name
    from ..utilities.path_utils import get_excel_paths, get_database_path
except ImportError:
    # Если не работает, используем абсолютный импорт (для прямого запуска)
    import sys
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    utilities_dir = os.path.join(os.path.dirname(current_dir), 'utilities')
    if utilities_dir not in sys.path:
        sys.path.insert(0, utilities_dir)

    from db_utils import clean_column_name
    from path_utils import get_excel_paths, get_database_path

# Добавляем путь к папке utilities для импорта модулей
current_dir = os.path.dirname(os.path.abspath(__file__))
utilities_dir = os.path.join(current_dir, '..', 'utilities')
sys.path.insert(0, utilities_dir)

from db_utils import clean_data_values, print_column_cleaning_report
from date_format import normalize_logs_lnk_date
import pandas as pd
from datetime import datetime
import re
import pyxlsb
import time

# Импортируем функции нормализации
try:
    from .normalization_functions import normalize_vik_status, normalize_rk_status
except ImportError:
    # Если не работает относительный импорт, используем абсолютный
    import sys
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    scripts_dir = os.path.dirname(current_dir)

    # Добавляем пути для правильного импорта
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)

    from normalization_functions import normalize_vik_status, normalize_rk_status

# Константы
excel_paths = get_excel_paths()
excel_dir = excel_paths['nk_journal']
db_path = get_database_path()

# Столбцы для обновления из архива
ARCHIVE_UPDATE_COLUMNS = [
    'РК', 'Тип_РК', 'Внутренний_номер_RT', 'Статус_РК',
    'Дата_РК', 'Дата_контроля_РК', 'Дата_создания_РК',
    'Количество_экспозиций', 'Номер_акта_РК', 'Реестр_передачи_снимков'
]

# Имя архивной таблицы
ARCHIVE_TABLE_NAME = 'log_lnk_archive_from_27.01.2026'


def _norm_app_row_id_key(val):
    """
    Единый ключ для сравнения app_row_id между Excel, БД и архивной таблицей.
    Сводит '12345' и '12345.0' к одному ключу, не используя float для длинных
    целых (иначе int(float(s)) портит значения >2^53 и «склеивает» разные id).
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, bool):
        return None
    if isinstance(val, Integral):
        return str(int(val))
    if isinstance(val, float):
        if val == int(val) and abs(val) < 2**53:
            return str(int(val))
        s = str(val).strip()
        return s if s else None
    s = str(val).strip()
    if not s:
        return None
    if s.lower().startswith("aks_"):
        return s
    if re.fullmatch(r"-?\d+", s):
        try:
            return str(int(s))
        except ValueError:
            return s
    m = re.fullmatch(r"(-?\d+)\.(0+)", s)
    if m:
        return m.group(1)
    return s


# Настройки устойчивости к кратковременным блокировкам SQLite
SQLITE_TIMEOUT_SEC = 60
SQLITE_BUSY_TIMEOUT_MS = 60000
SQLITE_RETRY_ATTEMPTS = 5
SQLITE_RETRY_DELAY_SEC = 1.0


def _is_sqlite_locked_error(err):
    msg = str(err).lower()
    return 'database is locked' in msg or 'database table is locked' in msg


def _create_sqlite_connection(db_file_path):
    """Создает соединение SQLite с настройками, уменьшающими вероятность падения на блокировке."""
    conn = sqlite3.connect(db_file_path, timeout=SQLITE_TIMEOUT_SEC)
    cur = conn.cursor()
    # Ждём освобождения БД вместо мгновенного падения.
    cur.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
    # WAL снижает конкуренцию между чтением и записью.
    cur.execute("PRAGMA journal_mode = WAL")
    cur.execute("PRAGMA synchronous = NORMAL")
    conn.commit()
    return conn


def _execute_write_with_retry(cursor, sql, params=None, many=False):
    """Выполняет write-операцию с повторными попытками при database is locked."""
    for attempt in range(1, SQLITE_RETRY_ATTEMPTS + 1):
        try:
            if many:
                cursor.executemany(sql, params or [])
            else:
                if params is None:
                    cursor.execute(sql)
                else:
                    cursor.execute(sql, params)
            return
        except sqlite3.OperationalError as e:
            if not _is_sqlite_locked_error(e) or attempt == SQLITE_RETRY_ATTEMPTS:
                raise
            print(
                f"⚠️ SQLite занята (попытка {attempt}/{SQLITE_RETRY_ATTEMPTS}), "
                f"повтор через {SQLITE_RETRY_DELAY_SEC:.1f}с..."
            )
            time.sleep(SQLITE_RETRY_DELAY_SEC)

def ensure_logs_lnk_table_exists(conn):
    """Создаёт пустую таблицу logs_lnk, если её нет (при запуске из веб /scripts до загрузки Excel)."""
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='logs_lnk'")
        if cur.fetchone() is not None:
            return
        columns = [
            'id INTEGER PRIMARY KEY AUTOINCREMENT',
            'app_row_id TEXT',
            'Титул TEXT', 'Линия TEXT', 'Чертеж TEXT', 'Номер_стыка TEXT', 'Зона TEXT',
            'ВИК TEXT', 'Статус_ВИК TEXT', 'РК TEXT', 'Статус_РК TEXT', 'Тип_РК TEXT',
            'Внутренний_номер_RT TEXT', 'Дата_РК TEXT', 'Дата_контроля_РК TEXT', 'Дата_создания_РК TEXT',
            'Дата_контроля_ВИК TEXT', 'Дата_заявки TEXT', 'Дата_загрузки TEXT',
            'Количество_экспозиций TEXT', 'Номер_акта_РК TEXT', 'Реестр_передачи_снимков TEXT',
            'Заявленны_виды_контроля TEXT',
            '_Номер_заключения_ВИК TEXT', '_Номер_заключения_РК TEXT',
            'Источник TEXT'
        ]
        cur.execute("CREATE TABLE logs_lnk (" + ", ".join(columns) + ")")
        conn.commit()
        print("📋 Таблица logs_lnk создана (пустая): загрузка из Excel ещё не выполнялась.")
    except Exception as e:
        import traceback
        print(f"⚠️ Не удалось создать таблицу logs_lnk: {e}")
        traceback.print_exc()

def preprocess_lnk_data(df):
    """
    Предварительная обработка данных для таблицы logs_lnk
    """
    print("🔧 Выполняем предварительную обработку данных...")

    # Проверяем наличие необходимых столбцов
    if 'Линия' not in df.columns or 'Чертеж' not in df.columns:
        print("⚠️ Столбцы 'Линия' или 'Чертеж' не найдены. Пропускаем предварительную обработку.")
        return df

    changes_count = 0

    # 1. Найти GCC-NAG-DDD-12470-13-1400-TK-ISO-00044 в столбце Линия и поменять значения местами с Чертеж
    print("1️⃣ Обработка: замена значений между столбцами Линия и Чертеж...")
    mask_line = df['Линия'] == 'GCC-NAG-DDD-12470-13-1400-TK-ISO-00044'
    if mask_line.any():
        # Сохраняем значения из столбца Чертеж
        temp_drawing_values = df.loc[mask_line, 'Чертеж'].copy()
        # Заменяем значения в столбце Чертеж на значения из столбца Линия
        df.loc[mask_line, 'Чертеж'] = df.loc[mask_line, 'Линия']
        # Заменяем значения в столбце Линия на сохраненные значения из Чертеж
        df.loc[mask_line, 'Линия'] = temp_drawing_values
        changes_count += mask_line.sum()
        print(f"   ✅ Заменено {mask_line.sum()} записей между столбцами Линия и Чертеж")
    else:
        print("   ℹ️ Значение GCC-NAG-DDD-12470-13-1400-TK-ISO-00044 не найдено в столбце Линия")

    # 2. Заменить 12460-12-1500-TK-087-LC-0618-00101 на GCC-NAG-DDD-12460-12-1500-TK-ISO-00101 в столбце Чертеж
    print("2️⃣ Обработка: замена формата чертежа...")
    mask_drawing = df['Чертеж'] == '12460-12-1500-TK-087-LC-0618-00101'
    if mask_drawing.any():
        df.loc[mask_drawing, 'Чертеж'] = 'GCC-NAG-DDD-12460-12-1500-TK-ISO-00101'
        changes_count += mask_drawing.sum()
        print(f"   ✅ Заменено {mask_drawing.sum()} записей формата чертежа")
    else:
        print("   ℹ️ Значение 12460-12-1500-TK-087-LC-0618-00101 не найдено в столбце Чертеж")

    # 3. Заменить -- на - в столбце Чертеж
    print("3️⃣ Обработка: замена двойных дефисов на одинарные...")
    mask_double_dash = df['Чертеж'].astype(str).str.contains('--', na=False)
    if mask_double_dash.any():
        df.loc[mask_double_dash, 'Чертеж'] = df.loc[mask_double_dash, 'Чертеж'].astype(str).str.replace('--', '-')
        changes_count += mask_double_dash.sum()
        print(f"   ✅ Заменено {mask_double_dash.sum()} записей с двойными дефисами")
    else:
        print("   ℹ️ Двойные дефисы не найдены в столбце Чертеж")

    # 4. Заменить GCC-NAG-DDD-12460-12-1500-TK-ISO-00045 на GCC-NAG-DDD-12470-13-1400-TK-ISO-00045 в столбце Чертеж
    print("4️⃣ Обработка: замена конкретного значения чертежа...")
    mask_specific = df['Чертеж'] == 'GCC-NAG-DDD-12460-12-1500-TK-ISO-00045'
    if mask_specific.any():
        df.loc[mask_specific, 'Чертеж'] = 'GCC-NAG-DDD-12470-13-1400-TK-ISO-00045'
        changes_count += mask_specific.sum()
        print(f"   ✅ Заменено {mask_specific.sum()} записей конкретного значения чертежа")
    else:
        print("   ℹ️ Значение GCC-NAG-DDD-12460-12-1500-TK-ISO-00045 не найдено в столбце Чертеж")

    # 5. Найти 086-FL-2471 в столбце Линия и заменить соответствующие значения в Чертеж
    print("5️⃣ Обработка: поиск 086-FL-2471 в Линия и замена Чертеж...")
    mask_fl2471 = df['Линия'].astype(str).str.contains('086-FL-2471', na=False)
    if mask_fl2471.any():
        df.loc[mask_fl2471, 'Чертеж'] = 'GCC-NAG-DDD-12470-13-1400-TK-ISO-00045'
        changes_count += mask_fl2471.sum()
        print(f"   ✅ Найдено {mask_fl2471.sum()} записей с 086-FL-2471 в Линия, заменены значения в Чертеж")
    else:
        print("   ℹ️ Значение 086-FL-2471 не найдено в столбце Линия")

    # 6. Заменить GCC-NAG-DDD-12470-13-1500-TK-ISO-00045 на GCC-NAG-DDD-12470-13-1400-TK-ISO-00045 в столбце Чертеж
    print("6️⃣ Обработка: замена 1500 на 1400 в номере чертежа...")
    mask_1500_to_1400 = df['Чертеж'] == 'GCC-NAG-DDD-12470-13-1500-TK-ISO-00045'
    if mask_1500_to_1400.any():
        df.loc[mask_1500_to_1400, 'Чертеж'] = 'GCC-NAG-DDD-12470-13-1400-TK-ISO-00045'
        changes_count += mask_1500_to_1400.sum()
        print(f"   ✅ Заменено {mask_1500_to_1400.sum()} записей с 1500 на 1400 в номере чертежа")
    else:
        print("   ℹ️ Значение GCC-NAG-DDD-12470-13-1500-TK-ISO-00045 не найдено в столбце Чертеж")

    # 7. Заменить пробелы на дефисы в 086 FL 2471 в столбце Линия
    print("7️⃣ Обработка: замена пробелов на дефисы в 086 FL 2471...")
    mask_spaces = df['Линия'].astype(str).str.contains('086 FL 2471', na=False)
    if mask_spaces.any():
        df.loc[mask_spaces, 'Линия'] = df.loc[mask_spaces, 'Линия'].astype(str).str.replace('086 FL 2471', '086-FL-2471')
        changes_count += mask_spaces.sum()
        print(f"   ✅ Заменено {mask_spaces.sum()} записей с пробелами на дефисы в 086 FL 2471")
    else:
        print("   ℹ️ Значение '086 FL 2471' не найдено в столбце Линия")

    # 8. Если Линия содержит 087-ING-0604, установить Чертеж = GCC-NAG-DDD-12460-12-1500-TK-ISO-00045
    print("8️⃣ Обработка: выставить Чертеж по 087-ING-0604...")
    mask_ing0604 = df['Линия'].astype(str).str.contains('087-ING-0604', na=False)
    if mask_ing0604.any():
        df.loc[mask_ing0604, 'Чертеж'] = 'GCC-NAG-DDD-12460-12-1500-TK-ISO-00045'
        changes_count += mask_ing0604.sum()
        print(f"   ✅ Найдено {mask_ing0604.sum()} записей с 087-ING-0604, обновлён столбец Чертеж")
    else:
        print("   ℹ️ Значение 087-ING-0604 не найдено в столбце Линия")

    print(f"🎯 Предварительная обработка завершена. Всего изменений: {changes_count}")
    return df

def clean_value(value):
    # Преобразуем значение в строку и заменяем специальные символы
    if pd.isna(value):
        return None
    return str(value).replace('%', 'проц')

def get_date_from_filename(filename):
    # Ищем дату в имени файла: DD.MM.YYYY, DD_MM_YYYY или DD-MM-YYYY
    for pattern, fmt in [
        (r'(\d{2}\.\d{2}\.\d{4})', '%d.%m.%Y'),
        (r'(\d{2}_\d{2}_\d{4})', '%d_%m_%Y'),
        (r'(\d{2}-\d{2}-\d{4})', '%d-%m-%Y'),
    ]:
        match = re.search(pattern, filename)
        if match:
            try:
                return datetime.strptime(match.group(1), fmt)
            except ValueError:
                continue
    return None

def install_xlwings():
    """Устанавливает xlwings если он не установлен"""
    try:
        import xlwings
        return True
    except ImportError:
        print("📦 xlwings не установлен. Пытаемся установить...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "xlwings"])
            print("✅ xlwings успешно установлен")
            return True
        except subprocess.CalledProcessError:
            print("❌ Не удалось установить xlwings автоматически")
            print("💡 Установите вручную: pip install xlwings")
            return False

def _resolve_db_path():
    """Путь к БД: при запуске из веб — тот же, что у Flask (по PROJECT_ROOT), иначе из path_utils."""
    if os.environ.get('PROJECT_ROOT'):
        p = os.path.join(os.environ['PROJECT_ROOT'], 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db')
        return os.path.normpath(os.path.abspath(p))
    return (db_path if db_path else get_database_path()) or ""

def load_data(use_etl_lock: bool = True):
    # Всегда один и тот же файл, что у веб-приложения (D:\...\script_M_Kran\database\BD_Kingisepp\M_Kran_Kingesepp.db)
    actual_db_path = _resolve_db_path()
    if not actual_db_path:
        print("❌ Ошибка: не удалось определить путь к базе данных.")
        return
    print("Путь к базе данных:", actual_db_path)
    print(f"Путь к папке с Excel-файлами: {excel_dir}")

    etl_lock_obj = None
    if use_etl_lock:
        try:
            from ..utilities.logs_lnk_etl_lock import LogsLnkEtlLock
        except ImportError:
            _util_dir = os.path.normpath(
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "utilities")
            )
            if _util_dir not in sys.path:
                sys.path.insert(0, _util_dir)
            from logs_lnk_etl_lock import LogsLnkEtlLock
        etl_lock_obj = LogsLnkEtlLock(actual_db_path)
        try:
            etl_lock_obj.acquire()
        except TimeoutError as e:
            print(f"❌ {e}")
            return

    try:
        db_dir = os.path.dirname(actual_db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
            print(f"📁 Создана папка для БД: {db_dir}")

        # Подключаемся к базе данных (именно к файлу на диске)
        conn = _create_sqlite_connection(actual_db_path)
        cursor = conn.cursor()

        # Создаём таблицу logs_lnk при отсутствии
        ensure_logs_lnk_table_exists(conn)
        conn.commit()

        # Проверка: таблица есть в этом файле
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='logs_lnk'")
        if cursor.fetchone():
            print("✅ Таблица logs_lnk в базе присутствует.")
        else:
            print("⚠️ Таблица logs_lnk после ensure не найдена в базе.")

        # Получаем список Excel-файлов (исключаем временные файлы)
        excel_files = []

        # Ищем файлы в основной папке
        main_files = [f for f in os.listdir(excel_dir) if (f.endswith('.xlsx') or f.endswith('.xls') or f.endswith('.xlsb')) and not f.startswith('~$')]
        excel_files.extend([(f, excel_dir) for f in main_files])

        # Ищем файлы в папке "архив"
        archive_dir = os.path.join(excel_dir, 'архив')
        if os.path.exists(archive_dir):
            archive_files = [f for f in os.listdir(archive_dir) if (f.endswith('.xlsx') or f.endswith('.xls') or f.endswith('.xlsb')) and not f.startswith('~$')]
            excel_files.extend([(f, archive_dir) for f in archive_files])

        # Преобразуем обратно в список имен файлов для совместимости
        excel_file_names = [f[0] for f in excel_files]
        print(f"\nНайдено Excel-файлов: {len(excel_files)}")

        # Находим самый последний файл по дате в имени с приоритетом .xlsx
        file_dates = []

        for file_info in excel_files:
            file_name, file_dir = file_info
            print(f"Проверка файла: {file_name}")
            file_date = get_date_from_filename(file_name)

            # Если дата в имени не найдена, используем дату модификации файла
            if not file_date:
                file_path = os.path.join(file_dir, file_name)
                if os.path.exists(file_path):
                    mtime = os.path.getmtime(file_path)
                    file_date = datetime.fromtimestamp(mtime)
                    print(f"  Дата в имени не найдена, используем дату модификации: {file_date.strftime('%d.%m.%Y %H:%M:%S')}")
                else:
                    print(f"  ⚠️ Файл не найден: {file_path}")
                    continue

            if file_date:
                print(f"  Используемая дата: {file_date.strftime('%d.%m.%Y')}")
                # Даем приоритет .xlsx файлам (0), затем .xls (1), затем .xlsb (2)
                if file_name.endswith('.xlsx'):
                    priority = 0
                elif file_name.endswith('.xls'):
                    priority = 1
                else:  # .xlsb
                    priority = 2
                file_dates.append((file_name, file_dir, file_date, priority))

        # Сортируем по дате (убывание) и приоритету (возрастание)
        # Используем отрицательный timestamp для убывающей сортировки по дате
        # и положительный приоритет для возрастающей сортировки
        file_dates.sort(key=lambda x: (-x[2].timestamp(), x[3]))

        print(f"\nТоп-5 файлов по дате и приоритету:")
        for i, (file_name, file_dir, file_date, priority) in enumerate(file_dates[:5]):
            print(f"  {i+1}. {file_date.strftime('%d.%m.%Y')} - {file_name} (приоритет: {priority})")

        # Выбираем первый файл (самый последний с наивысшим приоритетом)
        if file_dates:
            latest_file, latest_file_path, latest_date, _ = file_dates[0]
        else:
            latest_file = None
            latest_file_path = None
            latest_date = None

        if latest_file and latest_date and latest_file_path:
            print(f"\nВыбран самый последний файл: {latest_file}")
            print(f"Дата в имени файла: {latest_date.strftime('%d.%m.%Y')}")

            file_path = os.path.join(latest_file_path, latest_file)

            # Читаем Excel-файл с автоматическим fallback
            print("\nЧтение файла...")
            df = None
            file_read_successfully = False

            # Список файлов для попытки чтения (текущий + резервные)
            files_to_try = [(latest_file, latest_file_path)]

            # Если текущий файл .xlsb, добавляем резервные .xlsx файлы
            if latest_file.endswith('.xlsb') and len(file_dates) > 1:
                for file_name, file_dir, file_date, priority in file_dates[1:]:
                    if file_name.endswith('.xlsx'):
                        files_to_try.append((file_name, file_dir))
                        if len(files_to_try) >= 3:  # Максимум 3 попытки
                            break

            # Пробуем прочитать файлы по очереди
            for attempt, (file_name, file_dir) in enumerate(files_to_try):
                current_file_path = os.path.join(file_dir, file_name)
                print(f"\nПопытка {attempt + 1}: {file_name}")

                try:
                    if current_file_path.endswith('.xlsb'):
                        # Для файлов .xlsb используем pyxlsb напрямую
                        print("Читаем файл .xlsb с помощью pyxlsb...")
                        try:
                            data = []
                            with pyxlsb.open_workbook(current_file_path) as wb:
                                with wb.get_sheet(1) as sheet:
                                    for row in sheet.rows():
                                        data.append([item.v for item in row])

                            if data:
                                # Первая строка - заголовки
                                headers = data[0]
                                # Остальные строки - данные
                                df = pd.DataFrame(data[1:], columns=headers)
                                print(f"✅ Успешно прочитано {len(df)} строк из файла .xlsb")
                                file_read_successfully = True
                                break
                            else:
                                raise Exception("Файл пустой или не содержит данных")
                        except Exception as e:
                            print(f"❌ Ошибка при чтении .xlsb файла: {e}")
                            # Пробуем конвертировать через xlwings
                            if install_xlwings():
                                try:
                                    print("🔄 Пробуем конвертировать .xlsb в .xlsx...")
                                    import xlwings as xw
                                    temp_xlsx_path = current_file_path.replace('.xlsb', '_temp.xlsx')
                                    app = xw.App(visible=False)
                                    wb = app.books.open(current_file_path)
                                    wb.save(temp_xlsx_path)
                                    wb.close()
                                    app.quit()

                                    # Читаем конвертированный файл
                                    df = pd.read_excel(temp_xlsx_path)
                                    print(f"✅ Успешно конвертирован и прочитан: {len(df)} строк")

                                    # Удаляем временный файл
                                    os.remove(temp_xlsx_path)
                                    file_read_successfully = True
                                    break
                                except Exception as e2:
                                    print(f"❌ Ошибка при конвертации: {e2}")
                            else:
                                print("ℹ️ xlwings недоступен, пропускаем конвертацию")

                    # Для .xlsx и .xls файлов
                    if current_file_path.endswith(('.xlsx', '.xls')):
                        # Пробуем разные движки
                        engines_to_try = [None, 'openpyxl', 'xlrd']
                        for engine in engines_to_try:
                            try:
                                if engine:
                                    df = pd.read_excel(current_file_path, engine=engine)
                                    print(f"✅ Успешно прочитано с помощью {engine}: {len(df)} строк")
                                else:
                                    df = pd.read_excel(current_file_path)
                                    print(f"✅ Успешно прочитано с помощью pd.read_excel: {len(df)} строк")
                                file_read_successfully = True
                                break
                            except Exception as e:
                                print(f"❌ Ошибка с движком {engine or 'default'}: {e}")
                                continue

                        if file_read_successfully:
                            break

                except Exception as e:
                    print(f"❌ Общая ошибка при чтении файла: {e}")
                    continue

            if not file_read_successfully or df is None:
                raise Exception("Не удалось прочитать ни один из доступных файлов")

            # Очистка заголовков отключена - используем оригинальные названия столбцов
            print(f"Оригинальные названия столбцов: {df.columns.tolist()}")

            # Минимальная очистка для совместимости с SQL - заменяем пробелы на подчеркивания и убираем специальные символы
            # Оптимизация: используем str.translate для более быстрой замены символов
            def clean_for_sql(col_name):
                if pd.isna(col_name):
                    return 'unnamed_column'
                
                col_str = str(col_name)
                # Заменяем пробелы и дефисы на подчеркивания
                col_str = col_str.replace(' ', '_').replace('-', '_')
                # Заменяем символ '%' на 'проц'
                col_str = col_str.replace('%', 'проц')
                
                # Убираем недопустимые символы одним проходом через translate
                # Создаём таблицу переводов для удаления символов
                remove_chars = '()[]{}\'"\\;,:!?@#$%^&*+=|/<>~`'
                trans_table = str.maketrans('', '', remove_chars)
                col_str = col_str.translate(trans_table)
                
                # Убираем множественные подчеркивания
                col_str = re.sub(r'_+', '_', col_str)
                # Убираем подчеркивания в начале и конце
                col_str = col_str.strip('_')
                
                # Если название пустое, возвращаем дефолтное
                return col_str if col_str else 'unnamed_column'

            df.columns = [clean_for_sql(col) for col in df.columns]
            print(f"Названия столбцов после минимальной очистки: {df.columns.tolist()}")

            # Добавляем столбец 'Дата_загрузки' (единый формат дат ГГГГ-ММ-ДД)
            df['Дата_загрузки'] = datetime.now().strftime('%Y-%m-%d')
            print(f"Добавлен столбец 'Дата_загрузки'.")
            # Откуда строка: журнал НГС (NGS) — первый источник; AKS догружается отдельным скриптом
            df['Источник'] = 'NGS'
            print("Добавлен столбец 'Источник' = NGS для загрузки из журнала НГС.")

            # Очищаем значения в данных; все столбцы Дата_* — в формат ГГГГ-ММ-ДД
            for col in df.columns:
                if isinstance(col, str) and col.startswith('Дата_'):
                    df[col] = df[col].apply(normalize_logs_lnk_date)
                else:
                    df[col] = df[col].apply(clean_value)

            # Предварительная обработка данных для таблицы logs_lnk
            print("\n🔄 Начинаем предварительную обработку данных...")
            df = preprocess_lnk_data(df)

            # Извлекаем все числа из столбцов ВИК и РК и объединяем их
            if 'ВИК' in df.columns:
                # Извлекаем все числа из ВИК и объединяем их
                df['_Номер_заключения_ВИК'] = df['ВИК'].astype(str).str.findall(r'\d+').str.join('')
                print("✅ Добавлен столбец '_Номер_заключения_ВИК' с извлеченными данными из ВИК")

            if 'РК' in df.columns:
                # Извлекаем все числа из РК и объединяем их
                df['_Номер_заключения_РК'] = df['РК'].astype(str).str.findall(r'\d+').str.join('')
                print("✅ Добавлен столбец '_Номер_заключения_РК' с извлеченными данными из РК")

            # Столбцы '_Номер_заключения_ВИК' и '_Номер_заключения_РК' уже созданы выше
            print("✅ Созданы отдельные столбцы для извлеченных номеров заключений из ВИК и РК")

            # Создаем таблицу logs_lnk с именами столбцов из Excel
            columns = ', '.join([f"{col} TEXT" for col in df.columns])
            _execute_write_with_retry(cursor, "DROP TABLE IF EXISTS logs_lnk")
            _execute_write_with_retry(
                cursor,
                f"CREATE TABLE logs_lnk (id INTEGER PRIMARY KEY AUTOINCREMENT, {columns})"
            )
            print(f"Таблица logs_lnk создана с столбцами: {df.columns.tolist()}")

            # Подготавливаем SQL-запрос для batch insert
            columns = ', '.join([f'"{c}"' for c in df.columns])
            placeholders = ', '.join(['?' for _ in df.columns])
            sql = f"INSERT INTO logs_lnk ({columns}) VALUES ({placeholders})"

            # Оптимизация: преобразуем DataFrame напрямую в список кортежей
            # Используем itertuples для лучшей производительности
            print("Загрузка данных в базу...")
            rows_data = [tuple(row) for row in df.itertuples(index=False)]
            _execute_write_with_retry(cursor, sql, rows_data, many=True)

            rows_loaded = len(rows_data)
            print(f"Загружено записей: {rows_loaded}")
            print("✅ Скрипт успешно завершён. Загружено строк:", rows_loaded)

            # Сохраняем изменения
            conn.commit()

            # Проверяем количество записей в таблице
            cursor.execute("SELECT COUNT(*) FROM logs_lnk")
            count = cursor.fetchone()[0]
            print(f"Всего записей в таблице logs_lnk: {count}")

            # Вставка новых записей из архива (у которых app_row_id нет в logs_lnk)
            try:
                cursor.execute(
                    f"SELECT name FROM sqlite_master WHERE type='table' AND name='{ARCHIVE_TABLE_NAME}'"
                )
                if not cursor.fetchone():
                    print(f"\n⚠️ Архивная таблица '{ARCHIVE_TABLE_NAME}' не найдена. Пропускаем вставку из архива.")
                else:
                    cursor.execute("PRAGMA table_info(logs_lnk)")
                    logs_lnk_cols = [row[1] for row in cursor.fetchall()]
                    insert_cols = [c for c in logs_lnk_cols if c != 'id']
                    if 'app_row_id' not in logs_lnk_cols:
                        print("\n⚠️ В таблице logs_lnk нет столбца app_row_id. Пропускаем вставку из архива.")
                    else:
                        # Оптимизация: получаем существующие ID одним запросом через pandas
                        # Это быстрее, чем цикл по cursor.fetchall()
                        existing_ids_df = pd.read_sql_query(
                            'SELECT app_row_id FROM logs_lnk WHERE app_row_id IS NOT NULL', 
                            conn
                        )
                        existing_ids = {
                            k
                            for k in (
                                _norm_app_row_id_key(x)
                                for x in existing_ids_df["app_row_id"].tolist()
                            )
                            if k is not None
                        }
                        # Ключи из текущего кадра Excel (до любых расхождений с тем, как SQLite вернёт app_row_id).
                        # Иначе после INSERT read_sql иногда даёт другой тип/вид строки, чем в архивной таблице,
                        # и пакет «только архив» ошибочно подмешивает сотни строк, уже есть в выгрузке НГС.
                        if "app_row_id" in df.columns:
                            existing_ids |= {
                                k
                                for k in (
                                    _norm_app_row_id_key(x)
                                    for x in df["app_row_id"].tolist()
                                )
                                if k is not None
                            }
                        
                        df_archive = pd.read_sql_query(f'SELECT * FROM "{ARCHIVE_TABLE_NAME}"', conn)
                        if 'app_row_id' not in df_archive.columns or len(df_archive) == 0:
                            print("\n⚠️ В архиве нет app_row_id или нет записей. Пропускаем вставку.")
                        else:
                            # Сравнение через нормализованный ключ (см. _norm_app_row_id_key)
                            archive_ids_norm = df_archive["app_row_id"].map(_norm_app_row_id_key)
                            archive_only = df_archive[~archive_ids_norm.isin(existing_ids)].copy()
                            archive_only["_nk"] = archive_only["app_row_id"].map(_norm_app_row_id_key)
                            _n_arch_cand = len(archive_only)
                            archive_only = archive_only.dropna(subset=["_nk"]).drop_duplicates(
                                subset=["_nk"], keep="first"
                            )
                            if len(archive_only) < _n_arch_cand:
                                print(
                                    f"\nℹ️ Кандидатов из архива на вставку: {_n_arch_cand}, "
                                    f"уникальных по app_row_id: {len(archive_only)}"
                                )
                            archive_only = archive_only.drop(columns=["_nk"], errors="ignore")
                            
                            if len(archive_only) == 0:
                                print("\n📋 Новых записей для вставки из архива нет.")
                            else:
                                # Подготовка SQL для batch insert
                                quoted_insert_cols = [f'"{c}"' for c in insert_cols]
                                placeholders = ', '.join(['?' for _ in insert_cols])
                                sql_insert = f"INSERT INTO logs_lnk ({', '.join(quoted_insert_cols)}) VALUES ({placeholders})"
                                
                                # Нормализация имён столбцов архива (пробел -> подчёркивание)
                                arch_col_map = {}
                                for col in insert_cols:
                                    if col in archive_only.columns:
                                        arch_col_map[col] = col
                                    else:
                                        arch_col_with_space = col.replace('_', ' ')
                                        if arch_col_with_space in archive_only.columns:
                                            arch_col_map[col] = arch_col_with_space
                                
                                # Подготовка данных для вставки: используем to_dict('records') вместо iterrows()
                                rows_to_insert = []
                                for _, row in archive_only.iterrows():
                                    row_vals = []
                                    for col in insert_cols:
                                        if col in arch_col_map:
                                            val = row.get(arch_col_map[col], None)
                                            if pd.isna(val):
                                                val = None
                                        else:
                                            val = None
                                        row_vals.append(val)
                                    rows_to_insert.append(tuple(row_vals))
                                
                                # Batch insert: вставляем все строки одним запросом
                                _execute_write_with_retry(cursor, sql_insert, rows_to_insert, many=True)
                                conn.commit()
                                print(f"\n✅ Из архива добавлено в logs_lnk новых записей: {len(rows_to_insert)}")
                                if 'Источник' in insert_cols:
                                    cursor.execute(
                                        """
                                        UPDATE logs_lnk SET "Источник" = ?
                                        WHERE "Источник" IS NULL OR TRIM(COALESCE("Источник", '')) = ''
                                        """,
                                        ('NGS',),
                                    )
                                    conn.commit()

                        # Обновление из архива: по совпадающему app_row_id заполняем только перечисленные столбцы
                        # и только если в logs_lnk ячейка пустая
                        print("\n🔄 Обновление пустых ячеек в существующих записях из архива...")
                        target_in_logs = [c for c in ARCHIVE_UPDATE_COLUMNS if c in logs_lnk_cols]
                        if target_in_logs and 'app_row_id' in df_archive.columns and len(df_archive) > 0:
                            # Читаем logs_lnk только один раз
                            df_logs = pd.read_sql_query("SELECT * FROM logs_lnk", conn)
                            _row_count_before_fill = len(df_logs)
                            col_order = df_logs.columns.tolist()
                            
                            # Нормализуем имена столбцов в архиве (пробел -> подчёркивание) для совпадения с logs_lnk
                            arch_rename = {
                                c: c.replace(' ', '_') 
                                for c in df_archive.columns 
                                if ' ' in c and c.replace(' ', '_') in target_in_logs
                            }
                            df_arch = df_archive.rename(columns=arch_rename).copy()
                            # В архиве может быть несколько строк на один app_row_id — для подстановки
                            # достаточно одной (как и раньше при неявном выборе через .loc).
                            if "app_row_id" in df_arch.columns:
                                _d = df_arch["app_row_id"].duplicated().sum()
                                if _d:
                                    df_arch = df_arch.drop_duplicates(
                                        subset=["app_row_id"], keep="first"
                                    )
                            
                            arch_id_keys = {
                                k
                                for k in df_arch["app_row_id"].map(_norm_app_row_id_key).tolist()
                                if k is not None
                            }
                            common_mask = (
                                df_logs["app_row_id"]
                                .map(_norm_app_row_id_key)
                                .isin(arch_id_keys)
                            )
                            
                            if common_mask.any():
                                updated_cells = 0
                                # Без set_index по app_row_id: при дубликатах app_row_id в logs_lnk
                                # .where() падает с «cannot reindex on an axis with duplicate labels».
                                for col in target_in_logs:
                                    arch_col = (
                                        col
                                        if col in df_arch.columns
                                        else col.replace("_", " ")
                                    )
                                    if arch_col not in df_arch.columns:
                                        continue
                                    
                                    arch_map_df = df_arch[
                                        ["app_row_id", arch_col]
                                    ].drop_duplicates("app_row_id", keep="first")
                                    _arch_fill_map = {
                                        _norm_app_row_id_key(aid): v
                                        for aid, v in zip(
                                            arch_map_df["app_row_id"],
                                            arch_map_df[arch_col],
                                        )
                                        if _norm_app_row_id_key(aid) is not None
                                    }
                                    fill_vals = df_logs["app_row_id"].map(
                                        lambda x, m=_arch_fill_map: m.get(_norm_app_row_id_key(x))
                                    )
                                    current = df_logs[col]
                                    empty_mask = current.isna() | (
                                        current.astype(str).str.strip() == ""
                                    )
                                    has_value = fill_vals.notna() & (
                                        fill_vals.astype(str).str.strip() != ""
                                    )
                                    fill_mask = empty_mask & has_value
                                    df_logs[col] = current.where(~fill_mask, fill_vals)
                                    updated_cells += int(fill_mask.sum())
                                
                                if len(df_logs) != _row_count_before_fill:
                                    print(
                                        f"\n⚠️ После подстановки из архива число строк изменилось "
                                        f"({_row_count_before_fill} → {len(df_logs)}); пересоздание таблицы "
                                        f"пропущено, чтобы не раздвоить logs_lnk."
                                    )
                                elif updated_cells > 0:
                                    # Сохраняем порядок столбцов
                                    df_logs = df_logs[[c for c in col_order if c in df_logs.columns]]
                                    # Старые id не вставляем — только AUTOINCREMENT (как при загрузке Excel).
                                    # Не вызывать drop_duplicates по id: в pandas все NaN в столбце считаются
                                    # «одинаковыми» и схлопываются в одну строку — резкая потеря данных.
                                    insert_df_cols = [
                                        c for c in df_logs.columns if c != "id"
                                    ]
                                    
                                    # Пересоздаём таблицу с обновлёнными данными
                                    _execute_write_with_retry(cursor, "DROP TABLE IF EXISTS logs_lnk")
                                    col_def = ', '.join([f'"{c}" TEXT' for c in insert_df_cols])
                                    _execute_write_with_retry(
                                        cursor,
                                        f"CREATE TABLE logs_lnk (id INTEGER PRIMARY KEY AUTOINCREMENT, {col_def})"
                                    )
                                    
                                    # Оптимизация: подготовка данных для batch insert (без столбца id)
                                    ins_cols = ', '.join([f'"{c}"' for c in insert_df_cols])
                                    ph = ', '.join(['?' for _ in insert_df_cols])
                                    
                                    # Преобразуем DataFrame в список кортежей с обработкой NaN
                                    def _prepare_rows_for_sql(rows_df):
                                        """Строки для INSERT; имя параметра не df — во избежание путаницы с кадром Excel."""
                                        rows = []
                                        for row in rows_df.itertuples(index=False):
                                            row_tuple = tuple(
                                                None if (isinstance(x, float) and pd.isna(x)) else x 
                                                for x in row
                                            )
                                            rows.append(row_tuple)
                                        return rows
                                    
                                    rows_data = _prepare_rows_for_sql(df_logs[insert_df_cols])
                                    _execute_write_with_retry(
                                        cursor,
                                        f"INSERT INTO logs_lnk ({ins_cols}) VALUES ({ph})",
                                        rows_data,
                                        many=True
                                    )
                                    conn.commit()
                                    print(f"✅ Обновлено пустых ячеек из архива: {updated_cells}")
                                else:
                                    print("\n📋 Пустых ячеек для заполнения из архива не найдено.")
                            else:
                                print("\n📋 Нет совпадающих app_row_id для обновления.")
                        elif not target_in_logs:
                            print("\n⚠️ Нет общих столбцов для обновления из архива.")
            except Exception as e:
                import traceback
                print(f"\n⚠️ Ошибка при работе с архивом: {e}")
                print(traceback.format_exc())
                conn.rollback()

            # Исправление: для app_row_id 8707944 в Номер_стыка заменяем кириллическую А на латинскую A
            try:
                cursor.execute(
                    'UPDATE logs_lnk SET "Номер_стыка" = replace("Номер_стыка", ?, ?) WHERE app_row_id = ?',
                    ('\u0410', 'A', '8707944')
                )
                if cursor.rowcount > 0:
                    conn.commit()
                    print(f"\n✅ Для app_row_id=8707944 в Номер_стыка кириллическая А заменена на латинскую A (обновлено строк: {cursor.rowcount})")
                else:
                    print("\nℹ️ Запись с app_row_id=8707944 не найдена или Номер_стыка не содержал кириллическую А.")
            except Exception as e:
                print(f"\n⚠️ Ошибка при замене А в Номер_стыка для app_row_id=8707944: {e}")

            # Закрываем соединение для загрузки данных
            conn.close()

            # Запускаем нормализацию данных
            print("\n" + "="*60)
            print("ЗАПУСК НОРМАЛИЗАЦИИ ДАННЫХ")
            print("="*60)

            # Нормализация ВИК статусов
            print("\n🔄 Нормализация столбцов Статус_ВИК и ВИК...")
            success_vik = normalize_vik_status()
            if success_vik:
                print("✅ Нормализация ВИК завершена успешно")
            else:
                print("❌ Ошибка при нормализации ВИК")

            # Нормализация РК статусов
            print("\n🔄 Нормализация столбцов Статус_РК и РК...")
            success_rk = normalize_rk_status()
            if success_rk:
                print("✅ Нормализация РК завершена успешно")
            else:
                print("❌ Ошибка при нормализации РК")

            print("\n" + "="*60)
            print("НОРМАЛИЗАЦИЯ ЗАВЕРШЕНА")
            print("="*60)

            # Запускаем дополнительный скрипт из папки ЖУРНАЛ_ОПОР_ЗАЗЕМЛЕНИЙ
            print("\n" + "="*60)
            print("ЗАПУСК ДОПОЛНИТЕЛЬНОГО СКРИПТА")
            print("="*60)

            # Журнал опор: отдельный Источник «ЖУРНАЛ_ОПОР» и DELETE перед вставкой — не дублирует NGS в logs_lnk.
            additional_script_path = r"D:\МК_Кран\МК_Кран_Кингесеп\ОГС\ЖУРНАЛ_ОПОР_ЗАЗЕМЛЕНИЙ\load_to_database.py"

            if os.path.exists(additional_script_path):
                print(f"\n🔄 Запуск скрипта: {additional_script_path}")
                try:
                    result = subprocess.run(
                        [sys.executable, additional_script_path],
                        capture_output=False,
                        text=True,
                        cwd=os.path.dirname(additional_script_path)
                    )
                    if result.returncode == 0:
                        print("✅ Дополнительный скрипт выполнен успешно")
                    else:
                        print(f"⚠️ Дополнительный скрипт завершился с кодом: {result.returncode}")
                except Exception as e:
                    print(f"❌ Ошибка при запуске дополнительного скрипта: {e}")
            else:
                print(f"⚠️ Файл не найден: {additional_script_path}")

            print("\n" + "="*60)
            print("ВСЕ СКРИПТЫ ЗАВЕРШЕНЫ")
            print("="*60)

        else:
            print("\n❌ Не найдено файлов с датой в имени!")
            print("💡 Убедитесь, что в папке есть Excel файлы с датами в формате DD.MM.YYYY")

        # Закрываем соединение
        if 'conn' in locals():
            conn.close()

    except Exception as e:
        print(f"\n❌ Произошла ошибка: {str(e)}")
        print("\n🔧 Рекомендации по устранению:")
        print("   1. Проверьте, что Excel файлы не открыты в других программах")
        print("   2. Убедитесь, что файлы имеют правильный формат (.xlsx, .xls, .xlsb)")
        print("   3. Проверьте права доступа к папке с файлами")
        print("   4. Установите xlwings для работы с .xlsb файлами: pip install xlwings")
        if 'conn' in locals():
            conn.close()
    finally:
        if etl_lock_obj is not None:
            etl_lock_obj.release()

def main():
    """Основная функция"""
    load_data()

if __name__ == "__main__":
    main()

# Для запуска через GUI
def run_script():
    """Функция для запуска скрипта через GUI"""
    main()
