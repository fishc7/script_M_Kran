import sqlite3
import sys
import os
import subprocess

# Импортируем утилиты
try:
    # Пробуем относительный импорт (для запуска через web-приложение)
    from ..utilities.db_utils import clean_column_name
    from ..utilities.path_utils import get_excel_paths
except ImportError:
    # Если не работает, используем абсолютный импорт (для прямого запуска)
    import sys
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    utilities_dir = os.path.join(os.path.dirname(current_dir), 'utilities')
    if utilities_dir not in sys.path:
        sys.path.insert(0, utilities_dir)
    
    from db_utils import clean_column_name
    from path_utils import get_excel_paths

# Добавляем путь к папке utilities для импорта модулей
current_dir = os.path.dirname(os.path.abspath(__file__))
utilities_dir = os.path.join(current_dir, '..', 'utilities')
sys.path.insert(0, utilities_dir)

from db_utils import clean_data_values, print_column_cleaning_report, get_database_path
import pandas as pd
from datetime import datetime
import re
import pyxlsb

# Импортируем функции нормализации
try:
    from .normalization_functions import normalize_vik_status, normalize_rk_status
except ImportError:
    # Если не работает относительный импорт, используем абсолютный
    import sys
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)
    from normalization_functions import normalize_vik_status, normalize_rk_status

excel_paths = get_excel_paths()
excel_dir = excel_paths['nk_journal']
db_path = get_database_path()

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
    # Ищем дату в формате DD.MM.YYYY в имени файла
    match = re.search(r'(\d{2}\.\d{2}\.\d{4})', filename)
    if match:
        try:
            return datetime.strptime(match.group(1), '%d.%m.%Y')
        except ValueError:
            return None
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

def load_data():
    print("Путь к базе данных:", db_path)
    print(f"Путь к папке с Excel-файлами: {excel_dir}")
    
    try:
        # Подключаемся к базе данных
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
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
            if file_date:
                print(f"  Найдена дата: {file_date.strftime('%d.%m.%Y')}")
                # Даем приоритет .xlsx файлам (0), затем .xls (1), затем .xlsb (2)
                if file_name.endswith('.xlsx'):
                    priority = 0
                elif file_name.endswith('.xls'):
                    priority = 1
                else:  # .xlsb
                    priority = 2
                file_dates.append((file_name, file_dir, file_date, priority))
        
        # Сортируем по дате (убывание) и приоритету (возрастание)
        file_dates.sort(key=lambda x: (x[2], x[3]), reverse=True)
        
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
            def clean_for_sql(col_name):
                if pd.isna(col_name):
                    return 'unnamed_column'
                # Заменяем пробелы на подчеркивания
                col_str = str(col_name).replace(' ', '_')
                # Заменяем символ '%' на 'проц'
                col_str = col_str.replace('%', 'проц')
                # Убираем символы, которые недопустимы в SQL
                col_str = col_str.replace('(', '').replace(')', '').replace('[', '').replace(']', '')
                col_str = col_str.replace('{', '').replace('}', '').replace('"', '').replace("'", '')
                col_str = col_str.replace(';', '').replace(',', '').replace(':', '').replace('!', '')
                col_str = col_str.replace('?', '').replace('@', '').replace('#', '').replace('$', '')
                col_str = col_str.replace('^', '').replace('&', '').replace('*', '').replace('+', '')
                col_str = col_str.replace('=', '').replace('|', '').replace('\\', '').replace('/', '')
                col_str = col_str.replace('<', '').replace('>', '').replace('~', '').replace('`', '')
                # Заменяем дефис на подчеркивание
                col_str = col_str.replace('-', '_')
                # Убираем множественные подчеркивания
                import re
                col_str = re.sub(r'_+', '_', col_str)
                # Убираем подчеркивания в начале и конце
                col_str = col_str.strip('_')
                # Если название пустое, возвращаем дефолтное
                if not col_str:
                    col_str = 'unnamed_column'
                return col_str

            df.columns = [clean_for_sql(col) for col in df.columns]
            print(f"Названия столбцов после минимальной очистки: {df.columns.tolist()}")

            # Добавляем столбец 'Дата_загрузки' в список столбцов
            df['Дата_загрузки'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"Добавлен столбец 'Дата_загрузки'.")
            
            # Очищаем значения в данных
            for col in df.columns:
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
            cursor.execute(f"DROP TABLE IF EXISTS logs_lnk")
            cursor.execute(f"CREATE TABLE logs_lnk (id INTEGER PRIMARY KEY AUTOINCREMENT, {columns})")
            print(f"Таблица logs_lnk создана с столбцами: {df.columns.tolist()}")
            
            # Преобразуем DataFrame в список кортежей для вставки
            records = df.to_dict('records')
            
            # Подготавливаем SQL-запрос
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['?' for _ in df.columns])
            sql = f"INSERT INTO logs_lnk ({columns}) VALUES ({placeholders})"
            
            # Вставляем данные
            print("Загрузка данных в базу...")
            cursor.executemany(sql, [tuple(record.values()) for record in records])
            
            rows_loaded = len(records)
            print(f"Загружено записей: {rows_loaded}")
            print("✅ Скрипт успешно завершён. Загружено строк:", rows_loaded)
            
            # Сохраняем изменения
            conn.commit()
            
            # Проверяем количество записей в таблице
            cursor.execute("SELECT COUNT(*) FROM logs_lnk")
            count = cursor.fetchone()[0]
            print(f"Всего записей в таблице logs_lnk: {count}")
            
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

def main():
    """Основная функция"""
    load_data()

if __name__ == "__main__":
    main()

# Для запуска через GUI
def run_script():
    """Функция для запуска скрипта через GUI"""
    main() 