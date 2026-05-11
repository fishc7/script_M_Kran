import sqlite3
import pandas as pd
import os
import numpy as np
from datetime import datetime
import re
import sys
import warnings
import unicodedata

# Подавляем предупреждения openpyxl о стилях
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# Настраиваем пути для импорта модулей
current_dir = os.path.dirname(os.path.abspath(__file__))
utilities_dir = os.path.join(current_dir, '..', 'utilities')
project_root = os.path.dirname(os.path.dirname(current_dir))

# Добавляем пути в sys.path
for path in [current_dir, utilities_dir, project_root]:
    if path not in sys.path:
        sys.path.insert(0, path)

# Импортируем утилиты
try:
    from utilities.db_utils import clean_column_name, get_database_path, get_database_connection, clean_data_values, print_column_cleaning_report
    from utilities.path_utils import get_excel_paths
except ImportError as e:
    print(f"Ошибка импорта модулей: {e}")
    # Создаем заглушки для функций
    def get_database_path():
        return os.path.join(project_root, 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db')
    
    def get_excel_paths():
        return {
            'ogs_journals': "D:/МК_Кран/МК_Кран_Кингесеп/ОГС/Журналы"
        }
    
    def clean_column_name(col):
        col_str = str(col)
        
        # Удаляем все символы с начала строки до первого переноса строки
        if '\n' in col_str:
            # Находим позицию первого переноса строки
            first_newline_pos = col_str.find('\n')
            # Удаляем все символы до первого переноса строки включительно
            col_str = col_str[first_newline_pos + 1:]
        
        # Удаляем китайские символы (иероглифы)
        import re
        col_str = re.sub(r'[\u4e00-\u9fff]', '', col_str)
        
        # Заменяем проблемные символы на безопасные аналоги
        replacements = {
            '№': 'N',      # Номер
            '√': 'V',      # Галочка
            '°': 'deg',    # Градус
            '±': 'pm',     # Плюс-минус
            '≤': 'le',     # Меньше или равно
            '≥': 'ge',     # Больше или равно
            '≠': 'ne',     # Не равно
            '≈': 'aprox',  # Приблизительно
            '∞': 'inf',    # Бесконечность
            '∑': 'sum',    # Сумма
            '∏': 'prod',   # Произведение
            '∫': 'int',    # Интеграл
            '∂': 'd',      # Частная производная
            '∇': 'nabla',  # Набла
            '∆': 'delta',  # Дельта
            'α': 'alpha',  # Альфа
            'β': 'beta',   # Бета
            'γ': 'gamma',  # Гамма
            'δ': 'delta',  # Дельта
            'ε': 'epsilon', # Эпсилон
            'μ': 'mu',     # Мю
            'σ': 'sigma',  # Сигма
            'τ': 'tau',    # Тау
            'φ': 'phi',    # Фи
            'ω': 'omega',  # Омега
        }
        
        for old_char, new_char in replacements.items():
            col_str = col_str.replace(old_char, new_char)
        
        # Заменяем переносы строк на пробелы
        col_str = re.sub(r'\n+', ' ', col_str)
        col_str = re.sub(r'\r+', ' ', col_str)
        col_str = re.sub(r'\t+', ' ', col_str)
        
        # Заменяем множественные пробелы на один
        col_str = re.sub(r'\s+', ' ', col_str)
        
        # Заменяем специальные символы на подчеркивания
        col_str = col_str.replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')
        col_str = col_str.replace('/', '_').replace('\\', '_').replace('.', '_')
        col_str = col_str.replace(',', '_').replace(';', '_').replace(':', '_')
        col_str = col_str.replace('!', '_').replace('?', '_').replace('"', '_')
        col_str = col_str.replace("'", '_').replace('`', '_').replace('~', '_')
        col_str = col_str.replace('@', '_').replace('#', '_').replace('$', '_')
        col_str = col_str.replace('%', '_').replace('^', '_').replace('&', '_')
        col_str = col_str.replace('*', '_').replace('+', '_').replace('=', '_')
        col_str = col_str.replace('[', '_').replace(']', '_').replace('{', '_')
        col_str = col_str.replace('}', '_').replace('|', '_').replace('<', '_')
        col_str = col_str.replace('>', '_')
        
        # Убираем множественные подчеркивания
        col_str = re.sub(r'_+', '_', col_str)
        
        # Убираем подчеркивания в начале и конце
        col_str = col_str.strip('_')
        
        # Если название пустое, возвращаем дефолтное
        if not col_str:
            col_str = 'unnamed_column'
        
        # Если название начинается с цифры, добавляем префикс
        if col_str and col_str[0].isdigit():
            col_str = 'col_' + col_str
        
        # Если название слишком длинное, обрезаем его
        if len(col_str) > 50:
            col_str = col_str[:50]
        
        return col_str
    
    def clean_data_values(df):
        return df
    
    def print_column_cleaning_report(original, cleaned):
        print(f"Очистка названий столбцов: {len(original)} -> {len(cleaned)}")
    
    def get_database_connection():
        db_path = get_database_path()
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"База данных не найдена: {db_path}")
        return sqlite3.connect(db_path)

# Инициализируем пути
excel_paths = get_excel_paths()
excel_path = excel_paths['ogs_journals'] + '/Журнал сварочных работ.xlsx'
db_path = get_database_path()

def clean_text(text):
    if pd.isna(text):
        return None
    # Преобразуем в строку
    text = str(text)
    
    # Удаляем только китайские символы (иероглифы)
    text = re.sub(r'[\u4e00-\u9fff]', '', text)
    
    # Заменяем переносы строк на пробелы
    text = re.sub(r'\n+', ' ', text)
    text = re.sub(r'\r+', ' ', text)
    text = re.sub(r'\t+', ' ', text)
    
    # Заменяем множественные пробелы на один
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

def format_date(date_value):
    """
    Форматирует дату в формат YYYY-MM-DD без времени
    """
    if pd.isna(date_value) or date_value is None:
        return None
    
    # Если это уже строка в нужном формате
    if isinstance(date_value, str):
        # Проверяем, есть ли время в строке
        if ' 00:00:00' in date_value:
            return date_value.replace(' 00:00:00', '')
        # Если это просто дата без времени
        elif re.match(r'^\d{4}-\d{2}-\d{2}$', date_value):
            return date_value
        # Пытаемся распарсить дату
        try:
            parsed_date = pd.to_datetime(date_value)
            return parsed_date.strftime('%Y-%m-%d')
        except:
            return str(date_value)
    
    # Если это pandas Timestamp
    elif hasattr(date_value, 'strftime'):
        return date_value.strftime('%Y-%m-%d')
    
    # Если это datetime объект
    elif hasattr(date_value, 'date'):
        return date_value.date().strftime('%Y-%m-%d')
    
    return str(date_value)

def clean_column_names_extra(columns):
    """
    Дополнительная очистка имен столбцов от китайских символов и текста до первого переноса строки
    """
    cleaned_columns = []
    for i, col in enumerate(columns):
        col_str = str(col)
        original_col = col_str
        
        # Проверяем наличие китайских символов
        chinese_chars = re.findall(r'[\u4e00-\u9fff]', col_str)
        if chinese_chars:
            print(f"⚠️  Найдены китайские символы в столбце {i+1}: '{original_col}' -> символы: {chinese_chars}")
        
        # Удаляем все символы с начала строки до первого переноса строки
        # Это удалит китайские символы и любой другой текст до первого \n
        if '\n' in col_str:
            # Находим позицию первого переноса строки
            first_newline_pos = col_str.find('\n')
            # Удаляем все символы до первого переноса строки включительно
            col_str = col_str[first_newline_pos + 1:]
            print(f"🗑️  Удален текст до первого переноса строки в столбце {i+1}: '{original_col}' -> '{col_str}'")
        
        # Удаляем только китайские символы (иероглифы) - на случай если они остались
        col_str = re.sub(r'[\u4e00-\u9fff]', '', col_str)
        
        # Заменяем проблемные символы на безопасные аналоги
        replacements = {
            '№': 'N',      # Номер
            '√': 'V',      # Галочка
            '°': 'deg',    # Градус
            '±': 'pm',     # Плюс-минус
            '≤': 'le',     # Меньше или равно
            '≥': 'ge',     # Больше или равно
            '≠': 'ne',     # Не равно
            '≈': 'aprox',  # Приблизительно
            '∞': 'inf',    # Бесконечность
            '∑': 'sum',    # Сумма
            '∏': 'prod',   # Произведение
            '∫': 'int',    # Интеграл
            '∂': 'd',      # Частная производная
            '∇': 'nabla',  # Набла
            '∆': 'delta',  # Дельта
            'α': 'alpha',  # Альфа
            'β': 'beta',   # Бета
            'γ': 'gamma',  # Гамма
            'δ': 'delta',  # Дельта
            'ε': 'epsilon', # Эпсилон
            'μ': 'mu',     # Мю
            'σ': 'sigma',  # Сигма
            'τ': 'tau',    # Тау
            'φ': 'phi',    # Фи
            'ω': 'omega',  # Омега
        }
        
        for old_char, new_char in replacements.items():
            col_str = col_str.replace(old_char, new_char)
        
        # Заменяем переносы строк на пробелы
        col_str = re.sub(r'\n+', ' ', col_str)
        col_str = re.sub(r'\r+', ' ', col_str)
        col_str = re.sub(r'\t+', ' ', col_str)
        
        # Заменяем множественные пробелы на один
        col_str = re.sub(r'\s+', ' ', col_str)
        
        # Заменяем специальные символы на подчеркивания
        col_str = col_str.replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')
        col_str = col_str.replace('/', '_').replace('\\', '_').replace('.', '_')
        col_str = col_str.replace(',', '_').replace(';', '_').replace(':', '_')
        col_str = col_str.replace('!', '_').replace('?', '_').replace('"', '_')
        col_str = col_str.replace("'", '_').replace('`', '_').replace('~', '_')
        col_str = col_str.replace('@', '_').replace('#', '_').replace('$', '_')
        col_str = col_str.replace('%', '_').replace('^', '_').replace('&', '_')
        col_str = col_str.replace('*', '_').replace('+', '_').replace('=', '_')
        col_str = col_str.replace('[', '_').replace(']', '_').replace('{', '_')
        col_str = col_str.replace('}', '_').replace('|', '_').replace('<', '_')
        col_str = col_str.replace('>', '_')
        
        # Убираем множественные подчеркивания
        col_str = re.sub(r'_+', '_', col_str)
        
        # Убираем подчеркивания в начале и конце
        col_str = col_str.strip('_')
        
        # Если название пустое, возвращаем дефолтное
        if not col_str:
            col_str = 'unnamed_column'
        
        # Если название начинается с цифры, добавляем префикс
        if col_str and col_str[0].isdigit():
            col_str = 'col_' + col_str
        
        # Если название слишком длинное, обрезаем его
        if len(col_str) > 50:
            col_str = col_str[:50]
        
        # Выводим информацию об изменении имени столбца
        if original_col != col_str:
            print(f"🔄 Столбец {i+1}: '{original_col}' -> '{col_str}'")
        
        cleaned_columns.append(col_str)
    
    return cleaned_columns



def create_table(conn):
    try:
        cursor = conn.cursor()
        # Удаляем только таблицу wl_china, если она существует
        cursor.execute('DROP TABLE IF EXISTS wl_china')
        print("🗑️  Удалена старая таблица wl_china (если существовала)")
        
        # Проверяем существование Excel файла
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"Excel файл не найден: {excel_path}")
        
        print(f"📁 Чтение Excel файла: {excel_path}")
        
        # Читаем Excel файл для получения структуры, пропуская первую строку
        df = pd.read_excel(excel_path, header=1)
        
        # Сохраняем оригинальные названия столбцов для отчета
        original_columns = df.columns.tolist()
        
        # Очищаем имена столбцов с использованием улучшенной функции
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Выводим отчет об очистке названий столбцов
        print_column_cleaning_report(original_columns, df.columns.tolist())
        
        # Добавляем новые столбцы для извлеченных номеров заключений
        df['_Номер_заключения_ВИК'] = ''
        df['_Номер_заключения_РК'] = ''
        
        # Столбцы с датами уже есть в Excel файле после очистки названий
        # Не добавляем их заново, чтобы не перезаписать исходные данные
        
        # Создаем SQL запрос для создания таблицы
        columns = ['id INTEGER PRIMARY KEY AUTOINCREMENT']
        for col in df.columns:
            columns.append(f'"{col}" TEXT')
        
        # Добавляем столбец с датой загрузки
        columns.append('Дата_загрузки TEXT')
        
        create_table_sql = f'''
        CREATE TABLE wl_china (
            {', '.join(columns)}
        )
        '''
        
        cursor.execute(create_table_sql)
        conn.commit()
        print("Таблица успешно создана!")
        return df  # Возвращаем DataFrame для использования в load_excel_data
    except sqlite3.Error as e:
        print(f"Ошибка при создании таблицы: {e}")
        return None

def load_excel_data(conn, df=None):
    try:
        if df is None:
            # Проверяем существование Excel файла
            if not os.path.exists(excel_path):
                raise FileNotFoundError(f"Excel файл не найден: {excel_path}")
            
            # Читаем Excel файл, пропуская первую строку
            print(f"📁 Чтение данных из Excel файла: {excel_path}")
            df = pd.read_excel(excel_path, header=1)
            
            # Сохраняем оригинальные названия столбцов для отчета
            original_columns = df.columns.tolist()
            
            # Выводим имена столбцов из Excel файла
            print("\nИмена столбцов в Excel файле:")
            for col in df.columns:
                print(f"- {col}")
            
            # Очищаем имена столбцов с использованием улучшенной функции
            df.columns = [clean_column_name(col) for col in df.columns]
            
            # Выводим отчет об очистке названий столбцов
            print_column_cleaning_report(original_columns, df.columns.tolist())
            
            # Очищаем значения в данных от переносов строк
            df = clean_data_values(df)
        else:
            print("Используем DataFrame, переданный из create_table")
        
        # Показываем все столбцы для отладки
        print(f"\nВсе столбцы в DataFrame:")
        for i, col in enumerate(df.columns):
            print(f"  {i+1}: '{col}'")
        
        # Очищаем все значения в DataFrame от иероглифов
        for column in df.columns:
            df[column] = df[column].apply(clean_text)
        
        # Заменяем NaN на None (NULL в SQLite)
        df = df.replace({np.nan: None})
        
        # Извлекаем номер заключения после последнего дефиса из столбцов VT АКТ_ВИК_ и RT Заключение_РК_
        if 'VT\nАКТ_ВИК_' in df.columns:
            # Показываем примеры данных для отладки
            print(f"\nПримеры данных в столбце VT АКТ_ВИК_:")
            sample_data = df['VT\nАКТ_ВИК_'].head(5).tolist()
            for i, data in enumerate(sample_data):
                print(f"  {i+1}: '{data}'")
            
            # Извлекаем все числа из VT АКТ_ВИК_ и объединяем их
            extracted_vik = df['VT\nАКТ_ВИК_'].astype(str).str.findall(r'\d+').str.join('')
            df['_Номер_заключения_ВИК'] = extracted_vik
            
            # Показываем результаты извлечения
            print(f"\nРезультаты извлечения из VT АКТ_ВИК_:")
            sample_extracted = extracted_vik.head(5).tolist()
            for i, data in enumerate(sample_extracted):
                print(f"  {i+1}: '{data}'")
            
            print("✅ Заполнен столбец '_Номер_заключения_ВИК' с извлеченными данными из VT АКТ_ВИК_")
        
        if 'RT\nЗаключение_РК_' in df.columns:
            # Показываем примеры данных для отладки
            print(f"\nПримеры данных в столбце RT Заключение_РК_:")
            sample_data = df['RT\nЗаключение_РК_'].head(5).tolist()
            for i, data in enumerate(sample_data):
                print(f"  {i+1}: '{data}'")
            
            # Извлекаем все числа из RT Заключение_РК_ и объединяем их
            extracted_rk = df['RT\nЗаключение_РК_'].astype(str).str.findall(r'\d+').str.join('')
            df['_Номер_заключения_РК'] = extracted_rk
            
            # Показываем результаты извлечения
            print(f"\nРезультаты извлечения из RT Заключение_РК_:")
            sample_extracted = extracted_rk.head(5).tolist()
            for i, data in enumerate(sample_extracted):
                print(f"  {i+1}: '{data}'")
            
            print("✅ Заполнен столбец '_Номер_заключения_РК' с извлеченными данными из RT Заключение_РК_")
        
        print("✅ Столбцы '_Номер_заключения_ВИК' и '_Номер_заключения_РК' заполнены извлеченными данными")

        # Проверяем наличие столбцов с датами и форматируем их
        print("\n🔄 Проверка и форматирование столбцов с датами...")
        
        date_columns = ['Дата_сварки', 'Дата_АКТ_ВИК', 'Дата_Заключения_РК']
        
        for col in date_columns:
            if col in df.columns:
                # Показываем примеры данных до форматирования
                sample_data_before = df[col].head(3).tolist()
                non_empty_count = df[col].notna().sum()
                print(f"✅ Столбец '{col}' найден: {non_empty_count} непустых значений")
                print(f"   Примеры данных до форматирования: {sample_data_before}")
                
                # Форматируем даты
                df[col] = df[col].apply(format_date)
                
                # Показываем примеры данных после форматирования
                sample_data_after = df[col].head(3).tolist()
                print(f"   Примеры данных после форматирования: {sample_data_after}")
            else:
                print(f"⚠️  Столбец '{col}' не найден в DataFrame")
        
        # Проверяем остальные столбцы без форматирования
        other_columns = ['АКТ_ВИК_N', 'Результаты_АКТ_ВИК']
        for col in other_columns:
            if col in df.columns:
                sample_data = df[col].head(3).tolist()
                non_empty_count = df[col].notna().sum()
                print(f"✅ Столбец '{col}' найден: {non_empty_count} непустых значений")
                print(f"   Примеры данных: {sample_data}")
            else:
                print(f"⚠️  Столбец '{col}' не найден в DataFrame")
        
        # Добавляем столбец с датой загрузки
        df['Дата_загрузки'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        cursor = conn.cursor()
        
        # Подготавливаем SQL запрос для вставки данных
        columns = df.columns.tolist()
        placeholders = ','.join(['?' for _ in range(len(columns))])
        columns_str = ','.join([f'"{col}"' for col in columns])
        insert_query = f'INSERT INTO wl_china ({columns_str}) VALUES ({placeholders})'
        
        # Преобразуем DataFrame в список кортежей для вставки
        data = df[columns].values.tolist()
        
        # Вставляем данные
        print("\nЗагрузка данных в базу...")
        cursor.executemany(insert_query, data)
        conn.commit()
        
        print(f"Успешно загружено {len(data)} записей в базу данных!")
        print("Скрипт успешно завершён. Загружено строк:", len(df))
        
    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        import traceback
        print("Полный стек ошибки:")
        print(traceback.format_exc())

def main():
    try:
        # Подключаемся к базе данных используя утилиту
        print("Подключение к базе данных...")
        conn = get_database_connection()
        print(f"✓ Подключение к базе данных успешно")
        
        # Создаем таблицу и получаем DataFrame
        df = create_table(conn)
        
        # Загружаем данные, передавая DataFrame
        if df is not None:
            load_excel_data(conn, df)
        else:
            print("Ошибка: не удалось создать таблицу")
        
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")
        import traceback
        print("Полный стек ошибки:")
        print(traceback.format_exc())
    finally:
        if 'conn' in locals():
            conn.close()
            print("\nСоединение с базой данных закрыто")

if __name__ == "__main__":
    main()

# Для запуска через GUI
def run_script():
    """Функция для запуска скрипта через GUI"""
    main()
