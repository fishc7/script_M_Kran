import os
import sqlite3
import pandas as pd
import re
import unicodedata

def get_database_path():
    """
    Автоматически определяет правильный путь к базе данных M_Kran_Kingesepp.db
    Работает для всех скриптов в проекте независимо от того, откуда они запускаются
    """
    # Получаем текущую директорию
    current_dir = os.getcwd()
    
    # Пробуем разные варианты путей для новой структуры проекта
    possible_paths = [
        # Если запускаем из корневой папки проекта (новая структура)
        os.path.join(current_dir, 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Если запускаем из папки scripts
        os.path.join(current_dir, '..', 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Если запускаем из папки scripts/data_loaders
        os.path.join(current_dir, '..', '..', 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Если запускаем из папки web/app
        os.path.join(current_dir, '..', '..', 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Если запускаем из папки desktop/qt_app
        os.path.join(current_dir, '..', '..', 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Старые пути для совместимости
        os.path.join(current_dir, 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        os.path.join(current_dir, '..', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
    ]
    
    for path in possible_paths:
        abs_path = os.path.abspath(path)
        if os.path.exists(abs_path):
            return abs_path
    
    # Если не нашли, возвращаем None
    return None

def get_database_connection():
    """
    Создает подключение к базе данных с автоматическим определением пути
    """
    db_path = get_database_path()
    
    if db_path is None:
        raise FileNotFoundError("База данных M_Kran_Kingesepp.db не найдена")
    
    return sqlite3.connect(db_path)

def test_connection():
    """
    Тестирует подключение к базе данных
    """
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Проверяем, что можем выполнить простой запрос
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        conn.close()
        
        print(f"✓ Подключение к базе данных успешно")
        print(f"✓ Найдено таблиц: {len(tables)}")
        return True
        
    except Exception as e:
        print(f"✗ Ошибка при подключении к базе данных: {e}")
        return False 

def clean_column_name(col_name):
    """
    Улучшенная функция очистки названий столбцов от специальных символов, включая переносы строк
    
    Args:
        col_name: Исходное название столбца
        
    Returns:
        str: Очищенное название столбца, пригодное для использования в SQL
    """
    if pd.isna(col_name):
        return 'unnamed_column'
    
    # Преобразуем в строку и очищаем
    col_str = str(col_name).strip()
    
    # Нормализуем Unicode символы (убираем диакритические знаки и т.д.)
    col_str = unicodedata.normalize('NFKD', col_str)
    
    # Удаляем все символы с начала строки до первого переноса строки
    # Это удалит китайские символы и любой другой текст до первого \n
    if '\n' in col_str:
        # Находим позицию первого переноса строки
        first_newline_pos = col_str.find('\n')
        # Удаляем все символы до первого переноса строки включительно
        col_str = col_str[first_newline_pos + 1:]
    
    # Удаляем все китайские символы (иероглифы) - расширенный диапазон
    # Основной диапазон китайских символов: \u4e00-\u9fff
    # Дополнительные китайские символы: \u3400-\u4dbf, \u20000-\u2a6df, \u2a700-\u2b73f, \u2b740-\u2b81f, \u2b820-\u2ceaf
    col_str = re.sub(r'[\u4e00-\u9fff\u3400-\u4dbf\u20000-\u2a6df\u2a700-\u2b73f\u2b740-\u2b81f\u2b820-\u2ceaf]', '', col_str)
    
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
    
    # Оставляем только допустимые символы: буквы (латиница + кириллица), цифры и подчеркивания
    # Латиница: A-Z, a-z
    # Кириллица: А-Я, а-я
    # Цифры: 0-9
    # Подчеркивания: _
    original_clean = re.sub(r'[^A-Za-zА-Яа-я0-9_]', '', col_str)
    
    # Если название пустое или начинается с цифры, добавляем префикс
    if not original_clean or original_clean[0].isdigit():
        col_str = 'col_' + original_clean
    else:
        col_str = original_clean
    
    # Добавляем уникальный суффикс для всех столбцов, чтобы избежать дублирования
    unique_suffix = str(abs(hash(col_name)) % 10000)
    col_str = col_str + '_' + unique_suffix
    
    # Убираем префиксы col_1 и col_2 (после того, как они могли быть добавлены)
    col_str = re.sub(r'^col_1_', '', col_str)
    col_str = re.sub(r'^col_2_', '', col_str)
    
    # Если название слишком длинное, обрезаем его
    if len(col_str) > 50:
        col_str = col_str[:50]
    
    # Если после всех преобразований название пустое, возвращаем дефолтное
    if not col_str:
        col_str = 'unnamed_column'
    
    return col_str

def clean_data_values(df):
    """
    Очищает значения в DataFrame от Unicode символов (эмодзи, проблемные символы)
    
    Args:
        df: DataFrame для очистки
        
    Returns:
        DataFrame: Очищенный DataFrame
    """
    # Создаем копию DataFrame
    cleaned_df = df.copy()
    
    for column in cleaned_df.columns:
        try:
            if cleaned_df[column].dtype == 'object':  # Только для текстовых столбцов
                cleaned_df[column] = cleaned_df[column].astype(str).apply(lambda x: 
                    clean_unicode_text(x) if pd.notna(x) and x != 'nan' else None
                )
        except Exception as e:
            print(f"Ошибка при очистке столбца {column}: {e}")
            continue
    
    return cleaned_df

def clean_unicode_text(text):
    """
    Очищает текст от проблемных Unicode символов
    
    Args:
        text: Текст для очистки
        
    Returns:
        str: Очищенный текст
    """
    if not isinstance(text, str):
        return text
    
    if text is None or text == '':
        return text
    
    try:
        # Заменяем все известные проблемные символы
        replacements = {
            '、': ', ',  # Японская запятая
            '。': '. ',  # Японская точка
            '　': ' ',   # Японский пробел
            '°C': 'C',  # Градус Цельсия
            '°F': 'F',  # Градус Фаренгейта
            '°': '',    # Градус
            '\u3001': ', ',  # Японская запятая (Unicode)
            '\u3002': '. ',  # Японская точка (Unicode)
            '\u3000': ' ',   # Японский пробел (Unicode)
            '\u2103': 'C',   # Градус Цельсия (Unicode)
            '\u2109': 'F',   # Градус Фаренгейта (Unicode)
            '\u00b0': '',    # Градус (Unicode)
            '\u2028': ' ',   # Разделитель строк
            '\u2029': ' ',   # Разделитель абзацев
            '\u00a0': ' ',   # Неразрывный пробел
            '\u200b': '',    # Нулевая ширина пробела
            '\u200c': '',    # Нулевая ширина не-соединитель
            '\u200d': '',    # Нулевая ширина соединитель
            '\u2060': '',    # Слово-соединитель
            '\ufeff': '',    # BOM
            '\u2013': '-',   # Короткое тире
            '\u2014': '-',   # Длинное тире
            '\u2018': "'",   # Левая одинарная кавычка
            '\u2019': "'",   # Правая одинарная кавычка
            '\u201c': '"',   # Левая двойная кавычка
            '\u201d': '"',   # Правая двойная кавычка
            '\u2022': '*',   # Маркер списка
            '\u2026': '...', # Многоточие
            '\u00ae': '(R)', # Зарегистрированная торговая марка
            '\u00a9': '(C)', # Авторское право
            '\u2122': '(TM)', # Торговая марка
        }
        
        cleaned = text
        for old_char, new_char in replacements.items():
            cleaned = cleaned.replace(old_char, new_char)
        
        # Удаляем все управляющие символы и эмодзи, кроме базовых
        result = ''
        for char in cleaned:
            code = ord(char)
            # Оставляем только печатные символы ASCII, пробелы, табуляцию, переносы строк
            if (code >= 32 and code <= 126) or code in [9, 10, 13]:
                result += char
            elif code > 127:  # Unicode символы
                # Проверяем, что это не проблемный символ и не эмодзи
                problematic_codes = [
                    0x3001, 0x3002, 0x3000, 0x2028, 0x2029, 0x00a0, 0x200b, 0x200c, 0x200d, 0x2060, 0xfeff,
                    0x2103, 0x2109, 0x00b0, 0x2013, 0x2014, 0x2018, 0x2019, 0x201c, 0x201d, 0x2022, 0x2026,
                    0x00ae, 0x00a9, 0x2122
                ]
                
                # Проверяем, что это не эмодзи (коды эмодзи начинаются с 0x1F600)
                if code not in problematic_codes and code < 0x1F600:
                    # Для остальных Unicode символов пытаемся их сохранить
                    try:
                        # Проверяем, можно ли закодировать в Windows-1251
                        char.encode('cp1251')
                        result += char
                    except UnicodeEncodeError:
                        # Если не можем закодировать, заменяем на пробел
                        result += ' '
        
        # Удаляем множественные пробелы
        result = re.sub(r'\s+', ' ', result)
        
        # Удаляем пробелы в начале и конце
        result = result.strip()
        
        return result if result else ''
        
    except Exception as e:
        # В случае ошибки возвращаем исходный текст
        return text



def validate_column_names(columns):
    """
    Проверяет и выводит информацию о проблемных названиях столбцов
    
    Args:
        columns: Список названий столбцов
        
    Returns:
        dict: Информация о проблемах с названиями столбцов
    """
    problems = {
        'newlines': [],
        'special_chars': [],
        'too_long': [],
        'starts_with_digit': []
    }
    
    for i, col in enumerate(columns):
        original = str(col)
        
        # Проверяем на переносы строк
        if '\n' in original or '\r' in original:
            problems['newlines'].append((i, original))
        
        # Проверяем на специальные символы
        if re.search(r'[^\w\s]', original):
            problems['special_chars'].append((i, original))
        
        # Проверяем длину
        if len(original) > 50:
            problems['too_long'].append((i, original))
        
        # Проверяем начало с цифры
        if original and original[0].isdigit():
            problems['starts_with_digit'].append((i, original))
    
    return problems

def print_column_cleaning_report(original_columns, cleaned_columns):
    """
    Выводит отчет о процессе очистки названий столбцов
    
    Args:
        original_columns: Исходные названия столбцов
        cleaned_columns: Очищенные названия столбцов
    """
    print("\n" + "="*80)
    print("ОТЧЕТ ОБ ОЧИСТКЕ НАЗВАНИЙ СТОЛБЦОВ")
    print("="*80)
    
    # Проверяем проблемы в исходных названиях
    problems = validate_column_names(original_columns)
    
    if problems['newlines']:
        print(f"\n⚠️  НАЙДЕНО {len(problems['newlines'])} СТОЛБЦОВ С ПЕРЕНОСАМИ СТРОК:")
        for idx, col in problems['newlines']:
            print(f"   {idx+1}. '{col}'")
    
    if problems['special_chars']:
        print(f"\n⚠️  НАЙДЕНО {len(problems['special_chars'])} СТОЛБЦОВ СО СПЕЦИАЛЬНЫМИ СИМВОЛАМИ:")
        for idx, col in problems['special_chars']:
            print(f"   {idx+1}. '{col}'")
    
    if problems['too_long']:
        print(f"\n⚠️  НАЙДЕНО {len(problems['too_long'])} СЛИШКОМ ДЛИННЫХ НАЗВАНИЙ:")
        for idx, col in problems['too_long']:
            print(f"   {idx+1}. '{col}' (длина: {len(col)})")
    
    if problems['starts_with_digit']:
        print(f"\n⚠️  НАЙДЕНО {len(problems['starts_with_digit'])} НАЗВАНИЙ, НАЧИНАЮЩИХСЯ С ЦИФРЫ:")
        for idx, col in problems['starts_with_digit']:
            print(f"   {idx+1}. '{col}'")
    
    # Выводим сопоставление
    print(f"\n📋 СОПОСТАВЛЕНИЕ ИСХОДНЫХ И ОЧИЩЕННЫХ НАЗВАНИЙ:")
    print("-" * 80)
    for i, (orig, cleaned) in enumerate(zip(original_columns, cleaned_columns), 1):
        if orig != cleaned:
            print(f"{i:2d}. '{orig}' → '{cleaned}'")
        else:
            print(f"{i:2d}. '{orig}' (без изменений)")
    
    print("="*80) 