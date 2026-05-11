import sqlite3
import pandas as pd
import os
import numpy as np
from datetime import datetime
import re
import sys
import warnings
import unicodedata
import locale

# Устанавливаем кодировку UTF-8 для Windows
if sys.platform.startswith('win'):
    try:
        locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')
    except:
        try:
            locale.setlocale(locale.LC_ALL, 'Russian_Russia.1251')
        except:
            pass

    os.environ['PYTHONIOENCODING'] = 'utf-8'
    os.environ['PYTHONLEGACYWINDOWSSTDIO'] = '1'

    import codecs
    if hasattr(sys.stdout, 'buffer'):
        if sys.stdout.encoding != 'utf-8':
            sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
        if sys.stderr.encoding != 'utf-8':
            sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

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

try:
    from .wl_china_schema import (
        CANONICAL_WL_CHINA_DATA_COLUMNS,
        ensure_wl_china_table,
        project_dataframe_to_canonical,
        fill_legacy_document_columns,
    )
except ImportError:
    from wl_china_schema import (
        CANONICAL_WL_CHINA_DATA_COLUMNS,
        ensure_wl_china_table,
        project_dataframe_to_canonical,
        fill_legacy_document_columns,
    )

# Импортируем утилиты
try:
    from utilities.db_utils import clean_column_name, get_database_path, get_database_connection, clean_data_values, print_column_cleaning_report
    from utilities.path_utils import get_excel_paths
    print("✅ Используется clean_column_name из utilities.db_utils")
except ImportError as e:
    print(f"⚠️ Ошибка импорта модулей: {e}")
    print("⚠️ Используется упрощенная версия clean_column_name из блока except ImportError")
    # Создаем заглушки для функций
    def get_database_path():
        return os.path.join(project_root, 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db')

    def get_excel_paths():
        return {
            'ogs_journals': "D:/МК_Кран/МК_Кран_Кингесеп/ОГС/Журналы"
        }

    def clean_column_name(col):
        col_str = str(col)
        original_col = col_str

        # Проверяем, является ли это дефолтным именем от pandas (Unnamed: 0, Unnamed: 1 и т.д.)
        if col_str.startswith('Unnamed:'):
            # Это дефолтное имя от pandas - оставляем как есть, чтобы потом заменить
            # Но сначала попробуем найти реальное имя в первой строке Excel
            pass

        # Удаляем все символы с начала строки до первого переноса строки
        # НО только если после переноса строки есть валидный текст
        if '\n' in col_str:
            # Находим позицию первого переноса строки
            first_newline_pos = col_str.find('\n')
            # Берем текст после переноса строки
            text_after_newline = col_str[first_newline_pos + 1:].strip()
            # Проверяем, есть ли валидный текст после переноса строки
            # (не только пробелы, не только китайские символы)
            if text_after_newline and not re.match(r'^[\u4e00-\u9fff\s]*$', text_after_newline):
                col_str = text_after_newline
            # Если после переноса строки нет валидного текста, оставляем оригинал
            # и просто удалим китайские символы позже

        # Удаляем китайские символы (иероглифы)
        # re уже импортирован в начале файла
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

        # Если название пустое после всех обработок, пробуем использовать оригинал
        if not col_str or col_str.strip() == '':
            # Пробуем взять оригинальное значение и обработать его более аккуратно
            original_cleaned = original_col
            # Удаляем только китайские символы, но сохраняем остальное
            original_cleaned = re.sub(r'[\u4e00-\u9fff]', '', original_cleaned)
            # Удаляем переносы строк, заменяя на пробелы
            original_cleaned = re.sub(r'\n+', ' ', original_cleaned)
            original_cleaned = re.sub(r'\r+', ' ', original_cleaned)
            original_cleaned = re.sub(r'\t+', ' ', original_cleaned)
            original_cleaned = re.sub(r'\s+', ' ', original_cleaned).strip()

            if original_cleaned and original_cleaned != 'nan' and not original_cleaned.startswith('Unnamed:'):
                # Применяем те же замены символов для оригинального значения
                original_cleaned = original_cleaned.replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')
                original_cleaned = original_cleaned.replace('/', '_').replace('\\', '_').replace('.', '_')
                original_cleaned = original_cleaned.replace(',', '_').replace(';', '_').replace(':', '_')
                original_cleaned = original_cleaned.replace('!', '_').replace('?', '_').replace('"', '_')
                original_cleaned = original_cleaned.replace("'", '_').replace('`', '_').replace('~', '_')
                original_cleaned = re.sub(r'_+', '_', original_cleaned).strip('_')
                if original_cleaned:
                    col_str = original_cleaned

            if not col_str or col_str.strip() == '' or col_str.startswith('Unnamed:'):
                # Если это Unnamed, оставляем как есть - обработка будет в create_table
                col_str = 'unnamed_column'

        # Если название пустое, возвращаем дефолтное
        if not col_str or col_str.strip() == '':
            col_str = 'unnamed_column'

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
try:
    excel_paths = get_excel_paths()
    excel_path = excel_paths['ogs_journals'] + '/Журнал сварочных работ.xlsx'
except Exception:
    # Если не удалось получить пути, используем дефолтные
    excel_path = "D:/МК_Кран/МК_Кран_Кингесеп/ОГС/Журналы/Журнал сварочных работ.xlsx"

try:
    db_path = get_database_path()
except Exception:
    # Если не удалось получить путь к БД, используем дефолтный
    db_path = os.path.join(project_root, 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db')

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


def normalize_joint_for_match(joint_text):
    """
    Нормализует номер стыка только для сопоставления:
    удаляет префикс S/F, ведущие нули сохраняет.
    Исходные значения в DataFrame не изменяет.
    """
    if pd.isna(joint_text) or joint_text is None:
        return ''

    text = str(joint_text).strip()
    if not text:
        return ''

    # Удаляем только 1 символ префикса S/F в начале строки.
    # Пример: F33RW -> 33RW, S01 -> 01.
    cleaned = text.strip()
    if cleaned and cleaned[0].upper() in ('S', 'F'):
        cleaned = cleaned[1:]
    cleaned = cleaned.strip().upper().replace(' ', '')

    # Если после удаления префикса строка уже в нужном формате, оставляем как есть.
    m = re.match(r'^(\d+)([A-Z0-9]*)$', cleaned)
    if m:
        return cleaned

    # Fallback: первое число + оставшийся хвост без разделителей (нули сохраняем).
    num_match = re.search(r'(\d+)', cleaned)
    if num_match:
        number = num_match.group(1)
        tail = cleaned[num_match.end():]
        tail = re.sub(r'[^A-Z0-9]', '', tail)
        return f"{number}{tail}"

    return re.sub(r'[^A-Z0-9]', '', cleaned)


def resolve_vik_china_excel_path():
    """Возвращает путь к файлу Журнал_ВИК_китайский, если он найден."""
    search_root = excel_paths.get('ogs_journals', '') if isinstance(excel_paths, dict) else ''
    candidates = [
        'Журнал_ВИК_китайский.xlsx',
        'Журнал_ВИК_китайский.xlsm',
        'Журнал_ВИК_китайский.xls',
    ]
    for name in candidates:
        path = os.path.join(search_root, name)
        if os.path.exists(path):
            return path
    return None


def resolve_rk_china_excel_path():
    """Возвращает путь к файлу Журнал_РК_китайский, если он найден."""
    search_root = excel_paths.get('ogs_journals', '') if isinstance(excel_paths, dict) else ''
    candidates = [
        'Журнал_РК_китайский.xlsx',
        'Журнал_РК_китайский.xlsm',
        'Журнал_РК_китайский.xls',
    ]
    for name in candidates:
        path = os.path.join(search_root, name)
        if os.path.exists(path):
            return path
    return None


def fill_vik_from_china_journal(df):
    """
    Fallback для _Номер_заключения_ВИК из файла Журнал_ВИК_китайский:
    B=блок_N, D=N_Линии, F=Номер_сварного_шва (с нормализацией только для match), U=ВИК.
    """
    vik_excel_path = resolve_vik_china_excel_path()
    if not vik_excel_path:
        print("⚠️ Файл Журнал_ВИК_китайский не найден — fallback для ВИК пропущен")
        return df

    required_df_cols = ['блок_N', 'N_Линии']
    missing_df_cols = [c for c in required_df_cols if c not in df.columns]
    if missing_df_cols:
        print(f"⚠️ Fallback ВИК пропущен: в wl_china DataFrame нет столбцов {missing_df_cols}")
        return df

    try:
        vik_df = pd.read_excel(
            vik_excel_path,
            header=None,
            skiprows=1,          # пропускаем первую строку заголовка
            usecols='B,D,F,U'
        )
    except Exception as e:
        print(f"⚠️ Не удалось прочитать Журнал_ВИК_китайский: {e}")
        return df

    if vik_df.empty:
        print("⚠️ Журнал_ВИК_китайский пустой — fallback для ВИК пропущен")
        return df

    vik_df.columns = ['блок_N', 'N_Линии', 'F_Номер_сварного_шва', 'U_ВИК_заключение']
    vik_df = vik_df.replace({np.nan: None})

    vik_df['match_block'] = vik_df['блок_N'].astype(str).str.strip()
    vik_df['match_line'] = vik_df['N_Линии'].astype(str).str.strip()
    vik_df['match_joint'] = vik_df['F_Номер_сварного_шва'].apply(normalize_joint_for_match)
    vik_df['vik_text'] = vik_df['U_ВИК_заключение'].fillna('').astype(str).str.strip()
    vik_df['vik_num'] = vik_df['vik_text'].str.findall(r'\d+').str.join('')

    # Убираем пустые ключи и пустые номера ВИК
    vik_df = vik_df[
        vik_df['match_block'].ne('') &
        vik_df['match_line'].ne('') &
        vik_df['match_joint'].ne('') &
        vik_df['vik_num'].ne('') &
        vik_df['vik_text'].ne('')
    ]

    if vik_df.empty:
        print("⚠️ В Журнал_ВИК_китайский нет валидных строк для сопоставления ВИК")
        return df

    # Первый непустой ВИК на ключ (блок, линия, стык)
    vik_map = (
        vik_df
        .drop_duplicates(subset=['match_block', 'match_line', 'match_joint'], keep='first')
        .set_index(['match_block', 'match_line', 'match_joint'])[['vik_num', 'vik_text']]
        .to_dict()
    )

    df_block = df['блок_N'].astype(str).str.strip()
    df_line = df['N_Линии'].astype(str).str.strip()
    if '_Номер_сварного_шва_без_S_F_' in df.columns:
        df_joint = df['_Номер_сварного_шва_без_S_F_'].apply(normalize_joint_for_match)
        # Если столбец без S/F присутствует, но местами пустой — добираем ключ из исходного номера шва.
        if 'Номер_сварного_шва' in df.columns:
            df_joint_raw = df['Номер_сварного_шва'].apply(normalize_joint_for_match)
            df_joint = df_joint.where(df_joint.astype(str).str.strip().ne(''), df_joint_raw)
    elif 'Номер_сварного_шва' in df.columns:
        df_joint = df['Номер_сварного_шва'].apply(normalize_joint_for_match)
    else:
        print("⚠️ В wl_china DataFrame нет столбца стыка для fallback ВИК")
        return df

    if '_Номер_заключения_ВИК' not in df.columns:
        df['_Номер_заключения_ВИК'] = ''

    if 'АКТ_ВИК_N' not in df.columns:
        df['АКТ_ВИК_N'] = ''

    filled_count = 0
    for idx in df.index:
        current = '' if pd.isna(df.at[idx, '_Номер_заключения_ВИК']) else str(df.at[idx, '_Номер_заключения_ВИК']).strip()
        if current:
            continue
        key = (df_block.at[idx], df_line.at[idx], df_joint.at[idx])
        mapped = vik_map.get('vik_num', {}).get(key, '')
        mapped_text = vik_map.get('vik_text', {}).get(key, '')
        if mapped:
            df.at[idx, '_Номер_заключения_ВИК'] = mapped
            # АКТ_ВИК_N храним как исходный текст вида "TT 038 М-КРАН-720"
            old_val = '' if pd.isna(df.at[idx, 'АКТ_ВИК_N']) else str(df.at[idx, 'АКТ_ВИК_N']).strip()
            if not old_val:
                df.at[idx, 'АКТ_ВИК_N'] = mapped_text or mapped
            filled_count += 1

    print(
        f"✅ Fallback ВИК из Журнал_ВИК_китайский: заполнено {filled_count} строк "
        f"(по ключам B/D/F и колонке U)"
    )
    return df


def fill_rk_from_china_journal(df):
    """
    Fallback для _Номер_заключения_РК / Заключение_РК_N из файла Журнал_РК_китайский:
    B=блок_N, D=N_Линии, F=Номер_сварного_шва (нормализация только для match), U=РК.
    """
    rk_excel_path = resolve_rk_china_excel_path()
    if not rk_excel_path:
        print("⚠️ Файл Журнал_РК_китайский не найден — fallback для РК пропущен")
        return df

    required_df_cols = ['блок_N', 'N_Линии']
    missing_df_cols = [c for c in required_df_cols if c not in df.columns]
    if missing_df_cols:
        print(f"⚠️ Fallback РК пропущен: в wl_china DataFrame нет столбцов {missing_df_cols}")
        return df

    try:
        rk_df = pd.read_excel(
            rk_excel_path,
            header=None,
            skiprows=1,
            usecols='B,D,F,U'
        )
    except Exception as e:
        print(f"⚠️ Не удалось прочитать Журнал_РК_китайский: {e}")
        return df

    if rk_df.empty:
        print("⚠️ Журнал_РК_китайский пустой — fallback для РК пропущен")
        return df

    rk_df.columns = ['блок_N', 'N_Линии', 'F_Номер_сварного_шва', 'U_РК_заключение']
    rk_df = rk_df.replace({np.nan: None})

    rk_df['match_block'] = rk_df['блок_N'].astype(str).str.strip()
    rk_df['match_line'] = rk_df['N_Линии'].astype(str).str.strip()
    rk_df['match_joint'] = rk_df['F_Номер_сварного_шва'].apply(normalize_joint_for_match)
    rk_df['rk_text'] = rk_df['U_РК_заключение'].fillna('').astype(str).str.strip()
    rk_df['rk_num'] = rk_df['rk_text'].str.findall(r'\d+').str.join('')

    rk_df = rk_df[
        rk_df['match_block'].ne('') &
        rk_df['match_line'].ne('') &
        rk_df['match_joint'].ne('') &
        rk_df['rk_num'].ne('') &
        rk_df['rk_text'].ne('')
    ]

    if rk_df.empty:
        print("⚠️ В Журнал_РК_китайский нет валидных строк для сопоставления РК")
        return df

    rk_map = (
        rk_df
        .drop_duplicates(subset=['match_block', 'match_line', 'match_joint'], keep='first')
        .set_index(['match_block', 'match_line', 'match_joint'])[['rk_num', 'rk_text']]
        .to_dict()
    )

    df_block = df['блок_N'].astype(str).str.strip()
    df_line = df['N_Линии'].astype(str).str.strip()
    if '_Номер_сварного_шва_без_S_F_' in df.columns:
        df_joint = df['_Номер_сварного_шва_без_S_F_'].apply(normalize_joint_for_match)
        if 'Номер_сварного_шва' in df.columns:
            df_joint_raw = df['Номер_сварного_шва'].apply(normalize_joint_for_match)
            df_joint = df_joint.where(df_joint.astype(str).str.strip().ne(''), df_joint_raw)
    elif 'Номер_сварного_шва' in df.columns:
        df_joint = df['Номер_сварного_шва'].apply(normalize_joint_for_match)
    else:
        print("⚠️ В wl_china DataFrame нет столбца стыка для fallback РК")
        return df

    if '_Номер_заключения_РК' not in df.columns:
        df['_Номер_заключения_РК'] = ''
    if 'Заключение_РК_N' not in df.columns:
        df['Заключение_РК_N'] = ''

    filled_count = 0
    for idx in df.index:
        current = '' if pd.isna(df.at[idx, '_Номер_заключения_РК']) else str(df.at[idx, '_Номер_заключения_РК']).strip()
        if current:
            continue
        key = (df_block.at[idx], df_line.at[idx], df_joint.at[idx])
        mapped_num = rk_map.get('rk_num', {}).get(key, '')
        mapped_text = rk_map.get('rk_text', {}).get(key, '')
        if mapped_num:
            df.at[idx, '_Номер_заключения_РК'] = mapped_num
            old_val = '' if pd.isna(df.at[idx, 'Заключение_РК_N']) else str(df.at[idx, 'Заключение_РК_N']).strip()
            if not old_val:
                df.at[idx, 'Заключение_РК_N'] = mapped_text or mapped_num
            filled_count += 1

    print(
        f"✅ Fallback РК из Журнал_РК_китайский: заполнено {filled_count} строк "
        f"(по ключам B/D/F и колонке U)"
    )
    return df

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

        # Если название пустое после всех обработок, пробуем использовать оригинал
        if not col_str or col_str.strip() == '':
            # Пробуем взять оригинальное значение и обработать его более аккуратно
            original_cleaned = original_col
            # Удаляем только китайские символы, но сохраняем остальное
            original_cleaned = re.sub(r'[\u4e00-\u9fff]', '', original_cleaned)
            # Удаляем переносы строк, заменяя на пробелы
            original_cleaned = re.sub(r'\n+', ' ', original_cleaned)
            original_cleaned = re.sub(r'\r+', ' ', original_cleaned)
            original_cleaned = re.sub(r'\t+', ' ', original_cleaned)
            original_cleaned = re.sub(r'\s+', ' ', original_cleaned).strip()

            if original_cleaned and original_cleaned != 'nan':
                col_str = original_cleaned
            else:
                col_str = 'unnamed_column'

        # Если название пустое, возвращаем дефолтное
        if not col_str or col_str.strip() == '':
            col_str = 'unnamed_column'

        # Если название слишком длинное, обрезаем его
        if len(col_str) > 50:
            col_str = col_str[:50]

        # Выводим информацию об изменении имени столбца
        if original_col != col_str:
            print(f"🔄 Столбец {i+1}: '{original_col}' -> '{col_str}'")

        cleaned_columns.append(col_str)

    return cleaned_columns



def read_wl_china_excel_for_load():
    """
    Читает Excel журнала, нормализует имена столбцов (как раньше).
    Возвращает «широкий» DataFrame до проекции на каноническую схему БД.
    """
    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"Excel файл не найден: {excel_path}")

    print(f"📁 Чтение Excel файла: {excel_path}")
    print("📖 Читаю Excel файл, используя вторую строку как заголовки...")
    df = pd.read_excel(excel_path, header=1)

    # Сохраняем оригинальные названия столбцов для отчета
    original_columns = df.columns.tolist()

    print(f"\n📋 Оригинальные названия столбцов из второй строки (первые 10):")
    for i, col in enumerate(original_columns[:10]):
        col_str = str(col)
        preview = col_str[:50] + ('...' if len(col_str) > 50 else '')
        print(f"   {i+1}: '{preview}' (длина: {len(col_str)}, тип: {type(col).__name__})")

    # Очищаем имена столбцов с использованием улучшенной функции
    cleaned_columns = []
    for i, col in enumerate(df.columns):
        orig_col = str(col)
        cleaned = clean_column_name(col)
        cleaned_columns.append(cleaned)
        if i < 5:
            print(f"   🔄 Столбец {i+1}: '{orig_col[:50]}' -> '{cleaned}'")

    # Функция для мягкой очистки - сохраняет больше исходной информации
    def soft_clean_column_name(col_name):
        """Мягкая очистка - удаляет только китайские символы, сохраняет остальное"""
        if pd.isna(col_name):
            return None

        col_str = str(col_name).strip()
        if not col_str or col_str == 'nan' or col_str.startswith('Unnamed:'):
            return None

        col_str = re.sub(r'[\u4e00-\u9fff\u3400-\u4dbf]', '', col_str)

        if '\n' in col_str:
            parts = col_str.split('\n')
            for part in parts[1:]:
                part_cleaned = re.sub(r'[\u4e00-\u9fff\u3400-\u4dbf]', '', part).strip()
                if part_cleaned and len(part_cleaned) > 0:
                    col_str = part_cleaned
                    break
            else:
                col_str = re.sub(r'[\u4e00-\u9fff\u3400-\u4dbf]', '', parts[0]).strip()

        col_str = col_str.replace('№', 'N').replace('√', 'V').replace('°', 'deg')
        col_str = re.sub(r'\s+', ' ', col_str)

        col_str = col_str.replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')
        col_str = col_str.replace('/', '_').replace('\\', '_').replace('.', '_')
        col_str = col_str.replace(',', '_').replace(';', '_').replace(':', '_')
        col_str = re.sub(r'[^A-Za-zА-Яа-я0-9_]', '_', col_str)
        col_str = re.sub(r'_+', '_', col_str).strip('_')

        if not col_str:
            return None

        if col_str[0].isdigit():
            col_str = 'Column_' + col_str

        return col_str[:50]

    for i, (orig, cleaned) in enumerate(zip(original_columns, cleaned_columns)):
        is_default = (not cleaned or
                     cleaned == 'unnamed_column' or
                     (cleaned.startswith('col_') and cleaned.replace('col_', '').isdigit()) or
                     (cleaned.startswith('Column_') and cleaned.replace('Column_', '').isdigit()))

        if is_default:
            if orig and str(orig) != 'nan' and str(orig).strip() and not str(orig).startswith('Unnamed:'):
                soft_cleaned = soft_clean_column_name(orig)
                if soft_cleaned and soft_cleaned != 'unnamed_column' and not (soft_cleaned.startswith('Column_') and soft_cleaned.replace('Column_', '').isdigit()):
                    cleaned_columns[i] = soft_cleaned
                    print(f"   ✅ Столбец {i+1}: '{orig}' -> '{cleaned_columns[i]}' (мягкая очистка)")
                    continue

            if orig and str(orig) != 'nan' and str(orig).strip() and not str(orig).startswith('Unnamed:'):
                orig_str = str(orig).strip()
                orig_str = re.sub(r'[\u4e00-\u9fff\u3400-\u4dbf]', '', orig_str)
                if '\n' in orig_str:
                    parts = [p.strip() for p in orig_str.split('\n')]
                    for part in parts:
                        part_clean = re.sub(r'[\u4e00-\u9fff\u3400-\u4dbf]', '', part).strip()
                        if part_clean:
                            orig_str = part_clean
                            break
                orig_str = orig_str.replace(' ', '_').replace('-', '_')
                orig_str = re.sub(r'[^A-Za-zА-Яа-я0-9_]', '_', orig_str)
                orig_str = re.sub(r'_+', '_', orig_str).strip('_')
                if orig_str and len(orig_str) > 0:
                    if orig_str[0].isdigit():
                        orig_str = 'Col_' + orig_str
                    cleaned_columns[i] = orig_str[:50]
                    print(f"   ✅ Столбец {i+1}: '{orig}' -> '{cleaned_columns[i]}' (консервативная очистка)")
                    continue

            cleaned_columns[i] = f'Column_{i+1}'
            print(f"   ⚠️  Столбец {i+1}: '{orig}' -> '{cleaned_columns[i]}' (использован номер по умолчанию)")

    n_linii_count = cleaned_columns.count('N_Линии')
    if n_linii_count > 1:
        print(f"\n⚠️  Найдено {n_linii_count} столбцов с именем 'N_Линии'. Переименовываем второй в 'N_Линии_по_старой_ревизии_РД'")
        first_found = False
        for i, col_name in enumerate(cleaned_columns):
            if col_name == 'N_Линии':
                if first_found:
                    cleaned_columns[i] = 'N_Линии_по_старой_ревизии_РД'
                    print(f"   ✅ Столбец {i+1} переименован: 'N_Линии' -> 'N_Линии_по_старой_ревизии_РД'")
                    break
                first_found = True
                print(f"   ✅ Столбец {i+1} оставлен как 'N_Линии'")

    status_count = cleaned_columns.count('Статус')
    if status_count > 1:
        print(f"\n⚠️  Найдено {status_count} столбцов с именем 'Статус'. Переименовываем дубликаты в 'Статус_2', 'Статус_3' и т.д.")
        status_idx = 0
        for i, col_name in enumerate(cleaned_columns):
            if col_name == 'Статус':
                status_idx += 1
                if status_idx == 1:
                    print(f"   ✅ Столбец {i+1} оставлен как 'Статус'")
                else:
                    new_name = f'Статус_{status_idx}'
                    cleaned_columns[i] = new_name
                    print(f"   ✅ Столбец {i+1} переименован: 'Статус' -> '{new_name}'")

    remont_count = cleaned_columns.count('Ремонтный_сварщик')
    if remont_count > 1:
        print(f"\n⚠️  Найдено {remont_count} столбцов с именем 'Ремонтный_сварщик'. Переименовываем дубликаты в 'Ремонтный_сварщик_2', 'Ремонтный_сварщик_3' и т.д.")
        remont_idx = 0
        for i, col_name in enumerate(cleaned_columns):
            if col_name == 'Ремонтный_сварщик':
                remont_idx += 1
                if remont_idx == 1:
                    print(f"   ✅ Столбец {i+1} оставлен как 'Ремонтный_сварщик'")
                else:
                    new_name = f'Ремонтный_сварщик_{remont_idx}'
                    cleaned_columns[i] = new_name
                    print(f"   ✅ Столбец {i+1} переименован: 'Ремонтный_сварщик' -> '{new_name}'")

    seen_names = {}
    for i, col_name in enumerate(cleaned_columns):
        if col_name in seen_names:
            seen_names[col_name] += 1
            new_name = f"{col_name}_{seen_names[col_name]}"
            print(f"   ✅ Столбец {i+1} переименован из-за дубликата: '{col_name}' -> '{new_name}'")
            cleaned_columns[i] = new_name
        else:
            seen_names[col_name] = 1

    df.columns = cleaned_columns

    print(f"\n📋 Очищенные названия столбцов (первые 10):")
    for i, col in enumerate(df.columns[:10]):
        print(f"   {i+1}: '{col}'")

    print_column_cleaning_report(original_columns, df.columns.tolist())

    df['_Номер_заключения_ВИК'] = ''
    df['_Номер_заключения_РК'] = ''

    return df


def create_table(conn):
    try:
        cursor = conn.cursor()
        ensure_wl_china_table(cursor, log_print=print)
        conn.commit()
        return read_wl_china_excel_for_load()
    except sqlite3.Error as e:
        print(f"Ошибка при подготовке таблицы wl_china: {e}")
        return None

def load_excel_data(conn, df=None):
    try:
        if df is None:
            df = read_wl_china_excel_for_load()
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

        # Извлекаем номера заключений из столбцов ВИК/РК.
        # Поддерживаем как сырые имена (с переводами строк), так и очищенные имена после clean_column_name.
        vik_source_candidates = [
            'VT\nАКТ_ВИК_', 'АКТ_ВИК', 'VT_АКТ_ВИК', 'VT_АКТ_ВИК_', 'АКТ_ВИК_N',
        ]
        rk_source_candidates = [
            'RT\nЗаключение_РК_', 'Заключение_РК', 'RT_Заключение_РК', 'RT_Заключение_РК_', 'Заключение_РК_N',
        ]

        vik_source_col = next((col for col in vik_source_candidates if col in df.columns), None)
        rk_source_col = next((col for col in rk_source_candidates if col in df.columns), None)

        if vik_source_col:
            # Показываем примеры данных для отладки
            print(f"\nПримеры данных в столбце {vik_source_col}:")
            sample_data = df[vik_source_col].head(5).tolist()
            for i, data in enumerate(sample_data):
                print(f"  {i+1}: '{data}'")

            # Извлекаем все числа и объединяем их
            extracted_vik = df[vik_source_col].astype(str).str.findall(r'\d+').str.join('')
            df['_Номер_заключения_ВИК'] = extracted_vik

            # Показываем результаты извлечения
            print(f"\nРезультаты извлечения из {vik_source_col}:")
            sample_extracted = extracted_vik.head(5).tolist()
            for i, data in enumerate(sample_extracted):
                print(f"  {i+1}: '{data}'")

            print(f"✅ Заполнен столбец '_Номер_заключения_ВИК' с извлеченными данными из {vik_source_col}")
        else:
            print("⚠️ Источник для '_Номер_заключения_ВИК' не найден. Проверены: VT\\nАКТ_ВИК_, АКТ_ВИК, VT_АКТ_ВИК, АКТ_ВИК_N")
            df = fill_vik_from_china_journal(df)

        # Для РК обрабатываем несколько источников построчно:
        # берем первое непустое извлечение цифр из доступных столбцов.
        # Это защищает от кейсов, когда один из "ранних" столбцов существует,
        # но в нем пусто, а реальные значения лежат, например, в Заключение_РК_N.
        rk_existing_candidates = [col for col in rk_source_candidates if col in df.columns]
        rk_extra_candidates = [
            col for col in df.columns
            if ('Заключение_РК' in col or 'РК_N' in col) and col not in rk_existing_candidates
        ]
        rk_all_candidates = rk_existing_candidates + rk_extra_candidates

        if rk_all_candidates:
            print(f"\nНайдены источники для '_Номер_заключения_РК': {rk_all_candidates}")
            extracted_rk = pd.Series([''] * len(df), index=df.index, dtype='object')
            rk_fill_stats = {}

            for source_col in rk_all_candidates:
                # Показываем примеры данных для отладки
                print(f"\nПримеры данных в столбце {source_col}:")
                sample_data = df[source_col].head(5).tolist()
                for i, data in enumerate(sample_data):
                    print(f"  {i+1}: '{data}'")

                source_extracted = (
                    df[source_col]
                    .fillna('')
                    .astype(str)
                    .str.findall(r'\d+')
                    .str.join('')
                )
                fill_mask = extracted_rk.eq('') & source_extracted.ne('')
                rk_fill_stats[source_col] = int(fill_mask.sum())
                extracted_rk = extracted_rk.where(~fill_mask, source_extracted)

            df['_Номер_заключения_РК'] = extracted_rk

            # Показываем результаты извлечения
            print("\nРезультаты извлечения для '_Номер_заключения_РК' (первые 5):")
            sample_extracted = extracted_rk.head(5).tolist()
            for i, data in enumerate(sample_extracted):
                print(f"  {i+1}: '{data}'")

            total_filled_rk = int(extracted_rk.ne('').sum())
            print(f"✅ Заполнен столбец '_Номер_заключения_РК'. Непустых значений: {total_filled_rk}")
            print(f"ℹ️ Вклад по источникам: {rk_fill_stats}")
        else:
            print("⚠️ Источник для '_Номер_заключения_РК' не найден. Проверены: RT\\nЗаключение_РК_, Заключение_РК, RT_Заключение_РК, Заключение_РК_N")
            df = fill_rk_from_china_journal(df)

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

        # Проекция на фиксированную схему БД (сопоставление имён Excel → wl_china)
        df = project_dataframe_to_canonical(df, log_print=print)
        df = fill_legacy_document_columns(df)

        df['Дата_загрузки'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        cursor = conn.cursor()

        insert_cols = list(CANONICAL_WL_CHINA_DATA_COLUMNS) + ['Дата_загрузки']
        placeholders = ','.join(['?' for _ in insert_cols])
        columns_str = ','.join([f'"{col}"' for col in insert_cols])
        insert_query = f'INSERT INTO wl_china ({columns_str}) VALUES ({placeholders})'

        data = df[insert_cols].values.tolist()

        # Вставляем данные
        print("\nЗагрузка данных в базу...")
        cursor.executemany(insert_query, data)
        conn.commit()

        print(f"Успешно загружено {len(data)} записей в базу данных!")
        print("Скрипт успешно завершён. Загружено строк:", len(df))

        # Автоматическое обновление таблицы сварено_сварщиком после загрузки wl_china
        print("\n🔄 Автоматическое обновление таблицы сварено_сварщиком...")
        try:
            # Путь к модулю create_svarenno_svarshchikom_table
            # current_dir = scripts/data_loaders
            # project_root = корень проекта
            # database_dir = scripts/database
            database_dir = os.path.join(project_root, 'scripts', 'database')
            if database_dir not in sys.path:
                sys.path.insert(0, database_dir)

            from create_svarenno_svarshchikom_table import SvarennoSvarshchikomCreator

            creator = SvarennoSvarshchikomCreator()
            if creator.connect_to_database():
                success = creator.run_creation()
                if success:
                    print("✅ Таблица сварено_сварщиком успешно обновлена")
                else:
                    print("⚠️ Предупреждение: таблица сварено_сварщиком не была обновлена")
            else:
                print("⚠️ Предупреждение: не удалось подключиться к БД для обновления сварено_сварщиком")
        except Exception as e:
            print(f"⚠️ Предупреждение: ошибка при автоматическом обновлении таблицы сварено_сварщиком: {e}")
            import traceback
            print(traceback.format_exc())
            # Не прерываем выполнение, так как основная загрузка wl_china прошла успешно

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
