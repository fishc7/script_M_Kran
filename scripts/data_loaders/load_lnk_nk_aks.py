# -*- coding: utf-8 -*-
"""
Догрузка строк в logs_lnk из файлов LOG_М-КРАН_RT_ТТ и LOG_М-КРАН_VT_TT (папка НК_АКС).
Форматы: .xlsx, .xls, .xlsb (лист LOG, без учёта регистра), .csv (без листов).
Строка 1 — служебная, строка 2 — подписи столбцов, данные с строки 3; колонки 0..N-1
соответствуют номерам столбцов Excel при сопоставлении.
Даты (Дата_заявки, Дата_сварки, даты РК) в БД — единый формат ГГГГ-ММ-ДД (2024-11-21).

Запускать после load_lnk_data.py (основной журнал НГС), чтобы не затереть данные.
Перед вставкой удаляются все строки с Источник=AKS — повторный запуск не дублирует догрузку.
app_row_id — хеш-ключ: SHA-256 от канонической строки полей строки AKS, вид aks_<32 hex> (см. _aks_row_hash_preimage / _aks_app_row_id_hash_key).

Папка с файлом АКС по умолчанию: <родитель каталога PROJECT_ROOT>/МК_Кран_Кингесеп/НК_АКС (path_utils.get_excel_paths).
Иначе задайте переменную окружения NK_AKS_FOLDER — абсолютный путь к каталогу с журналом.
"""

import hashlib
import os
import re
import sqlite3
import sys
import time
from datetime import datetime

import pandas as pd

try:
    from ..utilities.path_utils import get_database_path, get_excel_paths
    from ..utilities.date_format import normalize_logs_lnk_date
except ImportError:
    _cd = os.path.dirname(os.path.abspath(__file__))
    _ud = os.path.join(os.path.dirname(_cd), 'utilities')
    if _ud not in sys.path:
        sys.path.insert(0, _ud)
    from path_utils import get_database_path, get_excel_paths
    from date_format import normalize_logs_lnk_date

# В веб ScriptRunner поток stdout может быть закрыт после выполнения других скриптов.
# Безопасный print предотвращает падение загрузчика на логировании.
_RAW_PRINT = print


def print(*args, **kwargs):  # type: ignore[override]
    try:
        _RAW_PRINT(*args, **kwargs)
    except ValueError:
        # I/O operation on closed file
        pass


# Лист и шаблоны имён файлов
SHEET_NAME = 'LOG'  # без учёта регистра см. _resolve_log_sheet_name
FILE_MARKERS_RT = ('LOG', 'М-КРАН', 'RT', 'ТТ')  # все подстроки — в имени файла
FILE_MARKERS_VT = ('LOG', 'М-КРАН', 'VT', 'TT')  # в VT-файле обычно латинские TT

# Поддерживаемые форматы (не только xlsx)
SOURCE_EXTENSIONS = ('.xlsx', '.xls', '.xlsb', '.csv')

# Источник строки в logs_lnk (NGS — журнал НГС в load_lnk_data.py; AKS — этот скрипт)
LOGS_LNK_SOURCE_AKS = 'AKS'
# Excel 51 заполнен, 32 (Статус_РК) пустой → в БД «Заказ отправлен»
STATUS_RK_ZAYAVLEN_AKS = 'Заказ отправлен'
FILE_MARKERS_FALLBACK = ('LOG', 'RT')  # если полное имя отличается (латиница и т.д.)

# Строка 1 в Excel (индекс 0) — служебная; строка 2 (индекс 1) — подписи столбцов; данные с строки 3 (индекс 2)
EXCEL_HEADER_ROW_INDEX = 1
EXCEL_FIRST_DATA_ROW_INDEX = 2

# Настройки устойчивости к блокировкам SQLite
SQLITE_TIMEOUT_SEC = 60
SQLITE_BUSY_TIMEOUT_MS = 60000
SQLITE_RETRY_ATTEMPTS = 5
SQLITE_RETRY_DELAY_SEC = 1.0


def _is_sqlite_locked_error(err):
    msg = str(err).lower()
    return 'database is locked' in msg or 'database table is locked' in msg


def _create_sqlite_connection(db_file_path):
    conn = sqlite3.connect(db_file_path, timeout=SQLITE_TIMEOUT_SEC)
    cur = conn.cursor()
    cur.execute(f'PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}')
    cur.execute('PRAGMA journal_mode = WAL')
    cur.execute('PRAGMA synchronous = NORMAL')
    conn.commit()
    return conn


def _resolve_db_path():
    if os.environ.get('PROJECT_ROOT'):
        p = os.path.join(
            os.environ['PROJECT_ROOT'],
            'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db',
        )
        return os.path.normpath(os.path.abspath(p))
    return get_database_path()


def _resolve_nk_aks_folder():
    """Каталог с Excel АКС: NK_AKS_FOLDER или get_excel_paths()['nk_aks']."""
    override = (os.environ.get('NK_AKS_FOLDER') or '').strip()
    if override:
        return os.path.normpath(os.path.abspath(override))
    raw = (get_excel_paths().get('nk_aks') or '').strip()
    return os.path.normpath(os.path.abspath(raw)) if raw else ''


def _default_nk_aks_folder_hint():
    """Путь по схеме path_utils (для сообщений об ошибке)."""
    raw = (get_excel_paths().get('nk_aks') or '').strip()
    return os.path.normpath(os.path.abspath(raw)) if raw else ''


def _find_source_file_by_markers(folder, markers, fallback=None):
    if not folder or not os.path.isdir(folder):
        return None
    candidates = []
    fallback_candidates = []
    any_log_file = []
    for fn in os.listdir(folder):
        if fn.startswith('~$'):
            continue
        low = fn.lower()
        if not low.endswith(SOURCE_EXTENSIONS):
            continue
        path = os.path.join(folder, fn)
        if all(m in fn for m in markers):
            candidates.append(path)
        elif fallback and all(m in fn for m in fallback) and fn.upper().startswith('LOG'):
            fallback_candidates.append(path)
        elif 'LOG' in fn.upper():
            any_log_file.append(path)
    pool = candidates or fallback_candidates or any_log_file
    if not pool:
        return None
    return max(pool, key=os.path.getmtime)


def _find_aks_source_files(folder):
    """Возвращает {'rt': path_or_none, 'vt': path_or_none}."""
    return {
        'rt': _find_source_file_by_markers(folder, FILE_MARKERS_RT, FILE_MARKERS_FALLBACK),
        'vt': _find_source_file_by_markers(folder, FILE_MARKERS_VT, ('LOG', 'VT')),
    }


def _resolve_log_sheet_name(excel_path):
    """Лист LOG без учёта регистра (для xlsx/xls/xlsb через pandas)."""
    ext = os.path.splitext(excel_path)[1].lower()
    try:
        if ext == '.xlsb':
            xl = pd.ExcelFile(excel_path, engine='pyxlsb')
        else:
            xl = pd.ExcelFile(excel_path)
    except Exception as e:
        print(f'[ERR] Не удалось открыть файл как Excel: {e}')
        return None
    for name in xl.sheet_names:
        if str(name).strip().casefold() == 'log':
            return name
    print(f'[ERR] Лист LOG не найден. Доступные листы: {xl.sheet_names}')
    return None


def _read_xlsb_raw(path, sheet_target='LOG'):
    """Чтение .xlsb через pyxlsb, если pandas не справился."""
    try:
        import pyxlsb
    except ImportError:
        print('[ERR] Для .xlsb нужен пакет: pip install pyxlsb')
        return None
    with pyxlsb.open_workbook(path) as wb:
        idx = None
        for i, n in enumerate(wb.sheets, start=1):
            if str(n).strip().casefold() == sheet_target.casefold():
                idx = i
                break
        if idx is None:
            print(f'[ERR] Лист LOG не найден в .xlsb. Листы: {list(wb.sheets)}')
            return None
        rows = []
        with wb.get_sheet(idx) as sh:
            for row in sh.rows():
                rows.append([c.v for c in row])
    return pd.DataFrame(rows)


def _read_csv_raw(path):
    """CSV: первая строка — служебная, вторая — заголовки, данные с третьей (как в Excel)."""
    last_err = None
    for enc in ('utf-8-sig', 'cp1251', 'utf-8'):
        for sep in (None, ';', '\t', ','):
            try:
                if sep is None:
                    raw = pd.read_csv(
                        path, header=None, dtype=object, encoding=enc, sep=None, engine='python',
                    )
                else:
                    raw = pd.read_csv(path, header=None, dtype=object, encoding=enc, sep=sep)
                if raw.shape[1] >= 2:
                    return raw
            except Exception as e:
                last_err = e
                continue
    print(f'[ERR] Не удалось прочитать CSV: {last_err}')
    return None


def _read_excel_raw(path, sheet_name):
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext == '.xlsx':
            return pd.read_excel(
                path, sheet_name=sheet_name, header=None, dtype=object, engine='openpyxl',
            )
        if ext == '.xlsb':
            try:
                return pd.read_excel(
                    path, sheet_name=sheet_name, header=None, dtype=object, engine='pyxlsb',
                )
            except Exception:
                return _read_xlsb_raw(path, 'LOG')
        return pd.read_excel(path, sheet_name=sheet_name, header=None, dtype=object)
    except Exception as e:
        print(f'[ERR] read_excel: {e}')
        if ext == '.xlsb':
            return _read_xlsb_raw(path, 'LOG')
        return None


def _apply_aks_row_layout(raw):
    """
    Строка Excel №2 (индекс 1) задаёт ширину таблицы; данные с Excel №3.
    Колонки 0..N-1 соответствуют номерам Excel (1→0, 2→1, …).
    """
    if raw is None or raw.empty:
        return None
    if raw.shape[0] <= EXCEL_FIRST_DATA_ROW_INDEX:
        print(f'[ERR] Мало строк (нужны данные с {EXCEL_FIRST_DATA_ROW_INDEX + 1}-й строки файла)')
        return None
    ncols = raw.shape[1]
    df = raw.iloc[EXCEL_FIRST_DATA_ROW_INDEX:].copy()
    df.columns = list(range(ncols))
    df = df.reset_index(drop=True)
    df = df.dropna(how='all')
    return df


def _read_aks_dataframe(path):
    """
    Читает источник АКС: .csv (без листа), .xlsb / .xlsx / .xls — лист LOG.
    Возвращает (dataframe, метка_для_лога) или (None, None).
    """
    ext = os.path.splitext(path)[1].lower()
    if ext == '.csv':
        raw = _read_csv_raw(path)
        label = 'CSV'
    else:
        sheet = _resolve_log_sheet_name(path)
        if not sheet:
            return None, None
        raw = _read_excel_raw(path, sheet)
        label = sheet
    df = _apply_aks_row_layout(raw)
    return df, label


def _as_text(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if hasattr(val, 'strftime'):
        return val.strftime('%Y-%m-%d %H:%M:%S') if hasattr(val, 'hour') else str(val)[:10]
    s = str(val).strip()
    return s if s else None


def _digits_only(val):
    s = _as_text(val)
    if not s:
        return None
    d = ''.join(c for c in s if c.isdigit())
    return d if d else None


def _format_titul_from_drawing_col2(raw):
    """
    Столбец 2 (чертеж), напр. GCC-NAG-DDD-12460-12-1500-TK-ISO-00016 → Титул «12460-12».
    Ищем первое вхождение пяти цифр, дефис, две цифры. Чертеж в БД — полное значение отдельно.
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    s = str(raw).strip()
    if not s:
        return None
    m = re.search(r'(\d{5})-(\d{2})', s)
    if not m:
        return None
    return f'{m.group(1)}-{m.group(2)}'


def _combine_welder_marks_col16_17(a, b):
    """Столбцы Excel 16 и 17 → Клейма_сварщиков; при разных значениях — через «;», при совпадении — одно."""
    sa = (_as_text(a) or '').strip()
    sb = (_as_text(b) or '').strip()
    if not sa and not sb:
        return None
    if not sa:
        return sb
    if not sb:
        return sa
    if sa.casefold() == sb.casefold():
        return sa
    return f'{sa};{sb}'


def _cell(row, excel_1based):
    idx = excel_1based - 1
    if idx < 0 or idx >= len(row):
        return None
    v = row.iloc[idx]
    return v


def _aks_row_hash_preimage(row, mode='rt'):
    """
    Каноническая строка (прообраз) для хеш-ключа строки AKS.
    Порядок и номера колонок Excel зафиксированы; даты — как после normalize_logs_lnk_date.
    Разделитель U+241E редко встречается в данных.

    Поля: версия схемы, источник, тип файла (RT/VT), чертёж(2), стык(7), дата сварки(14).
    RT и VT намеренно разделяются по app_row_id (разные записи в logs_lnk).
    """
    parts = (
        'aksid:v3',
        LOGS_LNK_SOURCE_AKS,
        str(mode).lower(),
        (_as_text(_cell(row, 2)) or '').strip(),
        (_as_text(_cell(row, 7)) or '').strip(),
        normalize_logs_lnk_date(_cell(row, 14)) or '',
    )
    return '\u241e'.join(parts)


def _aks_app_row_id_hash_key(row, mode='rt'):
    """Хеш-ключ для app_row_id: разные префиксы для RT/VT + SHA-256(прообраз)."""
    digest = hashlib.sha256(_aks_row_hash_preimage(row, mode).encode('utf-8')).hexdigest()[:32]
    m = str(mode).lower()
    if m == 'vt':
        return f'aks_vt_{digest}'
    return f'aks_rt_{digest}'


def _status_rk_from_excel_32_51(row):
    """Кол. 32 — статус РК; если пусто, а кол. 51 заполнено — считаем статус «Заявлен»."""
    s32 = _as_text(_cell(row, 32))
    if s32:
        return s32
    if _as_text(_cell(row, 51)):
        return STATUS_RK_ZAYAVLEN_AKS
    return None


def _ensure_columns(cursor, table, colnames):
    cursor.execute(f'PRAGMA table_info("{table}")')
    existing = {r[1] for r in cursor.fetchall()}
    for col in colnames:
        if col not in existing:
            cursor.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" TEXT')


def _row_to_logs_dict(row, mode='rt'):
    """Сопоставление: номер столбца Excel (1-based) → поле logs_lnk (имена как в БД проекта)."""
    c2 = _cell(row, 2)
    rk = _cell(row, 29)
    z28 = _cell(row, 28)
    is_vt = str(mode).lower() == 'vt'
    return {
        'app_row_id': _aks_app_row_id_hash_key(row, mode),
        '№_заявки': _as_text(z28),
        'Дата_заявки': normalize_logs_lnk_date(_cell(row, 41)) if is_vt else normalize_logs_lnk_date(_cell(row, 27)),
        'Титул': _format_titul_from_drawing_col2(c2),
        'Зона': _as_text(_cell(row, 1)),
        'Линия': _as_text(_cell(row, 6)),
        'проц_контроля': _as_text(_cell(row, 15)),
        'Чертеж': _as_text(c2),
        'Лист': _as_text(_cell(row, 4)),
        'Номер_стыка': _as_text(_cell(row, 7)),
        'Категория': _as_text(_cell(row, 20)),
        'Вид_сварного_соединения': _as_text(_cell(row, 18)),
        'Диаметр_1': _as_text(_cell(row, 8)),
        'Толщина_1': _as_text(_cell(row, 9)),
        'Диаметр_2': _as_text(_cell(row, 10)),
        'Толщина_2': _as_text(_cell(row, 11)),
        'Дата_сварки': normalize_logs_lnk_date(_cell(row, 14)),
        'Клейма_сварщиков': _combine_welder_marks_col16_17(_cell(row, 16), _cell(row, 17)),
        'Клеймо_бригады': _as_text(_cell(row, 35)),
        'ВИК': _as_text(_cell(row, 29)) if is_vt else None,
        'Статус_ВИК': _as_text(_cell(row, 32)) if is_vt else None,
        'Дата_ВИК': normalize_logs_lnk_date(_cell(row, 31)) if is_vt else None,
        'Дата_контроля_ВИК': normalize_logs_lnk_date(_cell(row, 31)) if is_vt else None,
        'РК': _as_text(rk) if not is_vt else None,
        'Статус_РК': _status_rk_from_excel_32_51(row) if not is_vt else None,
        'Дата_РК': normalize_logs_lnk_date(_cell(row, 48)) if not is_vt else None,
        'Дата_контроля_РК': normalize_logs_lnk_date(_cell(row, 48)) if not is_vt else None,
        'Дата_создания_РК': normalize_logs_lnk_date(_cell(row, 48)) if not is_vt else None,
        'Количество_экспозиций': _as_text(_cell(row, 36)),
        # В проекте везде используется опечатка Заявленны_ (две «н») — сохраняем совместимость
        'Заявленны_виды_контроля': _as_text(_cell(row, 21)),
        'Примечания_заключений': _as_text(_cell(row, 33)),
        '_Номер_заключения_ВИК': None,
        '_Номер_заключения_РК': _digits_only(rk),
        'Дата_загрузки': datetime.now().strftime('%Y-%m-%d'),
        'Источник': LOGS_LNK_SOURCE_AKS,
    }


def load_nk_aks_into_logs_lnk(db_path=None):
    db_path = db_path or _resolve_db_path()
    folder = _resolve_nk_aks_folder()
    default_hint = _default_nk_aks_folder_hint()
    if (os.environ.get('NK_AKS_FOLDER') or '').strip():
        print(f'[INFO] АКС: каталог из NK_AKS_FOLDER → {folder}')
    else:
        print(f'[INFO] АКС: каталог по умолчанию (nk_aks) → {folder}')

    if not folder or not os.path.isdir(folder):
        print(
            f'[ERR] Каталог журнала АКС не найден: {folder!r}\n'
            f'      Ожидаемый путь по структуре проекта: {default_hint!r}\n'
            f'      Создайте папку и поместите файл Excel (лист LOG; в имени — LOG, М-КРАН, RT, ТТ '
            f'или запасной вариант LOG…RT), либо задайте NK_AKS_FOLDER.'
        )
        return False

    files = _find_aks_source_files(folder)
    if not files.get('rt') and not files.get('vt'):
        try:
            names = os.listdir(folder)
        except OSError:
            names = []
        print(
            f'[ERR] В каталоге нет подходящего файла ({", ".join(SOURCE_EXTENSIONS)}).\n'
            f'      Папка: {folder}\n'
            f'      Файлов в каталоге: {len(names)}. '
            f'Ищем 2 файла: RT ({FILE_MARKERS_RT}) и VT ({FILE_MARKERS_VT}); '
            f'для RT есть fallback LOG+RT.\n'
            f'      Содержимое: {names[:20]}{"…" if len(names) > 20 else ""}'
        )
        return False

    datasets = []
    for mode in ('rt', 'vt'):
        p = files.get(mode)
        if not p:
            print(f'[WARN] Файл AKS {mode.upper()} не найден — пропуск этого источника.')
            continue
        print(f'[OK] Файл {mode.upper()}: {p}')
        df, sheet_label = _read_aks_dataframe(p)
        if df is None or df.empty:
            print(f'[WARN] Нет строк данных в файле {mode.upper()} после строк заголовков — пропуск.')
            continue
        need_cols = 51
        ncol = df.shape[1]
        print(f'[INFO] {mode.upper()} «{sheet_label}», столбцов: {ncol}, строк данных: {len(df)}')
        if ncol < need_cols:
            print(
                f'[WARN] {mode.upper()}: столбцов {ncol} < {need_cols} — часть полей будет пустой.'
            )
        if len(df):
            r0 = df.iloc[0]
            print(
                f'[INFO] Образец {mode.upper()}: col2(Чертеж)={_as_text(_cell(r0, 2))!r}, '
                f'col7(Стык)={_as_text(_cell(r0, 7))!r}'
            )
        datasets.append((mode, df))

    if not datasets:
        print('[ERR] Не удалось прочитать ни RT, ни VT файл АКС.')
        return False

    conn = _create_sqlite_connection(db_path)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='logs_lnk'")
    if not cur.fetchone():
        print('[ERR] Нет таблицы logs_lnk. Сначала load_lnk_data.py')
        conn.close()
        return False

    max_cols = max(df.shape[1] for _, df in datasets)
    probe = pd.Series([None] * max(55, max_cols or 55))
    _ensure_columns(cur, 'logs_lnk', list(_row_to_logs_dict(probe, 'rt').keys()))
    conn.commit()

    cur.execute('PRAGMA table_info(logs_lnk)')
    allowed = {r[1] for r in cur.fetchall()}

    # Иначе каждый запуск делает INSERT всех строк файла → дубликаты в logs_lnk
    if 'Источник' in allowed:
        cur.execute(
            'DELETE FROM logs_lnk WHERE "Источник" = ?',
            (LOGS_LNK_SOURCE_AKS,),
        )
        print(
            f'[INFO] Удалено строк с Источник={LOGS_LNK_SOURCE_AKS!r} '
            f'(перед загрузкой из файла): {cur.rowcount}'
        )
    else:
        print(
            '[WARN] В logs_lnk нет столбца «Источник» — пропуск очистки АКС; '
            'возможны дубликаты при повторном запуске.'
        )

    inserted = 0
    skipped = 0
    first_err = None
    for mode, df in datasets:
        for _, row in df.iterrows():
            d = _row_to_logs_dict(row, mode)
            if not d.get('Чертеж') and not d.get('Номер_стыка'):
                skipped += 1
                continue

            cols = [c for c, v in d.items() if c in allowed and v is not None]
            vals = [d[c] for c in cols]
            if not cols:
                skipped += 1
                continue
            qcols = ', '.join(f'"{c}"' for c in cols)
            ph = ', '.join('?' for _ in cols)
            try:
                insert_done = False
                for attempt in range(1, SQLITE_RETRY_ATTEMPTS + 1):
                    try:
                        cur.execute(f'INSERT INTO logs_lnk ({qcols}) VALUES ({ph})', vals)
                        insert_done = True
                        break
                    except sqlite3.OperationalError as e:
                        if not _is_sqlite_locked_error(e) or attempt == SQLITE_RETRY_ATTEMPTS:
                            raise
                        print(
                            f'[WARN] SQLite занята при вставке {mode.upper()} '
                            f'(попытка {attempt}/{SQLITE_RETRY_ATTEMPTS}), '
                            f'повтор через {SQLITE_RETRY_DELAY_SEC:.1f}с...'
                        )
                        time.sleep(SQLITE_RETRY_DELAY_SEC)
                if insert_done:
                    inserted += 1
            except sqlite3.Error as e:
                if first_err is None:
                    first_err = str(e)
                print(f'[ERR] SQLite при вставке ({mode.upper()}): {e}')
                break
        if first_err:
            break

    conn.commit()
    conn.close()
    print(f'[OK] Добавлено строк: {inserted}, пропущено пустых: {skipped}')
    total_rows = sum(len(df) for _, df in datasets)
    if inserted == 0 and total_rows > 0 and skipped == total_rows:
        print(
            '[ERR] Все строки пропущены: нет значений в колонках 2 (Чертеж) и 7 (Номер_стыка). '
            'Сверьте номера столбцов в Excel или строку, с которой начинаются данные.'
        )
        return False
    if first_err and inserted == 0:
        return False
    return inserted > 0


def main():
    return load_nk_aks_into_logs_lnk()


def run_script():
    load_nk_aks_into_logs_lnk()


if __name__ == '__main__':
    sys.exit(0 if main() else 1)
