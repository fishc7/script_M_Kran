# -*- coding: utf-8 -*-
"""
Полное обновление таблицы logs_lnk: по очереди
  1) load_lnk_data — журнал НГС (источник NGS);
  2) load_lnk_nk_aks — догрузка из LOG_М-КРАН_RT_ТТ (источник AKS).

Запуск из веб-интерфейса (кнопка «Полное обновление журнала») или: python run_full_logs_lnk_update.py
Если шаг 2 (АКС) не находит каталог/файл — в логе скрипта будет [ERR]; НГС при этом уже обновлён.
Переопределение папки АКС: переменная окружения NK_AKS_FOLDER (см. load_lnk_nk_aks.py).
"""

import os
import sqlite3
import sys
import builtins

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

try:
    from .load_lnk_data import load_data, _resolve_db_path
    from .load_lnk_nk_aks import load_nk_aks_into_logs_lnk
    from .utilities.logs_lnk_etl_lock import LogsLnkEtlLock
except ImportError:
    from load_lnk_data import load_data, _resolve_db_path
    from load_lnk_nk_aks import load_nk_aks_into_logs_lnk
    _util_dir = os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'utilities')
    )
    if _util_dir not in sys.path:
        sys.path.insert(0, _util_dir)
    from logs_lnk_etl_lock import LogsLnkEtlLock


def _print_logs_lnk_row_summary(db_path: str) -> None:
    """В лог — сколько строк по каждому Источнику (NGS / AKS / ЖУРНАЛ_ОПОР / …)."""
    try:
        conn = sqlite3.connect(db_path, timeout=60)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='logs_lnk'")
        if not cur.fetchone():
            conn.close()
            return
        cur.execute(
            'SELECT COALESCE(TRIM("Источник"), "(пусто)"), COUNT(*) '
            'FROM logs_lnk GROUP BY 1 ORDER BY 2 DESC'
        )
        parts = cur.fetchall()
        cur.execute('SELECT COUNT(*) FROM logs_lnk')
        total = cur.fetchone()[0]
        conn.close()
    except Exception as e:
        print(f'[WARN] Сводка logs_lnk: {e}')
        return
    print('')
    print('📊 Сводка logs_lnk по столбцу «Источник» (итог полного обновления):')
    for src, n in parts:
        print(f'   {src}: {n}')
    print(f'   ─── Всего строк: {total}')


def run_full_logs_lnk_update():
    """Один замок на оба шага: иначе второй процесс может начать load_data во время первого."""
    db_path = _resolve_db_path()
    if not db_path:
        print('[ERR] Не удалось определить путь к БД (полное обновление журнала).')
        return False
    ok = False
    with LogsLnkEtlLock(db_path):
        print('=' * 60)
        print('ШАГ 1/2: Журнал НГС (load_lnk_data.py)')
        print('=' * 60)
        load_data(use_etl_lock=False)
        print('=' * 60)
        print('ШАГ 2/2: Догрузка АКС (load_lnk_nk_aks.py)')
        print('=' * 60)
        ok = load_nk_aks_into_logs_lnk()
        if not ok:
            print('[WARN] Догрузка АКС не выполнена (нет файла или ошибка). Данные НГС уже обновлены.')
        print('=' * 60)
        print('Полное обновление журнала logs_lnk завершено')
        print('=' * 60)
        _print_logs_lnk_row_summary(db_path)
    return ok


def run_script():
    """Точка входа для веб ScriptRunner."""
    run_full_logs_lnk_update()


def main():
    ok = run_full_logs_lnk_update()
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
