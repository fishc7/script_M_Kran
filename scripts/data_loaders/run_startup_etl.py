# -*- coding: utf-8 -*-
"""
Одно обновление данных при запуске, в том порядке, что раньше нажимали вручную.

1. Журнал НК НГС
2. Журнал НК АКС
3. Данные китайских подрядчиков WELDLOG
4. Очистка префиксов S/F в logs_lnk, столбец Номер_стыка
5. Очистка префиксов S/F в wl_china, столбец Номер_сварного_шва
6. Синхронизация pipeline ↔ wl_china
7. Очистка журнала ремонта сварных швов
8. Синхронизация журнала ремонта швов
9. Обновление wl_china из основнаяНК
10. Последнее состояние сварных швов
11. Таблица «сварено сварщиком»

Если шаг не выполнился, следующие не запускаются.
Очистка префиксов пишет в столбец _Номер_сварного_шва_без_S_F_ режимом «только очистка».
"""

import importlib.util
import os
import sqlite3
import sys
import traceback

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.normpath(os.path.join(_THIS_DIR, '..', '..'))
_CLEAN_COLUMN = '_Номер_сварного_шва_без_S_F_'


def _import_file(alias, path):
    spec = importlib.util.spec_from_file_location(alias, path)
    if spec is None or spec.loader is None:
        raise ImportError(f'Не удалось загрузить {path}')
    module = importlib.util.module_from_spec(spec)
    sys.modules[alias] = module
    spec.loader.exec_module(module)
    return module


def _failed(result):
    # bool — подкласс int: True нельзя считать ненулевым кодом ошибки.
    if isinstance(result, bool):
        return not result
    if isinstance(result, int) and result != 0:
        return True
    return False


def _step(title, func):
    print('')
    print('=' * 60)
    print(title)
    print('=' * 60)
    try:
        result = func()
    except Exception as exc:
        print(f'[ERR] {title}: {exc}')
        traceback.print_exc()
        return False
    if _failed(result):
        print(f'[ERR] Шаг не выполнен: {title}')
        return False
    print(f'[OK] {title}')
    return True


def _clean_prefixes(db_path, table_name, source_column):
    """Тот же режим, что кнопка «Только очистка префиксов» в веб-интерфейсе."""
    web_app = os.path.join(_PROJECT_ROOT, 'web', 'app')
    if web_app not in sys.path:
        sys.path.insert(0, web_app)
    from extract_utils import clean_joint_number

    conn = sqlite3.connect(db_path, timeout=60)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        if not cursor.fetchone():
            print(f'[ERR] Таблица {table_name} не найдена')
            return False

        cursor.execute(f'PRAGMA table_info(`{table_name}`)')
        columns = [row[1] for row in cursor.fetchall()]
        if source_column not in columns:
            print(f'[ERR] Столбец {source_column} не найден в {table_name}')
            return False

        if _CLEAN_COLUMN not in columns:
            cursor.execute(
                f'ALTER TABLE `{table_name}` ADD COLUMN `{_CLEAN_COLUMN}` TEXT'
            )

        cursor.execute(
            f'SELECT rowid, `{source_column}` FROM `{table_name}` '
            f'WHERE `{source_column}` IS NOT NULL'
        )
        records = cursor.fetchall()
        if not records:
            print(f'[ERR] Нет данных в {table_name}.{source_column}')
            return False

        updated = 0
        for rowid, source_text in records:
            try:
                source_text_str = '' if source_text is None else str(source_text)
                processed = clean_joint_number(source_text_str)
                if processed is not None:
                    cursor.execute(
                        f'UPDATE `{table_name}` SET `{_CLEAN_COLUMN}` = ? WHERE rowid = ?',
                        (processed, rowid),
                    )
                    updated += 1
            except Exception:
                continue

        conn.commit()
        print(f'Обработано записей: {updated} из {len(records)}')
        return updated > 0
    finally:
        conn.close()


def run_startup_etl():
    loaders = _import_file('startup_load_lnk_data', os.path.join(_THIS_DIR, 'load_lnk_data.py'))
    db_path = loaders._resolve_db_path()
    if not db_path:
        print('[ERR] Не удалось определить путь к базе данных.')
        return False

    try:
        from .utilities.logs_lnk_etl_lock import LogsLnkEtlLock
    except ImportError:
        util_dir = os.path.normpath(os.path.join(_THIS_DIR, '..', 'utilities'))
        if util_dir not in sys.path:
            sys.path.insert(0, util_dir)
        from logs_lnk_etl_lock import LogsLnkEtlLock

    aks = _import_file('startup_load_lnk_nk_aks', os.path.join(_THIS_DIR, 'load_lnk_nk_aks.py'))

    lock = LogsLnkEtlLock(db_path)
    try:
        lock.acquire()
    except TimeoutError as exc:
        print(f'[ERR] {exc}')
        return False

    try:
        if not _step('ШАГ 1/11. Журнал НК НГС', lambda: loaders.load_data(use_etl_lock=False)):
            return False
        if not _step('ШАГ 2/11. Журнал НК АКС', aks.load_nk_aks_into_logs_lnk):
            return False

        wl_china = _import_file('startup_load_wl_china', os.path.join(_THIS_DIR, 'load_wl_china.py'))
        if not _step('ШАГ 3/11. Данные китайских подрядчиков WELDLOG', wl_china.main):
            return False

        if not _step(
            'ШАГ 4/11. Очистка префиксов S/F в журнале НК (Номер_стыка)',
            lambda: _clean_prefixes(db_path, 'logs_lnk', 'Номер_стыка'),
        ):
            return False
        if not _step(
            'ШАГ 5/11. Очистка префиксов S/F в WELDLOG (Номер_сварного_шва)',
            lambda: _clean_prefixes(db_path, 'wl_china', 'Номер_сварного_шва'),
        ):
            return False

        sync_mod = _import_file(
            'startup_sync_pipeline_wl_china',
            os.path.join(_PROJECT_ROOT, 'scripts', 'maintenance', 'sync_pipeline_wl_china.py'),
        )
        if not _step(
            'ШАГ 6/11. Синхронизация pipeline и wl_china',
            lambda: sync_mod.PipelineWLChinaSync().run_sync(dry_run=False),
        ):
            return False

        repair_clean = _import_file(
            'startup_clean_weld_repair_log',
            os.path.join(_PROJECT_ROOT, 'scripts', 'data_cleaners', 'clean_weld_repair_log.py'),
        )
        if not _step(
            'ШАГ 7/11. Очистка журнала ремонта сварных швов',
            lambda: repair_clean.clean_weld_repair_log(assume_yes=True),
        ):
            return False

        repair_sync = _import_file(
            'startup_sync_weld_repair_log',
            os.path.join(_PROJECT_ROOT, 'scripts', 'data_cleaners', 'sync_weld_repair_log.py'),
        )
        if not _step('ШАГ 8/11. Синхронизация журнала ремонта швов', repair_sync.sync_weld_repair_log):
            return False

        osnovnaya = _import_file(
            'startup_update_wl_china_from_osnovnaya_nk',
            os.path.join(_PROJECT_ROOT, 'scripts', 'data_cleaners', 'update_wl_china_from_osnovnaya_nk.py'),
        )
        if not _step('ШАГ 9/11. Обновление wl_china из основнаяНК', osnovnaya.main):
            return False

        condition = _import_file(
            'startup_create_condition_weld_table',
            os.path.join(_THIS_DIR, 'create_condition_weld_table.py'),
        )
        if not _step('ШАГ 10/11. Последнее состояние сварных швов', condition.main):
            return False

        svarenno = _import_file(
            'startup_create_svarenno_svarshchikom_table',
            os.path.join(_PROJECT_ROOT, 'scripts', 'database', 'create_svarenno_svarshchikom_table.py'),
        )

        def _build_svarenno():
            creator = svarenno.SvarennoSvarshchikomCreator()
            return creator.run_creation()

        if not _step('ШАГ 11/11. Таблица «сварено сварщиком»', _build_svarenno):
            return False

        print('')
        print('=' * 60)
        print('Обновление данных завершено')
        print('=' * 60)
        return True
    finally:
        lock.release()


def run_script():
    """Точка входа для веб-интерфейса."""
    if not run_startup_etl():
        raise RuntimeError('Обновление данных остановлено: один из шагов не выполнен')


def main():
    ok = run_startup_etl()
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
