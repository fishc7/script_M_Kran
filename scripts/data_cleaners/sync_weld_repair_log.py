#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Синхронизация weld_repair_log с основной статистикой
Проверяет и исправляет расхождения между количеством записей
"""

import sqlite3
import os
from pathlib import Path
from datetime import datetime

# Проверяем, нужно ли использовать PostgreSQL
USE_POSTGRESQL = os.environ.get('USE_POSTGRESQL', 'false').lower() == 'true'

if USE_POSTGRESQL:
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
        try:
            from config import DB_CONFIG
        except ImportError:
            # Используем переменные окружения
            DB_CONFIG = {
                'host': os.environ.get('PG_HOST', 'localhost'),
                'port': int(os.environ.get('PG_PORT', '5432')),
                'database': os.environ.get('PG_DATABASE', 'Test_OGS'),
                'user': os.environ.get('PG_USER', 'postgres'),
                'password': os.environ.get('PG_PASSWORD', 'Fishc1979')
            }
    except ImportError:
        print("WARNING: psycopg2 не установлен, используем SQLite")
        USE_POSTGRESQL = False

def get_value(row, index_or_key):
    """Получает значение из результата запроса (совместимость SQLite/PostgreSQL)"""
    if USE_POSTGRESQL:
        # В PostgreSQL с RealDictCursor row - это словарь
        if isinstance(index_or_key, str):
            return row.get(index_or_key)
        else:
            # Если передан индекс, берем значение по порядку
            values = list(row.values())
            return values[index_or_key] if index_or_key < len(values) else None
    else:
        # В SQLite row - это Row объект, доступ по индексу
        return row[index_or_key]

def get_db_connection():
    """Создает соединение с базой данных (PostgreSQL или SQLite)"""
    if USE_POSTGRESQL:
        # Подключение к PostgreSQL
        print(f"Подключение к PostgreSQL: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            connect_timeout=10
        )
        # Создаем курсор с RealDictCursor для совместимости с sqlite3.Row
        conn.cursor_factory = RealDictCursor
        return conn
    else:
        # Подключение к SQLite (старый код)
        project_root = Path(__file__).parent.parent.parent
        db_path = project_root / 'database' / 'BD_Kingisepp' / 'M_Kran_Kingesepp.db'
        
        print(f"Путь к БД: {db_path}")
        print(f"БД существует: {db_path.exists()}")
        
        conn = sqlite3.connect(str(db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        
        # Устанавливаем режим WAL для лучшей производительности и предотвращения блокировок
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=10000")
        conn.execute("PRAGMA temp_store=MEMORY")
        
        return conn

def sync_weld_repair_log():
    """Синхронизирует weld_repair_log с основной статистикой"""
    
    max_retries = 3
    retry_count = 0
    conn = None
    
    try:
        while retry_count < max_retries:
            try:
                conn = get_db_connection()
                cursor = conn.cursor()
                break
            except Exception as e:
                error_str = str(e).lower()
                if "database is locked" in error_str or "disk i/o error" in error_str or "could not connect" in error_str:
                    retry_count += 1
                    print(f"ВНИМАНИЕ: Проблема с подключением к БД, попытка {retry_count}/{max_retries}: {e}")
                    if retry_count < max_retries:
                        import time
                        time.sleep(2)  # Ждем 2 секунды перед повторной попыткой
                    else:
                        print("ОШИБКА: Не удалось подключиться к базе данных после нескольких попыток")
                        return
                else:
                    raise e
        
        print("СИНХРОНИЗАЦИЯ WELD_REPAIR_LOG С ОСНОВНОЙ СТАТИСТИКОЙ")
        print("=" * 80)
        print(f"Время проверки: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 1. Получаем статистику из logs_lnk (как в функции get_logs_lnk_stats)
        print("\nСТАТИСТИКА ИЗ LOGS_LNK:")
        print("-" * 40)
        
        # РК дефекты: Ремонт, Вырез, Вырезать
        if USE_POSTGRESQL:
            cursor.execute('SELECT COUNT(*) as count FROM logs_lnk WHERE "Статус_РК" LIKE \'%Не годен%\'')
        else:
            cursor.execute('SELECT COUNT(*) FROM logs_lnk WHERE "Статус_РК" LIKE "%Не годен%"')
        rk_defects = get_value(cursor.fetchone(), 'count' if USE_POSTGRESQL else 0)
        print(f"РК дефекты (Ремонт/Вырез): {rk_defects}")
        
        # РК Н/П (неофициальный ремонт или вырез)
        if USE_POSTGRESQL:
            cursor.execute('SELECT COUNT(*) as count FROM logs_lnk WHERE "Статус_РК" = %s', ('Н/П',))
        else:
            cursor.execute('SELECT COUNT(*) FROM logs_lnk WHERE "Статус_РК" = ?', ('Н/П',))
        rk_np = get_value(cursor.fetchone(), 'count' if USE_POSTGRESQL else 0)
        print(f"РК Н/П (неофициальный): {rk_np}")
        
        # Всего негодных по статистике
        total_defects_stats = rk_defects + rk_np
        print(f"ВСЕГО НЕГОДНЫХ (по статистике): {total_defects_stats}")
        
        # 2. Проверяем количество записей в weld_repair_log
        print("\nЗАПИСИ В WELD_REPAIR_LOG:")
        print("-" * 40)
        
        # Проверяем существование таблицы
        if USE_POSTGRESQL:
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = %s
            """, ('weld_repair_log',))
        else:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='weld_repair_log'")
        table_exists = cursor.fetchone()
        
        if not table_exists:
            print("ОШИБКА: Таблица weld_repair_log не существует")
            return
        
        if USE_POSTGRESQL:
            cursor.execute("SELECT COUNT(*) as count FROM weld_repair_log")
        else:
            cursor.execute("SELECT COUNT(*) FROM weld_repair_log")
        weld_repair_count = get_value(cursor.fetchone(), 'count' if USE_POSTGRESQL else 0)
        print(f"Всего записей в weld_repair_log: {weld_repair_count}")
        
        # 3. Анализируем расхождение
        print("\nАНАЛИЗ РАСХОЖДЕНИЯ:")
        print("-" * 40)
        
        if weld_repair_count == total_defects_stats:
            print("OK: Количество записей корректное - синхронизация не требуется")
            return
        
        print(f"ОШИБКА: ОБНАРУЖЕНО РАСХОЖДЕНИЕ: weld_repair_log ({weld_repair_count}) vs статистика ({total_defects_stats})")
        
        # 4. Получаем все записи из weld_repair_log для анализа
        cursor.execute("SELECT app_row_id FROM weld_repair_log")
        if USE_POSTGRESQL:
            weld_repair_ids = [row['app_row_id'] for row in cursor.fetchall()]
        else:
            weld_repair_ids = [get_value(row, 0) for row in cursor.fetchall()]
        
        # 5. Проверяем статусы этих записей в logs_lnk
        if weld_repair_ids:
            if USE_POSTGRESQL:
                placeholders = ','.join(['%s' for _ in weld_repair_ids])
            else:
                placeholders = ','.join(['?' for _ in weld_repair_ids])
            cursor.execute(f'''
                SELECT app_row_id, "Статус_РК", "Статус_ВИК", "Чертеж", "Номер_стыка"
                FROM logs_lnk 
                WHERE app_row_id IN ({placeholders})
            ''', weld_repair_ids)
            
            records_in_logs = cursor.fetchall()
            print(f"Найдено {len(records_in_logs)} записей в logs_lnk из {len(weld_repair_ids)} в weld_repair_log")
            
            # Определяем записи для удаления (неправильные)
            records_to_delete = []
            valid_records = []
            
            for record in records_in_logs:
                status_rk = record['Статус_РК'] or ''
                status_vik = record['Статус_ВИК'] or ''
                
                # Проверяем, является ли запись НЕГОДНОЙ по критериям статистики
                is_defect = (
                    'емонт' in status_rk or 'ырез' in status_rk or 
                    status_rk == 'Н/П'
                )
                
                if is_defect:
                    valid_records.append(record['app_row_id'])
                else:
                    records_to_delete.append({
                        'app_row_id': record['app_row_id'],
                        'status_rk': status_rk,
                        'status_vik': status_vik,
                        'drawing': record['Чертеж'],
                        'joint': record['Номер_стыка']
                    })
            
            print(f"OK: Правильных записей: {len(valid_records)}")
            print(f"ОШИБКА: Записей для удаления: {len(records_to_delete)}")
            
            # 6. Получаем все негодные записи из logs_lnk
            if USE_POSTGRESQL:
                cursor.execute('''
                    SELECT app_row_id, "Статус_РК", "Статус_ВИК", "Чертеж", "Номер_стыка", 
                           "Линия", "Диаметр_1", "Толщина_1", "Дата_сварки", 
                           "Примечания_заключений", "Заявленны_виды_контроля"
                    FROM logs_lnk 
                    WHERE ("Статус_РК" LIKE \'%Не годен%\' OR "Статус_РК" = %s)
                ''', ('Н/П',))
            else:
                cursor.execute('''
                    SELECT app_row_id, "Статус_РК", "Статус_ВИК", "Чертеж", "Номер_стыка", 
                           "Линия", "Диаметр_1", "Толщина_1", "Дата_сварки", 
                           "Примечания_заключений", "Заявленны_виды_контроля"
                    FROM logs_lnk 
                    WHERE ("Статус_РК" LIKE "%Не годен%" OR "Статус_РК" = ?)
                ''', ('Н/П',))
            
            all_defect_records = cursor.fetchall()
            
            # Находим недостающие записи
            missing_records = []
            for record in all_defect_records:
                if record['app_row_id'] not in valid_records:
                    missing_records.append(record)
            
            print(f"ДОБАВИТЬ: Записей для добавления: {len(missing_records)}")
            
            # 7. Выполняем синхронизацию
            if records_to_delete or missing_records:
                print(f"\nВЫПОЛНЯЕМ СИНХРОНИЗАЦИЮ:")
                print("-" * 40)
                
                # Удаляем неправильные записи
                if records_to_delete:
                    delete_ids = [record['app_row_id'] for record in records_to_delete]
                    if USE_POSTGRESQL:
                        placeholders = ','.join(['%s' for _ in delete_ids])
                    else:
                        placeholders = ','.join(['?' for _ in delete_ids])
                    
                    try:
                        cursor.execute(f"DELETE FROM weld_repair_log WHERE app_row_id IN ({placeholders})", delete_ids)
                        deleted_count = cursor.rowcount
                        print(f"УДАЛЕНО: Неправильных записей: {deleted_count}")
                        conn.commit()
                    except Exception as e:
                        error_str = str(e).lower()
                        if "database is locked" in error_str or "disk i/o error" in error_str:
                            print("ОШИБКА: База данных заблокирована при удалении записей")
                            conn.rollback()
                            return
                        else:
                            raise e
                
                # Добавляем недостающие записи
                if missing_records:
                    added_count = 0
                    
                    for record in missing_records:
                        # Подготавливаем данные для вставки
                        app_row_id = record['app_row_id']
                        chertezh = record['Чертеж'] or ''
                        liniya = record['Линия'] or ''
                        
                        # Объединяем диаметр и толщину
                        diametr = record['Диаметр_1'] or ''
                        tolshchina = record['Толщина_1'] or ''
                        diametr_tolshchina = f"{diametr}х{tolshchina}" if diametr and tolshchina else (diametr or tolshchina or '')
                        
                        nomer_styka = record['Номер_стыка'] or ''
                        data_svarki = record['Дата_сварки'] or ''
                        try:
                            razmer_vyborki = record['Примечания_заключений'] or ''
                        except KeyError:
                            razmer_vyborki = ''
                        
                        try:
                            sposob_kontrolya = record['Заявленны_виды_контроля'] or ''
                        except KeyError:
                            sposob_kontrolya = ''
                        
                        # Получаем Марка стали из wl_china.Базовый_материал_1 по ключам
                        marka_stали = ''
                        try:
                            if USE_POSTGRESQL:
                                cursor.execute("""
                                    SELECT column_name 
                                    FROM information_schema.columns 
                                    WHERE table_schema = 'public' AND table_name = 'wl_china'
                                """)
                                wl_china_columns = [row['column_name'] for row in cursor.fetchall()]
                            else:
                                cursor.execute("PRAGMA table_info(wl_china)")
                                wl_china_columns = [get_value(row, 1) for row in cursor.fetchall()]
                            
                            if 'Номер_чертежа' in wl_china_columns and 'Базовый_материал_1' in wl_china_columns and '_Номер_сварного_шва_без_S_F_' in wl_china_columns:
                                if USE_POSTGRESQL:
                                    cursor.execute(
                                        '''SELECT "Базовый_материал_1" FROM wl_china 
                                           WHERE "Номер_чертежа" = %s AND "_Номер_сварного_шва_без_S_F_" = %s LIMIT 1''',
                                        (chertezh, nomer_styka)
                                    )
                                else:
                                    cursor.execute(
                                        '''SELECT "Базовый_материал_1" FROM wl_china 
                                           WHERE "Номер_чертежа" = ? AND "_Номер_сварного_шва_без_S_F_" = ? LIMIT 1''',
                                        (chertezh, nomer_styka)
                                    )
                                row = cursor.fetchone()
                                if row:
                                    marka_stали = get_value(row, 'Базовый_материал_1' if USE_POSTGRESQL else 0) or ''
                                    if not marka_stали and not USE_POSTGRESQL:
                                        cursor.execute(
                                            '''SELECT "Базовый_материал_1" FROM wl_china 
                                               WHERE "Номер_чертежа" = ? AND CAST("_Номер_сварного_шва_без_S_F_" AS TEXT) = CAST(? AS TEXT) LIMIT 1''',
                                            (chertezh, nomer_styka)
                                        )
                                        row = cursor.fetchone()
                                        if row:
                                            marka_stали = get_value(row, 0) or ''
                        except Exception:
                            pass

                        # Вставляем запись
                        if USE_POSTGRESQL:
                            insert_query = '''
                                INSERT INTO weld_repair_log 
                                (app_row_id, "Чертеж", "Линия", "Диаметр и толщина стенки", "№ стыка", "Дата сварки", "Размер выборки (длина, ширина, глубина), мм", "Способ и результаты контроля выборки", "Марка стали")
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            '''
                        else:
                            insert_query = '''
                                INSERT INTO weld_repair_log 
                                (app_row_id, "Чертеж", "Линия", "Диаметр и толщина стенки", "№ стыка", "Дата сварки", "Размер выборки (длина, ширина, глубина), мм", "Способ и результаты контроля выборки", "Марка стали")
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            '''
                        
                        try:
                            cursor.execute(insert_query, (
                                app_row_id,
                                chertezh,
                                liniya, 
                                diametr_tolshchina,
                                nomer_styka,
                                data_svarki,
                                razmer_vyborki,
                                sposob_kontrolya,
                                marka_stали
                            ))
                            added_count += 1
                        except Exception as e:
                            if USE_POSTGRESQL:
                                import psycopg2
                                if isinstance(e, psycopg2.IntegrityError):
                                    # Игнорируем дубликаты
                                    pass
                                else:
                                    print(f"ОШИБКА: Ошибка при добавлении записи {app_row_id}: {e}")
                                    raise
                            else:
                                if isinstance(e, sqlite3.IntegrityError):
                                    # Игнорируем дубликаты
                                    pass
                                else:
                                    print(f"ОШИБКА: Ошибка при добавлении записи {app_row_id}: {e}")
                                    raise
                    
                    print(f"ДОБАВЛЕНО: Недостающих записей: {added_count}")
                
                # Сохраняем изменения
                try:
                    conn.commit()
                    print("OK: Изменения успешно сохранены")
                except Exception as e:
                    error_str = str(e).lower()
                    if "database is locked" in error_str or "disk i/o error" in error_str:
                        print("ОШИБКА: База данных заблокирована при сохранении изменений")
                        conn.rollback()
                        return
                    else:
                        raise e
                
                # 8. Проверяем результат
                if USE_POSTGRESQL:
                    cursor.execute("SELECT COUNT(*) as count FROM weld_repair_log")
                else:
                    cursor.execute("SELECT COUNT(*) FROM weld_repair_log")
                final_count = get_value(cursor.fetchone(), 'count' if USE_POSTGRESQL else 0)
                print(f"\nИТОГОВЫЙ РЕЗУЛЬТАТ:")
                print(f"Записей в weld_repair_log: {final_count}")
                print(f"Ожидаемое количество: {total_defects_stats}")
                
                if final_count == total_defects_stats:
                    print("OK: СИНХРОНИЗАЦИЯ УСПЕШНО ЗАВЕРШЕНА!")
                else:
                    print(f"ВНИМАНИЕ: Все еще есть расхождение: {final_count} vs {total_defects_stats}")
            else:
                print("OK: Синхронизация не требуется")
    
    finally:
        if conn:
            try:
                conn.close()
                print("OK: Соединение с базой данных закрыто")
            except:
                pass  # Игнорируем ошибки при закрытии соединения
    
    print("\n" + "=" * 80)
    print("OK: СИНХРОНИЗАЦИЯ ЗАВЕРШЕНА")

def run_script():
    """Функция для запуска скрипта через веб-интерфейс"""
    print("DEBUG: Функция run_script() вызвана")
    print("DEBUG: Запускаем sync_weld_repair_log()")
    sync_weld_repair_log()
    print("DEBUG: sync_weld_repair_log() завершена")

if __name__ == "__main__":
    sync_weld_repair_log()