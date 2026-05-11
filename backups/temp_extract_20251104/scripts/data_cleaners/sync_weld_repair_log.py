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

def get_db_connection():
    """Создает соединение с базой данных с обработкой блокировок"""
    # Получаем корневую директорию проекта (на 3 уровня выше от текущего скрипта)
    project_root = Path(__file__).parent.parent.parent
    db_path = project_root / 'database' / 'BD_Kingisepp' / 'M_Kran_Kingesepp.db'
    
    print(f"Путь к БД: {db_path}")
    print(f"БД существует: {db_path.exists()}")
    
    # Настройки для предотвращения блокировок
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
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e):
                    retry_count += 1
                    print(f"ВНИМАНИЕ: База данных заблокирована, попытка {retry_count}/{max_retries}")
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
        cursor.execute('SELECT COUNT(*) FROM logs_lnk WHERE "Статус_РК" LIKE "%Не годен%"')
        rk_defects = cursor.fetchone()[0]
        print(f"РК дефекты (Ремонт/Вырез): {rk_defects}")
        
        # РК Н/П (неофициальный ремонт или вырез)
        cursor.execute('SELECT COUNT(*) FROM logs_lnk WHERE "Статус_РК" = "Н/П"')
        rk_np = cursor.fetchone()[0]
        print(f"РК Н/П (неофициальный): {rk_np}")
        
        # Всего негодных по статистике
        total_defects_stats = rk_defects + rk_np
        print(f"ВСЕГО НЕГОДНЫХ (по статистике): {total_defects_stats}")
        
        # 2. Проверяем количество записей в weld_repair_log
        print("\nЗАПИСИ В WELD_REPAIR_LOG:")
        print("-" * 40)
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='weld_repair_log'")
        table_exists = cursor.fetchone()
        
        if not table_exists:
            print("ОШИБКА: Таблица weld_repair_log не существует")
            return
        
        cursor.execute("SELECT COUNT(*) FROM weld_repair_log")
        weld_repair_count = cursor.fetchone()[0]
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
        weld_repair_ids = [row[0] for row in cursor.fetchall()]
        
        # 5. Проверяем статусы этих записей в logs_lnk
        if weld_repair_ids:
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
            cursor.execute('''
                SELECT app_row_id, "Статус_РК", "Статус_ВИК", "Чертеж", "Номер_стыка", 
                       "Линия", "Диаметр_1", "Толщина_1", "Дата_сварки", 
                       "Примечания_заключений", "Заявленны_виды_контроля"
                FROM logs_lnk 
                WHERE ("Статус_РК" LIKE "%Не годен%" OR "Статус_РК" = "Н/П")
            ''')
            
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
                    placeholders = ','.join(['?' for _ in delete_ids])
                    
                    try:
                        cursor.execute(f"DELETE FROM weld_repair_log WHERE app_row_id IN ({placeholders})", delete_ids)
                        deleted_count = cursor.rowcount
                        print(f"УДАЛЕНО: Неправильных записей: {deleted_count}")
                    except sqlite3.OperationalError as e:
                        if "database is locked" in str(e):
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
                            cursor.execute("PRAGMA table_info(wl_china)")
                            wl_china_columns = [row[1] for row in cursor.fetchall()]
                            if 'Номер_чертежа' in wl_china_columns and 'Базовый_материал_1' in wl_china_columns and '_Номер_сварного_шва_без_S_F_' in wl_china_columns:
                                cursor.execute(
                                    '''SELECT "Базовый_материал_1" FROM wl_china 
                                       WHERE "Номер_чертежа" = ? AND "_Номер_сварного_шва_без_S_F_" = ? LIMIT 1''',
                                    (chertezh, nomer_styka)
                                )
                                row = cursor.fetchone()
                                if row and row[0] is not None:
                                    marka_stали = row[0]
                                else:
                                    cursor.execute(
                                        '''SELECT "Базовый_материал_1" FROM wl_china 
                                           WHERE "Номер_чертежа" = ? AND CAST("_Номер_сварного_шва_без_S_F_" AS TEXT) = CAST(? AS TEXT) LIMIT 1''',
                                        (chertezh, nomer_styka)
                                    )
                                    row = cursor.fetchone()
                                    if row and row[0] is not None:
                                        marka_stали = row[0]
                        except Exception:
                            pass

                        # Вставляем запись
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
                        except sqlite3.IntegrityError as e:
                            print(f"ВНИМАНИЕ: Ошибка при добавлении записи {app_row_id}: {e}")
                        except Exception as e:
                            print(f"ОШИБКА: Ошибка при добавлении записи {app_row_id}: {e}")
                    
                    print(f"ДОБАВЛЕНО: Недостающих записей: {added_count}")
                
                # Сохраняем изменения
                try:
                    conn.commit()
                    print("OK: Изменения успешно сохранены")
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e):
                        print("ОШИБКА: База данных заблокирована при сохранении изменений")
                        conn.rollback()
                        return
                    else:
                        raise e
                
                # 8. Проверяем результат
                cursor.execute("SELECT COUNT(*) FROM weld_repair_log")
                final_count = cursor.fetchone()[0]
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