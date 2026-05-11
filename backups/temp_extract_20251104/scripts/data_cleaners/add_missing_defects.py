#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Добавление недостающих негодных записей в weld_repair_log
"""

import sqlite3
import os
from pathlib import Path

def get_db_connection():
    """Создает соединение с базой данных"""
    project_root = Path(__file__).parent
    db_path = project_root / 'BD_Kingisepp' / 'M_Kran_Kingesepp.db'
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def add_missing_defects():
    """Добавляет недостающие негодные записи в weld_repair_log"""
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("➕ ДОБАВЛЕНИЕ НЕДОСТАЮЩИХ НЕГОДНЫХ ЗАПИСЕЙ В WELD_REPAIR_LOG")
    print("=" * 80)
    
    # Получаем все негодные записи из logs_lnk
    cursor.execute('''
        SELECT app_row_id, "Статус_РК", "Статус_ВИК", "Чертеж", "Номер_стыка", 
               "Линия", "Диаметр_1", "Толщина_1", "Дата_сварки", 
               "Примечания_заключений", "Заявленны_виды_контроля"
        FROM logs_lnk 
        WHERE ("Статус_РК" LIKE "%Не годен%" OR "Статус_РК" = "Н/П")
    ''')
    
    all_defect_records = cursor.fetchall()
    print(f"Всего негодных записей в logs_lnk: {len(all_defect_records)}")
    
    # Получаем уже перенесенные записи
    cursor.execute("SELECT app_row_id FROM weld_repair_log")
    transferred_ids = [row[0] for row in cursor.fetchall()]
    print(f"Уже перенесено в weld_repair_log: {len(transferred_ids)}")
    
    # Находим недостающие записи
    missing_records = []
    for record in all_defect_records:
        if record['app_row_id'] not in transferred_ids:
            missing_records.append(record)
    
    print(f"Недостающих записей: {len(missing_records)}")
    
    if missing_records:
        print(f"\n📋 НЕДОСТАЮЩИЕ ЗАПИСИ:")
        for record in missing_records:
            print(f"  - app_row_id: {record['app_row_id']}, РК: '{record['Статус_РК']}', ВИК: '{record['Статус_ВИК']}', Чертеж: {record['Чертеж']}, Стык: {record['Номер_стыка']}")
        
        # Запрашиваем подтверждение
        print(f"\n⚠️ ВНИМАНИЕ: Будет добавлено {len(missing_records)} записей в weld_repair_log!")
        response = input("Продолжить добавление? (y/N): ")
        
        if response.lower() == 'y':
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
                razmer_vyborki = record['Примечания_заключений'] or ''
                sposob_kontrolya = record['Заявленны_виды_контроля'] or ''
                
                # Вставляем запись
                insert_query = '''
                    INSERT INTO weld_repair_log 
                    (app_row_id, "Чертеж", "Линия", "Диаметр и толщина стенки", "№ стыка", "Дата сварки", "Размер выборки (длина, ширина, глубина), мм", "Способ и результаты контроля выборки")
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
                        sposob_kontrolya
                    ))
                    added_count += 1
                except sqlite3.IntegrityError as e:
                    print(f"⚠️ Ошибка при добавлении записи {app_row_id}: {e}")
                except Exception as e:
                    print(f"❌ Ошибка при добавлении записи {app_row_id}: {e}")
            
            # Сохраняем изменения
            conn.commit()
            
            print(f"✅ Добавлено записей: {added_count}")
            
            # Проверяем результат
            cursor.execute("SELECT COUNT(*) FROM weld_repair_log")
            final_count = cursor.fetchone()[0]
            print(f"Итоговое количество в weld_repair_log: {final_count}")
            
            # Проверяем соответствие статистике
            cursor.execute('SELECT COUNT(*) FROM logs_lnk WHERE "Статус_РК" LIKE "%Не годен%"')
            rk_defects = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM logs_lnk WHERE "Статус_РК" = "Н/П"')
            rk_np = cursor.fetchone()[0]
            
            expected_count = rk_defects + rk_np
            print(f"Ожидаемое количество по статистике: {expected_count}")
            
            if final_count == expected_count:
                print("✅ Количество записей теперь полностью соответствует статистике!")
            else:
                print(f"⚠️ Все еще есть расхождение: {final_count} vs {expected_count}")
        else:
            print("❌ Добавление отменено")
    else:
        print("✅ Все негодные записи уже перенесены в weld_repair_log")
    
    conn.close()
    print("\n" + "=" * 80)
    print("✅ ДОБАВЛЕНИЕ ЗАВЕРШЕНО")

if __name__ == "__main__":
    add_missing_defects()






