#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт экспорта данных condition_weld в Excel
Создает подробный отчет с несколькими листами
"""

import sqlite3
import os
import sys
import pandas as pd
from datetime import datetime
import logging

# Импортируем утилиты
try:
    # Пробуем относительный импорт (для запуска через web-приложение)
    from ..utilities.db_utils import get_database_connection
    from ..utilities.path_utils import get_log_path
except ImportError:
    # Если не работает, используем абсолютный импорт (для прямого запуска)
    import sys
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    utilities_dir = os.path.join(os.path.dirname(current_dir), 'utilities')
    if utilities_dir not in sys.path:
        sys.path.insert(0, utilities_dir)
    
    try:
        from db_utils import get_database_connection
        from path_utils import get_log_path
    except ImportError:
        # Если и это не работает, используем прямой путь
        sys.path.insert(0, os.path.join(current_dir, '..', 'utilities'))
        from db_utils import get_database_connection
        from path_utils import get_log_path

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(get_log_path('export_condition_weld_to_excel'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_filename_timestamp():
    """
    Возвращает временную метку для имени файла в формате день.месяц.год_часы-минуты-секунды
    (двоеточия заменяются на дефисы для совместимости с Windows)
    """
    return datetime.now().strftime('%d.%m.%Y_%H:%M:%S').replace(':', '-')

def export_condition_weld_to_excel():
    """
    Экспортирует данные из таблицы condition_weld в Excel файл
    """
    try:
        # Подключение к базе данных
        conn = get_database_connection()
        cursor = conn.cursor()
        
        logger.info("🚀 Начинаем экспорт данных condition_weld в Excel")
        
        # Проверяем наличие таблицы
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='condition_weld'")
        if not cursor.fetchone():
            logger.error("❌ Таблица condition_weld не найдена")
            return False
        
        logger.info("✅ Таблица condition_weld найдена")
        
        # Загружаем все данные из таблицы
        logger.info("📊 Загружаем данные из таблицы condition_weld...")
        query = """
        SELECT 
            id,
            Титул,
            ISO,
            Линия,
            стык,
            Код_удаления,
            Тип_шва,
            ID_RT,
            РК,
            Статус_РК,
            Дата_Заключения_РК,
            Дата_контроля_РК,
            "Количество_RT_записей",
            ID_VT,
            ВИК,
            Статус_ВИК,
            Дата_ВИК,
            Дата_контроля_ВИК,
            "Количество_VT_записей",
            ID_WC,
            Заключение_РК_N,
            Результаты_Заключения_РК
        FROM condition_weld
        ORDER BY ISO, стык
        """
        
        df = pd.read_sql_query(query, conn)
        logger.info(f"✅ Загружено {len(df)} записей")
        
        # Создаем имя файла с датой и временем
        timestamp = get_filename_timestamp()
        filename = f"condition_weld_report_{timestamp}.xlsx"
        
        # Создаем папку для результатов, если её нет
        results_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'archive', 'results')
        os.makedirs(results_dir, exist_ok=True)
        
        filepath = os.path.join(results_dir, filename)
        logger.info(f"📊 Создаем Excel отчет: {filepath}")
        
        # Создаем Excel файл с несколькими листами
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            
            # 1. Все данные
            df.to_excel(writer, sheet_name='Все_данные', index=False)
            logger.info("✅ Лист 'Все_данные' создан")
            
            # 2. RT данные (только записи с RT)
            rt_df = df[df['ID_RT'].notna()].copy()
            if not rt_df.empty:
                rt_df.to_excel(writer, sheet_name='RT_данные', index=False)
                logger.info(f"✅ Лист 'RT_данные' создан ({len(rt_df)} записей)")
            
            # 3. VT данные (только записи с VT)
            vt_df = df[df['ID_VT'].notna()].copy()
            if not vt_df.empty:
                vt_df.to_excel(writer, sheet_name='VT_данные', index=False)
                logger.info(f"✅ Лист 'VT_данные' создан ({len(vt_df)} записей)")
            
            # 4. WL_China данные (только записи с wl_china)
            wc_df = df[df['ID_WC'].notna()].copy()
            if not wc_df.empty:
                wc_df.to_excel(writer, sheet_name='WL_China_данные', index=False)
                logger.info(f"✅ Лист 'WL_China_данные' создан ({len(wc_df)} записей)")
            
            # 5. Статистика
            stats_data = create_statistics_data(df)
            stats_df = pd.DataFrame(stats_data)
            stats_df.to_excel(writer, sheet_name='Статистика', index=False)
            logger.info("✅ Лист 'Статистика' создан")
        
        logger.info(f"✅ Excel отчет успешно создан: {filepath}")
        logger.info(f"📁 Файл сохранен: {filepath}")
        
        # Выводим краткую сводку
        logger.info("📈 Краткая сводка:")
        logger.info(f"   Всего записей: {len(df)}")
        logger.info(f"   RT записи: {len(rt_df)}")
        logger.info(f"   VT записи: {len(vt_df)}")
        logger.info(f"   WL_China записи: {len(wc_df)}")
        
        # Уникальные значения
        unique_iso = df['ISO'].nunique()
        unique_joints = df['стык'].nunique()
        logger.info(f"   Уникальных ISO: {unique_iso}")
        logger.info(f"   Уникальных стыков: {unique_joints}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка при экспорте: {e}")
        return False
    
    finally:
        if 'conn' in locals():
            conn.close()

def create_statistics_data(df):
    """
    Создает данные для листа статистики
    """
    stats = []
    
    # Общая статистика
    stats.append(['Общая статистика', ''])
    stats.append(['Всего записей', len(df)])
    stats.append(['Уникальных ISO', df['ISO'].nunique()])
    stats.append(['Уникальных стыков', df['стык'].nunique()])
    stats.append(['', ''])
    
    # RT статистика
    rt_df = df[df['ID_RT'].notna()]
    stats.append(['RT статистика', ''])
    stats.append(['Записей с RT данными', len(rt_df)])
    if not rt_df.empty:
        stats.append(['Уникальных ISO с RT', rt_df['ISO'].nunique()])
        stats.append(['Уникальных стыков с RT', rt_df['стык'].nunique()])
        
        # Статусы РК
        rk_status_counts = rt_df['Статус_РК'].value_counts()
        stats.append(['', ''])
        stats.append(['Статусы РК', 'Количество'])
        for status, count in rk_status_counts.items():
            stats.append([status, count])
    
    stats.append(['', ''])
    
    # VT статистика
    vt_df = df[df['ID_VT'].notna()]
    stats.append(['VT статистика', ''])
    stats.append(['Записей с VT данными', len(vt_df)])
    if not vt_df.empty:
        stats.append(['Уникальных ISO с VT', vt_df['ISO'].nunique()])
        stats.append(['Уникальных стыков с VT', vt_df['стык'].nunique()])
        
        # Статусы ВИК
        vik_status_counts = vt_df['Статус_ВИК'].value_counts()
        stats.append(['', ''])
        stats.append(['Статусы ВИК', 'Количество'])
        for status, count in vik_status_counts.items():
            stats.append([status, count])
    
    stats.append(['', ''])
    
    # WL_China статистика
    wc_df = df[df['ID_WC'].notna()]
    stats.append(['WL_China статистика', ''])
    stats.append(['Записей с WL_China данными', len(wc_df)])
    if not wc_df.empty:
        stats.append(['Уникальных ISO с WL_China', wc_df['ISO'].nunique()])
        stats.append(['Уникальных стыков с WL_China', wc_df['стык'].nunique()])
    
    stats.append(['', ''])
    
    # Код удаления статистика
    deletion_counts = df['Код_удаления'].value_counts()
    stats.append(['Коды удаления', 'Количество'])
    for code, count in deletion_counts.items():
        if pd.notna(code) and code != '':
            stats.append([code, count])
    
    stats.append(['', ''])
    
    # Тип шва статистика
    weld_type_counts = df['Тип_шва'].value_counts()
    stats.append(['Типы швов', 'Количество'])
    for weld_type, count in weld_type_counts.items():
        if pd.notna(weld_type) and weld_type != '':
            stats.append([weld_type, count])
    
    return stats

def run_script():
    """
    Функция для запуска скрипта через веб-интерфейс
    """
    return main()

def main():
    """
    Основная функция для запуска скрипта
    """
    logger.info("=" * 60)
    logger.info("📊 СКРИПТ ЭКСПОРТА CONDITION_WELD В EXCEL")
    logger.info("=" * 60)
    
    start_time = datetime.now()
    
    try:
        success = export_condition_weld_to_excel()
        
        if success:
            logger.info("✅ Скрипт выполнен успешно")
        else:
            logger.error("❌ Скрипт завершился с ошибками")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        return 1
    
    finally:
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"⏱️ Время выполнения: {duration}")
        logger.info("=" * 60)
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
