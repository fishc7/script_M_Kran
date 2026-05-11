#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для синхронизации данных между таблицами pipeline_weld_joint_iso и wl_china

Проверяет соответствие записей по:
- ISO (pipeline_weld_joint_iso) и Номер_чертежа (wl_china)
- стык (pipeline_weld_joint_iso) и _Номер_сварного_шва_без_S_F_ (wl_china)

Если записи не найдены, вставляет недостающие данные из wl_china в pipeline_weld_joint_iso
"""

import sqlite3
import os
import sys
import logging
from datetime import datetime
import pandas as pd

# Настройка путей для импорта модулей
current_dir = os.path.dirname(os.path.abspath(__file__))
utilities_dir = os.path.join(current_dir, 'scripts', 'utilities')
project_root = os.path.dirname(current_dir)

# Добавляем пути в sys.path
for path in [current_dir, utilities_dir, project_root]:
    if path not in sys.path:
        sys.path.insert(0, path)

# Импортируем утилиты
try:
    from scripts.utilities.db_utils import get_database_connection
    from scripts.utilities.path_utils import get_log_path
except ImportError:
    # Если не работает, используем абсолютный импорт
    def get_database_connection():
        # Используем абсолютный путь для избежания проблем с кодировкой
        db_path = r"D:\МК_Кран\script_M_Kran\database\BD_Kingisepp\M_Kran_Kingesepp.db"
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"База данных не найдена: {db_path}")
        return sqlite3.connect(db_path)
    
    def get_log_path(script_name):
        return os.path.join(project_root, 'logs', f'{script_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

# Настройка логирования
def setup_logging():
    """Настройка логирования с проверкой существования директории"""
    try:
        log_path = get_log_path('sync_pipeline_wl_china')
        log_dir = os.path.dirname(log_path)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_path, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)
    except Exception as e:
        # Если не удается создать файловый логгер, используем только консольный
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        return logging.getLogger(__name__)

logger = setup_logging()

class PipelineWLChinaSync:
    """Класс для синхронизации данных между pipeline_weld_joint_iso и wl_china"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.stats = {
            'total_pipeline_records': 0,
            'total_wl_china_records': 0,
            'matched_records': 0,
            'missing_records': 0,
            'inserted_records': 0,
            'errors': 0
        }
    
    def connect_to_database(self):
        """Подключение к базе данных"""
        try:
            self.conn = get_database_connection()
            self.cursor = self.conn.cursor()
            logger.info("[OK] Подключение к базе данных успешно")
            return True
        except Exception as e:
            logger.error(f"[ERROR] Ошибка подключения к базе данных: {e}")
            return False
    
    def get_table_stats(self):
        """Получение статистики по таблицам"""
        try:
            # Статистика pipeline_weld_joint_iso
            self.cursor.execute("SELECT COUNT(*) FROM pipeline_weld_joint_iso")
            self.stats['total_pipeline_records'] = self.cursor.fetchone()[0]
            
            # Статистика wl_china
            self.cursor.execute("SELECT COUNT(*) FROM wl_china")
            self.stats['total_wl_china_records'] = self.cursor.fetchone()[0]
            
            logger.info(f"[STATS] Статистика таблиц:")
            logger.info(f"   - pipeline_weld_joint_iso: {self.stats['total_pipeline_records']} записей")
            logger.info(f"   - wl_china: {self.stats['total_wl_china_records']} записей")
            
        except Exception as e:
            logger.error(f"[ERROR] Ошибка получения статистики: {e}")
            self.stats['errors'] += 1
    
    def check_matching_records(self):
        """Проверка соответствия записей между таблицами"""
        try:
            logger.info("[CHECK] Проверка соответствия записей...")
            
            # Запрос для поиска записей в pipeline_weld_joint_iso, которые есть в wl_china
            query = """
            SELECT 
                p.id,
                p.ISO,
                p.стык,
                p.Линия,
                p.лист,
                p.Титул,
                w.Номер_чертежа,
                w._Номер_сварного_шва_без_S_F_,
                w.N_Линии,
                w.Номер_листа,
                w.блок_N
            FROM pipeline_weld_joint_iso p
            INNER JOIN wl_china w ON 
                p.ISO = w.Номер_чертежа 
                AND p.стык = w._Номер_сварного_шва_без_S_F_
            """
            
            self.cursor.execute(query)
            matched_records = self.cursor.fetchall()
            self.stats['matched_records'] = len(matched_records)
            
            logger.info(f"[OK] Найдено {self.stats['matched_records']} соответствующих записей")
            
            # Показываем примеры найденных соответствий
            if matched_records:
                logger.info("[EXAMPLES] Примеры найденных соответствий:")
                for i, record in enumerate(matched_records[:5], 1):
                    logger.info(f"   {i}. ISO: {record[1]}, стык: {record[2]}")
            
            return matched_records
            
        except Exception as e:
            logger.error(f"[ERROR] Ошибка проверки соответствия: {e}")
            self.stats['errors'] += 1
            return []
    
    def find_missing_records(self):
        """Поиск записей в wl_china, которых нет в pipeline_weld_joint_iso"""
        try:
            logger.info("[CHECK] Поиск недостающих записей...")
            
            # Запрос для поиска записей в wl_china, которых нет в pipeline_weld_joint_iso
            query = """
            SELECT 
                w.id,
                w.Номер_чертежа,
                w._Номер_сварного_шва_без_S_F_,
                w.N_Линии,
                w.Номер_листа,
                w.блок_N
            FROM wl_china w
            LEFT JOIN pipeline_weld_joint_iso p ON 
                w.Номер_чертежа = p.ISO 
                AND w._Номер_сварного_шва_без_S_F_ = p.стык
            WHERE p.id IS NULL
            AND w.Номер_чертежа IS NOT NULL 
            AND w._Номер_сварного_шва_без_S_F_ IS NOT NULL
            AND w.Номер_чертежа != ''
            AND w._Номер_сварного_шва_без_S_F_ != ''
            """
            
            self.cursor.execute(query)
            missing_records = self.cursor.fetchall()
            self.stats['missing_records'] = len(missing_records)
            
            logger.info(f"[WARNING] Найдено {self.stats['missing_records']} недостающих записей")
            
            # Показываем детальную статистику по недостающим записям
            if missing_records:
                logger.info("[EXAMPLES] Примеры недостающих записей:")
                for i, record in enumerate(missing_records[:5], 1):
                    logger.info(f"   {i}. ISO: {record[1]}, стык: {record[2]}")
                
                # Группируем по титулам для статистики
                titul_stats = {}
                iso_stats = {}
                for record in missing_records:
                    titul = record[5] if record[5] else 'Не указан'
                    iso = record[1] if record[1] else 'Не указан'
                    
                    titul_stats[titul] = titul_stats.get(titul, 0) + 1
                    iso_stats[iso] = iso_stats.get(iso, 0) + 1
                
                logger.info("[STATS] Статистика по титулам (топ-10):")
                sorted_tituls = sorted(titul_stats.items(), key=lambda x: x[1], reverse=True)
                for titul, count in sorted_tituls[:10]:
                    logger.info(f"   - {titul}: {count} записей")
                
                logger.info("[STATS] Статистика по ISO (топ-10):")
                sorted_isos = sorted(iso_stats.items(), key=lambda x: x[1], reverse=True)
                for iso, count in sorted_isos[:10]:
                    logger.info(f"   - {iso}: {count} записей")
                
                # Показываем общую статистику
                logger.info(f"[STATS] Общая статистика:")
                logger.info(f"   - Всего уникальных титулов: {len(titul_stats)}")
                logger.info(f"   - Всего уникальных ISO: {len(iso_stats)}")
                logger.info(f"   - Среднее количество записей на титул: {len(missing_records) / len(titul_stats):.1f}")
                logger.info(f"   - Среднее количество записей на ISO: {len(missing_records) / len(iso_stats):.1f}")
            
            return missing_records
            
        except Exception as e:
            logger.error(f"[ERROR] Ошибка поиска недостающих записей: {e}")
            self.stats['errors'] += 1
            return []
    
    def insert_missing_records(self, missing_records):
        """Вставка недостающих записей в pipeline_weld_joint_iso"""
        if not missing_records:
            logger.info("[INFO] Нет записей для вставки")
            return
        
        try:
            logger.info(f"[PROCESS] Начинаем вставку {len(missing_records)} записей...")
            
            # Подготавливаем данные для вставки
            insert_data = []
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            for record in missing_records:
                wl_id, номер_чертежа, номер_шва, n_линии, номер_листа, блок_n = record
                
                # Создаем запись для вставки
                insert_record = (
                    блок_n,  # Титул
                    номер_чертежа,  # ISO
                    n_линии,  # Линия
                    '',  # ключь_жср_смр (пустое значение)
                    '',  # Линия2 (пустое значение)
                    номер_шва,  # стык
                    '1',  # Код_удаления (по умолчанию '1')
                    номер_листа,  # лист
                    '',  # повтор (пустое значение)
                    '',  # открыть (пустое значение)
                    current_time  # Дата_загрузки
                )
                insert_data.append(insert_record)
            
            # SQL запрос для вставки
            insert_query = """
            INSERT INTO pipeline_weld_joint_iso 
            (Титул, ISO, Линия, ключь_жср_смр, Линия2, стык, Код_удаления, лист, повтор, открыть, Дата_загрузки)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            # Выполняем вставку
            self.cursor.executemany(insert_query, insert_data)
            self.conn.commit()
            
            self.stats['inserted_records'] = len(insert_data)
            logger.info(f"[OK] Успешно вставлено {self.stats['inserted_records']} записей")
            
            # Показываем примеры вставленных записей
            logger.info("[EXAMPLES] Примеры вставленных записей:")
            for i, record in enumerate(insert_data[:5], 1):
                logger.info(f"   {i}. Титул: {record[0]}, ISO: {record[1]}, стык: {record[5]}")
            
        except Exception as e:
            logger.error(f"[ERROR] Ошибка вставки записей: {e}")
            self.stats['errors'] += 1
            if self.conn:
                self.conn.rollback()
    
    def verify_sync_results(self):
        """Проверка результатов синхронизации"""
        try:
            logger.info("[CHECK] Проверка результатов синхронизации...")
            
            # Получаем новую статистику
            self.cursor.execute("SELECT COUNT(*) FROM pipeline_weld_joint_iso")
            new_total = self.cursor.fetchone()[0]
            
            # Проверяем, что все записи из wl_china теперь есть в pipeline_weld_joint_iso
            query = """
            SELECT COUNT(*)
            FROM wl_china w
            LEFT JOIN pipeline_weld_joint_iso p ON 
                w.Номер_чертежа = p.ISO 
                AND w._Номер_сварного_шва_без_S_F_ = p.стык
            WHERE p.id IS NULL
            AND w.Номер_чертежа IS NOT NULL 
            AND w._Номер_сварного_шва_без_S_F_ IS NOT NULL
            AND w.Номер_чертежа != ''
            AND w._Номер_сварного_шва_без_S_F_ != ''
            """
            
            self.cursor.execute(query)
            still_missing = self.cursor.fetchone()[0]
            
            logger.info(f"[STATS] Результаты синхронизации:")
            logger.info(f"   - Записей в pipeline_weld_joint_iso до: {self.stats['total_pipeline_records']}")
            logger.info(f"   - Записей в pipeline_weld_joint_iso после: {new_total}")
            logger.info(f"   - Добавлено записей: {new_total - self.stats['total_pipeline_records']}")
            logger.info(f"   - Все еще отсутствует: {still_missing}")
            
            if still_missing == 0:
                logger.info("[OK] Синхронизация завершена успешно! Все записи из wl_china теперь есть в pipeline_weld_joint_iso")
            else:
                logger.warning(f"[WARNING] Остается {still_missing} записей без соответствия")
            
        except Exception as e:
            logger.error(f"[ERROR] Ошибка проверки результатов: {e}")
            self.stats['errors'] += 1
    
    def print_final_report(self):
        """Печать итогового отчета"""
        logger.info("=" * 60)
        logger.info("ИТОГОВЫЙ ОТЧЕТ СИНХРОНИЗАЦИИ")
        logger.info("=" * 60)
        logger.info(f"[STATS] Статистика:")
        logger.info(f"   - Всего записей в pipeline_weld_joint_iso: {self.stats['total_pipeline_records']}")
        logger.info(f"   - Всего записей в wl_china: {self.stats['total_wl_china_records']}")
        logger.info(f"   - Найдено соответствий: {self.stats['matched_records']}")
        logger.info(f"   - Найдено недостающих записей: {self.stats['missing_records']}")
        logger.info(f"   - Вставлено записей: {self.stats['inserted_records']}")
        logger.info(f"   - Ошибок: {self.stats['errors']}")
        logger.info("=" * 60)
        
        if self.stats['errors'] == 0:
            logger.info("[OK] Синхронизация завершена без ошибок!")
        else:
            logger.warning(f"[WARNING] Синхронизация завершена с {self.stats['errors']} ошибками")
    
    def preview_sync(self):
        """Предварительный просмотр синхронизации без внесения изменений"""
        logger.info("=" * 60)
        logger.info("ПРЕДВАРИТЕЛЬНЫЙ ПРОСМОТР СИНХРОНИЗАЦИИ")
        logger.info("=" * 60)
        
        # Подключение к базе данных
        if not self.connect_to_database():
            return False
        
        try:
            # Получение статистики
            self.get_table_stats()
            
            # Проверка соответствия записей
            matched_records = self.check_matching_records()
            
            # Поиск недостающих записей
            missing_records = self.find_missing_records()
            
            if not missing_records:
                logger.info("[OK] Все записи из wl_china уже есть в pipeline_weld_joint_iso")
                logger.info("[STATS] Синхронизация не требуется")
            else:
                logger.info("=" * 60)
                logger.info("ПЛАН СИНХРОНИЗАЦИИ")
                logger.info("=" * 60)
                logger.info(f"[PROCESS] Будет добавлено {len(missing_records)} новых записей")
                logger.info(f"[STATS] Из них:")
                
                # Группируем по титулам
                titul_stats = {}
                for record in missing_records:
                    titul = record[5] if record[5] else 'Не указан'
                    titul_stats[titul] = titul_stats.get(titul, 0) + 1
                
                for titul, count in sorted(titul_stats.items(), key=lambda x: x[1], reverse=True):
                    logger.info(f"   - {titul}: {count} записей")
                
                logger.info("=" * 60)
                logger.info("РЕКОМЕНДАЦИИ")
                logger.info("=" * 60)
                logger.info("1. Рекомендуется сначала запустить в режиме просмотра (--dry-run)")
                logger.info("2. Проверить логи на наличие ошибок")
                logger.info("3. Убедиться в корректности данных")
                logger.info("4. Запустить полную синхронизацию")
            
            return True
            
        except Exception as e:
            logger.error(f"[ERROR] Критическая ошибка предварительного просмотра: {e}")
            import traceback
            logger.error("Полный стек ошибки:")
            logger.error(traceback.format_exc())
            return False
        
        finally:
            if self.conn:
                self.conn.close()
                logger.info("🔌 Соединение с базой данных закрыто")

    def create_html_report(self, mode='preview'):
        """Создает HTML отчет и возвращает путь к файлу"""
        try:
            # Подключение к базе данных
            if not self.connect_to_database():
                return None
            
            # Получение статистики
            self.get_table_stats()
            
            # Проверка соответствия записей
            matched_records = self.check_matching_records()
            
            # Поиск недостающих записей
            missing_records = self.find_missing_records()
            
            # Создание HTML отчета
            html_content = self._generate_html_content(mode, matched_records, missing_records)
            
            # Сохранение в файл
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"sync_report_{mode}_{timestamp}.html"
            filepath = os.path.join(current_dir, 'results', filename)
            
            # Создаем директорию results если её нет
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            logger.info(f"[FILE] HTML отчет сохранен: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"[ERROR] Ошибка при создании HTML отчета: {str(e)}")
            return None
        finally:
            if self.conn:
                self.conn.close()
                logger.info("🔌 Соединение с базой данных закрыто")

    def _generate_html_content(self, mode, matched_records, missing_records):
        """Генерирует HTML содержимое отчета"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Группируем недостающие записи по титулам
        titul_stats = {}
        iso_stats = {}
        
        if missing_records:
            for record in missing_records:
                titul = record[5] if record[5] else 'Не указан'
                iso = record[1] if record[1] else 'Не указан'
                titul_stats[titul] = titul_stats.get(titul, 0) + 1
                iso_stats[iso] = iso_stats.get(iso, 0) + 1
        
        html = f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Отчет синхронизации pipeline_weld_joint_iso и wl_china</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
            color: #333;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}
        .header h1 {{
            margin: 0;
            font-size: 2.5em;
            font-weight: 300;
        }}
        .header p {{
            margin: 10px 0 0 0;
            opacity: 0.9;
            font-size: 1.1em;
        }}
        .content {{
            padding: 30px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: #f8f9fa;
            border-radius: 8px;
            padding: 20px;
            text-align: center;
            border-left: 4px solid #667eea;
        }}
        .stat-number {{
            font-size: 2.5em;
            font-weight: bold;
            color: #667eea;
            margin-bottom: 10px;
        }}
        .stat-label {{
            color: #666;
            font-size: 1.1em;
        }}
        .section {{
            margin-bottom: 30px;
        }}
        .section h2 {{
            color: #333;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
            margin-bottom: 20px;
        }}
        .table {{
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 20px;
        }}
        .table th, .table td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        .table th {{
            background-color: #f8f9fa;
            font-weight: 600;
            color: #333;
        }}
        .table tr:hover {{
            background-color: #f5f5f5;
        }}
        .status-success {{
            color: #28a745;
            font-weight: bold;
        }}
        .status-warning {{
            color: #ffc107;
            font-weight: bold;
        }}
        .status-info {{
            color: #17a2b8;
            font-weight: bold;
        }}
        .footer {{
            background: #f8f9fa;
            padding: 20px;
            text-align: center;
            color: #666;
            border-top: 1px solid #ddd;
        }}
        .mode-badge {{
            display: inline-block;
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 0.9em;
            font-weight: bold;
            margin-left: 10px;
        }}
        .mode-preview {{
            background: #e3f2fd;
            color: #1976d2;
        }}
        .mode-dry-run {{
            background: #fff3e0;
            color: #f57c00;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>[STATS] Отчет синхронизации</h1>
            <p>pipeline_weld_joint_iso ↔ wl_china</p>
            <span class="mode-badge mode-{mode}">
                {'Предварительный просмотр' if mode == 'preview' else 'Режим просмотра'}
            </span>
        </div>
        
        <div class="content">
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-number">{self.stats['total_pipeline_records']:,}</div>
                    <div class="stat-label">Записей в pipeline_weld_joint_iso</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">{self.stats['total_wl_china_records']:,}</div>
                    <div class="stat-label">Записей в wl_china</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">{len(matched_records):,}</div>
                    <div class="stat-label">Найдено соответствий</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">{len(missing_records):,}</div>
                    <div class="stat-label">Недостающих записей</div>
                </div>
            </div>
            
            <div class="section">
                <h2>[STATS] Общая статистика</h2>
                <table class="table">
                    <tr>
                        <th>Параметр</th>
                        <th>Значение</th>
                        <th>Статус</th>
                    </tr>
                    <tr>
                        <td>Всего записей в pipeline_weld_joint_iso</td>
                        <td>{self.stats['total_pipeline_records']:,}</td>
                        <td class="status-info">Базовая таблица</td>
                    </tr>
                    <tr>
                        <td>Всего записей в wl_china</td>
                        <td>{self.stats['total_wl_china_records']:,}</td>
                        <td class="status-info">Источник данных</td>
                    </tr>
                    <tr>
                        <td>Найдено соответствий</td>
                        <td>{len(matched_records):,}</td>
                        <td class="status-success">[OK] Синхронизировано</td>
                    </tr>
                    <tr>
                        <td>Недостающих записей</td>
                        <td>{len(missing_records):,}</td>
                        <td class="status-warning">[WARNING] Требует синхронизации</td>
                    </tr>
                </table>
            </div>
        """
        
        if missing_records:
            # Статистика по титулам
            html += f"""
            <div class="section">
                <h2>[STATS] Статистика по титулам (топ-10)</h2>
                <table class="table">
                    <tr>
                        <th>Титул</th>
                        <th>Количество записей</th>
                        <th>Процент</th>
                    </tr>
            """
            
            total_missing = len(missing_records)
            for titul, count in sorted(titul_stats.items(), key=lambda x: x[1], reverse=True)[:10]:
                percentage = (count / total_missing) * 100
                html += f"""
                    <tr>
                        <td>{titul}</td>
                        <td>{count:,}</td>
                        <td>{percentage:.1f}%</td>
                    </tr>
                """
            
            html += """
                </table>
            </div>
            
            <div class="section">
                <h2>[STATS] Статистика по ISO (топ-10)</h2>
                <table class="table">
                    <tr>
                        <th>ISO</th>
                        <th>Количество записей</th>
                        <th>Процент</th>
                    </tr>
            """
            
            for iso, count in sorted(iso_stats.items(), key=lambda x: x[1], reverse=True)[:10]:
                percentage = (count / total_missing) * 100
                html += f"""
                    <tr>
                        <td>{iso}</td>
                        <td>{count:,}</td>
                        <td>{percentage:.1f}%</td>
                    </tr>
                """
            
            html += """
                </table>
            </div>
            """
        else:
            html += """
            <div class="section">
                <h2>[OK] Результат</h2>
                <p class="status-success">Все записи из wl_china уже есть в pipeline_weld_joint_iso. Синхронизация не требуется.</p>
            </div>
            """
        
        html += f"""
        </div>
        
        <div class="footer">
            <p>Отчет создан: {timestamp}</p>
            <p>Режим: {'Предварительный просмотр' if mode == 'preview' else 'Режим просмотра'}</p>
        </div>
    </div>
</body>
</html>
        """
        
        return html

    def run_sync(self, dry_run=False):
        """Основной метод синхронизации"""
        logger.info("=" * 60)
        logger.info("НАЧАЛО СИНХРОНИЗАЦИИ PIPELINE_WELD_JOINT_ISO И WL_CHINA")
        if dry_run:
            logger.info("[PREVIEW] РЕЖИМ ПРОСМОТРА (изменения не будут сохранены)")
        logger.info("=" * 60)
        
        # Подключение к базе данных
        if not self.connect_to_database():
            return False
        
        try:
            # Получение статистики
            self.get_table_stats()
            
            # Проверка соответствия записей
            matched_records = self.check_matching_records()
            
            # Поиск недостающих записей
            missing_records = self.find_missing_records()
            
            if not missing_records:
                logger.info("[OK] Все записи из wl_china уже есть в pipeline_weld_joint_iso")
                self.print_final_report()
                return True
            
            if dry_run:
                logger.info("[PREVIEW] РЕЖИМ ПРОСМОТРА: записи не будут вставлены")
                logger.info(f"[PROCESS] Будет вставлено {len(missing_records)} записей")
            else:
                # Вставка недостающих записей
                self.insert_missing_records(missing_records)
                
                # Проверка результатов
                self.verify_sync_results()
            
            # Итоговый отчет
            self.print_final_report()
            
            return True
            
        except Exception as e:
            logger.error(f"[ERROR] Критическая ошибка синхронизации: {e}")
            import traceback
            logger.error("Полный стек ошибки:")
            logger.error(traceback.format_exc())
            return False
        
        finally:
            if self.conn:
                self.conn.close()
                logger.info("🔌 Соединение с базой данных закрыто")

def main():
    """Основная функция"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Синхронизация данных между pipeline_weld_joint_iso и wl_china')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Режим просмотра без внесения изменений')
    parser.add_argument('--check-only', action='store_true',
                       help='Только проверка соответствия без вставки')
    parser.add_argument('--preview', action='store_true',
                       help='Предварительный просмотр с детальной статистикой')
    parser.add_argument('--html-preview', action='store_true',
                       help='Создать HTML отчет предварительного просмотра')
    parser.add_argument('--html-dry-run', action='store_true',
                       help='Создать HTML отчет режима просмотра')
    
    args = parser.parse_args()
    
    sync = PipelineWLChinaSync()
    
    if args.html_preview:
        # Создание HTML отчета предварительного просмотра
        filepath = sync.create_html_report('preview')
        if filepath:
            logger.info(f"🌐 HTML отчет создан: {filepath}")
            logger.info(f"[FILE] Откройте файл в браузере: file:///{filepath.replace(os.sep, '/')}")
        else:
            logger.error("[ERROR] Не удалось создать HTML отчет")
    elif args.html_dry_run:
        # Создание HTML отчета режима просмотра
        filepath = sync.create_html_report('dry-run')
        if filepath:
            logger.info(f"🌐 HTML отчет создан: {filepath}")
            logger.info(f"[FILE] Откройте файл в браузере: file:///{filepath.replace(os.sep, '/')}")
        else:
            logger.error("[ERROR] Не удалось создать HTML отчет")
    elif args.preview:
        # Предварительный просмотр с детальной статистикой
        sync.preview_sync()
    elif args.check_only:
        # Только проверка
        if sync.connect_to_database():
            sync.get_table_stats()
            sync.check_matching_records()
            missing_records = sync.find_missing_records()
            if missing_records:
                logger.info(f"[WARNING] Найдено {len(missing_records)} записей для синхронизации")
            else:
                logger.info("[OK] Все записи синхронизированы")
            sync.conn.close()
    else:
        # Полная синхронизация
        sync.run_sync(dry_run=args.dry_run)

if __name__ == "__main__":
    main()

# Для запуска через GUI
def run_script():
    """Функция для запуска скрипта через GUI"""
    print(f"[DEBUG] run_script() вызвана с аргументами: {sys.argv}")
    
    # Проверяем аргументы командной строки
    if len(sys.argv) > 1:
        print(f"[ARGS] Найдены аргументы командной строки: {sys.argv[1:]}")
        if '--html-preview' in sys.argv:
            print("🌐 Запускаем HTML preview...")
            return run_script_html_preview()
        elif '--html-dry-run' in sys.argv:
            print("🌐 Запускаем HTML dry-run...")
            return run_script_html_dry_run()
        elif '--preview' in sys.argv:
            print("📊 Запускаем preview...")
            return run_script_preview()
        elif '--dry-run' in sys.argv:
            print("👁️ Запускаем dry-run...")
            return run_script_dry()
    
    # По умолчанию запускаем полную синхронизацию
    print("🚀 Запускаем полную синхронизацию...")
    sync = PipelineWLChinaSync()
    return sync.run_sync(dry_run=False)

def run_script_dry():
    """Функция для запуска скрипта через GUI в режиме просмотра"""
    sync = PipelineWLChinaSync()
    return sync.run_sync(dry_run=True)

def run_script_preview():
    """Функция для предварительного просмотра через GUI"""
    sync = PipelineWLChinaSync()
    return sync.preview_sync()

def run_script_html_preview():
    """Функция для создания HTML отчета предварительного просмотра через GUI"""
    print("🔧 run_script_html_preview() вызвана")
    sync = PipelineWLChinaSync()
    print("📊 Создаем HTML отчет...")
    filepath = sync.create_html_report('preview')
    if filepath:
        logger.info(f"🌐 HTML отчет создан: {filepath}")
        logger.info(f"📂 Откройте файл в браузере: file:///{filepath.replace(os.sep, '/')}")
        # Дополнительный вывод в stdout для веб-интерфейса
        print(f"HTML отчет создан: {filepath}")
        print(f"file:///{filepath.replace(os.sep, '/')}")
        print("✅ HTML отчет успешно создан!")
        return True
    else:
        logger.error("❌ Не удалось создать HTML отчет")
        print("❌ Не удалось создать HTML отчет")
        return False

def run_script_html_dry_run():
    """Функция для создания HTML отчета режима просмотра через GUI"""
    print("🔧 run_script_html_dry_run() вызвана")
    sync = PipelineWLChinaSync()
    print("📊 Создаем HTML отчет...")
    filepath = sync.create_html_report('dry-run')
    if filepath:
        logger.info(f"🌐 HTML отчет создан: {filepath}")
        logger.info(f"📂 Откройте файл в браузере: file:///{filepath.replace(os.sep, '/')}")
        # Дополнительный вывод в stdout для веб-интерфейса
        print(f"HTML отчет создан: {filepath}")
        print(f"file:///{filepath.replace(os.sep, '/')}")
        print("✅ HTML отчет успешно создан!")
        return True
    else:
        logger.error("❌ Не удалось создать HTML отчет")
        print("❌ Не удалось создать HTML отчет")
        return False

def run_script_html_preview_gui():
    """Функция для создания HTML отчета предварительного просмотра через GUI (альтернативная)"""
    import argparse
    import sys
    
    # Устанавливаем аргументы командной строки для HTML preview
    sys.argv = ['sync_pipeline_wl_china.py', '--html-preview']
    
    # Запускаем main функцию
    main()

def run_script_html_dry_run_gui():
    """Функция для создания HTML отчета режима просмотра через GUI (альтернативная)"""
    import argparse
    import sys
    
    # Устанавливаем аргументы командной строки для HTML dry-run
    sys.argv = ['sync_pipeline_wl_china.py', '--html-dry-run']
    
    # Запускаем main функцию
    main()

