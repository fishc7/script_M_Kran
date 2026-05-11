#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL Scheduler - Автоматический планировщик ETL процессов
Автор: AI Assistant
Версия: 1.0

Этот скрипт обеспечивает автоматический ежедневный запуск ETL процессов
в правильной последовательности согласно приоритетам.
"""

import os
import sys
import logging
import subprocess
import time
from datetime import datetime, timedelta
import json
import sqlite3
from pathlib import Path
import schedule
import threading

# Добавляем пути для импорта модулей
current_dir = os.path.dirname(os.path.abspath(__file__))
scripts_dir = os.path.join(current_dir, 'scripts')
sys.path.insert(0, scripts_dir)

# Настройка логирования
log_dir = os.path.join(current_dir, 'logs')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'etl_scheduler.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ETLScheduler:
    """Класс для управления автоматическим запуском ETL процессов"""
    
    def __init__(self):
        self.project_root = current_dir
        self.scripts_dir = os.path.join(self.project_root, 'scripts', 'data_loaders')
        self.db_path = os.path.join(self.project_root, 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db')
        
        # ETL процессы с приоритетами (ежедневные)
        self.daily_etl_processes = [
            {
                'name': 'load_lnk_data.py',
                'priority': 1,
                'description': '📖 Журнал НК НГС - результаты контроля качества',
                'script_path': os.path.join(self.scripts_dir, 'load_lnk_data.py')
            },
            {
                'name': 'load_staff_titles_M_Kran.py',
                'priority': 2,
                'description': '👥 Расстановка персонала М_Кран по участкам',
                'script_path': os.path.join(self.scripts_dir, 'load_staff_titles_M_Kran.py')
            },
            {
                'name': 'load_ndt_findings_transmission_register.py',
                'priority': 3,
                'description': '📋 Реестр заключений НГС Эксперт',
                'script_path': os.path.join(self.scripts_dir, 'load_ndt_findings_transmission_register.py')
            },
            {
                'name': 'load_wl_report_smr_web.py',
                'priority': 4,
                'description': '📋 Отчеты мастеров СМР (оптимизированная версия)',
                'script_path': os.path.join(self.scripts_dir, 'load_wl_report_smr_web.py')
            },
            {
                'name': 'load_work_order_log_NDT.py',
                'priority': 5,
                'description': '📝 Заявки на НК от М_Кран - планирование работ',
                'script_path': os.path.join(self.scripts_dir, 'load_work_order_log_NDT.py')
            },
            {
                'name': 'load_wl_china.py',
                'priority': 6,
                'description': '🇨🇳 Данные китайских подрядчиков WELDLOG',
                'script_path': os.path.join(self.scripts_dir, 'load_wl_china.py')
            },
            {
                'name': 'create_ndt_reports_table.py',
                'priority': 7,
                'description': '📁 Загрузить перечень заключений НК',
                'script_path': os.path.join(self.scripts_dir, 'create_ndt_reports_table.py')
            },
            {
                'name': 'load_pipeline_weld_joint_iso.py',
                'priority': 8,
                'description': '🔗 Сварные соединения ISO - номерация стыков',
                'script_path': os.path.join(self.scripts_dir, 'load_pipeline_weld_joint_iso.py')
            },
            {
                'name': 'create_condition_weld_table.py',
                'priority': 9,
                'description': '🏗️ Создание последнее состояния сварных швов',
                'script_path': os.path.join(self.scripts_dir, 'create_condition_weld_table.py')
            }
        ]
        
        # Сортируем по приоритету
        self.daily_etl_processes.sort(key=lambda x: x['priority'])
        
        # Статистика выполнения
        self.execution_stats = {
            'total_runs': 0,
            'successful_runs': 0,
            'failed_runs': 0,
            'last_run': None,
            'last_success': None,
            'last_error': None
        }
    
    def check_dependencies(self):
        """Проверяет зависимости для запуска ETL процессов"""
        logger.info("Проверка зависимостей...")
        
        # Проверяем существование базы данных
        if not os.path.exists(self.db_path):
            logger.error(f"База данных не найдена: {self.db_path}")
            return False
        
        # Проверяем существование скриптов
        for process in self.daily_etl_processes:
            if not os.path.exists(process['script_path']):
                logger.error(f"Скрипт не найден: {process['script_path']}")
                return False
        
        logger.info("Все зависимости проверены успешно")
        return True
    
    def run_etl_process(self, process):
        """Запускает один ETL процесс"""
        logger.info(f"Запуск ETL процесса: {process['name']} - {process['description']}")
        
        start_time = time.time()
        
        try:
            # Запускаем скрипт
            result = subprocess.run(
                [sys.executable, process['script_path']],
                capture_output=True,
                text=True,
                encoding='utf-8',
                cwd=self.project_root,
                timeout=300  # 5 минут таймаут
            )
            
            execution_time = time.time() - start_time
            
            if result.returncode == 0:
                logger.info(f"✅ ETL процесс {process['name']} выполнен успешно за {execution_time:.2f} сек")
                return True, result.stdout
            else:
                logger.error(f"❌ ETL процесс {process['name']} завершился с ошибкой (код: {result.returncode})")
                logger.error(f"STDOUT: {result.stdout}")
                logger.error(f"STDERR: {result.stderr}")
                return False, result.stderr
                
        except subprocess.TimeoutExpired:
            logger.error(f"⏰ ETL процесс {process['name']} превысил время выполнения (5 минут)")
            return False, "Timeout exceeded"
        except Exception as e:
            logger.error(f"💥 Ошибка запуска ETL процесса {process['name']}: {e}")
            return False, str(e)
    
    def run_daily_etl_pipeline(self):
        """Запускает ежедневный ETL пайплайн"""
        logger.info("=" * 80)
        logger.info("🚀 НАЧАЛО ЕЖЕДНЕВНОГО ETL ПАЙПЛАЙНА")
        logger.info("=" * 80)
        
        start_time = time.time()
        pipeline_stats = {
            'total_processes': len(self.daily_etl_processes),
            'successful': 0,
            'failed': 0,
            'errors': []
        }
        
        # Проверяем зависимости
        if not self.check_dependencies():
            logger.error("❌ Зависимости не выполнены, прерываем выполнение")
            return False
        
        # Запускаем процессы в порядке приоритета
        for process in self.daily_etl_processes:
            logger.info(f"🔄 Запуск процесса {process['priority']}/{len(self.daily_etl_processes)}: {process['name']}")
            
            success, output = self.run_etl_process(process)
            
            if success:
                pipeline_stats['successful'] += 1
                logger.info(f"✅ Процесс {process['name']} завершен успешно")
            else:
                pipeline_stats['failed'] += 1
                pipeline_stats['errors'].append({
                    'process': process['name'],
                    'error': output
                })
                logger.error(f"❌ Процесс {process['name']} завершен с ошибкой")
            
            # Небольшая пауза между процессами
            time.sleep(2)
        
        # Итоговая статистика
        total_time = time.time() - start_time
        logger.info("=" * 80)
        logger.info("📊 ИТОГОВАЯ СТАТИСТИКА ETL ПАЙПЛАЙНА")
        logger.info("=" * 80)
        logger.info(f"Общее время выполнения: {total_time:.2f} сек")
        logger.info(f"Всего процессов: {pipeline_stats['total_processes']}")
        logger.info(f"Успешно выполнено: {pipeline_stats['successful']}")
        logger.info(f"Ошибок: {pipeline_stats['failed']}")
        
        if pipeline_stats['errors']:
            logger.info("Детали ошибок:")
            for error in pipeline_stats['errors']:
                logger.error(f"  - {error['process']}: {error['error']}")
        
        # Обновляем общую статистику
        self.execution_stats['total_runs'] += 1
        if pipeline_stats['failed'] == 0:
            self.execution_stats['successful_runs'] += 1
            self.execution_stats['last_success'] = datetime.now().isoformat()
        else:
            self.execution_stats['failed_runs'] += 1
            self.execution_stats['last_error'] = datetime.now().isoformat()
        
        self.execution_stats['last_run'] = datetime.now().isoformat()
        
        # Сохраняем статистику
        self.save_execution_stats()
        
        logger.info("🏁 ЕЖЕДНЕВНЫЙ ETL ПАЙПЛАЙН ЗАВЕРШЕН")
        logger.info("=" * 80)
        
        return pipeline_stats['failed'] == 0
    
    def save_execution_stats(self):
        """Сохраняет статистику выполнения в файл"""
        stats_file = os.path.join(log_dir, 'etl_execution_stats.json')
        try:
            with open(stats_file, 'w', encoding='utf-8') as f:
                json.dump(self.execution_stats, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения статистики: {e}")
    
    def load_execution_stats(self):
        """Загружает статистику выполнения из файла"""
        stats_file = os.path.join(log_dir, 'etl_execution_stats.json')
        if os.path.exists(stats_file):
            try:
                with open(stats_file, 'r', encoding='utf-8') as f:
                    self.execution_stats = json.load(f)
            except Exception as e:
                logger.error(f"Ошибка загрузки статистики: {e}")
    
    def schedule_daily_run(self, time_str="06:00"):
        """Планирует ежедневный запуск ETL процессов"""
        logger.info(f"Планирование ежедневного запуска ETL процессов на {time_str}")
        
        schedule.every().day.at(time_str).do(self.run_daily_etl_pipeline)
        
        logger.info("Расписание установлено. Ожидание следующего запуска...")
        
        while True:
            schedule.run_pending()
            time.sleep(60)  # Проверяем каждую минуту
    
    def run_once(self):
        """Запускает ETL пайплайн один раз (для тестирования)"""
        logger.info("Запуск ETL пайплайна один раз (тестовый режим)")
        return self.run_daily_etl_pipeline()
    
    def show_status(self):
        """Показывает статус ETL планировщика"""
        logger.info("=" * 60)
        logger.info("📊 СТАТУС ETL ПЛАНИРОВЩИКА")
        logger.info("=" * 60)
        logger.info(f"Всего запусков: {self.execution_stats['total_runs']}")
        logger.info(f"Успешных запусков: {self.execution_stats['successful_runs']}")
        logger.info(f"Неудачных запусков: {self.execution_stats['failed_runs']}")
        
        if self.execution_stats['last_run']:
            logger.info(f"Последний запуск: {self.execution_stats['last_run']}")
        if self.execution_stats['last_success']:
            logger.info(f"Последний успешный запуск: {self.execution_stats['last_success']}")
        if self.execution_stats['last_error']:
            logger.info(f"Последняя ошибка: {self.execution_stats['last_error']}")
        
        logger.info("\n📋 Ежедневные ETL процессы:")
        for process in self.daily_etl_processes:
            logger.info(f"  {process['priority']}. {process['name']} - {process['description']}")
        
        logger.info("=" * 60)

def main():
    """Главная функция"""
    import argparse
    
    parser = argparse.ArgumentParser(description='ETL Scheduler - Автоматический планировщик ETL процессов')
    parser.add_argument('--run-once', action='store_true', help='Запустить ETL пайплайн один раз')
    parser.add_argument('--schedule', action='store_true', help='Запустить планировщик')
    parser.add_argument('--time', default='06:00', help='Время ежедневного запуска (по умолчанию 06:00)')
    parser.add_argument('--status', action='store_true', help='Показать статус планировщика')
    
    args = parser.parse_args()
    
    # Создаем планировщик
    scheduler = ETLScheduler()
    scheduler.load_execution_stats()
    
    if args.status:
        scheduler.show_status()
    elif args.run_once:
        success = scheduler.run_once()
        sys.exit(0 if success else 1)
    elif args.schedule:
        scheduler.schedule_daily_run(args.time)
    else:
        # По умолчанию показываем статус
        scheduler.show_status()
        print("\nИспользование:")
        print("  python etl_scheduler.py --run-once    # Запустить один раз")
        print("  python etl_scheduler.py --schedule    # Запустить планировщик")
        print("  python etl_scheduler.py --time 07:00  # Установить время запуска")
        print("  python etl_scheduler.py --status      # Показать статус")

if __name__ == "__main__":
    main()
