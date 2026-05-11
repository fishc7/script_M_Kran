#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Модуль для работы с парсингом НАКС
"""

from flask import Blueprint, render_template, request, jsonify
import os
import sys
import logging
import threading
import time
from pathlib import Path
import pandas as pd
import sqlite3

# Добавляем путь к корню проекта для импорта script_runner
# Вычисляем правильный путь к корню проекта
# __file__ = web/app/modules/routes/naks_routes.py
# Нужно подняться на 5 уровней вверх: routes -> modules -> app -> web -> корень проекта
def get_project_root():
    """Вычисляет корень проекта"""
    current_file = os.path.abspath(__file__)
    # routes (dirname 1) -> modules (dirname 2) -> app (dirname 3) -> web (dirname 4) -> корень (dirname 5)
    calculated_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(current_file)))))
    
    # Проверяем правильность пути
    if not os.path.exists(os.path.join(calculated_root, 'archive', 'NAKS')):
        # Если путь неправильный, пробуем другой способ
        # Ищем корень проекта по наличию файла launch.bat или web_launcher.py
        test_path = calculated_root
        for _ in range(3):
            if os.path.exists(os.path.join(test_path, 'launch.bat')) or os.path.exists(os.path.join(test_path, 'web_launcher.py')):
                calculated_root = test_path
                break
            test_path = os.path.dirname(test_path)
    
    return calculated_root

# Вычисляем project_root при импорте модуля
project_root = get_project_root()

sys.path.insert(0, os.path.join(project_root, 'web', 'app'))
from script_runner import get_script_runner

logger = logging.getLogger(__name__)

# Создаем Blueprint для работы с НАКС
naks_bp = Blueprint('naks', __name__, url_prefix='')

# Словарь для хранения статуса выполнения парсинга
naks_status = {}

# Тестовый роут для проверки работы Blueprint
@naks_bp.route('/test_naks')
def test_naks():
    """Тестовый роут для проверки работы Blueprint"""
    return "NAKS Blueprint работает!"

@naks_bp.route('/naks')
def naks_parser_page():
    """Страница управления парсингом НАКС"""
    try:
        return render_template('naks_parser.html')
    except Exception as e:
        logger.error(f"Ошибка загрузки страницы парсинга НАКС: {e}")
        return f"Ошибка: {str(e)}", 500

@naks_bp.route('/api/naks/parse', methods=['POST'])
def start_naks_parsing():
    """Запуск парсинга НАКС"""
    try:
        # Вычисляем project_root заново при каждом вызове для надежности
        calculated_project_root = get_project_root()
        
        # Получаем путь к скрипту парсинга
        script_path = os.path.join(
            calculated_project_root,
            'archive',
            'NAKS',
            'НАКС_парсинг',
            '_naks_',
            'naks_search.py'
        )
        
        # Нормализуем путь
        script_path = os.path.normpath(script_path)
        
        # Логируем для отладки
        logger.info(f"Ищем скрипт по пути: {script_path}")
        logger.info(f"Calculated project root: {calculated_project_root}")
        logger.info(f"Скрипт существует: {os.path.exists(script_path)}")
        
        # Проверяем существование скрипта
        if not os.path.exists(script_path):
            # Пробуем альтернативные пути
            alt_paths = [
                os.path.join(calculated_project_root, 'archive', 'NAKS', 'НАКС_парсинг', '_naks_', 'naks_search.py'),
                os.path.join(os.path.dirname(calculated_project_root), 'archive', 'NAKS', 'НАКС_парсинг', '_naks_', 'naks_search.py'),
            ]
            
            found = False
            for alt_path in alt_paths:
                alt_path = os.path.normpath(alt_path)
                if os.path.exists(alt_path):
                    script_path = alt_path
                    found = True
                    logger.info(f"Найден альтернативный путь: {script_path}")
                    break
            
            if not found:
                error_msg = f'Скрипт парсинга не найден.\nОсновной путь: {script_path}\nProject root: {calculated_project_root}\nПроверенные альтернативные пути:\n' + '\n'.join([f'  - {p}' for p in alt_paths])
                logger.error(error_msg)
                return jsonify({
                    'success': False,
                    'message': error_msg
                }), 404
        
        # Получаем script_runner
        try:
            script_runner = get_script_runner()
        except RuntimeError:
            # Инициализируем script_runner если он не инициализирован
            from script_runner import init_script_runner
            init_script_runner(calculated_project_root)
            script_runner = get_script_runner()
        
        # Запускаем парсинг асинхронно
        script_id = script_runner.run_script_async(script_path)
        
        # Сохраняем информацию о запуске
        naks_status[script_id] = {
            'script_id': script_id,
            'start_time': time.time(),
            'status': 'running'
        }
        
        logger.info(f"Запущен парсинг НАКС с ID: {script_id}")
        
        return jsonify({
            'success': True,
            'message': 'Парсинг НАКС запущен',
            'script_id': script_id
        })
        
    except Exception as e:
        logger.error(f"Ошибка запуска парсинга НАКС: {e}")
        return jsonify({
            'success': False,
            'message': f'Ошибка запуска: {str(e)}'
        }), 500

@naks_bp.route('/api/naks/status/<script_id>')
def get_naks_status(script_id):
    """Получение статуса выполнения парсинга НАКС"""
    try:
        # Получаем статус из script_runner
        try:
            script_runner = get_script_runner()
            status = script_runner.get_script_status(script_id)
            
            if status is None:
                return jsonify({
                    'success': False,
                    'message': 'Задача не найдена'
                }), 404
            
            # Добавляем дополнительную информацию
            result = {
                'success': True,
                'script_id': script_id,
                'status': status.get('status', 'unknown'),
                'progress': status.get('progress', 0),
                'message': status.get('message', ''),
                'output': status.get('output', ''),
                'errors': status.get('errors', ''),
                'elapsed_time': status.get('elapsed_time', 0)
            }
            
            return jsonify(result)
            
        except RuntimeError:
            return jsonify({
                'success': False,
                'message': 'ScriptRunner не инициализирован'
            }), 500
            
    except Exception as e:
        logger.error(f"Ошибка получения статуса парсинга НАКС: {e}")
        return jsonify({
            'success': False,
            'message': f'Ошибка: {str(e)}'
        }), 500

@naks_bp.route('/api/naks/results')
def get_naks_results():
    """Получение информации о результатах парсинга"""
    try:
        # Вычисляем project_root заново
        calculated_project_root = get_project_root()
        
        # Путь к результатам (проверяем оба возможных пути)
        possible_paths = [
            os.path.join(calculated_project_root, 'archive', 'NAKS', 'НАКС_парсинг'),
            os.path.join(calculated_project_root, 'NAKS', 'НАКС_парсинг'),
        ]
        
        results_path = None
        for path in possible_paths:
            if os.path.exists(path):
                results_path = path
                break
        
        if not results_path:
            # Если ни один путь не найден, используем первый по умолчанию
            results_path = possible_paths[0]
        
        results = {
            'main_file': os.path.join(results_path, 'naks_главное.xlsx'),
            'details_file': os.path.join(results_path, 'naks_подробнее.xlsx'),
            'merged_file': os.path.join(results_path, 'naks_merged.xlsx')
        }
        
        # Проверяем существование файлов
        files_info = {}
        for key, file_path in results.items():
            if os.path.exists(file_path):
                stat = os.stat(file_path)
                files_info[key] = {
                    'exists': True,
                    'path': file_path,
                    'size': stat.st_size,
                    'modified': time.ctime(stat.st_mtime)
                }
            else:
                files_info[key] = {
                    'exists': False,
                    'path': file_path
                }
        
        return jsonify({
            'success': True,
            'files': files_info
        })
        
    except Exception as e:
        logger.error(f"Ошибка получения результатов парсинга НАКС: {e}")
        return jsonify({
            'success': False,
            'message': f'Ошибка: {str(e)}'
        }), 500

@naks_bp.route('/api/naks/load_to_db', methods=['POST'])
def load_naks_to_db():
    """Загрузка результатов парсинга в базу данных"""
    try:
        # Вычисляем project_root заново
        calculated_project_root = get_project_root()
        
        # Путь к скрипту загрузки (проверяем оба возможных пути)
        possible_script_paths = [
            os.path.join(calculated_project_root, 'scripts', 'tools', 'load_naks_to_db.py'),
            os.path.join(calculated_project_root, 'archive', 'NAKS', 'НАКС_парсинг', '_naks_', 'load_naks_to_db.py'),
            os.path.join(calculated_project_root, 'NAKS', 'НАКС_парсинг', '_naks_', 'load_naks_to_db.py'),
        ]
        
        load_script_path = None
        for path in possible_script_paths:
            path = os.path.normpath(path)
            if os.path.exists(path):
                load_script_path = path
                break
        
        if not load_script_path:
            return jsonify({
                'success': False,
                'message': f'Скрипт загрузки не найден. Проверенные пути:\n' + '\n'.join([f'  - {p}' for p in possible_script_paths])
            }), 404
        
        # Получаем script_runner
        try:
            script_runner = get_script_runner()
        except RuntimeError:
            # Инициализируем script_runner если он не инициализирован
            from script_runner import init_script_runner
            init_script_runner(calculated_project_root)
            script_runner = get_script_runner()
        
        # Запускаем загрузку в БД асинхронно
        script_id = script_runner.run_script_async(load_script_path)
        
        logger.info(f"Запущена загрузка НАКС в БД с ID: {script_id}")
        
        return jsonify({
            'success': True,
            'message': 'Загрузка в БД запущена',
            'script_id': script_id
        })
        
    except Exception as e:
        logger.error(f"Ошибка запуска загрузки в БД: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'message': f'Ошибка запуска: {str(e)}'
        }), 500

@naks_bp.route('/api/naks/data', methods=['GET'])
def get_naks_data():
    """Получение данных НАКС для отображения в таблице"""
    try:
        if pd is None or sqlite3 is None:
            return jsonify({
                'success': False,
                'message': 'Библиотеки pandas и sqlite3 не установлены'
            }), 500
        
        # Вычисляем project_root заново
        calculated_project_root = get_project_root()
        
        # Получаем параметр source из запроса (db или excel)
        source_param = request.args.get('source', 'auto')  # auto, db, excel
        
        data_source = None
        df = None
        
        # Проверяем все возможные пути к Excel файлам (приоритет новым данным)
        possible_excel_paths = [
            os.path.join(calculated_project_root, 'NAKS', 'НАКС_парсинг', 'naks_merged.xlsx'),  # Новые данные
            os.path.join(calculated_project_root, 'archive', 'NAKS', 'НАКС_парсинг', 'naks_merged.xlsx'),  # Старые данные
        ]
        
        excel_files_info = []
        for path in possible_excel_paths:
            if os.path.exists(path):
                stat = os.stat(path)
                excel_files_info.append({
                    'path': path,
                    'modified': stat.st_mtime,
                    'size': stat.st_size
                })
        
        # Сортируем по дате модификации (новые первыми)
        excel_files_info.sort(key=lambda x: x['modified'], reverse=True)
        
        # Проверяем пути к БД
        possible_db_paths = [
            os.path.join(calculated_project_root, 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
            os.path.join(calculated_project_root, 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        ]
        
        db_path = None
        for path in possible_db_paths:
            if os.path.exists(path):
                db_path = path
                break
        
        # Определяем источник данных
        if source_param == 'excel' or (source_param == 'auto' and excel_files_info):
            # Загружаем из самого свежего Excel файла
            if excel_files_info:
                excel_file = excel_files_info[0]['path']
                try:
                    df = pd.read_excel(excel_file)
                    file_name = os.path.basename(excel_file)
                    file_dir = os.path.basename(os.path.dirname(excel_file))
                    data_source = f"Excel: {file_dir}/{file_name} ({len(df)} записей)"
                    logger.info(f"Загружено {len(df)} записей из Excel: {excel_file}")
                except Exception as e:
                    logger.error(f"Ошибка чтения Excel: {e}")
                    return jsonify({
                        'success': False,
                        'message': f'Ошибка чтения Excel файла: {str(e)}'
                    }), 500
        
        elif source_param == 'db' or (source_param == 'auto' and db_path and (not excel_files_info or df is None)):
            # Загружаем из БД
            if db_path:
                try:
                    conn = sqlite3.connect(db_path)
                    query = "SELECT * FROM naks_data LIMIT 1000"
                    df_db = pd.read_sql_query(query, conn)
                    conn.close()
                    
                    if len(df_db) > 0:
                        df = df_db
                        data_source = f"База данных ({len(df)} записей)"
                        logger.info(f"Загружено {len(df)} записей из БД")
                except Exception as e:
                    logger.warning(f"Не удалось загрузить из БД: {e}")
        
        # Если ничего не загрузилось, пробуем Excel как запасной вариант
        if df is None or len(df) == 0:
            if excel_files_info:
                excel_file = excel_files_info[0]['path']
                try:
                    df = pd.read_excel(excel_file)
                    file_name = os.path.basename(excel_file)
                    file_dir = os.path.basename(os.path.dirname(excel_file))
                    data_source = f"Excel: {file_dir}/{file_name} ({len(df)} записей)"
                    logger.info(f"Загружено {len(df)} записей из Excel: {excel_file}")
                except Exception as e:
                    logger.error(f"Ошибка чтения Excel: {e}")
                    return jsonify({
                        'success': False,
                        'message': f'Ошибка чтения Excel файла: {str(e)}'
                    }), 500
            else:
                return jsonify({
                    'success': False,
                    'message': 'Данные не найдены. Сначала выполните парсинг.'
                }), 404
        
        if df is None or len(df) == 0:
            return jsonify({
                'success': False,
                'message': 'Данные не найдены'
            }), 404
        
        # Преобразуем DataFrame в JSON
        # Заменяем NaN на None для корректной сериализации
        df = df.fillna('')
        
        # Ограничиваем количество записей для производительности
        max_records = 1000
        if len(df) > max_records:
            df = df.head(max_records)
            total_records = len(df)
            message = f"Показано {max_records} из {total_records} записей"
        else:
            total_records = len(df)
            message = f"Всего записей: {total_records}"
        
        # Получаем колонки
        columns = df.columns.tolist()
        
        # Преобразуем данные в список словарей
        records = df.to_dict('records')
        
        return jsonify({
            'success': True,
            'data_source': data_source,
            'total_records': total_records,
            'message': message,
            'columns': columns,
            'records': records
        })
        
    except Exception as e:
        logger.error(f"Ошибка получения данных НАКС: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'message': f'Ошибка: {str(e)}'
        }), 500

