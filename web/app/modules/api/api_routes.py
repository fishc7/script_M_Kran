#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API маршруты приложения M_Kran
"""

from flask import Blueprint, request, jsonify, send_file
from ..database.db_manager import db_manager, data_processor
from ..utils.text_utils import ValidationUtils, DataUtils
import logging
import pandas as pd
from io import BytesIO

logger = logging.getLogger(__name__)

# Создаем Blueprint для API
api_bp = Blueprint('api', __name__, url_prefix='/api')

@api_bp.route('/title_stats')
def title_stats():
    """Статистика по титулам"""
    try:
        # Здесь должна быть логика получения статистики по титулам
        stats = {
            'total_titles': 0,
            'titles': []
        }
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Ошибка получения статистики титулов: {e}")
        return jsonify({'error': str(e)}), 500

@api_bp.route('/etl_scripts')
def etl_scripts():
    """Список ETL скриптов"""
    try:
        scripts = [
            {'name': 'load_wl_china', 'description': 'Загрузка данных wl_china'},
            {'name': 'load_wl_report_smr', 'description': 'Загрузка данных wl_report_smr'},
            {'name': 'sync_pipeline_wl_china', 'description': 'Синхронизация данных'}
        ]
        return jsonify(scripts)
    except Exception as e:
        logger.error(f"Ошибка получения списка скриптов: {e}")
        return jsonify({'error': str(e)}), 500

@api_bp.route('/update_stats')
def update_stats():
    """Обновление статистики"""
    try:
        stats = db_manager.get_database_stats()
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Ошибка обновления статистики: {e}")
        return jsonify({'error': str(e)}), 500

@api_bp.route('/table/<table_name>/search')
def table_search(table_name):
    """Поиск в таблице"""
    try:
        if not ValidationUtils.is_valid_table_name(table_name):
            return jsonify({'error': 'Недопустимое имя таблицы'}), 400
        
        search_term = request.args.get('q', '')
        if not search_term:
            return jsonify({'error': 'Поисковый запрос не указан'}), 400
        
        # Получаем колонки для поиска
        columns = request.args.getlist('columns')
        
        # Выполняем поиск
        results = db_manager.search_in_table(table_name, search_term, columns)
        
        return jsonify({
            'results': results.to_dict('records'),
            'total': len(results)
        })
    except Exception as e:
        logger.error(f"Ошибка поиска в таблице {table_name}: {e}")
        return jsonify({'error': str(e)}), 500

@api_bp.route('/table_info')
def table_info():
    """Информация о всех таблицах"""
    try:
        tables = db_manager.get_tables_list()
        table_info_list = []
        
        for table in tables:
            info = db_manager.get_table_info(table)
            table_info_list.append(info)
        
        return jsonify(table_info_list)
    except Exception as e:
        logger.error(f"Ошибка получения информации о таблицах: {e}")
        return jsonify({'error': str(e)}), 500

@api_bp.route('/table_info/<table_name>')
def table_info_detail(table_name):
    """Детальная информация о таблице"""
    try:
        if not ValidationUtils.is_valid_table_name(table_name):
            return jsonify({'error': 'Недопустимое имя таблицы'}), 400
        
        info = db_manager.get_table_info(table_name)
        return jsonify(info)
    except Exception as e:
        logger.error(f"Ошибка получения информации о таблице {table_name}: {e}")
        return jsonify({'error': str(e)}), 500

@api_bp.route('/get_tables')
def get_tables():
    """Получение списка таблиц"""
    try:
        tables = db_manager.get_tables_list()
        return jsonify(tables)
    except Exception as e:
        logger.error(f"Ошибка получения списка таблиц: {e}")
        return jsonify({'error': str(e)}), 500

@api_bp.route('/get_columns', methods=['POST'])
def get_columns():
    """Получение колонок таблицы"""
    try:
        data = request.get_json()
        table_name = data.get('table_name')
        
        if not table_name or not ValidationUtils.is_valid_table_name(table_name):
            return jsonify({'error': 'Недопустимое имя таблицы'}), 400
        
        info = db_manager.get_table_info(table_name)
        columns = [col['name'] for col in info['columns']]
        
        return jsonify(columns)
    except Exception as e:
        logger.error(f"Ошибка получения колонок таблицы: {e}")
        return jsonify({'error': str(e)}), 500

@api_bp.route('/status_statistics')
def status_statistics():
    """Статистика по статусам"""
    try:
        # Здесь должна быть логика получения статистики по статусам
        stats = {
            'total_records': 0,
            'statuses': {}
        }
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Ошибка получения статистики статусов: {e}")
        return jsonify({'error': str(e)}), 500

@api_bp.route('/results_statistics')
def results_statistics():
    """Статистика по результатам"""
    try:
        # Здесь должна быть логика получения статистики по результатам
        stats = {
            'total_records': 0,
            'results': {}
        }
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Ошибка получения статистики результатов: {e}")
        return jsonify({'error': str(e)}), 500

@api_bp.route('/backups/list')
def backups_list():
    """Список бэкапов"""
    try:
        # Здесь должна быть логика получения списка бэкапов
        backups = []
        return jsonify(backups)
    except Exception as e:
        logger.error(f"Ошибка получения списка бэкапов: {e}")
        return jsonify({'error': str(e)}), 500

@api_bp.route('/backups/restore', methods=['POST'])
def backup_restore():
    """Восстановление из бэкапа"""
    try:
        data = request.get_json()
        backup_name = data.get('backup_name')
        
        if not backup_name:
            return jsonify({'error': 'Имя бэкапа не указано'}), 400
        
        # Здесь должна быть логика восстановления из бэкапа
        return jsonify({'message': f'Восстановление из бэкапа {backup_name} запущено'})
    except Exception as e:
        logger.error(f"Ошибка восстановления из бэкапа: {e}")
        return jsonify({'error': str(e)}), 500

@api_bp.route('/backup', methods=['POST'])
def create_backup():
    """Создание бэкапа"""
    try:
        data = request.get_json()
        backup_type = data.get('type', 'full')
        
        # Здесь должна быть логика создания бэкапа
        return jsonify({'message': f'Создание бэкапа типа {backup_type} запущено'})
    except Exception as e:
        logger.error(f"Ошибка создания бэкапа: {e}")
        return jsonify({'error': str(e)}), 500

@api_bp.route('/shutdown', methods=['POST'])
def shutdown():
    """Завершение работы приложения"""
    try:
        # Здесь должна быть логика корректного завершения
        return jsonify({'message': 'Приложение будет завершено'})
    except Exception as e:
        logger.error(f"Ошибка завершения приложения: {e}")
        return jsonify({'error': str(e)}), 500
