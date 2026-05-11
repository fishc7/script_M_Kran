#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Модуль для работы со скриптами и ETL процессами
"""

from flask import Blueprint, request, jsonify, render_template
from ..database.db_manager import db_manager
from ..utils.text_utils import ValidationUtils
import logging
import subprocess
import threading
import time
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# Создаем Blueprint для работы со скриптами
scripts_bp = Blueprint('scripts', __name__)

# Словарь для хранения статуса выполнения скриптов
script_status = {}

@scripts_bp.route('/run_script', methods=['POST'])
def run_script():
    """Запуск скрипта"""
    try:
        data = request.get_json()
        script_name = data.get('script_name')
        parameters = data.get('parameters', {})
        
        if not script_name:
            return jsonify({'error': 'Имя скрипта не указано'}), 400
        
        # Генерируем уникальный ID для отслеживания
        script_id = f"script_{int(time.time())}"
        
        # Запускаем скрипт в отдельном потоке
        thread = threading.Thread(
            target=execute_script,
            args=(script_id, script_name, parameters)
        )
        thread.daemon = True
        thread.start()
        
        # Инициализируем статус
        script_status[script_id] = {
            'status': 'running',
            'start_time': time.time(),
            'script_name': script_name,
            'output': '',
            'error': None
        }
        
        return jsonify({
            'message': f'Скрипт {script_name} запущен',
            'script_id': script_id
        })
        
    except Exception as e:
        logger.error(f"Ошибка запуска скрипта: {e}")
        return jsonify({'error': str(e)}), 500

@scripts_bp.route('/run_etl_stage', methods=['POST'])
def run_etl_stage():
    """Запуск этапа ETL процесса"""
    try:
        data = request.get_json()
        stage_name = data.get('stage_name')
        parameters = data.get('parameters', {})
        
        if not stage_name:
            return jsonify({'error': 'Имя этапа не указано'}), 400
        
        # Генерируем уникальный ID
        script_id = f"etl_{int(time.time())}"
        
        # Запускаем ETL этап в отдельном потоке
        thread = threading.Thread(
            target=execute_etl_stage,
            args=(script_id, stage_name, parameters)
        )
        thread.daemon = True
        thread.start()
        
        # Инициализируем статус
        script_status[script_id] = {
            'status': 'running',
            'start_time': time.time(),
            'stage_name': stage_name,
            'output': '',
            'error': None
        }
        
        return jsonify({
            'message': f'ETL этап {stage_name} запущен',
            'script_id': script_id
        })
        
    except Exception as e:
        logger.error(f"Ошибка запуска ETL этапа: {e}")
        return jsonify({'error': str(e)}), 500

@scripts_bp.route('/run_etl_pipeline', methods=['POST'])
def run_etl_pipeline():
    """Запуск полного ETL пайплайна"""
    try:
        data = request.get_json()
        pipeline_name = data.get('pipeline_name', 'default')
        parameters = data.get('parameters', {})
        
        # Генерируем уникальный ID
        script_id = f"pipeline_{int(time.time())}"
        
        # Запускаем пайплайн в отдельном потоке
        thread = threading.Thread(
            target=execute_etl_pipeline,
            args=(script_id, pipeline_name, parameters)
        )
        thread.daemon = True
        thread.start()
        
        # Инициализируем статус
        script_status[script_id] = {
            'status': 'running',
            'start_time': time.time(),
            'pipeline_name': pipeline_name,
            'output': '',
            'error': None,
            'stages_completed': 0,
            'total_stages': 5  # Примерное количество этапов
        }
        
        return jsonify({
            'message': f'ETL пайплайн {pipeline_name} запущен',
            'script_id': script_id
        })
        
    except Exception as e:
        logger.error(f"Ошибка запуска ETL пайплайна: {e}")
        return jsonify({'error': str(e)}), 500

@scripts_bp.route('/script_status/<script_id>')
def get_script_status(script_id):
    """Получение статуса выполнения скрипта"""
    try:
        if script_id not in script_status:
            return jsonify({'error': 'Скрипт не найден'}), 404
        
        status_info = script_status[script_id].copy()
        
        # Добавляем время выполнения
        if status_info['status'] == 'running':
            status_info['running_time'] = time.time() - status_info['start_time']
        
        return jsonify(status_info)
        
    except Exception as e:
        logger.error(f"Ошибка получения статуса скрипта {script_id}: {e}")
        return jsonify({'error': str(e)}), 500

def execute_script(script_id, script_name, parameters):
    """Выполнение скрипта"""
    try:
        script_status[script_id]['output'] = f"Запуск скрипта {script_name}..."
        
        # Определяем путь к скрипту
        script_path = Path("scripts") / f"{script_name}.py"
        
        if not script_path.exists():
            script_status[script_id]['status'] = 'error'
            script_status[script_id]['error'] = f"Скрипт {script_name} не найден"
            return
        
        # Выполняем скрипт
        result = subprocess.run(
            ['python', str(script_path)],
            capture_output=True,
            text=True,
            timeout=300  # 5 минут таймаут
        )
        
        if result.returncode == 0:
            script_status[script_id]['status'] = 'completed'
            script_status[script_id]['output'] = result.stdout
        else:
            script_status[script_id]['status'] = 'error'
            script_status[script_id]['error'] = result.stderr
            
    except subprocess.TimeoutExpired:
        script_status[script_id]['status'] = 'timeout'
        script_status[script_id]['error'] = 'Превышено время выполнения'
    except Exception as e:
        script_status[script_id]['status'] = 'error'
        script_status[script_id]['error'] = str(e)

def execute_etl_stage(script_id, stage_name, parameters):
    """Выполнение ETL этапа"""
    try:
        script_status[script_id]['output'] = f"Выполнение ETL этапа {stage_name}..."
        
        # Здесь должна быть логика выполнения конкретного ETL этапа
        # Пока имитируем выполнение
        time.sleep(2)  # Имитация работы
        
        script_status[script_id]['status'] = 'completed'
        script_status[script_id]['output'] = f"ETL этап {stage_name} выполнен успешно"
        
    except Exception as e:
        script_status[script_id]['status'] = 'error'
        script_status[script_id]['error'] = str(e)

def execute_etl_pipeline(script_id, pipeline_name, parameters):
    """Выполнение полного ETL пайплайна"""
    try:
        script_status[script_id]['output'] = f"Запуск ETL пайплайна {pipeline_name}..."
        
        # Список этапов пайплайна
        stages = [
            'load_data',
            'clean_data', 
            'transform_data',
            'validate_data',
            'load_to_database'
        ]
        
        for i, stage in enumerate(stages):
            script_status[script_id]['output'] = f"Выполнение этапа: {stage}"
            script_status[script_id]['stages_completed'] = i
            
            # Имитация выполнения этапа
            time.sleep(1)
        
        script_status[script_id]['status'] = 'completed'
        script_status[script_id]['stages_completed'] = len(stages)
        script_status[script_id]['output'] = f"ETL пайплайн {pipeline_name} выполнен успешно"
        
    except Exception as e:
        script_status[script_id]['status'] = 'error'
        script_status[script_id]['error'] = str(e)
