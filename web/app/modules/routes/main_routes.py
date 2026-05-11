#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Основные маршруты приложения M_Kran
"""

from flask import Blueprint, render_template, request, jsonify, redirect, url_for
from ..database.db_manager import db_manager, data_processor
from ..utils.text_utils import DataUtils
import logging

logger = logging.getLogger(__name__)

# Создаем Blueprint для основных маршрутов
main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def index():
    """Главная страница"""
    try:
        # Получаем статистику
        stats = db_manager.get_database_stats()
        duplicates = data_processor.get_duplicates_count()
        activities = data_processor.get_recent_activities()
        
        return render_template('index.html', 
                             stats=stats,
                             duplicates=duplicates,
                             activities=activities)
    except Exception as e:
        logger.error(f"Ошибка загрузки главной страницы: {e}")
        return render_template('error.html', error=str(e))

@main_bp.route('/database')
def database_view():
    """Страница просмотра базы данных"""
    try:
        tables = db_manager.get_tables_list()
        stats = db_manager.get_database_stats()
        
        return render_template('database.html',
                             tables=tables,
                             stats=stats)
    except Exception as e:
        logger.error(f"Ошибка загрузки страницы БД: {e}")
        return render_template('error.html', error=str(e))

@main_bp.route('/table/<table_name>')
def table_view(table_name):
    """Просмотр таблицы"""
    try:
        # Валидация имени таблицы
        from ..utils.text_utils import ValidationUtils
        if not ValidationUtils.is_valid_table_name(table_name):
            return render_template('error.html', error="Недопустимое имя таблицы")
        
        # Получаем информацию о таблице
        table_info = db_manager.get_table_info(table_name)
        
        # Получаем данные
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)
        offset = (page - 1) * per_page
        
        data = db_manager.get_table_data(table_name, limit=per_page, offset=offset)
        
        # Подготавливаем данные для отображения
        data = data_processor.clean_data_for_excel(data)
        
        return render_template('table.html',
                             table_name=table_name,
                             table_info=table_info,
                             data=data,
                             page=page,
                             per_page=per_page)
    except Exception as e:
        logger.error(f"Ошибка загрузки таблицы {table_name}: {e}")
        return render_template('error.html', error=str(e))

@main_bp.route('/scripts')
def scripts_view():
    """Страница управления скриптами"""
    try:
        return render_template('scripts.html')
    except Exception as e:
        logger.error(f"Ошибка загрузки страницы скриптов: {e}")
        return render_template('error.html', error=str(e))

@main_bp.route('/duplicates')
def duplicates_view():
    """Страница дубликатов"""
    try:
        duplicates = data_processor.get_duplicates_count()
        return render_template('duplicates.html', duplicates=duplicates)
    except Exception as e:
        logger.error(f"Ошибка загрузки страницы дубликатов: {e}")
        return render_template('error.html', error=str(e))

@main_bp.route('/duplicates/<table_name>')
def duplicates_table_view(table_name):
    """Просмотр дубликатов в конкретной таблице"""
    try:
        from ..utils.text_utils import ValidationUtils
        if not ValidationUtils.is_valid_table_name(table_name):
            return render_template('error.html', error="Недопустимое имя таблицы")
        
        # Получаем данные дубликатов
        data = db_manager.get_table_data(table_name, limit=1000)
        
        return render_template('duplicates_table.html',
                             table_name=table_name,
                             data=data)
    except Exception as e:
        logger.error(f"Ошибка загрузки дубликатов {table_name}: {e}")
        return render_template('error.html', error=str(e))

@main_bp.route('/upload', methods=['GET', 'POST'])
def upload_view():
    """Страница загрузки файлов"""
    try:
        if request.method == 'POST':
            # Обработка загрузки файла
            if 'file' not in request.files:
                return jsonify({'error': 'Файл не выбран'}), 400
            
            file = request.files['file']
            if file.filename == '':
                return jsonify({'error': 'Файл не выбран'}), 400
            
            # Здесь должна быть логика обработки файла
            return jsonify({'message': 'Файл успешно загружен'})
        
        return render_template('upload.html')
    except Exception as e:
        logger.error(f"Ошибка загрузки файла: {e}")
        return render_template('error.html', error=str(e))

@main_bp.route('/logs')
def logs_view():
    """Страница логов"""
    try:
        return render_template('logs.html')
    except Exception as e:
        logger.error(f"Ошибка загрузки страницы логов: {e}")
        return render_template('error.html', error=str(e))

@main_bp.route('/reports')
def reports_view():
    """Страница отчетов"""
    try:
        return render_template('reports.html')
    except Exception as e:
        logger.error(f"Ошибка загрузки страницы отчетов: {e}")
        return render_template('error.html', error=str(e))

@main_bp.route('/backups')
def backups_view():
    """Страница бэкапов"""
    try:
        return render_template('backups.html')
    except Exception as e:
        logger.error(f"Ошибка загрузки страницы бэкапов: {e}")
        return render_template('error.html', error=str(e))
