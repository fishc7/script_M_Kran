#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Модуль для работы с файлами и экспортом данных
"""

from flask import Blueprint, request, jsonify, send_file, send_from_directory
from ..database.db_manager import db_manager, data_processor
from ..utils.text_utils import ValidationUtils, FileUtils
import logging
import pandas as pd
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# Создаем Blueprint для работы с файлами
files_bp = Blueprint('files', __name__)

@files_bp.route('/export/<table_name>')
def export_table(table_name):
    """Экспорт таблицы в Excel"""
    try:
        if not ValidationUtils.is_valid_table_name(table_name):
            return jsonify({'error': 'Недопустимое имя таблицы'}), 400
        
        # Получаем данные из таблицы
        data = db_manager.get_table_data(table_name, limit=10000)
        
        if data.empty:
            return jsonify({'error': 'Таблица пуста'}), 404
        
        # Очищаем данные для экспорта
        data = data_processor.clean_data_for_excel(data)
        
        # Создаем временный файл
        filename = f"{table_name}_{FileUtils.get_filename_timestamp()}.xlsx"
        filepath = Path("results") / filename
        
        # Создаем директорию если не существует
        FileUtils.ensure_directory("results")
        
        # Сохраняем в Excel
        data.to_excel(filepath, index=False, engine='openpyxl')
        
        return send_file(filepath, as_attachment=True, download_name=filename)
        
    except Exception as e:
        logger.error(f"Ошибка экспорта таблицы {table_name}: {e}")
        return jsonify({'error': str(e)}), 500

@files_bp.route('/export_filtered/<table_name>')
def export_filtered_table(table_name):
    """Экспорт отфильтрованной таблицы"""
    try:
        if not ValidationUtils.is_valid_table_name(table_name):
            return jsonify({'error': 'Недопустимое имя таблицы'}), 400
        
        # Получаем параметры фильтрации
        search_term = request.args.get('search', '')
        columns = request.args.getlist('columns')
        
        if search_term:
            # Выполняем поиск
            data = db_manager.search_in_table(table_name, search_term, columns, limit=10000)
        else:
            # Получаем все данные
            data = db_manager.get_table_data(table_name, limit=10000)
        
        if data.empty:
            return jsonify({'error': 'Данные не найдены'}), 404
        
        # Очищаем данные для экспорта
        data = data_processor.clean_data_for_excel(data)
        
        # Создаем временный файл
        filename = f"{table_name}_filtered_{FileUtils.get_filename_timestamp()}.xlsx"
        filepath = Path("results") / filename
        
        # Создаем директорию если не существует
        FileUtils.ensure_directory("results")
        
        # Сохраняем в Excel
        data.to_excel(filepath, index=False, engine='openpyxl')
        
        return send_file(filepath, as_attachment=True, download_name=filename)
        
    except Exception as e:
        logger.error(f"Ошибка экспорта отфильтрованной таблицы {table_name}: {e}")
        return jsonify({'error': str(e)}), 500

@files_bp.route('/download/<filename>')
def download_file(filename):
    """Скачивание файла"""
    try:
        if not ValidationUtils.is_valid_filename(filename):
            return jsonify({'error': 'Недопустимое имя файла'}), 400
        
        # Безопасное имя файла
        safe_filename = FileUtils.safe_filename(filename)
        
        # Проверяем существование файла
        filepath = Path("results") / safe_filename
        if not filepath.exists():
            return jsonify({'error': 'Файл не найден'}), 404
        
        return send_file(filepath, as_attachment=True)
        
    except Exception as e:
        logger.error(f"Ошибка скачивания файла {filename}: {e}")
        return jsonify({'error': str(e)}), 500

@files_bp.route('/delete_file/<filename>', methods=['POST'])
def delete_file(filename):
    """Удаление файла"""
    try:
        if not ValidationUtils.is_valid_filename(filename):
            return jsonify({'error': 'Недопустимое имя файла'}), 400
        
        # Безопасное имя файла
        safe_filename = FileUtils.safe_filename(filename)
        
        # Проверяем существование файла
        filepath = Path("results") / safe_filename
        if not filepath.exists():
            return jsonify({'error': 'Файл не найден'}), 404
        
        # Удаляем файл
        filepath.unlink()
        
        return jsonify({'message': f'Файл {filename} успешно удален'})
        
    except Exception as e:
        logger.error(f"Ошибка удаления файла {filename}: {e}")
        return jsonify({'error': str(e)}), 500

@files_bp.route('/export_filtered_data', methods=['POST'])
def export_filtered_data():
    """Экспорт отфильтрованных данных с параметрами из POST"""
    try:
        data = request.get_json()
        table_name = data.get('table_name')
        filters = data.get('filters', {})
        
        if not table_name or not ValidationUtils.is_valid_table_name(table_name):
            return jsonify({'error': 'Недопустимое имя таблицы'}), 400
        
        # Здесь должна быть логика применения фильтров
        # Пока просто экспортируем всю таблицу
        export_data = db_manager.get_table_data(table_name, limit=10000)
        
        if export_data.empty:
            return jsonify({'error': 'Данные не найдены'}), 404
        
        # Очищаем данные для экспорта
        export_data = data_processor.clean_data_for_excel(export_data)
        
        # Создаем временный файл
        filename = f"{table_name}_custom_export_{FileUtils.get_filename_timestamp()}.xlsx"
        filepath = Path("results") / filename
        
        # Создаем директорию если не существует
        FileUtils.ensure_directory("results")
        
        # Сохраняем в Excel
        export_data.to_excel(filepath, index=False, engine='openpyxl')
        
        return jsonify({
            'message': 'Экспорт выполнен успешно',
            'filename': filename,
            'download_url': f'/download/{filename}'
        })
        
    except Exception as e:
        logger.error(f"Ошибка экспорта отфильтрованных данных: {e}")
        return jsonify({'error': str(e)}), 500

@files_bp.route('/export_cards_pdf', methods=['POST'])
def export_cards_pdf():
    """Экспорт карточек в PDF"""
    try:
        data = request.get_json()
        table_name = data.get('table_name')
        record_ids = data.get('record_ids', [])
        
        if not table_name or not ValidationUtils.is_valid_table_name(table_name):
            return jsonify({'error': 'Недопустимое имя таблицы'}), 400
        
        # Здесь должна быть логика генерации PDF
        # Пока возвращаем заглушку
        return jsonify({
            'message': 'Экспорт в PDF будет реализован в следующих версиях',
            'status': 'not_implemented'
        })
        
    except Exception as e:
        logger.error(f"Ошибка экспорта в PDF: {e}")
        return jsonify({'error': str(e)}), 500
