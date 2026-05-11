#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Дополнительные API маршруты для приложения M_Kran
"""

from flask import Blueprint, request, jsonify
from ..database.db_manager import db_manager
from ..utils.text_utils import ValidationUtils, TextUtils
import logging
import pandas as pd
import re

logger = logging.getLogger(__name__)

# Создаем Blueprint для дополнительных API
extended_api_bp = Blueprint('extended_api', __name__, url_prefix='/api')

@extended_api_bp.route('/extract_numbers', methods=['POST'])
def extract_numbers():
    """Извлечение номеров из текста"""
    try:
        data = request.get_json()
        text = data.get('text', '')
        
        if not text:
            return jsonify({'error': 'Текст не указан'}), 400
        
        # Извлекаем различные типы номеров
        numbers = {
            'iso_numbers': re.findall(r'GCC-NAG-DDD-\d+-\d+-\d+-TK-ISO-\d+', text),
            'line_numbers': re.findall(r'\d{3}-[A-Z]{2}-\d{4}', text),
            'joint_numbers': re.findall(r'S\d+', text),
            'title_numbers': re.findall(r'\d{5}-\d{2}', text),
            'all_numbers': re.findall(r'\d+', text)
        }
        
        return jsonify(numbers)
        
    except Exception as e:
        logger.error(f"Ошибка извлечения номеров: {e}")
        return jsonify({'error': str(e)}), 500

@extended_api_bp.route('/process_wl_china_numbers', methods=['POST'])
def process_wl_china_numbers():
    """Обработка номеров из таблицы wl_china"""
    try:
        data = request.get_json()
        operation = data.get('operation', 'extract')
        
        if operation == 'extract':
            # Извлекаем номера из таблицы wl_china
            query = """
                SELECT DISTINCT `ISO`, `Линия`, `Стык` 
                FROM wl_china 
                WHERE `ISO` IS NOT NULL
                LIMIT 1000
            """
            result = db_manager.execute_query(query)
            
            return jsonify({
                'message': 'Номера извлечены успешно',
                'count': len(result),
                'data': result.to_dict('records')
            })
        
        elif operation == 'validate':
            # Валидация номеров
            query = """
                SELECT `ISO`, `Линия`, `Стык`
                FROM wl_china 
                WHERE `ISO` IS NOT NULL
                AND (`ISO` NOT LIKE 'GCC-NAG-DDD-%' OR `Линия` NOT LIKE '___-__-____')
            """
            invalid_records = db_manager.execute_query(query)
            
            return jsonify({
                'message': 'Валидация завершена',
                'invalid_count': len(invalid_records),
                'invalid_records': invalid_records.to_dict('records')
            })
        
        else:
            return jsonify({'error': 'Недопустимая операция'}), 400
            
    except Exception as e:
        logger.error(f"Ошибка обработки номеров wl_china: {e}")
        return jsonify({'error': str(e)}), 500

@extended_api_bp.route('/process_table_numbers', methods=['POST'])
def process_table_numbers():
    """Обработка номеров из произвольной таблицы"""
    try:
        data = request.get_json()
        table_name = data.get('table_name')
        operation = data.get('operation', 'extract')
        
        if not table_name or not ValidationUtils.is_valid_table_name(table_name):
            return jsonify({'error': 'Недопустимое имя таблицы'}), 400
        
        # Получаем структуру таблицы
        table_info = db_manager.get_table_info(table_name)
        columns = [col['name'] for col in table_info['columns']]
        
        # Ищем колонки с номерами
        number_columns = [col for col in columns if any(keyword in col.lower() 
                          for keyword in ['iso', 'линия', 'стык', 'номер', 'number'])]
        
        if not number_columns:
            return jsonify({'error': 'Колонки с номерами не найдены'}), 404
        
        # Формируем запрос
        columns_str = ', '.join([f'`{col}`' for col in number_columns])
        query = f"SELECT {columns_str} FROM `{table_name}` LIMIT 1000"
        
        result = db_manager.execute_query(query)
        
        return jsonify({
            'message': f'Номера из таблицы {table_name} обработаны',
            'table_name': table_name,
            'number_columns': number_columns,
            'count': len(result),
            'data': result.to_dict('records')
        })
        
    except Exception as e:
        logger.error(f"Ошибка обработки номеров таблицы: {e}")
        return jsonify({'error': str(e)}), 500

@extended_api_bp.route('/get_drawings')
def get_drawings():
    """Получение списка чертежей"""
    try:
        # Получаем чертежи из различных таблиц
        drawings_query = """
            SELECT DISTINCT `Drawing_Number` as drawing_number, 'NDT_Findings_Transmission_Register' as source_table
            FROM NDT_Findings_Transmission_Register 
            WHERE `Drawing_Number` IS NOT NULL AND `Drawing_Number` != '-'
            UNION
            SELECT DISTINCT `ISO` as drawing_number, 'wl_china' as source_table
            FROM wl_china 
            WHERE `ISO` IS NOT NULL
            LIMIT 1000
        """
        
        drawings = db_manager.execute_query(drawings_query)
        
        return jsonify({
            'drawings': drawings.to_dict('records'),
            'count': len(drawings)
        })
        
    except Exception as e:
        logger.error(f"Ошибка получения чертежей: {e}")
        return jsonify({'error': str(e)}), 500

@extended_api_bp.route('/get_lines')
def get_lines():
    """Получение списка линий"""
    try:
        # Получаем линии из различных таблиц
        lines_query = """
            SELECT DISTINCT `Линия` as line_number, 'logs_lnk' as source_table
            FROM logs_lnk 
            WHERE `Линия` IS NOT NULL
            UNION
            SELECT DISTINCT `Линия` as line_number, 'wl_china' as source_table
            FROM wl_china 
            WHERE `Линия` IS NOT NULL
            ORDER BY line_number
            LIMIT 1000
        """
        
        lines = db_manager.execute_query(lines_query)
        
        return jsonify({
            'lines': lines.to_dict('records'),
            'count': len(lines)
        })
        
    except Exception as e:
        logger.error(f"Ошибка получения линий: {e}")
        return jsonify({'error': str(e)}), 500

@extended_api_bp.route('/get_lines_by_drawing', methods=['POST'])
def get_lines_by_drawing():
    """Получение линий по чертежу"""
    try:
        data = request.get_json()
        drawing_number = data.get('drawing_number')
        
        if not drawing_number:
            return jsonify({'error': 'Номер чертежа не указан'}), 400
        
        query = """
            SELECT DISTINCT `Линия` as line_number
            FROM logs_lnk 
            WHERE `ISO` LIKE ?
            ORDER BY line_number
        """
        
        lines = db_manager.execute_query(query, (f'%{drawing_number}%',))
        
        return jsonify({
            'drawing_number': drawing_number,
            'lines': lines.to_dict('records'),
            'count': len(lines)
        })
        
    except Exception as e:
        logger.error(f"Ошибка получения линий по чертежу: {e}")
        return jsonify({'error': str(e)}), 500

@extended_api_bp.route('/get_drawings_by_line', methods=['POST'])
def get_drawings_by_line():
    """Получение чертежей по линии"""
    try:
        data = request.get_json()
        line_number = data.get('line_number')
        
        if not line_number:
            return jsonify({'error': 'Номер линии не указан'}), 400
        
        query = """
            SELECT DISTINCT `ISO` as drawing_number
            FROM logs_lnk 
            WHERE `Линия` = ?
            ORDER BY drawing_number
        """
        
        drawings = db_manager.execute_query(query, (line_number,))
        
        return jsonify({
            'line_number': line_number,
            'drawings': drawings.to_dict('records'),
            'count': len(drawings)
        })
        
    except Exception as e:
        logger.error(f"Ошибка получения чертежей по линии: {e}")
        return jsonify({'error': str(e)}), 500

@extended_api_bp.route('/get_weld_types')
def get_weld_types():
    """Получение типов сварки"""
    try:
        query = """
            SELECT DISTINCT `Тип сварки` as weld_type, COUNT(*) as count
            FROM logs_lnk 
            WHERE `Тип сварки` IS NOT NULL
            GROUP BY `Тип сварки`
            ORDER BY count DESC
        """
        
        weld_types = db_manager.execute_query(query)
        
        return jsonify({
            'weld_types': weld_types.to_dict('records'),
            'count': len(weld_types)
        })
        
    except Exception as e:
        logger.error(f"Ошибка получения типов сварки: {e}")
        return jsonify({'error': str(e)}), 500

@extended_api_bp.route('/get_deletion_codes')
def get_deletion_codes():
    """Получение кодов удаления"""
    try:
        query = """
            SELECT `Код`, `Описание`, `Русское_описание`
            FROM коды_удаления
            ORDER BY `Код`
        """
        
        codes = db_manager.execute_query(query)
        
        return jsonify({
            'deletion_codes': codes.to_dict('records'),
            'count': len(codes)
        })
        
    except Exception as e:
        logger.error(f"Ошибка получения кодов удаления: {e}")
        return jsonify({'error': str(e)}), 500

@extended_api_bp.route('/debug_table_structure')
def debug_table_structure():
    """Отладочная информация о структуре таблиц"""
    try:
        tables = db_manager.get_tables_list()
        structure_info = {}
        
        for table in tables[:10]:  # Ограничиваем первыми 10 таблицами
            try:
                table_info = db_manager.get_table_info(table)
                structure_info[table] = {
                    'columns': table_info['columns'],
                    'count': table_info['count']
                }
            except Exception as e:
                structure_info[table] = {'error': str(e)}
        
        return jsonify({
            'tables_structure': structure_info,
            'total_tables': len(tables)
        })
        
    except Exception as e:
        logger.error(f"Ошибка получения структуры таблиц: {e}")
        return jsonify({'error': str(e)}), 500

@extended_api_bp.route('/get_table_columns/<table_name>')
def get_table_columns(table_name):
    """Получение колонок таблицы"""
    try:
        if not ValidationUtils.is_valid_table_name(table_name):
            return jsonify({'error': 'Недопустимое имя таблицы'}), 400
        
        table_info = db_manager.get_table_info(table_name)
        
        return jsonify({
            'table_name': table_name,
            'columns': table_info['columns'],
            'count': table_info['count']
        })
        
    except Exception as e:
        logger.error(f"Ошибка получения колонок таблицы {table_name}: {e}")
        return jsonify({'error': str(e)}), 500
