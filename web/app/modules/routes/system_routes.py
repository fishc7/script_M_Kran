#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Модуль для системных функций и диагностики
"""

from flask import Blueprint, request, jsonify, render_template
from ..database.db_manager import db_manager
from ..utils.text_utils import ValidationUtils
import logging
import os
import subprocess
import platform
import psutil
from pathlib import Path

logger = logging.getLogger(__name__)

# Создаем Blueprint для системных функций
system_bp = Blueprint('system', __name__)

@system_bp.route('/debug_table/<table_name>')
def debug_table(table_name):
    """Отладочная информация о таблице"""
    try:
        if not ValidationUtils.is_valid_table_name(table_name):
            return render_template('error.html', error="Недопустимое имя таблицы")
        
        # Получаем детальную информацию о таблице
        table_info = db_manager.get_table_info(table_name)
        
        # Получаем структуру таблицы
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info(`{table_name}`)")
            columns = [dict(row) for row in cursor.fetchall()]
            
            # Получаем индексы
            cursor.execute(f"PRAGMA index_list(`{table_name}`)")
            indexes = [dict(row) for row in cursor.fetchall()]
            
            # Получаем статистику
            cursor.execute(f"SELECT COUNT(*) as total FROM `{table_name}`")
            total_count = cursor.fetchone()['total']
            
            # Получаем примеры данных
            cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 5")
            sample_data = [dict(row) for row in cursor.fetchall()]
        
        return render_template('debug_table.html',
                             table_name=table_name,
                             table_info=table_info,
                             columns=columns,
                             indexes=indexes,
                             total_count=total_count,
                             sample_data=sample_data)
        
    except Exception as e:
        logger.error(f"Ошибка отладки таблицы {table_name}: {e}")
        return render_template('error.html', error=str(e))

@system_bp.route('/system_logs')
def system_logs():
    """Системные логи"""
    try:
        # Получаем системную информацию
        system_info = {
            'platform': platform.platform(),
            'python_version': platform.python_version(),
            'cpu_count': psutil.cpu_count(),
            'memory_total': psutil.virtual_memory().total,
            'memory_available': psutil.virtual_memory().available,
            'disk_usage': psutil.disk_usage('/').percent if os.name != 'nt' else psutil.disk_usage('C:').percent
        }
        
        # Получаем логи из базы данных
        logs = db_manager.execute_query("""
            SELECT action, description, timestamp 
            FROM system_activities 
            ORDER BY timestamp DESC 
            LIMIT 100
        """)
        
        return render_template('system_logs.html',
                             system_info=system_info,
                             logs=logs.to_dict('records'))
        
    except Exception as e:
        logger.error(f"Ошибка получения системных логов: {e}")
        return render_template('error.html', error=str(e))

@system_bp.route('/logs_lnk_table')
def logs_lnk_table():
    """Таблица логов LNK"""
    try:
        # Получаем данные из таблицы logs_lnk
        data = db_manager.get_table_data('logs_lnk', limit=1000)
        
        # Получаем статистику
        stats = db_manager.execute_query("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT `Титул`) as unique_titles,
                COUNT(DISTINCT `Линия`) as unique_lines,
                MIN(`Дата сварки`) as earliest_date,
                MAX(`Дата сварки`) as latest_date
            FROM logs_lnk
        """)
        
        return render_template('logs_lnk_table.html',
                             data=data,
                             stats=stats.to_dict('records')[0] if not stats.empty else {})
        
    except Exception as e:
        logger.error(f"Ошибка получения таблицы logs_lnk: {e}")
        return render_template('error.html', error=str(e))

@system_bp.route('/enhanced_filter')
def enhanced_filter():
    """Расширенный фильтр"""
    try:
        # Получаем список таблиц для фильтрации
        tables = db_manager.get_tables_list()
        
        # Получаем основные таблицы
        main_tables = [t for t in tables if t in [
            'logs_lnk', 'wl_china', 'wl_report_smr', 
            'condition_weld', 'pipeline_weld_joint_iso'
        ]]
        
        return render_template('enhanced_filter.html',
                             tables=tables,
                             main_tables=main_tables)
        
    except Exception as e:
        logger.error(f"Ошибка загрузки расширенного фильтра: {e}")
        return render_template('error.html', error=str(e))

@system_bp.route('/filtered_data/<filter_type>')
def filtered_data(filter_type):
    """Отфильтрованные данные по типу"""
    try:
        # Валидация типа фильтра
        valid_filters = ['duplicates', 'errors', 'warnings', 'recent']
        if filter_type not in valid_filters:
            return render_template('error.html', error="Недопустимый тип фильтра")
        
        data = None
        title = ""
        
        if filter_type == 'duplicates':
            # Получаем дубликаты
            data = db_manager.execute_query("""
                SELECT 'wl_china' as table_name, COUNT(*) as count 
                FROM duplicates_wl_china
                UNION ALL
                SELECT 'wl_report_smr' as table_name, COUNT(*) as count 
                FROM duplicates_wl_report_smr
            """)
            title = "Дубликаты"
            
        elif filter_type == 'errors':
            # Получаем записи с ошибками
            data = db_manager.execute_query("""
                SELECT * FROM logs_lnk 
                WHERE `Статус` LIKE '%ошибка%' OR `Статус` LIKE '%error%'
                LIMIT 100
            """)
            title = "Записи с ошибками"
            
        elif filter_type == 'warnings':
            # Получаем записи с предупреждениями
            data = db_manager.execute_query("""
                SELECT * FROM logs_lnk 
                WHERE `Статус` LIKE '%предупреждение%' OR `Статус` LIKE '%warning%'
                LIMIT 100
            """)
            title = "Записи с предупреждениями"
            
        elif filter_type == 'recent':
            # Получаем недавние записи
            data = db_manager.execute_query("""
                SELECT * FROM logs_lnk 
                ORDER BY `Дата сварки` DESC 
                LIMIT 100
            """)
            title = "Недавние записи"
        
        return render_template('filtered_data.html',
                             data=data,
                             filter_type=filter_type,
                             title=title)
        
    except Exception as e:
        logger.error(f"Ошибка получения отфильтрованных данных {filter_type}: {e}")
        return render_template('error.html', error=str(e))

@system_bp.route('/vue')
def vue_app():
    """Vue.js приложение"""
    try:
        return render_template('vue_app.html')
    except Exception as e:
        logger.error(f"Ошибка загрузки Vue приложения: {e}")
        return render_template('error.html', error=str(e))

@system_bp.route('/nk_results')
def nk_results():
    """Результаты НК"""
    try:
        # Получаем данные из таблицы NDT_Findings_Transmission_Register
        data = db_manager.get_table_data('NDT_Findings_Transmission_Register', limit=1000)
        
        # Получаем статистику
        stats = db_manager.execute_query("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT `Result`) as unique_results,
                COUNT(DISTINCT `Type_of_Control_NDT`) as control_types,
                MIN(`Date_Control`) as earliest_date,
                MAX(`Date_Control`) as latest_date
            FROM NDT_Findings_Transmission_Register
        """)
        
        return render_template('nk_results.html',
                             data=data,
                             stats=stats.to_dict('records')[0] if not stats.empty else {})
        
    except Exception as e:
        logger.error(f"Ошибка получения результатов НК: {e}")
        return render_template('error.html', error=str(e))

@system_bp.route('/id_formation')
def id_formation():
    """Формирование ID"""
    try:
        return render_template('id_formation.html')
    except Exception as e:
        logger.error(f"Ошибка загрузки страницы формирования ID: {e}")
        return render_template('error.html', error=str(e))

@system_bp.route('/folder_formation')
def folder_formation():
    """Формирование папок"""
    try:
        return render_template('folder_formation.html')
    except Exception as e:
        logger.error(f"Ошибка загрузки страницы формирования папок: {e}")
        return render_template('error.html', error=str(e))

@system_bp.route('/joint_manager')
def joint_manager():
    """Менеджер соединений"""
    try:
        return render_template('joint_manager.html')
    except Exception as e:
        logger.error(f"Ошибка загрузки менеджера соединений: {e}")
        return render_template('error.html', error=str(e))
