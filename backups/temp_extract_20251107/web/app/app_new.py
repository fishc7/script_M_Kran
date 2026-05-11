#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Основной файл приложения M_Kran - Рефакторированная версия
"""

import sys
import os
import logging
from flask import Flask, render_template
from werkzeug.utils import secure_filename

# Настройка кодировки
from modules.config import Config
Config.setup_encoding()

# Импорт модулей
from modules.routes.main_routes import main_bp
from modules.routes.files_routes import files_bp
from modules.routes.scripts_routes import scripts_bp
from modules.routes.system_routes import system_bp
from modules.api.api_routes import api_bp
from modules.api.extended_api_routes import extended_api_bp
from modules.database.db_manager import db_manager

# Создание приложения Flask
app = Flask(__name__, 
           template_folder=Config.TEMPLATE_DIR,
           static_folder=Config.STATIC_DIR)

# Конфигурация приложения
app.config.from_object(Config)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Регистрация Blueprint'ов
app.register_blueprint(main_bp)
app.register_blueprint(files_bp)
app.register_blueprint(scripts_bp)
app.register_blueprint(system_bp)
app.register_blueprint(api_bp)
app.register_blueprint(extended_api_bp)

# Добавляем кастомные фильтры для Jinja2
from modules.utils.text_utils import DataUtils

@app.template_filter('format_number')
def format_number(value):
    """Форматирует число с разделителями тысяч"""
    return DataUtils.format_number(value)

@app.template_filter('safe_int')
def safe_int(value, default=0):
    """Безопасное преобразование в int"""
    return DataUtils.safe_int(value, default)

@app.template_filter('safe_float')
def safe_float(value, default=0.0):
    """Безопасное преобразование в float"""
    return DataUtils.safe_float(value, default)

# Настройка заголовков безопасности
import secrets

@app.after_request
def add_security_headers(response):
    """Добавляет заголовки безопасности"""
    from modules.config import SecurityConfig
    
    # Генерируем уникальный nonce для каждого запроса
    nonce = secrets.token_urlsafe(16)
    response.set_cookie('nonce', nonce)
    
    # Устанавливаем CSP заголовки
    response.headers['Content-Security-Policy'] = SecurityConfig.CSP_POLICY
    
    # Кэш-контроль
    response.headers['Cache-Control'] = SecurityConfig.CACHE_CONTROL
    response.headers['Pragma'] = SecurityConfig.PRAGMA
    response.headers['Expires'] = SecurityConfig.EXPIRES
    
    # CORS заголовки
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
    
    # Устанавливаем правильную кодировку UTF-8
    if response.content_type and 'text/html' in response.content_type:
        response.headers['Content-Type'] = 'text/html; charset=utf-8'
    
    return response

# Обработка ошибок
@app.errorhandler(404)
def not_found_error(error):
    """Обработка ошибки 404"""
    logger.warning(f"404 ошибка: {request.url}")
    return render_template('error.html', 
                         error='Страница не найдена',
                         error_code=404), 404

@app.errorhandler(500)
def internal_error(error):
    """Обработка ошибки 500"""
    logger.error(f"500 ошибка: {error}")
    return render_template('error.html', 
                         error='Внутренняя ошибка сервера',
                         error_code=500), 500

@app.errorhandler(400)
def bad_request_error(error):
    """Обработка ошибки 400"""
    logger.warning(f"400 ошибка: {error}")
    return render_template('error.html', 
                         error='Некорректный запрос',
                         error_code=400), 400

# Инициализация приложения
def init_app():
    """Инициализация приложения"""
    try:
        # Проверяем подключение к БД
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1")
            cursor.fetchone()
        
        logger.info("Приложение M_Kran успешно инициализировано")
        logger.info(f"База данных: {Config.DATABASE_PATH}")
        logger.info(f"Шаблоны: {Config.TEMPLATE_DIR}")
        logger.info(f"Статические файлы: {Config.STATIC_DIR}")
        
    except Exception as e:
        logger.error(f"Ошибка инициализации приложения: {e}")
        raise

if __name__ == '__main__':
    # Инициализация
    init_app()
    
    # Запуск приложения
    app.run(host='127.0.0.1', port=5000, debug=True)
