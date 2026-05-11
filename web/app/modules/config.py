#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Конфигурация приложения M_Kran
"""

import os
import sys
import locale

class Config:
    """Основная конфигурация приложения"""
    
    # Основные настройки Flask
    SECRET_KEY = 'm_kran_secret_key_2025'
    JSON_AS_ASCII = False
    JSONIFY_PRETTYPRINT_REGULAR = True
    
    # Настройки кодировки
    ENCODING = 'utf-8'
    
    # Пути к файлам и папкам
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    TEMPLATE_DIR = os.path.join(BASE_DIR, 'templates')
    STATIC_DIR = os.path.join(BASE_DIR, 'static')
    DATABASE_DIR = os.path.join(BASE_DIR, '..', 'database', 'BD_Kingisepp')
    DATABASE_PATH = os.path.join(DATABASE_DIR, 'M_Kran_Kingesepp.db')
    UPLOADS_DIR = os.path.join(BASE_DIR, '..', 'uploads')
    LOGS_DIR = os.path.join(BASE_DIR, '..', 'logs')
    
    # Настройки загрузки файлов
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB
    ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv', 'json', 'zip'}
    
    # Настройки пагинации
    DEFAULT_PAGE_SIZE = 50
    MAX_PAGE_SIZE = 1000
    
    # Настройки экспорта
    EXPORT_CHUNK_SIZE = 1000
    
    @staticmethod
    def setup_encoding():
        """Настройка кодировки для Windows"""
        if sys.platform.startswith('win'):
            # Устанавливаем локаль для корректной работы с русскими символами
            try:
                locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')
            except:
                try:
                    locale.setlocale(locale.LC_ALL, 'Russian_Russia.1251')
                except:
                    pass
            
            # Устанавливаем переменные окружения для кодировки
            os.environ['PYTHONIOENCODING'] = Config.ENCODING
            os.environ['PYTHONLEGACYWINDOWSSTDIO'] = '1'
            
            # Настраиваем стандартные потоки для UTF-8
            import codecs
            sys.stdout = codecs.getwriter(Config.ENCODING)(sys.stdout.detach())
            sys.stderr = codecs.getwriter(Config.ENCODING)(sys.stderr.detach())
            sys.stdin = codecs.getreader(Config.ENCODING)(sys.stdin.detach())

class DatabaseConfig:
    """Конфигурация базы данных"""
    
    # Основные таблицы
    MAIN_TABLES = [
        'pipeline_weld_joint_iso',
        'wl_china', 
        'wl_report_smr',
        'condition_weld',
        'logs_lnk',
        'NDT_Findings_Transmission_Register',
        'work_order_log_NDT',
        'weld_repair_log'
    ]
    
    # Таблицы для анализа дубликатов
    DUPLICATE_TABLES = [
        'duplicates_wl_china',
        'duplicates_wl_report_smr'
    ]
    
    # Системные таблицы
    SYSTEM_TABLES = [
        'LoadedFiles',
        'system_activities',
        'sqlite_sequence'
    ]

class APIConfig:
    """Конфигурация API"""
    
    # Лимиты запросов
    RATE_LIMIT_PER_MINUTE = 100
    
    # Таймауты
    REQUEST_TIMEOUT = 30
    DATABASE_TIMEOUT = 10
    
    # Размеры ответов
    MAX_RESPONSE_SIZE = 10 * 1024 * 1024  # 10MB
    DEFAULT_PAGE_SIZE = 50

class SecurityConfig:
    """Конфигурация безопасности"""
    
    # CSP заголовки
    CSP_POLICY = (
        "default-src 'self' *; "
        "script-src 'self' 'unsafe-inline' 'unsafe-eval' *; "
        "style-src 'self' 'unsafe-inline' *; "
        "img-src 'self' data: *; "
        "font-src 'self' *;"
    )
    
    # CORS настройки
    CORS_ORIGINS = ['*']
    CORS_METHODS = ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS']
    CORS_HEADERS = ['Content-Type', 'Authorization']
    
    # Кэш-контроль
    CACHE_CONTROL = 'no-cache, no-store, must-revalidate'
    PRAGMA = 'no-cache'
    EXPIRES = '0'
