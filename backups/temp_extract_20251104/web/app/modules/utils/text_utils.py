#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Утилиты для приложения M_Kran
"""

import os
import re
import unicodedata
from datetime import datetime
from typing import Any, Optional
from werkzeug.utils import secure_filename
import logging

logger = logging.getLogger(__name__)

class TextUtils:
    """Утилиты для работы с текстом"""
    
    @staticmethod
    def extract_titul_from_iso_string(iso_string: str) -> Optional[str]:
        """Извлекает титул из строки ISO"""
        if not iso_string:
            return None
        
        # Паттерн для извлечения номера титула
        pattern = r'(\d{5}-\d{2})'
        match = re.search(pattern, iso_string)
        
        if match:
            return match.group(1)
        
        return None
    
    @staticmethod
    def safe_encode_value(value: Any) -> str:
        """Безопасное кодирование значения"""
        if value is None:
            return ''
        
        try:
            if isinstance(value, (int, float)):
                return str(value)
            
            # Нормализуем Unicode
            if isinstance(value, str):
                normalized = unicodedata.normalize('NFKC', value)
                return normalized.encode('utf-8', errors='ignore').decode('utf-8')
            
            return str(value)
        except Exception as e:
            logger.warning(f"Ошибка кодирования значения {value}: {e}")
            return str(value)
    
    @staticmethod
    def clean_filename(filename: str) -> str:
        """Очищает имя файла от недопустимых символов"""
        if not filename:
            return 'unnamed_file'
        
        # Удаляем недопустимые символы
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        
        # Ограничиваем длину
        if len(filename) > 200:
            name, ext = os.path.splitext(filename)
            filename = name[:200-len(ext)] + ext
        
        return filename

class FileUtils:
    """Утилиты для работы с файлами"""
    
    @staticmethod
    def get_filename_timestamp() -> str:
        """Генерирует временную метку для имени файла"""
        return datetime.now().strftime('%Y%m%d_%H%M%S')
    
    @staticmethod
    def safe_filename(filename: str) -> str:
        """Безопасное имя файла"""
        return secure_filename(filename)
    
    @staticmethod
    def get_unique_filename(directory: str, filename: str) -> str:
        """Получает уникальное имя файла"""
        if not os.path.exists(os.path.join(directory, filename)):
            return filename
        
        name, ext = os.path.splitext(filename)
        counter = 1
        
        while True:
            new_filename = f"{name}_{counter}{ext}"
            if not os.path.exists(os.path.join(directory, new_filename)):
                return new_filename
            counter += 1
    
    @staticmethod
    def ensure_directory(directory: str) -> bool:
        """Создает директорию если она не существует"""
        try:
            os.makedirs(directory, exist_ok=True)
            return True
        except Exception as e:
            logger.error(f"Ошибка создания директории {directory}: {e}")
            return False

class DataUtils:
    """Утилиты для работы с данными"""
    
    @staticmethod
    def format_number(value: Any) -> str:
        """Форматирует число с разделителями тысяч"""
        if value is None:
            return '0'
        
        try:
            if isinstance(value, str):
                value = float(value)
            return f"{value:,.0f}".replace(',', ' ')
        except (ValueError, TypeError):
            return str(value)
    
    @staticmethod
    def safe_int(value: Any, default: int = 0) -> int:
        """Безопасное преобразование в int"""
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    
    @staticmethod
    def safe_float(value: Any, default: float = 0.0) -> float:
        """Безопасное преобразование в float"""
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

class ValidationUtils:
    """Утилиты для валидации"""
    
    @staticmethod
    def is_valid_table_name(table_name: str) -> bool:
        """Проверяет валидность имени таблицы"""
        if not table_name:
            return False
        
        # Проверяем на SQL инъекции
        dangerous_patterns = [
            r'[;\'"]',
            r'--',
            r'/\*',
            r'\*/',
            r'union\s+select',
            r'drop\s+table',
            r'delete\s+from',
            r'insert\s+into',
            r'update\s+set'
        ]
        
        table_name_lower = table_name.lower()
        for pattern in dangerous_patterns:
            if re.search(pattern, table_name_lower):
                return False
        
        return True
    
    @staticmethod
    def is_valid_filename(filename: str) -> bool:
        """Проверяет валидность имени файла"""
        if not filename:
            return False
        
        # Проверяем на недопустимые символы
        invalid_chars = r'[<>:"/\\|?*\x00-\x1f]'
        if re.search(invalid_chars, filename):
            return False
        
        # Проверяем длину
        if len(filename) > 255:
            return False
        
        return True

class DateUtils:
    """Утилиты для работы с датами"""
    
    @staticmethod
    def format_datetime(dt: datetime) -> str:
        """Форматирует дату и время"""
        return dt.strftime('%d.%m.%Y %H:%M:%S')
    
    @staticmethod
    def format_date(dt: datetime) -> str:
        """Форматирует дату"""
        return dt.strftime('%d.%m.%Y')
    
    @staticmethod
    def parse_date(date_str: str) -> Optional[datetime]:
        """Парсит дату из строки"""
        if not date_str:
            return None
        
        formats = [
            '%Y-%m-%d',
            '%d.%m.%Y',
            '%d/%m/%Y',
            '%Y-%m-%d %H:%M:%S',
            '%d.%m.%Y %H:%M:%S'
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        return None
