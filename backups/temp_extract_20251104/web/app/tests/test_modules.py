#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit тесты для модулей приложения M_Kran
"""

import unittest
import sys
import os
from pathlib import Path

# Добавляем путь к модулям
sys.path.insert(0, str(Path(__file__).parent))

from modules.utils.text_utils import TextUtils, FileUtils, DataUtils, ValidationUtils
from modules.database.db_manager import DatabaseManager
from modules.config import Config

class TestTextUtils(unittest.TestCase):
    """Тесты для TextUtils"""
    
    def test_extract_titul_from_iso_string(self):
        """Тест извлечения титула из ISO строки"""
        # Тест с валидной ISO строкой
        iso_string = "GCC-NAG-DDD-12460-12-1500-TK-ISO-00001"
        result = TextUtils.extract_titul_from_iso_string(iso_string)
        self.assertEqual(result, "12460-12")
        
        # Тест с пустой строкой
        result = TextUtils.extract_titul_from_iso_string("")
        self.assertIsNone(result)
        
        # Тест с None
        result = TextUtils.extract_titul_from_iso_string(None)
        self.assertIsNone(result)
    
    def test_safe_encode_value(self):
        """Тест безопасного кодирования значений"""
        # Тест с числом
        result = TextUtils.safe_encode_value(123)
        self.assertEqual(result, "123")
        
        # Тест с None
        result = TextUtils.safe_encode_value(None)
        self.assertEqual(result, "")
        
        # Тест со строкой
        result = TextUtils.safe_encode_value("test")
        self.assertEqual(result, "test")
    
    def test_format_number(self):
        """Тест форматирования чисел"""
        # Тест с большим числом
        result = DataUtils.format_number(1234567)
        self.assertEqual(result, "1 234 567")
        
        # Тест с None
        result = DataUtils.format_number(None)
        self.assertEqual(result, "0")
        
        # Тест со строкой
        result = DataUtils.format_number("123")
        self.assertEqual(result, "123")

class TestFileUtils(unittest.TestCase):
    """Тесты для FileUtils"""
    
    def test_get_filename_timestamp(self):
        """Тест генерации временной метки"""
        timestamp = FileUtils.get_filename_timestamp()
        self.assertIsInstance(timestamp, str)
        self.assertEqual(len(timestamp), 15)  # YYYYMMDD_HHMMSS
    
    def test_safe_filename(self):
        """Тест безопасного имени файла"""
        # Тест с нормальным именем
        result = FileUtils.safe_filename("test_file.xlsx")
        self.assertEqual(result, "test_file.xlsx")
        
        # Тест с недопустимыми символами
        result = FileUtils.safe_filename("test<file>.xlsx")
        self.assertNotIn("<", result)
        self.assertNotIn(">", result)
    
    def test_get_unique_filename(self):
        """Тест получения уникального имени файла"""
        # Создаем временную директорию
        test_dir = Path("test_temp")
        test_dir.mkdir(exist_ok=True)
        
        try:
            # Тест с несуществующим файлом
            result = FileUtils.get_unique_filename(str(test_dir), "test.xlsx")
            self.assertEqual(result, "test.xlsx")
            
            # Создаем файл
            (test_dir / "test.xlsx").touch()
            
            # Тест с существующим файлом
            result = FileUtils.get_unique_filename(str(test_dir), "test.xlsx")
            self.assertEqual(result, "test_1.xlsx")
            
        finally:
            # Очищаем
            import shutil
            shutil.rmtree(test_dir, ignore_errors=True)

class TestValidationUtils(unittest.TestCase):
    """Тесты для ValidationUtils"""
    
    def test_is_valid_table_name(self):
        """Тест валидации имени таблицы"""
        # Валидные имена
        self.assertTrue(ValidationUtils.is_valid_table_name("test_table"))
        self.assertTrue(ValidationUtils.is_valid_table_name("table123"))
        
        # Невалидные имена
        self.assertFalse(ValidationUtils.is_valid_table_name(""))
        self.assertFalse(ValidationUtils.is_valid_table_name(None))
        self.assertFalse(ValidationUtils.is_valid_table_name("table; DROP TABLE users;"))
        self.assertFalse(ValidationUtils.is_valid_table_name("table'"))
    
    def test_is_valid_filename(self):
        """Тест валидации имени файла"""
        # Валидные имена
        self.assertTrue(ValidationUtils.is_valid_filename("test.xlsx"))
        self.assertTrue(ValidationUtils.is_valid_filename("file_123.csv"))
        
        # Невалидные имена
        self.assertFalse(ValidationUtils.is_valid_filename(""))
        self.assertFalse(ValidationUtils.is_valid_filename(None))
        self.assertFalse(ValidationUtils.is_valid_filename("file<test>.xlsx"))
        self.assertFalse(ValidationUtils.is_valid_filename("file\x00test.xlsx"))

class TestDatabaseManager(unittest.TestCase):
    """Тесты для DatabaseManager"""
    
    def setUp(self):
        """Настройка тестов"""
        # Используем правильный путь к базе данных
        db_path = Path(__file__).parents[2] / "database" / "BD_Kingisepp" / "M_Kran_Kingesepp.db"
        self.db_manager = DatabaseManager(str(db_path))
    
    def test_get_tables_list(self):
        """Тест получения списка таблиц"""
        tables = self.db_manager.get_tables_list()
        self.assertIsInstance(tables, list)
        self.assertGreater(len(tables), 0)
        
        # Проверяем наличие основных таблиц
        expected_tables = ['logs_lnk', 'wl_china', 'wl_report_smr']
        for table in expected_tables:
            if table in tables:
                self.assertIn(table, tables)
    
    def test_get_table_info(self):
        """Тест получения информации о таблице"""
        tables = self.db_manager.get_tables_list()
        if tables:
            table_name = tables[0]
            info = self.db_manager.get_table_info(table_name)
            
            self.assertIsInstance(info, dict)
            self.assertIn('name', info)
            self.assertIn('count', info)
            self.assertIn('columns', info)
            self.assertEqual(info['name'], table_name)
            self.assertIsInstance(info['count'], int)
            self.assertIsInstance(info['columns'], list)
    
    def test_get_table_data(self):
        """Тест получения данных из таблицы"""
        tables = self.db_manager.get_tables_list()
        if tables:
            table_name = tables[0]
            data = self.db_manager.get_table_data(table_name, limit=5)
            
            self.assertIsInstance(data, object)  # pandas DataFrame
            self.assertLessEqual(len(data), 5)

class TestConfig(unittest.TestCase):
    """Тесты для Config"""
    
    def test_config_attributes(self):
        """Тест атрибутов конфигурации"""
        self.assertIsNotNone(Config.SECRET_KEY)
        self.assertIsNotNone(Config.ENCODING)
        self.assertIsNotNone(Config.BASE_DIR)
        self.assertIsNotNone(Config.DATABASE_PATH)
    
    def test_database_config(self):
        """Тест конфигурации базы данных"""
        from modules.config import DatabaseConfig
        
        self.assertIsInstance(DatabaseConfig.MAIN_TABLES, list)
        self.assertIsInstance(DatabaseConfig.DUPLICATE_TABLES, list)
        self.assertIsInstance(DatabaseConfig.SYSTEM_TABLES, list)
        
        self.assertGreater(len(DatabaseConfig.MAIN_TABLES), 0)

if __name__ == '__main__':
    # Запуск тестов
    unittest.main(verbosity=2)
