#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Модуль для работы с базой данных M_Kran
"""

import sqlite3
import pandas as pd
import logging
from typing import List, Dict, Any, Optional, Tuple
from contextlib import contextmanager
from ..config import Config, DatabaseConfig

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Менеджер для работы с базой данных"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or Config.DATABASE_PATH
        
    @contextmanager
    def get_connection(self):
        """Контекстный менеджер для подключения к БД"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            yield conn
        except Exception as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
    
    def get_tables_list(self) -> List[str]:
        """Получает список всех таблиц в БД"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            return [row['name'] for row in cursor.fetchall()]
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Получает информацию о таблице"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Получаем количество записей
            cursor.execute(f"SELECT COUNT(*) as count FROM `{table_name}`")
            count = cursor.fetchone()['count']
            
            # Получаем структуру таблицы
            cursor.execute(f"PRAGMA table_info(`{table_name}`)")
            columns = [dict(row) for row in cursor.fetchall()]
            
            return {
                'name': table_name,
                'count': count,
                'columns': columns
            }
    
    def get_table_data(self, table_name: str, limit: int = 100, offset: int = 0) -> pd.DataFrame:
        """Получает данные из таблицы"""
        with self.get_connection() as conn:
            query = f"SELECT * FROM `{table_name}` LIMIT {limit} OFFSET {offset}"
            return pd.read_sql_query(query, conn)
    
    def search_in_table(self, table_name: str, search_term: str, 
                       columns: List[str] = None, limit: int = 100) -> pd.DataFrame:
        """Поиск в таблице"""
        with self.get_connection() as conn:
            if not columns:
                # Получаем все текстовые колонки
                cursor = conn.cursor()
                cursor.execute(f"PRAGMA table_info(`{table_name}`)")
                columns = [row['name'] for row in cursor.fetchall() 
                          if row['type'].upper() in ['TEXT', 'VARCHAR', 'CHAR']]
            
            if not columns:
                return pd.DataFrame()
            
            # Строим условие поиска
            conditions = []
            params = []
            for col in columns:
                conditions.append(f"`{col}` LIKE ?")
                params.append(f"%{search_term}%")
            
            where_clause = " OR ".join(conditions)
            query = f"SELECT * FROM `{table_name}` WHERE {where_clause} LIMIT {limit}"
            
            return pd.read_sql_query(query, conn, params=params)
    
    def execute_query(self, query: str, params: tuple = None) -> pd.DataFrame:
        """Выполняет произвольный SQL запрос"""
        with self.get_connection() as conn:
            return pd.read_sql_query(query, conn, params=params)
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Получает статистику по базе данных"""
        stats = {
            'total_tables': 0,
            'total_records': 0,
            'database_size': 0,
            'tables': {}
        }
        
        try:
            # Размер файла БД
            import os
            if os.path.exists(self.db_path):
                stats['database_size'] = os.path.getsize(self.db_path)
            
            # Информация о таблицах
            tables = self.get_tables_list()
            stats['total_tables'] = len(tables)
            
            for table in tables:
                table_info = self.get_table_info(table)
                stats['tables'][table] = table_info['count']
                stats['total_records'] += table_info['count']
                
        except Exception as e:
            logger.error(f"Ошибка получения статистики БД: {e}")
        
        return stats

class DataProcessor:
    """Класс для обработки данных"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def clean_data_for_excel(self, df: pd.DataFrame) -> pd.DataFrame:
        """Очищает данные для экспорта в Excel"""
        if df.empty:
            return df
        
        # Заменяем NaN на пустые строки
        df = df.fillna('')
        
        # Очищаем от специальных символов
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.replace('\x00', '', regex=False)
        
        return df
    
    def get_duplicates_count(self) -> Dict[str, int]:
        """Получает количество дубликатов"""
        counts = {}
        
        for table in DatabaseConfig.DUPLICATE_TABLES:
            try:
                table_info = self.db.get_table_info(table)
                counts[table] = table_info['count']
            except Exception as e:
                logger.error(f"Ошибка получения дубликатов из {table}: {e}")
                counts[table] = 0
        
        return counts
    
    def get_recent_activities(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Получает последние активности"""
        try:
            query = """
            SELECT action, description, timestamp 
            FROM system_activities 
            ORDER BY timestamp DESC 
            LIMIT ?
            """
            df = self.db.execute_query(query, (limit,))
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Ошибка получения активностей: {e}")
            return []

# Глобальный экземпляр менеджера БД
db_manager = DatabaseManager()
data_processor = DataProcessor(db_manager)
