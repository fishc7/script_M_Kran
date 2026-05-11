#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Единый модуль для подключения к базе данных M_Kran
Устраняет дублирование кода подключения к БД во всех скриптах проекта

Использование:
    from scripts.core.database import get_database_connection
    
    # Простое использование
    conn = get_database_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM table_name")
    conn.close()
    
    # Использование с контекстным менеджером
    from scripts.core.database import DatabaseConnection
    
    with DatabaseConnection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM table_name")
"""

import os
import sqlite3
import logging
from typing import Optional, Union
from contextlib import contextmanager

# Настройка логирования
logger = logging.getLogger(__name__)

# Проверяем, нужно ли использовать PostgreSQL
USE_POSTGRESQL = os.environ.get('USE_POSTGRESQL', 'false').lower() == 'true'

if USE_POSTGRESQL:
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
        try:
            from config import DB_CONFIG
        except ImportError:
            # Используем переменные окружения
            DB_CONFIG = {
                'host': os.environ.get('PG_HOST', 'localhost'),
                'port': int(os.environ.get('PG_PORT', '5432')),
                'database': os.environ.get('PG_DATABASE', 'Test_OGS'),
                'user': os.environ.get('PG_USER', 'postgres'),
                'password': os.environ.get('PG_PASSWORD', 'Fishc1979')
            }
    except ImportError:
        logger.warning("psycopg2 не установлен, используем SQLite")
        USE_POSTGRESQL = False


def get_database_path() -> Optional[str]:
    """
    Автоматически определяет правильный путь к базе данных M_Kran_Kingesepp.db
    Работает для всех скриптов в проекте независимо от того, откуда они запускаются
    
    Returns:
        str: Абсолютный путь к базе данных или None, если не найдена
        
    Example:
        >>> db_path = get_database_path()
        >>> if db_path:
        ...     print(f"База данных найдена: {db_path}")
    """
    # Получаем текущую директорию
    current_dir = os.getcwd()
    
    # Пробуем разные варианты путей для новой структуры проекта
    possible_paths = [
        # Если запускаем из корневой папки проекта (новая структура)
        os.path.join(current_dir, 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Если запускаем из папки scripts
        os.path.join(current_dir, '..', 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Если запускаем из папки scripts/data_loaders
        os.path.join(current_dir, '..', '..', 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Если запускаем из папки scripts/data_cleaners
        os.path.join(current_dir, '..', '..', 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Если запускаем из папки web/app
        os.path.join(current_dir, '..', '..', 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Если запускаем из папки web/app/modules
        os.path.join(current_dir, '..', '..', '..', 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Старые пути для совместимости
        os.path.join(current_dir, 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        os.path.join(current_dir, '..', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # Альтернативный путь через path_utils (если доступен)
    ]
    
    # Пробуем использовать path_utils, если доступен
    try:
        import sys
        # Добавляем путь к utilities в sys.path, если его там нет
        core_dir = os.path.dirname(os.path.abspath(__file__))
        scripts_dir = os.path.dirname(core_dir)
        utilities_path = os.path.join(scripts_dir, 'utilities')
        if utilities_path not in sys.path:
            sys.path.insert(0, utilities_path)
        
        from utilities.path_utils import get_database_path as get_path_from_utils
        path_from_utils = get_path_from_utils()
        if path_from_utils and os.path.exists(path_from_utils):
            return os.path.abspath(path_from_utils)
    except (ImportError, Exception) as e:
        logger.debug(f"Не удалось использовать path_utils: {e}")
    
    # Проверяем все возможные пути
    for path in possible_paths:
        abs_path = os.path.abspath(path)
        if os.path.exists(abs_path):
            logger.debug(f"База данных найдена: {abs_path}")
            return abs_path
    
    # Если не нашли, возвращаем None
    logger.warning("База данных M_Kran_Kingesepp.db не найдена ни по одному из путей")
    return None


def get_database_connection(timeout: float = 30.0, check_same_thread: bool = False) -> Union[sqlite3.Connection, 'psycopg2.extensions.connection']:
    """
    Создает подключение к базе данных (PostgreSQL или SQLite)
    
    Args:
        timeout: Таймаут для подключения в секундах (по умолчанию 30.0)
        check_same_thread: Разрешить использование в разных потоках (по умолчанию False, только для SQLite)
        
    Returns:
        sqlite3.Connection или psycopg2.extensions.connection: Подключение к базе данных
        
    Raises:
        FileNotFoundError: Если база данных не найдена (SQLite)
        sqlite3.Error: При ошибках подключения к SQLite
        psycopg2.Error: При ошибках подключения к PostgreSQL
        
    Example:
        >>> conn = get_database_connection()
        >>> cursor = conn.cursor()
        >>> cursor.execute("SELECT COUNT(*) FROM logs_lnk")
        >>> count = cursor.fetchone()[0]
        >>> conn.close()
    """
    if USE_POSTGRESQL:
        # Подключение к PostgreSQL
        try:
            logger.info(f"Подключение к PostgreSQL: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
            conn = psycopg2.connect(
                host=DB_CONFIG['host'],
                port=DB_CONFIG['port'],
                database=DB_CONFIG['database'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password'],
                connect_timeout=int(timeout)
            )
            # Устанавливаем курсор с RealDictCursor для совместимости с sqlite3.Row
            conn.cursor_factory = RealDictCursor
            logger.debug(f"Подключение к PostgreSQL успешно установлено")
            return conn
        except Exception as e:
            error_msg = f"Ошибка подключения к PostgreSQL: {e}"
            logger.error(error_msg)
            raise
    else:
        # Подключение к SQLite (старый код)
        db_path = get_database_path()
        
        if db_path is None:
            error_msg = "База данных M_Kran_Kingesepp.db не найдена. Проверьте структуру проекта."
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        try:
            # Создаем соединение с настройками для надежности
            conn = sqlite3.connect(
                db_path,
                timeout=timeout,
                check_same_thread=check_same_thread
            )
            
            # Включаем WAL режим для лучшей производительности и меньших блокировок
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=10000")
            conn.execute("PRAGMA temp_store=MEMORY")
            
            # Устанавливаем кодировку UTF-8
            conn.execute("PRAGMA encoding='UTF-8'")
            
            # Устанавливаем row_factory для удобства работы
            conn.row_factory = sqlite3.Row
            
            logger.debug(f"Подключение к БД успешно установлено: {db_path}")
            return conn
            
        except sqlite3.OperationalError as e:
            error_msg = f"Ошибка подключения к БД (OperationalError): {e}"
            logger.error(error_msg)
            if "database is locked" in str(e).lower():
                logger.error("База данных заблокирована другим процессом")
            raise
        except Exception as e:
            error_msg = f"Неожиданная ошибка при подключении к БД: {e}"
            logger.error(error_msg)
            raise


class DatabaseConnection:
    """
    Контекстный менеджер для работы с базой данных
    Автоматически закрывает соединение при выходе из контекста
    
    Example:
        >>> with DatabaseConnection() as conn:
        ...     cursor = conn.cursor()
        ...     cursor.execute("SELECT * FROM logs_lnk LIMIT 10")
        ...     rows = cursor.fetchall()
        >>> # Соединение автоматически закрыто
    """
    
    def __init__(self, timeout: float = 30.0, check_same_thread: bool = False):
        """
        Args:
            timeout: Таймаут для подключения в секундах
            check_same_thread: Разрешить использование в разных потоках
        """
        self.timeout = timeout
        self.check_same_thread = check_same_thread
        self.conn = None
    
    def __enter__(self):
        """Открывает соединение при входе в контекст"""
        self.conn = get_database_connection(
            timeout=self.timeout,
            check_same_thread=self.check_same_thread
        )
        return self.conn
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Закрывает соединение при выходе из контекста"""
        if self.conn:
            try:
                if exc_type is not None:
                    # Если была ошибка, откатываем транзакцию
                    if USE_POSTGRESQL:
                        self.conn.rollback()
                    else:
                        self.conn.rollback()
                else:
                    # Если все хорошо, коммитим изменения
                    if USE_POSTGRESQL:
                        self.conn.commit()
                    else:
                        self.conn.commit()
            except Exception as e:
                logger.error(f"Ошибка при закрытии соединения: {e}")
            finally:
                self.conn.close()
                self.conn = None
        
        # Возвращаем False, чтобы исключения не подавлялись
        return False


@contextmanager
def database_transaction():
    """
    Контекстный менеджер для транзакций
    Автоматически коммитит изменения при успешном выполнении или откатывает при ошибке
    
    Example:
        >>> with database_transaction() as conn:
        ...     cursor = conn.cursor()
        ...     cursor.execute("INSERT INTO table_name (col1) VALUES (?)", ('value',))
        >>> # Изменения автоматически закоммичены
    """
    conn = None
    try:
        conn = get_database_connection()
        yield conn
        conn.commit()
        logger.debug("Транзакция успешно закоммичена")
    except Exception as e:
        if conn:
            conn.rollback()
            logger.error(f"Транзакция откачена из-за ошибки: {e}")
        raise
    finally:
        if conn:
            conn.close()


def test_connection() -> bool:
    """
    Тестирует подключение к базе данных
    
    Returns:
        bool: True если подключение успешно, False иначе
        
    Example:
        >>> if test_connection():
        ...     print("Подключение к БД успешно")
        ... else:
        ...     print("Ошибка подключения к БД")
    """
    try:
        with DatabaseConnection() as conn:
            cursor = conn.cursor()
            
            # Проверяем, что можем выполнить простой запрос
            if USE_POSTGRESQL:
                cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' LIMIT 1")
            else:
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1")
            result = cursor.fetchone()
            
            if result:
                logger.info("✓ Подключение к базе данных успешно")
                return True
            else:
                logger.warning("✓ Подключение установлено, но таблицы не найдены")
                return True
                
    except FileNotFoundError as e:
        logger.error(f"✗ База данных не найдена: {e}")
        return False
    except Exception as e:
        logger.error(f"✗ Ошибка при подключении к базе данных: {e}")
        return False


if __name__ == "__main__":
    # Тестирование модуля
    import sys
    import io
    
    # Настройка кодировки для Windows
    if sys.platform == 'win32':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    
    print("Тестирование модуля database.py...")
    print("-" * 50)
    
    # Тест определения пути
    db_path = get_database_path()
    if db_path:
        print(f"[OK] Путь к БД найден: {db_path}")
    else:
        print("[ERROR] Путь к БД не найден")
    
    # Тест подключения
    if test_connection():
        print("[OK] Все тесты пройдены успешно")
    else:
        print("[ERROR] Тесты не пройдены")

