#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ядро системы M_Kran - единые модули для всех скриптов
"""

from .database import (
    get_database_path,
    get_database_connection,
    DatabaseConnection,
    test_connection
)

__all__ = [
    'get_database_path',
    'get_database_connection',
    'DatabaseConnection',
    'test_connection'
]

















