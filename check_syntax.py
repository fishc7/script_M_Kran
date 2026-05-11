#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Минимальный тест для проверки синтаксиса app.py
"""

import ast

def check_syntax():
    """Проверяет синтаксис app.py"""
    
    file_path = "web/app/app.py"
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Парсим файл
        ast.parse(content)
        print("SUCCESS: Синтаксис файла корректен")
        return True
        
    except SyntaxError as e:
        print(f"ERROR: Синтаксическая ошибка на строке {e.lineno}: {e.msg}")
        print(f"Текст: {e.text}")
        return False
    except Exception as e:
        print(f"ERROR: Ошибка при проверке синтаксиса: {e}")
        return False

if __name__ == "__main__":
    check_syntax()


