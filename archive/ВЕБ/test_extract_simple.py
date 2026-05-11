#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os

# Добавляем путь к модулям
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from extract_utils import extract_joint_number, clean_joint_number, extract_and_clean_joint_number

def test_extraction():
    """Тестирует функции извлечения чисел"""
    print("=== ТЕСТ ФУНКЦИЙ ИЗВЛЕЧЕНИЯ ===")
    
    # Тестовые данные
    test_cases = [
        "S123",
        "F456",
        "S-789",
        "F - 012",
        "123",
        "S 000123",
        "F000456",
        "Test S789 Test",
        "Test F-012 Test",
        "No numbers here",
        "",
        None
    ]
    
    print("\nТест extract_joint_number:")
    for test in test_cases:
        try:
            result = extract_joint_number(test)
            print(f"  '{test}' -> '{result}'")
        except Exception as e:
            print(f"  '{test}' -> ERROR: {e}")
    
    print("\nТест clean_joint_number:")
    for test in test_cases:
        try:
            result = clean_joint_number(test)
            print(f"  '{test}' -> '{result}'")
        except Exception as e:
            print(f"  '{test}' -> ERROR: {e}")
    
    print("\nТест extract_and_clean_joint_number:")
    for test in test_cases:
        try:
            result = extract_and_clean_joint_number(test)
            print(f"  '{test}' -> '{result}'")
        except Exception as e:
            print(f"  '{test}' -> ERROR: {e}")
    
    print("\n✅ Тест завершен успешно!")

if __name__ == "__main__":
    test_extraction()

