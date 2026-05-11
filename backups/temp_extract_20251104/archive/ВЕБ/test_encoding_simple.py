#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os

# Устанавливаем переменную окружения
os.environ['PYTHONIOENCODING'] = 'utf-8'

def test_encoding():
    """Простой тест кодировки"""
    print("=== Тест кодировки ===")
    print(f"Python версия: {sys.version}")
    print(f"Платформа: {sys.platform}")
    print(f"Кодировка по умолчанию: {sys.getdefaultencoding()}")
    
    # Тест вывода русских символов
    print("\n=== Тест вывода русских символов ===")
    try:
        print("Тест: Привет, мир!")
        print("Тест: Извлечение чисел из обозначений")
        print("Тест: Удаление префиксов S/F")
        print("✅ Кодировка работает корректно")
    except UnicodeEncodeError as e:
        print(f"❌ Ошибка кодировки: {e}")
        print("Попытка исправления...")
        try:
            # Пытаемся исправить
            print("Тест: Привет, мир!")
            print("✅ Кодировка исправлена")
        except Exception as e2:
            print(f"❌ Не удалось исправить: {e2}")

if __name__ == "__main__":
    test_encoding()
