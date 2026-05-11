#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Модуль для автоматической очистки Unicode символов при загрузке данных
Интегрируется в процесс импорта Excel файлов
"""

import re
import pandas as pd
from typing import Union, Any

def clean_unicode_text(text: Union[str, Any]) -> Union[str, Any]:
    """
    Очищает текст от проблемных Unicode символов
    
    Args:
        text: Текст для очистки
        
    Returns:
        Очищенный текст или исходное значение, если это не строка
    """
    if not isinstance(text, str):
        return text
    
    if text is None or text == '':
        return text
    
    try:
        # Заменяем все известные проблемные символы
        replacements = {
            '、': ', ',  # Японская запятая
            '。': '. ',  # Японская точка
            '　': ' ',   # Японский пробел
            '°C': 'C',  # Градус Цельсия
            '°F': 'F',  # Градус Фаренгейта
            '°': '',    # Градус
            '\u3001': ', ',  # Японская запятая (Unicode)
            '\u3002': '. ',  # Японская точка (Unicode)
            '\u3000': ' ',   # Японский пробел (Unicode)
            '\u2103': 'C',   # Градус Цельсия (Unicode)
            '\u2109': 'F',   # Градус Фаренгейта (Unicode)
            '\u00b0': '',    # Градус (Unicode)
            '\u2028': ' ',   # Разделитель строк
            '\u2029': ' ',   # Разделитель абзацев
            '\u00a0': ' ',   # Неразрывный пробел
            '\u200b': '',    # Нулевая ширина пробела
            '\u200c': '',    # Нулевая ширина не-соединитель
            '\u200d': '',    # Нулевая ширина соединитель
            '\u2060': '',    # Слово-соединитель
            '\ufeff': '',    # BOM
            '\u2013': '-',   # Короткое тире
            '\u2014': '-',   # Длинное тире
            '\u2018': "'",   # Левая одинарная кавычка
            '\u2019': "'",   # Правая одинарная кавычка
            '\u201c': '"',   # Левая двойная кавычка
            '\u201d': '"',   # Правая двойная кавычка
            '\u2022': '*',   # Маркер списка
            '\u2026': '...', # Многоточие
            '\u00ae': '(R)', # Зарегистрированная торговая марка
            '\u00a9': '(C)', # Авторское право
            '\u2122': '(TM)', # Торговая марка
        }
        
        cleaned = text
        for old_char, new_char in replacements.items():
            cleaned = cleaned.replace(old_char, new_char)
        
        # Удаляем все управляющие символы и эмодзи, кроме базовых
        result = ''
        for char in cleaned:
            code = ord(char)
            # Оставляем только печатные символы ASCII, пробелы, табуляцию, переносы строк
            if (code >= 32 and code <= 126) or code in [9, 10, 13]:
                result += char
            elif code > 127:  # Unicode символы
                # Проверяем, что это не проблемный символ и не эмодзи
                problematic_codes = [
                    0x3001, 0x3002, 0x3000, 0x2028, 0x2029, 0x00a0, 0x200b, 0x200c, 0x200d, 0x2060, 0xfeff,
                    0x2103, 0x2109, 0x00b0, 0x2013, 0x2014, 0x2018, 0x2019, 0x201c, 0x201d, 0x2022, 0x2026,
                    0x00ae, 0x00a9, 0x2122
                ]
                
                # Проверяем, что это не эмодзи (коды эмодзи начинаются с 0x1F600)
                if code not in problematic_codes and code < 0x1F600:
                    # Для остальных Unicode символов пытаемся их сохранить
                    try:
                        # Проверяем, можно ли закодировать в Windows-1251
                        char.encode('cp1251')
                        result += char
                    except UnicodeEncodeError:
                        # Если не можем закодировать, заменяем на пробел
                        result += ' '
        
        # Удаляем множественные пробелы
        result = re.sub(r'\s+', ' ', result)
        
        # Удаляем пробелы в начале и конце
        result = result.strip()
        
        return result if result else ''
        
    except Exception as e:
        # В случае ошибки возвращаем исходный текст
        return text

def clean_dataframe_unicode(df: pd.DataFrame) -> pd.DataFrame:
    """
    Очищает DataFrame от проблемных Unicode символов
    
    Args:
        df: DataFrame для очистки
        
    Returns:
        Очищенный DataFrame
    """
    if df is None or df.empty:
        return df
    
    # Создаем копию DataFrame
    cleaned_df = df.copy()
    
    # Очищаем все строковые столбцы
    for column in cleaned_df.columns:
        if cleaned_df[column].dtype == 'object':
            cleaned_df[column] = cleaned_df[column].apply(clean_unicode_text)
    
    return cleaned_df

def clean_dataframe_unicode_inplace(df: pd.DataFrame) -> None:
    """
    Очищает DataFrame от проблемных Unicode символов на месте (без создания копии)
    
    Args:
        df: DataFrame для очистки (изменяется на месте)
    """
    if df is None or df.empty:
        return
    
    # Очищаем все строковые столбцы
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = df[column].apply(clean_unicode_text)

def get_unicode_cleaning_stats(df: pd.DataFrame) -> dict:
    """
    Получает статистику по очистке Unicode символов
    
    Args:
        df: DataFrame для анализа
        
    Returns:
        Словарь со статистикой
    """
    stats = {
        'total_cells': 0,
        'string_cells': 0,
        'cleaned_cells': 0,
        'problematic_chars_found': 0
    }
    
    if df is None or df.empty:
        return stats
    
    for column in df.columns:
        if df[column].dtype == 'object':
            for value in df[column]:
                stats['total_cells'] += 1
                if isinstance(value, str):
                    stats['string_cells'] += 1
                    # Проверяем наличие проблемных символов
                    problematic_chars = ['、', '\u3001', '\u2103', '\u00b0', '\u2028', '\u2029']
                    for char in problematic_chars:
                        if char in str(value):
                            stats['problematic_chars_found'] += 1
                            break
    
    return stats
