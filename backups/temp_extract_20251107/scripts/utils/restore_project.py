#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Интерактивный скрипт для восстановления проекта из архива
"""

import os
import sys
import json
import zipfile
import shutil
from datetime import datetime
from pathlib import Path

def list_available_backups():
    """Список доступных архивов"""
    backups_dir = "backups"
    if not os.path.exists(backups_dir):
        print("❌ Папка backups не найдена!")
        return []
    
    # Ищем все .zip файлы
    zip_files = []
    for file in os.listdir(backups_dir):
        if file.endswith('.zip') and file.startswith('full_backup_'):
            zip_path = os.path.join(backups_dir, file)
            json_path = os.path.join(backups_dir, file.replace('.zip', '.json'))
            
            # Читаем метаданные
            metadata = {}
            if os.path.exists(json_path):
                try:
                    with open(json_path, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                except:
                    pass
            
            zip_files.append({
                'file': file,
                'path': zip_path,
                'metadata': metadata,
                'size': os.path.getsize(zip_path)
            })
    
    # Сортируем по дате создания (новые сначала)
    zip_files.sort(key=lambda x: x['file'], reverse=True)
    return zip_files

def show_backup_info(backup):
    """Показать информацию об архиве"""
    metadata = backup['metadata']
    created_at = metadata.get('created_at', 'Неизвестно')
    files_count = metadata.get('files_count', 'Неизвестно')
    backup_size = backup['size']
    
    print(f"📦 Архив: {backup['file']}")
    print(f"📅 Создан: {created_at}")
    print(f"📊 Файлов: {files_count}")
    print(f"💾 Размер: {backup_size / 1024 / 1024:.1f} MB")
    print("-" * 50)

def restore_backup(backup_path, target_dir, overwrite=False):
    """Восстановление из архива"""
    try:
        print(f"🔄 Восстановление из архива: {backup_path}")
        print(f"📁 Целевая директория: {target_dir}")
        
        # Создаем целевую директорию если не существует
        os.makedirs(target_dir, exist_ok=True)
        
        with zipfile.ZipFile(backup_path, 'r') as zip_ref:
            # Получаем список файлов
            file_list = zip_ref.namelist()
            print(f"📋 Найдено файлов в архиве: {len(file_list)}")
            
            # Извлекаем файлы
            extracted_count = 0
            skipped_count = 0
            
            for file_info in zip_ref.infolist():
                file_path = file_info.filename
                
                # Пропускаем директории
                if file_path.endswith('/'):
                    continue
                
                # Определяем целевой путь
                target_path = os.path.join(target_dir, file_path)
                
                # Проверяем, существует ли файл
                if os.path.exists(target_path) and not overwrite:
                    print(f"⏭️ Пропущен (существует): {file_path}")
                    skipped_count += 1
                    continue
                
                # Создаем директорию если нужно
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
                
                # Извлекаем файл
                with zip_ref.open(file_info) as source, open(target_path, 'wb') as target:
                    shutil.copyfileobj(source, target)
                
                extracted_count += 1
                if extracted_count % 50 == 0:
                    print(f"📁 Извлечено файлов: {extracted_count}")
            
            print(f"✅ Восстановление завершено!")
            print(f"📊 Статистика:")
            print(f"   - Извлечено файлов: {extracted_count}")
            print(f"   - Пропущено файлов: {skipped_count}")
            
            return True
            
    except Exception as e:
        print(f"❌ Ошибка восстановления: {e}")
        return False

def main():
    """Основная функция"""
    print("🔄 ИНТЕРАКТИВНОЕ ВОССТАНОВЛЕНИЕ ПРОЕКТА")
    print("=" * 50)
    
    # Получаем список архивов
    backups = list_available_backups()
    
    if not backups:
        print("❌ Архивы не найдены!")
        return
    
    print(f"📦 Найдено архивов: {len(backups)}")
    print()
    
    # Показываем доступные архивы
    print("📋 ДОСТУПНЫЕ АРХИВЫ:")
    for i, backup in enumerate(backups[:10], 1):  # Показываем только первые 10
        print(f"{i:2d}. ", end="")
        show_backup_info(backup)
    
    if len(backups) > 10:
        print(f"... и еще {len(backups) - 10} архивов")
    
    print()
    
    # Выбор архива
    while True:
        try:
            choice = input("🔢 Выберите номер архива для восстановления (или 'q' для выхода): ").strip()
            
            if choice.lower() == 'q':
                print("👋 Выход...")
                return
            
            choice_num = int(choice)
            if 1 <= choice_num <= min(10, len(backups)):
                selected_backup = backups[choice_num - 1]
                break
            else:
                print("❌ Неверный номер! Попробуйте снова.")
        except ValueError:
            print("❌ Введите число или 'q' для выхода.")
    
    print()
    print("✅ Выбранный архив:")
    show_backup_info(selected_backup)
    
    # Выбор целевой директории
    current_dir = os.getcwd()
    print(f"📁 Текущая директория: {current_dir}")
    
    target_choice = input("📁 Восстановить в текущую директорию? (y/n): ").strip().lower()
    
    if target_choice == 'y':
        target_dir = current_dir
    else:
        target_dir = input("📁 Введите путь к целевой директории: ").strip()
        if not target_dir:
            target_dir = current_dir
    
    # Проверяем, не пуста ли целевая директория
    if os.path.exists(target_dir) and os.listdir(target_dir):
        print(f"⚠️ Целевая директория не пуста: {target_dir}")
        overwrite_choice = input("🔄 Перезаписать существующие файлы? (y/n): ").strip().lower()
        overwrite = overwrite_choice == 'y'
    else:
        overwrite = True
    
    print()
    print("🚀 НАЧИНАЕМ ВОССТАНОВЛЕНИЕ...")
    print("=" * 50)
    
    # Восстанавливаем
    success = restore_backup(selected_backup['path'], target_dir, overwrite)
    
    if success:
        print()
        print("🎉 ВОССТАНОВЛЕНИЕ ЗАВЕРШЕНО УСПЕШНО!")
        print(f"📁 Проект восстановлен в: {target_dir}")
    else:
        print()
        print("❌ ВОССТАНОВЛЕНИЕ ЗАВЕРШИЛОСЬ С ОШИБКОЙ!")

if __name__ == "__main__":
    main()

