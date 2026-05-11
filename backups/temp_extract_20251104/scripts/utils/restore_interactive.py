#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Интерактивное восстановление из бэкапа с выбором
Автор: AI Assistant
Версия: 1.0
"""

import sys
import os
from pathlib import Path
from backup_system import BackupSystem
import datetime

def format_size(size_bytes):
    """Форматирование размера файла"""
    if size_bytes == 0:
        return "0B"
    size_names = ["B", "KB", "MB", "GB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    return f"{size_bytes:.1f}{size_names[i]}"

def format_date(date_str):
    """Форматирование даты"""
    try:
        dt = datetime.datetime.fromisoformat(date_str)
        return dt.strftime("%d.%m.%Y %H:%M:%S")
    except:
        return date_str

def main():
    """Основная функция"""
    print("=" * 60)
    print("        ИНТЕРАКТИВНОЕ ВОССТАНОВЛЕНИЕ ИЗ БЭКАПА")
    print("=" * 60)
    print()
    
    # Инициализация системы бэкапов
    backup_system = BackupSystem()
    
    # Получение списка бэкапов
    print("Получение списка доступных бэкапов...")
    backups = backup_system.list_backups()
    
    if not backups:
        print("❌ Нет доступных бэкапов для восстановления!")
        print("Создайте бэкап с помощью backup_manager.bat")
        input("\nНажмите Enter для выхода...")
        return
    
    print(f"\n✅ Найдено {len(backups)} бэкапов:")
    print("-" * 80)
    
    # Вывод списка бэкапов
    for i, backup in enumerate(backups, 1):
        name = backup.get("name", "Неизвестно")
        created_at = format_date(backup.get("created_at", ""))
        size = format_size(backup.get("size", 0))
        backup_type = backup.get("type", "unknown")
        
        print(f"{i:2d}. {name}")
        print(f"    📅 Дата: {created_at}")
        print(f"    📦 Размер: {size}")
        print(f"    🏷️  Тип: {backup_type}")
        print()
    
    # Выбор бэкапа
    while True:
        try:
            choice = input(f"Выберите номер бэкапа (1-{len(backups)}) или 0 для отмены: ").strip()
            
            if choice == "0":
                print("Восстановление отменено.")
                return
            
            choice_num = int(choice)
            if 1 <= choice_num <= len(backups):
                selected_backup = backups[choice_num - 1]
                break
            else:
                print(f"❌ Неверный номер! Введите число от 1 до {len(backups)}")
        except ValueError:
            print("❌ Введите корректное число!")
    
    # Показываем информацию о выбранном бэкапе
    print("\n" + "=" * 60)
    print("           ВЫБРАННЫЙ БЭКАП")
    print("=" * 60)
    print(f"📁 Файл: {selected_backup['name']}")
    print(f"📅 Дата создания: {format_date(selected_backup['created_at'])}")
    print(f"📦 Размер: {format_size(selected_backup['size'])}")
    print(f"🏷️  Тип: {selected_backup.get('type', 'unknown')}")
    print(f"📍 Путь: {selected_backup['path']}")
    
    # Проверка целостности
    print("\n🔍 Проверка целостности бэкапа...")
    if backup_system.verify_backup(selected_backup['path']):
        print("✅ Бэкап прошел проверку целостности")
    else:
        print("❌ Бэкап поврежден! Рекомендуется выбрать другой бэкап.")
        response = input("Продолжить восстановление? (y/N): ").strip().lower()
        if response != 'y':
            print("Восстановление отменено.")
            return
    
    # Выбор папки для восстановления
    print("\n" + "=" * 60)
    print("           ПАПКА ВОССТАНОВЛЕНИЯ")
    print("=" * 60)
    
    default_restore_dir = Path.cwd() / f"restored_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"📁 Папка по умолчанию: {default_restore_dir}")
    
    custom_dir = input("\nВведите путь к папке для восстановления (Enter для авто): ").strip()
    
    if custom_dir:
        restore_dir = Path(custom_dir)
        if restore_dir.exists():
            response = input(f"Папка {restore_dir} уже существует. Перезаписать? (y/N): ").strip().lower()
            if response != 'y':
                print("Восстановление отменено.")
                return
    else:
        restore_dir = default_restore_dir
    
    # Подтверждение восстановления
    print("\n" + "=" * 60)
    print("           ПОДТВЕРЖДЕНИЕ")
    print("=" * 60)
    print(f"📁 Бэкап: {selected_backup['name']}")
    print(f"📂 Восстановление в: {restore_dir}")
    print()
    
    confirm = input("Начать восстановление? (y/N): ").strip().lower()
    if confirm != 'y':
        print("Восстановление отменено.")
        return
    
    # Восстановление
    print("\n🔄 Начинаю восстановление...")
    print("Это может занять несколько минут...")
    
    try:
        success = backup_system.restore_backup(selected_backup['path'], str(restore_dir))
        
        if success:
            print("\n" + "=" * 60)
            print("           ✅ ВОССТАНОВЛЕНИЕ ЗАВЕРШЕНО")
            print("=" * 60)
            print(f"📂 Файлы восстановлены в: {restore_dir}")
            print(f"📁 Откройте папку для просмотра восстановленных файлов")
            print()
            
            # Предложение открыть папку
            open_folder = input("Открыть папку с восстановленными файлами? (y/N): ").strip().lower()
            if open_folder == 'y':
                os.startfile(restore_dir)
        else:
            print("\n❌ Ошибка при восстановлении!")
            print("Проверьте права доступа и свободное место на диске.")
            
    except Exception as e:
        print(f"\n❌ Ошибка при восстановлении: {e}")
    
    input("\nНажмите Enter для выхода...")

if __name__ == "__main__":
    main()

