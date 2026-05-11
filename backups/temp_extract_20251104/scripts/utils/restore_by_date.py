#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Интерактивный скрипт для восстановления системы по выбранной дате
"""

import os
import shutil
import sys
import json
import zipfile
from pathlib import Path
import datetime
from backup_system import BackupSystem

def get_backup_info(backup_path):
    """Получение информации о бэкапе"""
    metadata_path = backup_path.with_suffix('.json')
    
    if metadata_path.exists():
        with open(metadata_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
            return metadata
    else:
        # Если метаданные нет, создаем базовую информацию
        stat = backup_path.stat()
        return {
            "created_at": datetime.datetime.fromtimestamp(stat.st_mtime).isoformat(),
            "backup_type": "unknown",
            "files_count": 0,
            "backup_size": stat.st_size
        }

def list_backups_with_dates():
    """Список бэкапов с датами"""
    backup_system = BackupSystem()
    backups = backup_system.list_backups()
    
    print("📅 Доступные резервные копии:")
    print("=" * 80)
    
    for i, backup in enumerate(backups, 1):
        size_mb = backup["size"] / (1024 * 1024)
        created_at = backup["created_at"]
        
        # Парсим дату для красивого отображения
        try:
            dt = datetime.datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            formatted_date = dt.strftime("%d.%m.%Y %H:%M:%S")
        except:
            formatted_date = created_at
        
        print(f"{i:2d}. {backup['name']}")
        print(f"    📅 Дата: {formatted_date}")
        print(f"    📊 Размер: {size_mb:.1f} MB")
        print(f"    📁 Тип: {backup.get('type', 'unknown')}")
        print()
    
    return backups

def restore_from_backup(backup_path, restore_dir=None):
    """Восстановление из выбранного бэкапа"""
    backup_system = BackupSystem()
    
    if not restore_dir:
        restore_dir = Path.cwd() / f"restored_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    else:
        restore_dir = Path(restore_dir)
    
    print(f"🔄 Восстанавливаю из: {backup_path}")
    print(f"📁 В папку: {restore_dir}")
    
    # Проверяем целостность бэкапа
    print("🔍 Проверяю целостность бэкапа...")
    if not backup_system.verify_backup(str(backup_path)):
        print("❌ Бэкап поврежден!")
        return False
    
    # Восстанавливаем
    success = backup_system.restore_backup(str(backup_path), str(restore_dir))
    
    if success:
        print(f"✅ Восстановление завершено: {restore_dir}")
        return restore_dir
    else:
        print("❌ Ошибка при восстановлении")
        return False

def apply_restored_system(restored_dir):
    """Применение восстановленной системы"""
    current_dir = Path.cwd()
    restored_dir = Path(restored_dir)
    
    if not restored_dir.exists():
        print("❌ Папка восстановления не найдена!")
        return False
    
    print("🔄 Применяю восстановленную систему...")
    
    # Список папок и файлов для восстановления
    items_to_restore = [
        'web',
        'scripts', 
        'desktop',
        'database',
        'config',
        'docs',
        'book_scripts',
        'archive',
        'web_launcher.py',
        'backup_manager.bat',
        'backup_scheduler.ps1',
        'backup_system.py',
        'check_db.py',
        'etl_scheduler.py',
        'launch.bat',
        'launch.ps1',
        'launch_console.bat',
        'launch_desktop.bat',
        'launch_hidden.vbs',
        'launch_web.bat',
        'prefix_remover.py',
        'remove_auto_backup.bat',
        'requirements.txt',
        'setup_auto_backup.bat',
        'setup_double_backup_fixed.ps1'
    ]
    
    # Создаем папку для временного хранения
    temp_backup_dir = current_dir / f"temp_backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    temp_backup_dir.mkdir(exist_ok=True)
    
    restored_count = 0
    skipped_count = 0
    
    print("\n📋 Восстанавливаю файлы и папки:")
    
    for item in items_to_restore:
        restored_item = restored_dir / item
        current_item = current_dir / item
        
        if restored_item.exists():
            try:
                print(f"  🔄 {item}")
                
                # Если текущий элемент существует, перемещаем его во временную папку
                if current_item.exists():
                    temp_item = temp_backup_dir / item
                    if current_item.is_dir():
                        shutil.move(str(current_item), str(temp_item))
                    else:
                        temp_item.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(str(current_item), str(temp_item))
                
                # Копируем восстановленный элемент
                if restored_item.is_dir():
                    shutil.copytree(str(restored_item), str(current_item))
                else:
                    shutil.copy2(str(restored_item), str(current_item))
                
                restored_count += 1
                
            except Exception as e:
                print(f"    ⚠️  Пропущен {item}: {e}")
                skipped_count += 1
    
    print(f"\n✅ Восстановление завершено!")
    print(f"📊 Восстановлено: {restored_count} элементов")
    print(f"⚠️  Пропущено: {skipped_count} элементов")
    print(f"📁 Временная папка с замененными файлами: {temp_backup_dir}")
    
    return True

def main():
    """Основная функция"""
    print("🚀 Интерактивное восстановление системы M_Kran")
    print("=" * 60)
    
    # Показываем список бэкапов
    backups = list_backups_with_dates()
    
    if not backups:
        print("❌ Резервные копии не найдены!")
        return
    
    # Выбор бэкапа
    while True:
        try:
            choice = input(f"\n📝 Выберите номер бэкапа (1-{len(backups)}): ").strip()
            backup_index = int(choice) - 1
            
            if 0 <= backup_index < len(backups):
                selected_backup = backups[backup_index]
                break
            else:
                print("❌ Неверный номер!")
        except ValueError:
            print("❌ Введите число!")
    
    print(f"\n✅ Выбран: {selected_backup['name']}")
    
    # Подтверждение
    response = input("⚠️  Восстановить систему из этого бэкапа? (y/N): ")
    if response.lower() not in ['y', 'yes', 'да', 'д']:
        print("❌ Восстановление отменено")
        return
    
    # Восстановление
    backup_path = Path(selected_backup['path'])
    restored_dir = restore_from_backup(backup_path)
    
    if not restored_dir:
        print("❌ Не удалось восстановить из бэкапа!")
        return
    
    # Применение восстановленной системы
    response = input("\n⚠️  Применить восстановленную систему? (y/N): ")
    if response.lower() in ['y', 'yes', 'да', 'д']:
        success = apply_restored_system(restored_dir)
        if success:
            print("\n🎉 Система успешно восстановлена!")
        else:
            print("\n💥 Ошибка при применении восстановленной системы!")
    else:
        print(f"\n📁 Восстановленная система находится в: {restored_dir}")
        print("💡 Вы можете вручную скопировать нужные файлы")

if __name__ == "__main__":
    main()
