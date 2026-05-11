#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для восстановления базы данных из архива
"""

import os
import sys
import locale

# Устанавливаем кодировку UTF-8 для Windows
if sys.platform.startswith('win'):
    try:
        locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')
    except:
        try:
            locale.setlocale(locale.LC_ALL, 'Russian_Russia.1251')
        except:
            pass
    
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    os.environ['PYTHONLEGACYWINDOWSSTDIO'] = '1'
    
    import codecs
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    if sys.stderr.encoding != 'utf-8':
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

import json
import zipfile
import shutil
from pathlib import Path
from datetime import datetime
from backup_system import BackupSystem

def list_database_backups():
    """Список архивов с базами данных"""
    backup_system = BackupSystem()
    backups = backup_system.list_backups()
    
    print("📅 Доступные резервные копии с базами данных:")
    print("=" * 80)
    
    database_backups = []
    for i, backup in enumerate(backups, 1):
        # Проверяем, есть ли в архиве база данных
        backup_path = Path(backup['path'])
        if not backup_path.exists():
            continue
            
        try:
            with zipfile.ZipFile(backup_path, 'r') as zipf:
                # Ищем файлы базы данных
                db_files = [f for f in zipf.namelist() if f.endswith('.db') or 'database' in f.lower()]
                
                if db_files:
                    size_mb = backup["size"] / (1024 * 1024)
                    created_at = backup["created_at"]
                    
                    # Парсим дату для красивого отображения
                    try:
                        dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        formatted_date = dt.strftime("%d.%m.%Y %H:%M:%S")
                    except:
                        formatted_date = created_at
                    
                    print(f"{len(database_backups) + 1:2d}. {backup['name']}")
                    print(f"    📅 Дата: {formatted_date}")
                    print(f"    📊 Размер: {size_mb:.1f} MB")
                    print(f"    💾 Файлов БД: {len(db_files)}")
                    for db_file in db_files[:3]:  # Показываем первые 3
                        print(f"       - {db_file}")
                    if len(db_files) > 3:
                        print(f"       ... и еще {len(db_files) - 3} файлов")
                    print()
                    
                    database_backups.append({
                        'index': len(database_backups) + 1,
                        'backup': backup,
                        'db_files': db_files
                    })
        except Exception as e:
            continue
    
    return database_backups

def restore_database_from_backup(backup_path, target_db_path=None):
    """Восстановление базы данных из архива"""
    backup_path = Path(backup_path)
    
    if not backup_path.exists():
        print(f"❌ Архив не найден: {backup_path}")
        return False
    
    # Определяем путь к базе данных
    if not target_db_path:
        # Ищем базу данных в стандартных местах
        possible_paths = [
            Path('database/BD_Kingisepp/M_Kran_Kingesepp.db'),
            Path('database/M_Kran_Kingesepp.db'),
        ]
        
        for path in possible_paths:
            if path.exists():
                target_db_path = path
                break
        
        if not target_db_path:
            print("❌ Не найдена текущая база данных!")
            print("💡 Укажите путь к базе данных вручную")
            return False
    
    target_db_path = Path(target_db_path)
    target_db_dir = target_db_path.parent
    
    print(f"🔄 Восстановление базы данных из архива...")
    print(f"📦 Архив: {backup_path}")
    print(f"💾 Целевая БД: {target_db_path}")
    
    # Создаем резервную копию текущей БД
    if target_db_path.exists():
        backup_current = target_db_path.with_suffix(f'.db.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
        print(f"💾 Создаю резервную копию текущей БД: {backup_current}")
        try:
            shutil.copy2(target_db_path, backup_current)
            print(f"✅ Резервная копия создана")
        except Exception as e:
            print(f"⚠️  Не удалось создать резервную копию: {e}")
            response = input("Продолжить без резервной копии? (y/N): ")
            if response.lower() not in ['y', 'yes', 'да', 'д']:
                return False
    
    try:
        with zipfile.ZipFile(backup_path, 'r') as zipf:
            # Ищем файлы базы данных в архиве
            db_files = [f for f in zipf.namelist() if f.endswith('.db')]
            
            if not db_files:
                print("❌ В архиве не найдены файлы базы данных!")
                return False
            
            # Находим основной файл БД (обычно M_Kran_Kingesepp.db)
            main_db_file = None
            for db_file in db_files:
                if 'M_Kran_Kingesepp' in db_file or 'Kingesepp' in db_file:
                    main_db_file = db_file
                    break
            
            if not main_db_file:
                # Берем первый найденный файл БД
                main_db_file = db_files[0]
                print(f"⚠️  Основной файл БД не найден, используем: {main_db_file}")
            
            print(f"📥 Извлекаю базу данных: {main_db_file}")
            
            # Извлекаем во временную папку
            temp_dir = Path('temp_restore_db')
            temp_dir.mkdir(exist_ok=True)
            
            zipf.extract(main_db_file, temp_dir)
            extracted_db = temp_dir / main_db_file
            
            # Если файл в подпапке, находим его
            if extracted_db.is_dir():
                # Ищем .db файл в подпапке
                for db_file in extracted_db.rglob('*.db'):
                    extracted_db = db_file
                    break
            
            if not extracted_db.exists() or extracted_db.is_dir():
                print(f"❌ Не удалось извлечь базу данных из архива!")
                shutil.rmtree(temp_dir, ignore_errors=True)
                return False
            
            # Копируем извлеченную БД на место текущей
            print(f"📋 Копирую базу данных...")
            target_db_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(extracted_db, target_db_path)
            
            # Очищаем временную папку
            shutil.rmtree(temp_dir, ignore_errors=True)
            
            print(f"✅ База данных успешно восстановлена!")
            print(f"💾 Путь: {target_db_path}")
            
            # Проверяем размер восстановленной БД
            size_mb = target_db_path.stat().st_size / (1024 * 1024)
            print(f"📊 Размер восстановленной БД: {size_mb:.2f} MB")
            
            return True
            
    except Exception as e:
        print(f"❌ Ошибка при восстановлении: {e}")
        import traceback
        print(traceback.format_exc())
        return False

def main():
    """Основная функция"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Восстановление базы данных из архива')
    parser.add_argument('--backup-number', type=int, help='Номер архива для восстановления (1-N)')
    parser.add_argument('--latest', action='store_true', help='Восстановить из последнего архива')
    parser.add_argument('--auto', action='store_true', help='Автоматически восстановить без подтверждения')
    args = parser.parse_args()
    
    print("🚀 Восстановление базы данных M_Kran из архива")
    print("=" * 60)
    
    # Показываем список архивов с БД
    database_backups = list_database_backups()
    
    if not database_backups:
        print("❌ Резервные копии с базами данных не найдены!")
        return
    
    # Выбор архива
    if args.latest:
        # Используем последний архив
        selected = database_backups[0]
        print(f"\n✅ Автоматически выбран последний архив: {selected['backup']['name']}")
    elif args.backup_number:
        # Используем указанный номер
        backup_index = args.backup_number - 1
        if 0 <= backup_index < len(database_backups):
            selected = database_backups[backup_index]
            print(f"\n✅ Выбран архив по номеру: {selected['backup']['name']}")
        else:
            print(f"❌ Неверный номер! Доступны архивы 1-{len(database_backups)}")
            return
    else:
        # Интерактивный выбор
        while True:
            try:
                choice = input(f"\n📝 Выберите номер архива (1-{len(database_backups)}): ").strip()
                backup_index = int(choice) - 1
                
                if 0 <= backup_index < len(database_backups):
                    selected = database_backups[backup_index]
                    break
                else:
                    print("❌ Неверный номер!")
            except ValueError:
                print("❌ Введите число!")
            except (EOFError, KeyboardInterrupt):
                print("\n❌ Восстановление отменено")
                return
    
    selected_backup = selected['backup']
    print(f"💾 Файлов БД в архиве: {len(selected['db_files'])}")
    
    # Подтверждение
    if not args.auto:
        print("\n⚠️  ВНИМАНИЕ: Текущая база данных будет заменена!")
        try:
            response = input("Продолжить восстановление? (y/N): ")
            if response.lower() not in ['y', 'yes', 'да', 'д']:
                print("❌ Восстановление отменено")
                return
        except (EOFError, KeyboardInterrupt):
            print("\n❌ Восстановление отменено")
            return
    
    # Восстановление
    backup_path = Path(selected_backup['path'])
    success = restore_database_from_backup(backup_path)
    
    if success:
        print("\n🎉 База данных успешно восстановлена!")
        print("💡 Рекомендуется перезапустить приложение для применения изменений")
    else:
        print("\n💥 Ошибка при восстановлении базы данных!")

if __name__ == "__main__":
    main()

