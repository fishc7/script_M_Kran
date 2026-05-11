#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Система резервного копирования проекта M_Kran
Автор: AI Assistant
Версия: 1.0
"""

import os
import sys
import shutil
import zipfile
import tarfile
import json
import datetime
import logging
from pathlib import Path
import argparse
import hashlib
from typing import List, Dict, Optional

class BackupSystem:
    def __init__(self, project_root: str = None):
        """Инициализация системы резервного копирования"""
        self.project_root = Path(project_root) if project_root else Path(__file__).parent
        self.backup_dir = self.project_root / "backups"
        self.backup_dir.mkdir(exist_ok=True)
        
        # Настройка логирования
        self.setup_logging()
        
        # Папки и файлы для исключения из бэкапа
        self.exclude_patterns = [
            '__pycache__',
            '*.pyc',
            '*.pyo',
            '*.pyd',
            '.git',
            '.gitignore',
            'venv',
            'env',
            'node_modules',
            '*.log',
            '*.tmp',
            '*.temp',
            'backups',
            'build',
            'dist',
            '.pytest_cache',
            '.coverage',
            '*.egg-info',
            '.DS_Store',
            'Thumbs.db'
        ]
        
        # Критически важные папки для бэкапа
        self.critical_dirs = [
            'scripts',
            'web',
            'database',
            'config',
            'docs'
        ]
        
        # Критически важные файлы
        self.critical_files = [
            'README.md',
            'launch.py',
            'launch.ps1',
            'web_launcher.py',
            'requirements.txt'
        ]

    def setup_logging(self):
        """Настройка системы логирования"""
        log_file = self.backup_dir / f"backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

    def should_exclude(self, path: Path) -> bool:
        """Проверка, нужно ли исключить файл/папку из бэкапа"""
        path_str = str(path)
        
        for pattern in self.exclude_patterns:
            if pattern.startswith('*'):
                if path_str.endswith(pattern[1:]):
                    return True
            elif pattern in path_str:
                return True
        
        return False

    def create_full_backup(self, backup_name: str = None) -> str:
        """Создание полного бэкапа проекта"""
        if not backup_name:
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_name = f"full_backup_{timestamp}"
        
        backup_path = self.backup_dir / f"{backup_name}.zip"
        
        self.logger.info(f"Начинаю создание полного бэкапа: {backup_path}")
        
        try:
            with zipfile.ZipFile(backup_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(self.project_root):
                    # Исключаем папки
                    dirs[:] = [d for d in dirs if not self.should_exclude(Path(root) / d)]
                    
                    for file in files:
                        file_path = Path(root) / file
                        
                        if not self.should_exclude(file_path):
                            try:
                                arcname = file_path.relative_to(self.project_root)
                                zipf.write(file_path, arcname)
                                self.logger.debug(f"Добавлен файл: {arcname}")
                            except Exception as e:
                                self.logger.warning(f"Ошибка при добавлении файла {file_path}: {e}")
            
            # Создание метаданных бэкапа
            self.create_backup_metadata(backup_path, "full")
            
            self.logger.info(f"Полный бэкап успешно создан: {backup_path}")
            return str(backup_path)
            
        except Exception as e:
            self.logger.error(f"Ошибка при создании полного бэкапа: {e}")
            raise

    def create_critical_backup(self, backup_name: str = None) -> str:
        """Создание бэкапа только критически важных файлов и папок"""
        if not backup_name:
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_name = f"critical_backup_{timestamp}"
        
        backup_path = self.backup_dir / f"{backup_name}.zip"
        
        self.logger.info(f"Начинаю создание критического бэкапа: {backup_path}")
        
        try:
            with zipfile.ZipFile(backup_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # Добавляем критически важные папки
                for dir_name in self.critical_dirs:
                    dir_path = self.project_root / dir_name
                    if dir_path.exists():
                        for root, dirs, files in os.walk(dir_path):
                            # Исключаем папки
                            dirs[:] = [d for d in dirs if not self.should_exclude(Path(root) / d)]
                            
                            for file in files:
                                file_path = Path(root) / file
                                if not self.should_exclude(file_path):
                                    try:
                                        arcname = file_path.relative_to(self.project_root)
                                        zipf.write(file_path, arcname)
                                        self.logger.debug(f"Добавлен файл: {arcname}")
                                    except Exception as e:
                                        self.logger.warning(f"Ошибка при добавлении файла {file_path}: {e}")
                
                # Добавляем критически важные файлы
                for file_name in self.critical_files:
                    file_path = self.project_root / file_name
                    if file_path.exists():
                        try:
                            arcname = file_path.relative_to(self.project_root)
                            zipf.write(file_path, arcname)
                            self.logger.debug(f"Добавлен файл: {arcname}")
                        except Exception as e:
                            self.logger.warning(f"Ошибка при добавлении файла {file_path}: {e}")
            
            # Создание метаданных бэкапа
            self.create_backup_metadata(backup_path, "critical")
            
            self.logger.info(f"Критический бэкап успешно создан: {backup_path}")
            return str(backup_path)
            
        except Exception as e:
            self.logger.error(f"Ошибка при создании критического бэкапа: {e}")
            raise

    def create_database_backup(self, backup_name: str = None) -> str:
        """Создание бэкапа только базы данных"""
        if not backup_name:
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_name = f"database_backup_{timestamp}"
        
        backup_path = self.backup_dir / f"{backup_name}.zip"
        database_dir = self.project_root / "database"
        
        if not database_dir.exists():
            self.logger.warning("Папка database не найдена")
            return None
        
        self.logger.info(f"Начинаю создание бэкапа базы данных: {backup_path}")
        
        try:
            with zipfile.ZipFile(backup_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(database_dir):
                    for file in files:
                        file_path = Path(root) / file
                        try:
                            arcname = file_path.relative_to(self.project_root)
                            zipf.write(file_path, arcname)
                            self.logger.debug(f"Добавлен файл БД: {arcname}")
                        except Exception as e:
                            self.logger.warning(f"Ошибка при добавлении файла БД {file_path}: {e}")
            
            # Создание метаданных бэкапа
            self.create_backup_metadata(backup_path, "database")
            
            self.logger.info(f"Бэкап базы данных успешно создан: {backup_path}")
            return str(backup_path)
            
        except Exception as e:
            self.logger.error(f"Ошибка при создании бэкапа базы данных: {e}")
            raise

    def create_backup_metadata(self, backup_path: Path, backup_type: str):
        """Создание метаданных бэкапа"""
        metadata = {
            "backup_type": backup_type,
            "created_at": datetime.datetime.now().isoformat(),
            "project_root": str(self.project_root),
            "backup_size": backup_path.stat().st_size,
            "files_count": 0,
            "excluded_patterns": self.exclude_patterns
        }
        
        # Подсчет файлов в архиве
        with zipfile.ZipFile(backup_path, 'r') as zipf:
            metadata["files_count"] = len(zipf.namelist())
        
        # Сохранение метаданных
        metadata_path = backup_path.with_suffix('.json')
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Метаданные сохранены: {metadata_path}")

    def list_backups(self) -> List[Dict]:
        """Список всех доступных бэкапов"""
        backups = []
        
        for backup_file in self.backup_dir.glob("*.zip"):
            metadata_file = backup_file.with_suffix('.json')
            
            backup_info = {
                "name": backup_file.name,
                "path": str(backup_file),
                "size": backup_file.stat().st_size,
                "created_at": datetime.datetime.fromtimestamp(backup_file.stat().st_mtime).isoformat(),
                "type": "unknown"
            }
            
            if metadata_file.exists():
                try:
                    with open(metadata_file, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                        backup_info.update(metadata)
                except Exception as e:
                    self.logger.warning(f"Ошибка чтения метаданных {metadata_file}: {e}")
            
            backups.append(backup_info)
        
        return sorted(backups, key=lambda x: x["created_at"], reverse=True)

    def restore_backup(self, backup_path: str, restore_dir: str = None) -> bool:
        """Восстановление из бэкапа"""
        backup_path = Path(backup_path)
        
        if not backup_path.exists():
            self.logger.error(f"Бэкап не найден: {backup_path}")
            return False
        
        if not restore_dir:
            restore_dir = self.project_root / f"restored_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
        else:
            restore_dir = Path(restore_dir)
        
        restore_dir.mkdir(exist_ok=True)
        
        self.logger.info(f"Начинаю восстановление из бэкапа: {backup_path}")
        self.logger.info(f"Папка восстановления: {restore_dir}")
        
        try:
            with zipfile.ZipFile(backup_path, 'r') as zipf:
                zipf.extractall(restore_dir)
            
            self.logger.info(f"Восстановление завершено успешно: {restore_dir}")
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка при восстановлении: {e}")
            return False

    def cleanup_old_backups(self, keep_count: int = 10):
        """Очистка старых бэкапов, оставляя только последние N"""
        backups = self.list_backups()
        
        if len(backups) <= keep_count:
            self.logger.info(f"Количество бэкапов ({len(backups)}) не превышает лимит ({keep_count})")
            return
        
        backups_to_delete = backups[keep_count:]
        
        for backup in backups_to_delete:
            backup_path = Path(backup["path"])
            metadata_path = backup_path.with_suffix('.json')
            
            try:
                if backup_path.exists():
                    backup_path.unlink()
                    self.logger.info(f"Удален бэкап: {backup_path}")
                
                if metadata_path.exists():
                    metadata_path.unlink()
                    self.logger.info(f"Удалены метаданные: {metadata_path}")
                    
            except Exception as e:
                self.logger.error(f"Ошибка при удалении {backup_path}: {e}")

    def verify_backup(self, backup_path: str) -> bool:
        """Проверка целостности бэкапа"""
        backup_path = Path(backup_path)
        
        if not backup_path.exists():
            self.logger.error(f"Бэкап не найден: {backup_path}")
            return False
        
        self.logger.info(f"Проверяю целостность бэкапа: {backup_path}")
        
        try:
            with zipfile.ZipFile(backup_path, 'r') as zipf:
                # Проверяем целостность архива
                if zipf.testzip() is not None:
                    self.logger.error("Обнаружены поврежденные файлы в архиве")
                    return False
                
                # Подсчитываем файлы
                file_count = len(zipf.namelist())
                self.logger.info(f"Архив содержит {file_count} файлов")
                
                # Проверяем метаданные
                metadata_file = backup_path.with_suffix('.json')
                if metadata_file.exists():
                    with open(metadata_file, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                        if metadata.get("files_count") != file_count:
                            self.logger.warning("Количество файлов в метаданных не совпадает с архивом")
                
                self.logger.info("Проверка целостности завершена успешно")
                return True
                
        except Exception as e:
            self.logger.error(f"Ошибка при проверке бэкапа: {e}")
            return False

def main():
    """Основная функция для запуска из командной строки"""
    parser = argparse.ArgumentParser(description="Система резервного копирования проекта M_Kran")
    parser.add_argument("--type", choices=["full", "critical", "database"], 
                       default="full", help="Тип бэкапа")
    parser.add_argument("--name", help="Имя бэкапа")
    parser.add_argument("--list", action="store_true", help="Показать список бэкапов")
    parser.add_argument("--restore", help="Путь к бэкапу для восстановления")
    parser.add_argument("--restore-dir", help="Папка для восстановления")
    parser.add_argument("--verify", help="Проверить целостность бэкапа")
    parser.add_argument("--cleanup", type=int, metavar="COUNT", 
                       help="Очистить старые бэкапы, оставив COUNT последних")
    parser.add_argument("--project-root", help="Корневая папка проекта")
    
    args = parser.parse_args()
    
    backup_system = BackupSystem(args.project_root)
    
    try:
        if args.list:
            backups = backup_system.list_backups()
            print(f"\nНайдено {len(backups)} бэкапов:")
            for backup in backups:
                size_mb = backup["size"] / (1024 * 1024)
                print(f"  {backup['name']} ({size_mb:.1f} MB) - {backup['created_at']} - {backup.get('type', 'unknown')}")
        
        elif args.restore:
            success = backup_system.restore_backup(args.restore, args.restore_dir)
            if success:
                print("Восстановление завершено успешно")
            else:
                print("Ошибка при восстановлении")
                sys.exit(1)
        
        elif args.verify:
            success = backup_system.verify_backup(args.verify)
            if success:
                print("Проверка целостности пройдена успешно")
            else:
                print("Обнаружены проблемы с целостностью")
                sys.exit(1)
        
        elif args.cleanup:
            backup_system.cleanup_old_backups(args.cleanup)
            print(f"Очистка завершена, оставлено {args.cleanup} последних бэкапов")
        
        else:
            # Создание бэкапа
            if args.type == "full":
                backup_path = backup_system.create_full_backup(args.name)
            elif args.type == "critical":
                backup_path = backup_system.create_critical_backup(args.name)
            elif args.type == "database":
                backup_path = backup_system.create_database_backup(args.name)
            
            if backup_path:
                size_mb = Path(backup_path).stat().st_size / (1024 * 1024)
                print(f"Бэкап создан успешно: {backup_path} ({size_mb:.1f} MB)")
    
    except Exception as e:
        print(f"Ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
