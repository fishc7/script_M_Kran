#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script Runner для веб-приложения M_Kran
Реализует принцип запуска скриптов как в десктопном приложении
"""

import os
import sys
import importlib.util
import logging
import threading
import time
import warnings
from io import StringIO
from typing import Optional, Dict, Any

# Подавляем предупреждения openpyxl о стилях
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

class WebScriptRunner:
    """Класс для запуска скриптов в веб-приложении"""
    
    def __init__(self, project_root: str):
        self.project_root = project_root
        self.logger = logging.getLogger(__name__)
        self.running_scripts = {}
        
    def setup_environment(self, script_path: str) -> Dict[str, Any]:
        """Настраивает окружение для запуска скрипта"""
        script_dir = os.path.dirname(script_path)
        
        # Сохраняем текущее состояние
        original_cwd = os.getcwd()
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        original_path = sys.path.copy()
        
        # Настраиваем переменные окружения
        os.environ['SCRIPT_DIR'] = script_dir
        os.environ['PROJECT_ROOT'] = self.project_root
        os.environ['PYTHONIOENCODING'] = 'utf-8'
        os.environ['PYTHONLEGACYWINDOWSSTDIO'] = 'utf-8'
        
        # Устанавливаем кодировку для Windows
        if sys.platform.startswith('win'):
            import locale
            try:
                locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')
            except:
                try:
                    locale.setlocale(locale.LC_ALL, 'Russian_Russia.1251')
                except:
                    pass
        
        # Переходим в корневую директорию проекта для правильных импортов
        os.chdir(self.project_root)
        
        # Добавляем корневую папку проекта в sys.path для импорта модулей
        if self.project_root not in sys.path:
            sys.path.insert(0, self.project_root)
        
        # Добавляем рабочую директорию в sys.path
        if script_dir not in sys.path:
            sys.path.insert(0, script_dir)
        
        return {
            'original_cwd': original_cwd,
            'original_stdout': original_stdout,
            'original_stderr': original_stderr,
            'original_path': original_path,
            'script_dir': script_dir
        }
    
    def restore_environment(self, env_state: Dict[str, Any]):
        """Восстанавливает исходное состояние окружения"""
        try:
            # Восстанавливаем рабочую директорию
            os.chdir(env_state['original_cwd'])
            
            # Восстанавливаем sys.path
            sys.path = env_state['original_path']
            
            # Восстанавливаем stdout/stderr
            sys.stdout = env_state['original_stdout']
            sys.stderr = env_state['original_stderr']
            
        except Exception as e:
            self.logger.error(f"Ошибка при восстановлении окружения: {e}")
    
    def run_script_direct_import(self, script_path: str, script_id: str, script_args: list = None) -> Dict[str, Any]:
        """Запускает скрипт через прямой импорт (как в десктопном приложении)"""
        # Логируем информацию о контексте выполнения
        self.logger.info(f"[{script_id}] Контекст выполнения:")
        self.logger.info(f"[{script_id}] Python executable: {sys.executable}")
        self.logger.info(f"[{script_id}] Python version: {sys.version}")
        self.logger.info(f"[{script_id}] Working directory: {os.getcwd()}")
        
        result = {
            'success': False,
            'output': '',
            'errors': '',
            'message': ''
        }
        
        # Настраиваем окружение
        env_state = self.setup_environment(script_path)
        
        # Проверяем доступность SQLAlchemy
        try:
            import sqlalchemy
            self.logger.info(f"[{script_id}] SQLAlchemy доступен: {sqlalchemy.__version__}")
        except ImportError as e:
            self.logger.error(f"[{script_id}] SQLAlchemy недоступен: {e}")
            result['errors'] = f"SQLAlchemy недоступен: {e}\n"
        
        # Создаем StringIO для захвата вывода
        captured_output = StringIO()
        captured_errors = StringIO()
        
        # Инициализируем переменную для аргументов командной строки
        original_argv = sys.argv.copy()
        
        try:
            # Перенаправляем вывод
            sys.stdout = captured_output
            sys.stderr = captured_errors
            
            # Настраиваем логирование для перехвата логов
            class WebLogHandler(logging.Handler):
                def __init__(self, output_stream):
                    super().__init__()
                    self.output_stream = output_stream
                
                def emit(self, record):
                    try:
                        msg = self.format(record)
                        self.output_stream.write(msg + '\n')
                    except Exception:
                        pass
            
            # Добавляем обработчик логов
            web_handler = WebLogHandler(captured_output)
            web_handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(message)s')
            web_handler.setFormatter(formatter)
            
            root_logger = logging.getLogger()
            root_logger.addHandler(web_handler)
            
            # Обновляем прогресс - импорт модуля
            if script_id in self.running_scripts:
                self.running_scripts[script_id]['progress'] = 30
                self.running_scripts[script_id]['message'] = 'Импорт модуля...'
            
            # Импортируем и выполняем скрипт
            spec = importlib.util.spec_from_file_location("script_module", script_path)
            if spec is None:
                raise ImportError(f"Не удалось создать spec для {script_path}")
            
            module = importlib.util.module_from_spec(spec)
            
            # Обновляем прогресс - выполнение модуля
            if script_id in self.running_scripts:
                self.running_scripts[script_id]['progress'] = 50
                self.running_scripts[script_id]['message'] = 'Выполнение модуля...'
            
            # Устанавливаем аргументы командной строки если они переданы
            if script_args:
                sys.argv = [script_path] + script_args
                self.logger.info(f"[{script_id}] Установлены аргументы командной строки: {sys.argv}")
            
            # Выполняем скрипт
            if spec.loader is not None:
                spec.loader.exec_module(module)
            
            # Обновляем прогресс - поиск функции run_script
            if script_id in self.running_scripts:
                self.running_scripts[script_id]['progress'] = 70
                self.running_scripts[script_id]['message'] = 'Поиск функции run_script...'
            
            # Проверяем, есть ли функция run_script для запуска через GUI
            self.logger.info(f"[{script_id}] Проверяем атрибуты модуля: {dir(module)}")
            self.logger.info(f"[{script_id}] hasattr(module, 'run_script'): {hasattr(module, 'run_script')}")
            
            if hasattr(module, 'run_script'):
                self.logger.info(f"[{script_id}] Вызываем функцию run_script()...")
                
                # Обновляем прогресс - выполнение функции
                if script_id in self.running_scripts:
                    self.running_scripts[script_id]['progress'] = 85
                    self.running_scripts[script_id]['message'] = 'Выполнение функции run_script...'
                
                module.run_script()
            else:
                self.logger.info(f"[{script_id}] Модуль выполнен успешно (без функции run_script)")
                # Модуль уже выполнен при загрузке, дополнительных действий не требуется
            
            # Удаляем обработчик логов
            root_logger.removeHandler(web_handler)
            
            # Восстанавливаем оригинальные аргументы командной строки
            sys.argv = original_argv
            
            # Получаем захваченный вывод
            result['output'] = captured_output.getvalue()
            result['errors'] = captured_errors.getvalue()
            
            # Проверяем, есть ли ошибки в выводе или логах
            output_text = result['output'].lower()
            error_text = result['errors'].lower()
            
            # Проверяем наличие ключевых слов ошибок
            error_keywords = ['error', 'ошибка', 'failed', 'не удалось', 'не существует']
            # Исключаем ложные срабатывания
            exclude_keywords = ['успех', 'success', 'успешно', 'завершен', 'всего ошибок: 0', 'не найдена']
            
            has_errors = any(keyword in output_text or keyword in error_text for keyword in error_keywords)
            # Если есть исключающие ключевые слова, не считаем это ошибкой
            if any(exclude in output_text or exclude in error_text for exclude in exclude_keywords):
                has_errors = False
            
            # Дополнительная проверка: если в выводе есть только сообщение о том, что функция run_script не найдена,
            # но нет других ошибок, то это не считается ошибкой
            if 'функция run_script() не найдена' in output_text.lower() and not any(keyword in output_text.lower() for keyword in ['error', 'ошибка', 'failed', 'не удалось', 'не существует']):
                has_errors = False
            
            # Дополнительная проверка: если в выводе есть "всего ошибок: 0", то это не ошибка
            if 'всего ошибок: 0' in output_text.lower():
                has_errors = False
            

            
            # Упрощенная логика определения успешности
            # Считаем скрипт успешным, если нет реальных ошибок
            real_errors = False
            if has_errors:
                # Проверяем, есть ли реальные ошибки (исключая сообщения о run_script)
                real_error_keywords = ['error', 'ошибка', 'failed', 'не удалось', 'не существует', 'traceback', 'exception']
                real_errors = any(keyword in output_text.lower() for keyword in real_error_keywords)
            
            if real_errors:
                result['success'] = False
                result['message'] = f"Скрипт {os.path.basename(script_path)} завершился с ошибками"
                self.logger.warning(f"[{script_id}] Обнаружены ошибки в выводе скрипта")
            else:
                result['success'] = True
                result['message'] = f"Скрипт {os.path.basename(script_path)} выполнен успешно"
                self.logger.info(f"[{script_id}] Скрипт выполнен успешно")
            
        except Exception as e:
            # Восстанавливаем оригинальные аргументы командной строки
            sys.argv = original_argv
            
            # Получаем захваченный вывод даже в случае ошибки
            result['output'] = captured_output.getvalue()
            result['errors'] = captured_errors.getvalue()
            result['message'] = f"Ошибка выполнения: {str(e)}"
            
            self.logger.error(f"[{script_id}] Ошибка выполнения скрипта: {str(e)}")
            import traceback
            error_traceback = traceback.format_exc()
            self.logger.error(f"[{script_id}] Полный стек ошибки:\n{error_traceback}")
            result['errors'] += f"\nПолный стек ошибки:\n{error_traceback}"
            
        finally:
            # Восстанавливаем оригинальные аргументы командной строки
            sys.argv = original_argv
            
            # Восстанавливаем окружение
            self.restore_environment(env_state)
            
            # Закрываем StringIO
            captured_output.close()
            captured_errors.close()
        
        return result
    
    def run_script_async(self, script_path: str, script_args: list = None) -> str:
        """Запускает скрипт асинхронно и возвращает ID задачи"""
        script_id = f"script_{int(time.time() * 1000)}"
        
        # Инициализируем статус
        self.running_scripts[script_id] = {
            'success': None,
            'output': '',
            'errors': '',
            'message': 'Скрипт запускается...',
            'progress': 0,
            'status': 'running',
            'start_time': time.time()
        }
        
        def run_script():
            try:
                self.logger.info(f"[{script_id}] Запуск скрипта: {script_path}")
                
                # Обновляем статус
                self.running_scripts[script_id]['message'] = 'Скрипт выполняется...'
                self.running_scripts[script_id]['progress'] = 10
                
                result = self.run_script_direct_import(script_path, script_id, script_args or [])
                
                # Добавляем информацию о прогрессе
                result['progress'] = 100
                result['status'] = 'completed' if result['success'] else 'failed'
                
                # Логируем результат
                self.logger.info(f"[{script_id}] Результат выполнения: success={result['success']}, status={result['status']}")
                self.logger.info(f"[{script_id}] Сообщение: {result['message']}")
                
                # Сохраняем результат
                self.running_scripts[script_id] = result
                
                if result['success']:
                    self.logger.info(f"[{script_id}] Скрипт завершился успешно")
                else:
                    self.logger.error(f"[{script_id}] Скрипт завершился с ошибкой: {result['message']}")
                    
            except Exception as e:
                self.logger.error(f"[{script_id}] Исключение при запуске: {str(e)}")
                self.running_scripts[script_id] = {
                    'success': False,
                    'output': '',
                    'errors': str(e),
                    'message': f'Исключение при запуске: {str(e)}',
                    'progress': 0,
                    'status': 'failed'
                }
        
        # Запускаем в отдельном потоке
        thread = threading.Thread(target=run_script)
        thread.daemon = True
        thread.start()
        
        return script_id
    
    def get_script_status(self, script_id: str) -> Optional[Dict[str, Any]]:
        """Получает статус выполнения скрипта"""
        status = self.running_scripts.get(script_id)
        if status is None:
            return None
        
        # Добавляем временную метку, если её нет
        if 'timestamp' not in status:
            status['timestamp'] = time.time()
        
        # Добавляем время выполнения
        if 'start_time' in status:
            elapsed_time = time.time() - status['start_time']
            status['elapsed_time'] = round(elapsed_time, 1)
        
        # Если скрипт выполняется, обновляем прогресс на основе времени
        if status.get('status') == 'running' and 'start_time' in status:
            elapsed_time = time.time() - status['start_time']
            # Если прошло больше 30 секунд, показываем 95% прогресса
            if elapsed_time > 30 and status.get('progress', 0) < 95:
                status['progress'] = 95
                status['message'] = 'Завершение выполнения...'
        
        return status
    
    def cleanup_script(self, script_id: str):
        """Удаляет результат выполнения скрипта"""
        if script_id in self.running_scripts:
            del self.running_scripts[script_id]
    
    def cleanup_old_scripts(self, max_age_hours: int = 24):
        """Удаляет старые результаты выполнения скриптов"""
        current_time = time.time()
        script_ids_to_remove = []
        
        for script_id in self.running_scripts.keys():
            # Извлекаем время создания из ID (если возможно)
            try:
                script_time = int(script_id.split('_')[1]) / 1000
                if current_time - script_time > max_age_hours * 3600:
                    script_ids_to_remove.append(script_id)
            except (IndexError, ValueError):
                # Если не удается извлечь время, оставляем как есть
                pass
        
        for script_id in script_ids_to_remove:
            self.cleanup_script(script_id)
            self.logger.info(f"Удален старый результат скрипта: {script_id}")

# Глобальный экземпляр для использования в веб-приложении
script_runner = None

def init_script_runner(project_root: str):
    """Инициализирует глобальный экземпляр ScriptRunner"""
    global script_runner
    script_runner = WebScriptRunner(project_root)

def get_script_runner() -> WebScriptRunner:
    """Возвращает глобальный экземпляр ScriptRunner"""
    if script_runner is None:
        raise RuntimeError("ScriptRunner не инициализирован. Вызовите init_script_runner() сначала.")
    return script_runner
