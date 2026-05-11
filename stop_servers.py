#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
M_Kran Server Stopper
Скрипт для корректной остановки всех серверов M_Kran
"""

import os
import sys
import time
import signal
import subprocess
import psutil
from pathlib import Path

def find_processes_by_name(process_name):
    """Находит процессы по имени"""
    processes = []
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['name'] and process_name.lower() in proc.info['name'].lower():
                processes.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return processes

def find_processes_by_port(port):
    """Находит процессы, использующие определенный порт"""
    processes = []
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            # Системные PID (0, 4) не относятся к пользовательским процессам,
            # их нельзя штатно завершать и они только создают ложные ошибки.
            if proc.pid in (0, 4):
                continue
            for conn in proc.net_connections(kind='inet'):
                if conn.laddr and conn.laddr.port == port:
                    processes.append(proc)
                    break
        except (psutil.NoSuchProcess, psutil.AccessDenied, AttributeError):
            continue
    return processes

def find_processes_by_cmdline(keywords):
    """Находит процессы по ключевым словам в командной строке"""
    processes = []
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['cmdline']:
                cmdline = ' '.join(proc.info['cmdline']).lower()
                if all(keyword.lower() in cmdline for keyword in keywords):
                    processes.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return processes

def stop_process(proc, timeout=5):
    """Корректно останавливает процесс"""
    try:
        print(f"  Останавливаем процесс {proc.pid} ({proc.name()})...")
        
        # Сначала пробуем мягкое завершение
        proc.terminate()
        
        # Ждем завершения
        try:
            proc.wait(timeout=timeout)
            print(f"    [OK] Процесс {proc.pid} завершен корректно")
            return True
        except psutil.TimeoutExpired:
            print(f"    [WARN] Процесс {proc.pid} не завершился, принудительно завершаем...")
            proc.kill()
            try:
                proc.wait(timeout=2)
                print(f"    [OK] Процесс {proc.pid} принудительно завершен")
                return True
            except psutil.TimeoutExpired:
                print(f"    [ERROR] Не удалось завершить процесс {proc.pid}")
                return False
                
    except psutil.NoSuchProcess:
        print(f"    [OK] Процесс {proc.pid} уже завершен")
        return True
    except psutil.AccessDenied:
        print(f"    [ERROR] Нет прав для завершения процесса {proc.pid}")
        return False
    except Exception as e:
        print(f"    [ERROR] Ошибка при завершении процесса {proc.pid}: {e}")
        return False

def stop_m_kran_servers():
    """Останавливает все серверы M_Kran"""
    print("=" * 60)
    print("    M_Kran Server Stopper")
    print("    Остановка всех серверов M_Kran")
    print("=" * 60)
    print()
    
    stopped_count = 0
    total_count = 0
    protected_pids = {os.getpid(), os.getppid()}

    def stop_if_allowed(proc):
        """Не останавливает текущий и родительский процессы."""
        if proc.pid in protected_pids:
            print(f"  Пропускаем защищенный процесс {proc.pid} ({proc.name()})")
            return None
        return stop_process(proc)
    
    # 1. Останавливаем Flask серверы (порт 5000)
    print("1. Поиск Flask серверов (порт 5000)...")
    flask_processes = find_processes_by_port(5000)
    if flask_processes:
        print(f"   Найдено {len(flask_processes)} процессов на порту 5000:")
        for proc in flask_processes:
            result = stop_if_allowed(proc)
            if result is not None:
                total_count += 1
                if result:
                    stopped_count += 1
    else:
        print("   Flask серверы не найдены")
    
    # 2. Останавливаем HTML серверы (порт 8080)
    print("\n2. Поиск HTML серверов (порт 8080)...")
    html_processes = find_processes_by_port(8080)
    if html_processes:
        print(f"   Найдено {len(html_processes)} процессов на порту 8080:")
        for proc in html_processes:
            result = stop_if_allowed(proc)
            if result is not None:
                total_count += 1
                if result:
                    stopped_count += 1
    else:
        print("   HTML серверы не найдены")
    
    # 3. Останавливаем процессы по имени Python с app.py
    print("\n3. Поиск Python процессов с app.py...")
    python_app_processes = find_processes_by_cmdline(['python', 'app.py'])
    if python_app_processes:
        print(f"   Найдено {len(python_app_processes)} Python процессов с app.py:")
        for proc in python_app_processes:
            result = stop_if_allowed(proc)
            if result is not None:
                total_count += 1
                if result:
                    stopped_count += 1
    else:
        print("   Python процессы с app.py не найдены")
    
    # 4. Останавливаем процессы по имени Python с web_launcher.py
    print("\n4. Поиск Python процессов с web_launcher.py...")
    launcher_processes = find_processes_by_cmdline(['python', 'web_launcher.py'])
    if launcher_processes:
        print(f"   Найдено {len(launcher_processes)} процессов с web_launcher.py:")
        for proc in launcher_processes:
            result = stop_if_allowed(proc)
            if result is not None:
                total_count += 1
                if result:
                    stopped_count += 1
    else:
        print("   Процессы с web_launcher.py не найдены")
    
    # 5. Останавливаем Node.js процессы (Vue.js dev server)
    print("\n5. Поиск Node.js процессов...")
    node_processes = find_processes_by_name('node')
    vue_processes = []
    for proc in node_processes:
        try:
            if proc.info['cmdline']:
                cmdline = ' '.join(proc.info['cmdline']).lower()
                if 'vue' in cmdline or 'vite' in cmdline or 'dev' in cmdline:
                    vue_processes.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    
    if vue_processes:
        print(f"   Найдено {len(vue_processes)} Vue.js/Node.js процессов:")
        for proc in vue_processes:
            result = stop_if_allowed(proc)
            if result is not None:
                total_count += 1
                if result:
                    stopped_count += 1
    else:
        print("   Vue.js/Node.js процессы не найдены")
    
    # 6. Останавливаем процессы PowerShell с Vue скриптами
    print("\n6. Поиск PowerShell процессов с Vue скриптами...")
    ps_processes = find_processes_by_name('powershell')
    vue_ps_processes = []
    for proc in ps_processes:
        try:
            if proc.info['cmdline']:
                cmdline = ' '.join(proc.info['cmdline']).lower()
                if 'vue' in cmdline or 'launch_vue' in cmdline:
                    vue_ps_processes.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    
    if vue_ps_processes:
        print(f"   Найдено {len(vue_ps_processes)} PowerShell процессов с Vue:")
        for proc in vue_ps_processes:
            result = stop_if_allowed(proc)
            if result is not None:
                total_count += 1
                if result:
                    stopped_count += 1
    else:
        print("   PowerShell процессы с Vue не найдены")
    
    # Итоговая статистика
    print("\n" + "=" * 60)
    print("ИТОГОВАЯ СТАТИСТИКА:")
    print(f"   Всего найдено процессов: {total_count}")
    print(f"   Успешно остановлено: {stopped_count}")
    print(f"   Не удалось остановить: {total_count - stopped_count}")
    
    if total_count == 0:
        print("   [OK] Все серверы M_Kran уже остановлены")
    elif stopped_count == total_count:
        print("   [OK] Все серверы M_Kran успешно остановлены")
    else:
        print("   [WARN] Некоторые серверы не удалось остановить")
        print("   Попробуйте запустить скрипт от имени администратора")
    
    print("=" * 60)
    
    return stopped_count == total_count

def main():
    """Главная функция"""
    try:
        # Проверяем наличие psutil
        try:
            import psutil
        except ImportError:
            print("ОШИБКА: Модуль psutil не установлен!")
            print("Установите его командой: pip install psutil")
            return 1
        
        # Останавливаем серверы
        success = stop_m_kran_servers()
        
        if success:
            print("\n[OK] Все серверы M_Kran остановлены корректно")
            return 0
        else:
            print("\n[WARN] Некоторые серверы не удалось остановить")
            return 1
            
    except KeyboardInterrupt:
        print("\n\nОстановка прервана пользователем")
        return 1
    except Exception as e:
        print(f"\nОШИБКА: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
