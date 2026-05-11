#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import socket
import subprocess
import sys
import os

def check_port(port):
    """Проверяет, занят ли порт"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            result = s.connect_ex(('127.0.0.1', port))
            return result == 0
    except Exception as e:
        print(f"Ошибка проверки порта {port}: {e}")
        return False

def get_process_using_port(port):
    """Получает информацию о процессе, использующем порт"""
    try:
        result = subprocess.run(
            ['netstat', '-ano'], 
            capture_output=True, 
            text=True, 
            shell=True
        )
        
        for line in result.stdout.split('\n'):
            if f':{port}' in line and 'LISTENING' in line:
                parts = line.split()
                if len(parts) >= 5:
                    pid = parts[-1]
                    return pid
        return None
    except Exception as e:
        print(f"Ошибка получения информации о процессе: {e}")
        return None

def main():
    print("ДИАГНОСТИКА ПОРТОВ")
    print("=" * 50)
    
    # Проверяем основные порты
    ports_to_check = [5000, 8080, 3000, 8000]
    
    for port in ports_to_check:
        is_occupied = check_port(port)
        if is_occupied:
            pid = get_process_using_port(port)
            print(f"[ЗАНЯТ] Порт {port}")
            if pid:
                print(f"   Процесс ID: {pid}")
                try:
                    # Пытаемся получить имя процесса
                    result = subprocess.run(
                        ['tasklist', '/FI', f'PID eq {pid}'], 
                        capture_output=True, 
                        text=True, 
                        shell=True
                    )
                    lines = result.stdout.split('\n')
                    if len(lines) > 1:
                        process_info = lines[1].split()
                        if len(process_info) > 0:
                            print(f"   Процесс: {process_info[0]}")
                except:
                    pass
        else:
            print(f"[СВОБОДЕН] Порт {port}")
    
    print("\n" + "=" * 50)
    print("РЕКОМЕНДАЦИИ:")
    print()
    
    if check_port(5000):
        print("[ВНИМАНИЕ] Порт 5000 занят!")
        print("   Возможные решения:")
        print("   1. Остановите процесс, использующий порт 5000")
        print("   2. Измените порт в настройках приложения")
        print("   3. Перезагрузите компьютер")
    else:
        print("[OK] Порт 5000 свободен - можно запускать веб-приложение")
    
    print("\nДля запуска веб-приложения используйте:")
    print("   - run_web_debug.cmd (с отладкой)")
    print("   - run_web.cmd (обычный запуск)")
    
    try:
        input("\nНажмите Enter для выхода...")
    except EOFError:
        pass

if __name__ == "__main__":
    main()
