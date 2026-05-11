#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import time

def check_script_status(script_id):
    """Проверяет статус выполнения скрипта"""
    url = f'http://localhost:5000/script_status/{script_id}'
    
    print('Проверяем статус выполнения скрипта...')
    for i in range(10):
        try:
            response = requests.get(url)
            print(f'Попытка {i+1}: Статус {response.status_code}')
            if response.status_code == 200:
                data = response.json()
                status = data.get('status', 'неизвестно')
                progress = data.get('progress', 0)
                print(f'Статус: {status}')
                print(f'Прогресс: {progress}%')
                
                if status == 'completed':
                    print('[OK] Скрипт выполнен успешно!')
                    return True
                elif status == 'error':
                    error = data.get('error', 'неизвестная ошибка')
                    print(f'[ERROR] Ошибка: {error}')
                    return False
                elif status == 'running':
                    print('🔄 Скрипт выполняется...')
            else:
                print(f'Ошибка HTTP: {response.status_code}')
        except Exception as e:
            print(f'Ошибка при проверке статуса: {e}')
        
        time.sleep(3)
    
    print('⏰ Время ожидания истекло')
    return False

if __name__ == "__main__":
    script_id = 'script_1758453148210'
    check_script_status(script_id)

