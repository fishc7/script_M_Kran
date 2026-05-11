#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Простой HTTP сервер для обслуживания HTML отчетов
"""

import http.server
import socketserver
import os
import threading
import time
from pathlib import Path

class HTMLReportHandler(http.server.SimpleHTTPRequestHandler):
    """Обработчик для HTML отчетов"""
    
    def __init__(self, *args, **kwargs):
        # Устанавливаем рабочую директорию на папку results
        results_dir = Path(__file__).parent.parent.parent / 'results'
        os.chdir(results_dir)
        super().__init__(*args, **kwargs)
    
    def end_headers(self):
        # Добавляем CORS заголовки для разрешения доступа из веб-приложения
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        super().end_headers()

class HTMLReportServer:
    """Сервер для обслуживания HTML отчетов"""
    
    def __init__(self, port=8080):
        self.port = port
        self.server = None
        self.thread = None
        self.running = False
    
    def start(self):
        """Запуск сервера"""
        if self.running:
            return
        
        try:
            self.server = socketserver.TCPServer(("", self.port), HTMLReportHandler)
            self.thread = threading.Thread(target=self.server.serve_forever)
            self.thread.daemon = True
            self.thread.start()
            self.running = True
            print(f"🌐 HTML сервер запущен на порту {self.port}")
            print(f"📂 Обслуживает файлы из: {Path(__file__).parent.parent.parent / 'results'}")
        except Exception as e:
            print(f"❌ Ошибка запуска HTML сервера: {e}")
    
    def stop(self):
        """Остановка сервера"""
        if self.server and self.running:
            self.server.shutdown()
            self.server.server_close()
            self.running = False
            print("🛑 HTML сервер остановлен")
    
    def get_url(self, filename):
        """Получить URL для файла"""
        return f"http://localhost:{self.port}/{filename}"

# Глобальный экземпляр сервера
html_server = HTMLReportServer()

def start_html_server():
    """Запуск HTML сервера"""
    html_server.start()
    return html_server

def stop_html_server():
    """Остановка HTML сервера"""
    html_server.stop()

def get_html_url(filename):
    """Получить URL для HTML файла"""
    return html_server.get_url(filename)

if __name__ == "__main__":
    # Тестовый запуск
    server = start_html_server()
    try:
        print("Сервер запущен. Нажмите Ctrl+C для остановки.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        stop_html_server()
