import os
import pandas as pd
import sqlite3
import logging
from pathlib import Path
import re
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(get_log_path('force_reload_all_data'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Автоматически сгенерированный маппинг заголовков на основе анализа
HEADER_MAPPING = {
    '№ п/п': ['No', 'No.', '№ п/п', 'N п/п'],
    '№ заявки': ['№ заявки', 'Номер заявки', 'Заявка №'],
    'Дата заявки': ['Дата заявки', 'Дата', 'Date'],
    '№ сварного соединения': ['№ сварного соединения', 'Номер сварного соединения', 'Joint No'],
    'Тип сварного соединения': ['Тип сварного соединения', 'Тип соединения', 'Joint Type'],
    'Толщина стенки, мм': ['Толщина стенки, мм', 'Толщина, мм', 'Толщина'],
    'Диаметр, мм': ['Диаметр, мм', 'Диаметр', 'Ø, мм'],
    'Материал': ['Материал', 'Material'],
    'Способ сварки': ['Способ\nсварки /\nWelding\nProcess(es)', 'Способ сварки', 'Welding Process'],
    'Сварщик': [
        '№ клейма\nсварщика №1\n(корень) /\nWelder #1\nID\n(Root pass)',
        '№ клейма\nсварщика №2\n(заполнение) /\nWelder #2\nID\n(Filling pass)',
        '№ клейма\nсварщика №3\n(облицовка) /\nWelder #3\nID\n(Facing pass)',
        'Сварщик', 'Welder'
    ],
    '№ удостоверения сварщика': ['№ удостоверения сварщика', 'Удостоверение сварщика'],
    'Дата сварки': ['Дата сварки /\nWelding date', 'Дата сварки', 'Welding Date'],
    'Дата контроля': ['Дата контроля', 'Дата проверки'],
    'Результат контроля': ['Результат контроля', 'Результат', 'Result'],
    'Примечание': ['Примечание\n/ Note', 'Примечание', 'Note']
}

class ForceDataReloader:
    def __init__(self):
        self.db_path = r"BD_Kingisepp\M_Kran_Kingesepp.db"
        self.base_path = r"D:\МК_Кран\МК_Кран_Кингесеп\НК\Заявки_НК"
        self.new_format_path = os.path.join(self.base_path, "Заявки_excel")
        self.old_format_path = os.path.join(self.base_path, "Заявки_excel_старого вида")
        
    def create_tables(self):
        """Создание таблиц в базе данных"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Создание таблицы для хранения обработанных файлов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT UNIQUE,
                file_name TEXT,
                processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Создание основной таблицы для данных
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS work_order_log_NDT (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                "№ п/п" TEXT,
                "№ заявки" TEXT,
                "Дата заявки" TEXT,
                "№ сварного соединения" TEXT,
                "Тип сварного соединения" TEXT,
                "Толщина стенки, мм" TEXT,
                "Диаметр, мм" TEXT,
                "Материал" TEXT,
                "Способ сварки" TEXT,
                "Сварщик" TEXT,
                "№ удостоверения сварщика" TEXT,
                "Дата сварки" TEXT,
                "Дата контроля" TEXT,
                "Результат контроля" TEXT,
                "Примечание" TEXT,
                file_path TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("Таблицы созданы/обновлены")
    
    def clear_all_data(self):
        """Очистка всех данных из таблиц"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Очистка основной таблицы
        cursor.execute("DELETE FROM work_order_log_NDT")
        logger.info("Очищена таблица work_order_log_NDT")
        
        # Очистка таблицы обработанных файлов
        cursor.execute("DELETE FROM processed_files")
        logger.info("Очищена таблица processed_files")
        
        conn.commit()
        conn.close()
        logger.info("Все данные очищены")
    
    def find_excel_files(self):
        """Поиск всех Excel файлов в обеих папках"""
        files = []
        
        # Поиск в папке нового формата
        if os.path.exists(self.new_format_path):
            for file in os.listdir(self.new_format_path):
                if file.endswith('.xlsx'):
                    files.append(os.path.join(self.new_format_path, file))
        
        # Поиск в папке старого формата
        if os.path.exists(self.old_format_path):
            for file in os.listdir(self.old_format_path):
                if file.endswith('.xlsx'):
                    files.append(os.path.join(self.old_format_path, file))
        
        return files
    
    def extract_request_number(self, filename):
        """Извлечение номера заявки из имени файла"""
        # Паттерны для поиска номера заявки
        patterns = [
            r'ТТ\s*(\d+)',  # ТТ 123
            r'TT\s*(\d+)',  # TT 123
            r'№\s*ТТ\s*(\d+)',  # № ТТ 123
            r'НГС-ЭКСПЕРТ\s*№\s*ТТ\s*(\d+)',  # НГС-ЭКСПЕРТ № ТТ 123
        ]
        
        for pattern in patterns:
            match = re.search(pattern, filename, re.IGNORECASE)
            if match:
                return f"ТТ {match.group(1)}"
        
        return "Неизвестно"
    
    def find_headers_row(self, df):
        """Поиск строки с заголовками"""
        for i, row in df.iterrows():
            # Проверяем, содержит ли строка ключевые слова заголовков
            row_str = ' '.join(str(cell) for cell in row if pd.notna(cell)).lower()
            if any(keyword in row_str for keyword in ['№ п/п', 'заявки', 'сварного', 'сварки', 'сварщик']):
                return i
        return 0  # По умолчанию первая строка
    
    def normalize_headers(self, df):
        """Нормализация заголовков столбцов"""
        rename_dict = {}
        
        for col in df.columns:
            col_str = str(col).strip()
            
            # Поиск соответствия в маппинге
            for target_header, source_headers in HEADER_MAPPING.items():
                if any(source_header.strip() == col_str for source_header in source_headers):
                    rename_dict[col] = target_header
                    logger.info(f"Переименован столбец '{col_str}' -> '{target_header}'")
                    break
        
        if rename_dict:
            df = df.rename(columns=rename_dict)
        
        return df
    
    def process_file(self, file_path):
        """Обработка одного файла"""
        try:
            logger.info(f"Обрабатываю файл: {file_path}")
            
            # Чтение Excel файла
            df = pd.read_excel(file_path, header=None)
            
            # Поиск строки с заголовками
            headers_row = self.find_headers_row(df)
            logger.info(f"Найдены заголовки в строке {headers_row}")
            
            # Чтение файла с правильными заголовками
            df = pd.read_excel(file_path, header=headers_row)
            
            # Нормализация заголовков
            df = self.normalize_headers(df)
            
            # Удаление пустых строк
            df = df.dropna(how='all')
            
            # Извлечение номера заявки из имени файла
            request_number = self.extract_request_number(os.path.basename(file_path))
            
            # Подготовка данных для вставки
            data_to_insert = []
            for index, row in df.iterrows():
                if pd.isna(row.get('№ п/п', '')) and pd.isna(row.get('№ заявки', '')):
                    continue  # Пропускаем строки без ключевых данных
                
                data_row = {
                    '№ п/п': str(row.get('№ п/п', '')),
                    '№ заявки': request_number,
                    'Дата заявки': str(row.get('Дата заявки', '')),
                    '№ сварного соединения': str(row.get('№ сварного соединения', '')),
                    'Тип сварного соединения': str(row.get('Тип сварного соединения', '')),
                    'Толщина стенки, мм': str(row.get('Толщина стенки, мм', '')),
                    'Диаметр, мм': str(row.get('Диаметр, мм', '')),
                    'Материал': str(row.get('Материал', '')),
                    'Способ сварки': str(row.get('Способ сварки', '')),
                    'Сварщик': str(row.get('Сварщик', '')),
                    '№ удостоверения сварщика': str(row.get('№ удостоверения сварщика', '')),
                    'Дата сварки': str(row.get('Дата сварки', '')),
                    'Дата контроля': str(row.get('Дата контроля', '')),
                    'Результат контроля': str(row.get('Результат контроля', '')),
                    'Примечание': str(row.get('Примечание', '')),
                    'file_path': file_path
                }
                data_to_insert.append(data_row)
            
            # Вставка данных в базу
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Вставка данных
            cursor.executemany('''
                INSERT INTO work_order_log_NDT (
                    "№ п/п", "№ заявки", "Дата заявки", "№ сварного соединения",
                    "Тип сварного соединения", "Толщина стенки, мм", "Диаметр, мм",
                    "Материал", "Способ сварки", "Сварщик", "№ удостоверения сварщика",
                    "Дата сварки", "Дата контроля", "Результат контроля", "Примечание", file_path
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', [(
                row['№ п/п'], row['№ заявки'], row['Дата заявки'], row['№ сварного соединения'],
                row['Тип сварного соединения'], row['Толщина стенки, мм'], row['Диаметр, мм'],
                row['Материал'], row['Способ сварки'], row['Сварщик'], row['№ удостоверения сварщика'],
                row['Дата сварки'], row['Дата контроля'], row['Результат контроля'], row['Примечание'], row['file_path']
            ) for row in data_to_insert])
            
            # Запись информации об обработанном файле
            cursor.execute('''
                INSERT INTO processed_files (file_path, file_name)
                VALUES (?, ?)
            ''', (file_path, os.path.basename(file_path)))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Обработано строк: {len(data_to_insert)}")
            logger.info(f"Вставлено строк: {len(data_to_insert)}")
            logger.info(f"  ✓ {os.path.basename(file_path)}")
            
            return len(data_to_insert)
            
        except Exception as e:
            logger.error(f"Ошибка при обработке файла {file_path}: {str(e)}")
            return 0
    
    def run(self):
        """Основной метод запуска"""
        logger.info("Начинаю принудительную перезагрузку всех данных...")
        
        # Создание таблиц
        self.create_tables()
        
        # Очистка всех данных
        self.clear_all_data()
        
        # Поиск всех файлов
        files = self.find_excel_files()
        logger.info(f"Найдено файлов: {len(files)}")
        
        # Обработка файлов
        total_inserted = 0
        processed_count = 0
        
        for file_path in files:
            inserted = self.process_file(file_path)
            total_inserted += inserted
            if inserted > 0:
                processed_count += 1
        
        logger.info("=" * 60)
        logger.info("Принудительная перезагрузка завершена")
        logger.info(f"Обработано файлов: {processed_count}")
        logger.info(f"Вставлено строк: {total_inserted}")

if __name__ == "__main__":
    reloader = ForceDataReloader()
    reloader.run()

def run_script():
    """Функция для запуска скрипта из главной формы"""
    reloader = ForceDataReloader()
    reloader.run() 