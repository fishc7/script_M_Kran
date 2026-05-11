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
        logging.FileHandler(get_log_path('load_work_order_log_NDT_improved'), encoding='utf-8'),
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

class WorkOrderLogProcessor:
    def __init__(self, db_path):
        self.db_path = db_path
        self.base_path = r"D:\МК_Кран\МК_Кран_Кингесеп\НК\Заявки_НК"
        self.new_format_path = os.path.join(self.base_path, "Заявки_excel")
        self.old_format_path = os.path.join(self.base_path, "Заявки_excel_старого вида")
        
        # Ожидаемые столбцы для базы данных
        self.expected_columns = [
            '№ п/п', '№ заявки', 'Дата заявки', '№ сварного соединения', 
            'Тип сварного соединения', 'Толщина стенки, мм', 'Диаметр, мм',
            'Материал', 'Способ сварки', 'Сварщик', '№ удостоверения сварщика',
            'Дата сварки', 'Дата контроля', 'Результат контроля', 'Примечание'
        ]
        
    def find_excel_files(self, directory):
        """Рекурсивный поиск Excel файлов"""
        excel_files = []
        if os.path.exists(directory):
            for root, dirs, files in os.walk(directory):
                for file in files:
                    if file.lower().endswith(('.xlsx', '.xls')):
                        excel_files.append(os.path.join(root, file))
        return excel_files
    
    def extract_request_number(self, file_path):
        """Извлечение номера заявки из имени файла"""
        file_name = os.path.basename(file_path)
        
        # Паттерны для извлечения номера заявки
        patterns = [
            r'ТТ\s*(\d+)',  # ТТ 116, ТТ 117, etc.
            r'№\s*ТТ\s*(\d+)',  # № ТТ 116
            r'Заявка\s+ТТ\s*(\d+)',  # Заявка ТТ 116
            r'НГС-ЭКСПЕРТ\s+№\s*ТТ\s*(\d+)'  # НГС-ЭКСПЕРТ № ТТ 005
        ]
        
        for pattern in patterns:
            match = re.search(pattern, file_name, re.IGNORECASE)
            if match:
                return f"ТТ {match.group(1)}"
        
        return None
    
    def find_header_row(self, df):
        """Поиск строки с заголовками"""
        for i in range(min(10, len(df))):
            row = df.iloc[i]
            # Проверяем, содержит ли строка ключевые слова заголовков
            row_str = ' '.join(str(cell).lower() for cell in row if pd.notna(cell))
            
            # Ищем ключевые слова заголовков
            keywords = ['№', 'заявки', 'сварного', 'соединения', 'толщина', 'диаметр', 'материал']
            if any(keyword in row_str for keyword in keywords):
                return i
        
        return 0  # По умолчанию первая строка
    
    def normalize_headers(self, headers):
        """Нормализация заголовков"""
        normalized = []
        for header in headers:
            if pd.isna(header):
                normalized.append("")
            else:
                # Убираем лишние пробелы и символы
                clean_header = str(header).strip()
                # Заменяем переносы строк на пробелы
                clean_header = clean_header.replace('\n', ' ')
                # Убираем множественные пробелы
                clean_header = ' '.join(clean_header.split())
                normalized.append(clean_header)
        return normalized
    
    def apply_header_mapping(self, df, file_format='new'):
        """Применение маппинга заголовков к DataFrame"""
        if file_format == 'new':
            # Для нового формата используем базовые заголовки
            return df
        
        # Для старого формата применяем маппинг
        df_mapped = df.copy()
        
        # Нормализуем заголовки
        current_headers = self.normalize_headers(df_mapped.columns)
        df_mapped.columns = current_headers
        
        # Переименовываем столбцы согласно маппингу
        for base_header, similar_headers in HEADER_MAPPING.items():
            for similar_header in similar_headers:
                normalized_similar = self.normalize_headers([similar_header])[0]
                if normalized_similar in df_mapped.columns:
                    df_mapped = df_mapped.rename(columns={normalized_similar: base_header})
                    logger.info(f"Переименован столбец '{normalized_similar}' -> '{base_header}'")
                    break
        
        return df_mapped
    
    def process_file(self, file_path):
        """Обработка одного файла"""
        try:
            logger.info(f"Обрабатываю файл: {file_path}")
            
            # Определяем формат файла
            file_format = 'old' if 'старого вида' in file_path else 'new'
            
            # Читаем файл
            df = pd.read_excel(file_path, header=None)
            
            # Ищем строку с заголовками
            header_row = self.find_header_row(df)
            logger.info(f"Найдены заголовки в строке {header_row}")
            
            # Читаем файл с правильной строкой заголовков
            df = pd.read_excel(file_path, header=header_row)
            
            # Нормализуем заголовки
            df.columns = self.normalize_headers(df.columns)
            
            # Применяем маппинг заголовков
            df = self.apply_header_mapping(df, file_format)
            
            # Извлекаем номер заявки из имени файла
            request_number = self.extract_request_number(file_path)
            
            # Добавляем номер заявки, если его нет в данных
            if request_number and '№ заявки' not in df.columns:
                df['№ заявки'] = request_number
            
            # Добавляем путь к файлу для отслеживания
            df['file_path'] = file_path
            
            # Удаляем пустые строки
            df = df.dropna(how='all')
            
            # Удаляем строки, где все основные столбцы пустые
            main_columns = ['№ сварного соединения', 'Тип сварного соединения', 'Способ сварки']
            existing_main_columns = [col for col in main_columns if col in df.columns]
            if existing_main_columns:
                df = df.dropna(subset=existing_main_columns, how='all')
            
            logger.info(f"Обработано строк: {len(df)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Ошибка при обработке файла {file_path}: {e}")
            return None
    
    def create_table(self):
        """Создание таблицы в базе данных"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Создаем таблицу с правильными столбцами
        columns_sql = []
        for col in self.expected_columns:
            columns_sql.append(f'"{col}" TEXT')
        columns_sql.append('file_path TEXT')
        columns_sql.append('processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS work_order_log_NDT (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            {', '.join(columns_sql)}
        )
        """
        
        cursor.execute(create_sql)
        conn.commit()
        conn.close()
        
        logger.info("Таблица work_order_log_NDT создана/обновлена")
    
    def insert_data(self, df):
        """Вставка данных в базу"""
        if df is None or df.empty:
            return 0
        
        conn = sqlite3.connect(self.db_path)
        
        # Подготавливаем данные для вставки
        for col in self.expected_columns:
            if col not in df.columns:
                df[col] = None
        
        # Выбираем только нужные столбцы
        columns_to_insert = self.expected_columns + ['file_path']
        df_to_insert = df[columns_to_insert].copy()
        
        # Вставляем данные
        df_to_insert.to_sql('work_order_log_NDT', conn, if_exists='append', index=False)
        
        inserted_rows = len(df_to_insert)
        conn.close()
        
        logger.info(f"Вставлено строк: {inserted_rows}")
        return inserted_rows
    
    def check_existing_files(self):
        """Проверка уже обработанных файлов"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT DISTINCT file_path FROM work_order_log_NDT")
        existing_files = {row[0] for row in cursor.fetchall()}
        
        conn.close()
        return existing_files
    
    def run(self):
        """Основной метод запуска обработки"""
        logger.info("Начинаю обработку файлов заявок НДТ...")
        
        # Создаем таблицу
        self.create_table()
        
        # Находим все файлы
        new_files = self.find_excel_files(self.new_format_path)
        old_files = self.find_excel_files(self.old_format_path)
        all_files = new_files + old_files
        
        logger.info(f"Найдено файлов нового формата: {len(new_files)}")
        logger.info(f"Найдено файлов старого формата: {len(old_files)}")
        
        # Проверяем уже обработанные файлы
        existing_files = self.check_existing_files()
        logger.info(f"Уже обработано файлов: {len(existing_files)}")
        
        # Обрабатываем новые файлы
        total_processed = 0
        total_inserted = 0
        
        for file_path in all_files:
            if file_path in existing_files:
                logger.info(f"Файл уже обработан: {os.path.basename(file_path)}")
                continue
            
            # Обрабатываем файл
            df = self.process_file(file_path)
            if df is not None and not df.empty:
                inserted = self.insert_data(df)
                total_inserted += inserted
                total_processed += 1
                
                logger.info(f"  ✓ {os.path.basename(file_path)}")
        
        logger.info("=" * 60)
        logger.info("Обработка завершена")
        logger.info(f"Обработано файлов: {total_processed}")
        logger.info(f"Вставлено строк: {total_inserted}")

def main():
    db_path = r"BD_Kingisepp/M_Kran_Kingesepp.db"
    processor = WorkOrderLogProcessor(db_path)
    processor.run()

def run_script():
    """Функция для запуска скрипта из GUI приложения"""
    main()

if __name__ == "__main__":
    main() 