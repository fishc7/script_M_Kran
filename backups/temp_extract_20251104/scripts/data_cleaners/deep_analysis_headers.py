import os
import pandas as pd
import logging
from pathlib import Path
import json
from collections import defaultdict, Counter
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(get_log_path('deep_analysis_headers'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class HeaderAnalyzer:
    def __init__(self):
        self.base_path = r"D:\МК_Кран\МК_Кран_Кингесеп\НК\Заявки_НК"
        self.new_format_path = os.path.join(self.base_path, "Заявки_excel")
        self.old_format_path = os.path.join(self.base_path, "Заявки_excel_старого вида")
        
        # Базовые заголовки из нового формата (взяты за основу)
        self.base_headers = [
            '№ п/п', '№ заявки', 'Дата заявки', '№ сварного соединения', 
            'Тип сварного соединения', 'Толщина стенки, мм', 'Диаметр, мм',
            'Материал', 'Способ сварки', 'Сварщик', '№ удостоверения сварщика',
            'Дата сварки', 'Дата контроля', 'Результат контроля', 'Примечание'
        ]
        
        self.all_headers = []
        self.header_mapping = {}
        self.file_analysis = {}
        
    def find_excel_files(self, directory):
        """Рекурсивный поиск Excel файлов"""
        excel_files = []
        if os.path.exists(directory):
            for root, dirs, files in os.walk(directory):
                for file in files:
                    if file.lower().endswith(('.xlsx', '.xls')):
                        excel_files.append(os.path.join(root, file))
        return excel_files
    
    def extract_headers_from_file(self, file_path):
        """Извлечение заголовков из файла с детальным анализом"""
        try:
            logger.info(f"Анализирую файл: {file_path}")
            
            # Пробуем разные способы чтения
            df = None
            header_row = None
            
            # Сначала пробуем стандартное чтение
            try:
                df = pd.read_excel(file_path, header=0)
                header_row = 0
            except Exception as e:
                logger.warning(f"Не удалось прочитать с header=0: {e}")
                
                # Пробуем найти заголовки в первых строках
                for i in range(10):  # Проверяем первые 10 строк
                    try:
                        temp_df = pd.read_excel(file_path, header=i)
                        # Проверяем, есть ли осмысленные заголовки
                        if len(temp_df.columns) > 0 and not temp_df.columns[0].startswith('Unnamed'):
                            df = temp_df
                            header_row = i
                            logger.info(f"Найдены заголовки в строке {i}")
                            break
                    except:
                        continue
            
            if df is None:
                logger.error(f"Не удалось прочитать файл: {file_path}")
                return None, None
            
            # Получаем заголовки
            headers = list(df.columns)
            
            # Очищаем заголовки от NaN и приводим к строковому типу
            cleaned_headers = []
            for header in headers:
                if pd.isna(header):
                    cleaned_headers.append("")
                else:
                    cleaned_headers.append(str(header).strip())
            
            # Анализируем первые несколько строк данных
            sample_data = df.head(3).to_dict('records')
            
            return cleaned_headers, sample_data
            
        except Exception as e:
            logger.error(f"Ошибка при анализе файла {file_path}: {e}")
            return None, None
    
    def analyze_all_files(self):
        """Анализ всех файлов в обеих папках"""
        logger.info("Начинаю глубокий анализ заголовков...")
        
        # Находим все файлы
        new_files = self.find_excel_files(self.new_format_path)
        old_files = self.find_excel_files(self.old_format_path)
        
        logger.info(f"Найдено файлов нового формата: {len(new_files)}")
        logger.info(f"Найдено файлов старого формата: {len(old_files)}")
        
        all_files = []
        
        # Анализируем файлы нового формата
        for file_path in new_files:
            headers, sample_data = self.extract_headers_from_file(file_path)
            if headers:
                self.all_headers.extend(headers)
                self.file_analysis[file_path] = {
                    'format': 'new',
                    'headers': headers,
                    'sample_data': sample_data,
                    'header_row': 0
                }
                all_files.append(file_path)
        
        # Анализируем файлы старого формата
        for file_path in old_files:
            headers, sample_data = self.extract_headers_from_file(file_path)
            if headers:
                self.all_headers.extend(headers)
                self.file_analysis[file_path] = {
                    'format': 'old',
                    'headers': headers,
                    'sample_data': sample_data,
                    'header_row': 0
                }
                all_files.append(file_path)
        
        logger.info(f"Всего проанализировано файлов: {len(all_files)}")
        
        return all_files
    
    def analyze_header_patterns(self):
        """Анализ паттернов заголовков"""
        logger.info("Анализирую паттерны заголовков...")
        
        # Подсчитываем частоту заголовков
        header_counts = Counter(self.all_headers)
        
        # Группируем похожие заголовки
        similar_headers = defaultdict(list)
        
        for header in header_counts.keys():
            if header:  # Пропускаем пустые заголовки
                # Нормализуем заголовок для группировки
                normalized = self.normalize_header(header)
                similar_headers[normalized].append(header)
        
        # Создаем маппинг заголовков
        self.create_header_mapping(similar_headers, header_counts)
        
        return similar_headers, header_counts
    
    def normalize_header(self, header):
        """Нормализация заголовка для группировки"""
        if not header:
            return ""
        
        # Приводим к нижнему регистру
        normalized = header.lower()
        
        # Убираем лишние пробелы и символы
        normalized = ' '.join(normalized.split())
        
        # Заменяем русские символы на латинские аналоги
        replacements = {
            '№': 'n',
            'п/п': 'pp',
            'заявки': 'zayavki',
            'сварного': 'svarnogo',
            'соединения': 'soedineniya',
            'толщина': 'tolshchina',
            'стенки': 'stenki',
            'диаметр': 'diametr',
            'материал': 'material',
            'способ': 'sposob',
            'сварки': 'svarki',
            'сварщик': 'svarshchik',
            'удостоверения': 'udostovereniya',
            'дата': 'data',
            'контроля': 'kontrolya',
            'результат': 'rezultat',
            'примечание': 'primechanie'
        }
        
        for rus, lat in replacements.items():
            normalized = normalized.replace(rus, lat)
        
        return normalized
    
    def create_header_mapping(self, similar_headers, header_counts):
        """Создание маппинга заголовков к базовым"""
        logger.info("Создаю маппинг заголовков...")
        
        # Создаем маппинг для каждого базового заголовка
        for base_header in self.base_headers:
            normalized_base = self.normalize_header(base_header)
            
            # Ищем похожие заголовки
            similar = []
            for norm_header, headers in similar_headers.items():
                if self.is_similar(normalized_base, norm_header):
                    similar.extend(headers)
            
            if similar:
                self.header_mapping[base_header] = similar
                logger.info(f"Базовый заголовок '{base_header}' -> {similar}")
    
    def is_similar(self, base_normalized, header_normalized):
        """Проверка схожести заголовков"""
        if not base_normalized or not header_normalized:
            return False
        
        # Простая проверка на вхождение
        return (base_normalized in header_normalized or 
                header_normalized in base_normalized or
                self.calculate_similarity(base_normalized, header_normalized) > 0.7)
    
    def calculate_similarity(self, str1, str2):
        """Вычисление схожести строк"""
        if not str1 or not str2:
            return 0
        
        # Простой алгоритм схожести
        words1 = set(str1.split())
        words2 = set(str2.split())
        
        if not words1 or not words2:
            return 0
        
        intersection = words1.intersection(words2)
        union = words1.union(words2)
        
        return len(intersection) / len(union)
    
    def generate_mapping_code(self):
        """Генерация кода для маппинга заголовков"""
        logger.info("Генерирую код для маппинга заголовков...")
        
        mapping_code = """
# Автоматически сгенерированный маппинг заголовков
HEADER_MAPPING = {
"""
        
        for base_header, similar_headers in self.header_mapping.items():
            mapping_code += f"    '{base_header}': {similar_headers},\n"
        
        mapping_code += "}\n\n"
        
        # Добавляем функцию для применения маппинга
        mapping_code += """
def apply_header_mapping(df, file_format='new'):
    \"\"\"Применение маппинга заголовков к DataFrame\"\"\"
    if file_format == 'new':
        # Для нового формата используем базовые заголовки
        return df
    
    # Для старого формата применяем маппинг
    df_mapped = df.copy()
    
    # Переименовываем столбцы согласно маппингу
    for base_header, similar_headers in HEADER_MAPPING.items():
        for similar_header in similar_headers:
            if similar_header in df_mapped.columns:
                df_mapped = df_mapped.rename(columns={similar_header: base_header})
                break
    
    return df_mapped
"""
        
        return mapping_code
    
    def clean_data_for_json(self, data):
        """Очистка данных для сохранения в JSON"""
        if isinstance(data, dict):
            cleaned = {}
            for key, value in data.items():
                # Преобразуем ключи в строки
                if isinstance(key, (datetime, pd.Timestamp)):
                    cleaned[str(key)] = self.clean_data_for_json(value)
                else:
                    cleaned[str(key)] = self.clean_data_for_json(value)
            return cleaned
        elif isinstance(data, list):
            return [self.clean_data_for_json(item) for item in data]
        elif isinstance(data, (datetime, pd.Timestamp)):
            return str(data)
        elif pd.isna(data):
            return None
        else:
            return data
    
    def save_analysis_results(self):
        """Сохранение результатов анализа"""
        logger.info("Сохраняю результаты анализа...")
        
        # Очищаем данные для JSON
        cleaned_file_analysis = self.clean_data_for_json(self.file_analysis)
        
        # Сохраняем детальный анализ в JSON
        with open('header_analysis_results.json', 'w', encoding='utf-8') as f:
            json.dump(cleaned_file_analysis, f, ensure_ascii=False, indent=2)
        
        # Сохраняем маппинг заголовков
        with open('header_mapping.json', 'w', encoding='utf-8') as f:
            json.dump(self.header_mapping, f, ensure_ascii=False, indent=2)
        
        # Сохраняем код маппинга
        mapping_code = self.generate_mapping_code()
        with open('header_mapping_code.py', 'w', encoding='utf-8') as f:
            f.write(mapping_code)
        
        logger.info("Результаты сохранены в файлы:")
        logger.info("- header_analysis_results.json")
        logger.info("- header_mapping.json") 
        logger.info("- header_mapping_code.py")
    
    def print_summary(self):
        """Вывод сводки анализа"""
        logger.info("=" * 80)
        logger.info("СВОДКА АНАЛИЗА ЗАГОЛОВКОВ")
        logger.info("=" * 80)
        
        # Статистика по форматам
        new_count = sum(1 for info in self.file_analysis.values() if info['format'] == 'new')
        old_count = sum(1 for info in self.file_analysis.values() if info['format'] == 'old')
        
        logger.info(f"Файлов нового формата: {new_count}")
        logger.info(f"Файлов старого формата: {old_count}")
        logger.info(f"Всего уникальных заголовков: {len(set(self.all_headers))}")
        
        # Базовые заголовки
        logger.info("\nБазовые заголовки (новый формат):")
        for i, header in enumerate(self.base_headers, 1):
            logger.info(f"  {i:2d}. {header}")
        
        # Маппинг заголовков
        logger.info("\nМаппинг заголовков (старый -> новый формат):")
        for base_header, similar_headers in self.header_mapping.items():
            logger.info(f"\n{base_header}:")
            for similar in similar_headers:
                logger.info(f"  -> {similar}")
        
        logger.info("=" * 80)

def main():
    analyzer = HeaderAnalyzer()
    
    # Анализируем все файлы
    analyzer.analyze_all_files()
    
    # Анализируем паттерны заголовков
    analyzer.analyze_header_patterns()
    
    # Сохраняем результаты
    analyzer.save_analysis_results()
    
    # Выводим сводку
    analyzer.print_summary()
    
    logger.info("Глубокий анализ заголовков завершен!")

if __name__ == "__main__":
    main()

def run_script():
    """Функция для запуска скрипта из главной формы"""
    main() 