import os
import pandas as pd
import shutil
from pathlib import Path
import datetime
import logging

def setup_logging():
    """Настройка логирования для отслеживания процесса переименования"""
    log_filename = f"logs/rename_pdf_iso_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def load_excel_data(file_path):
    """
    Загружает данные из Excel файла для переименования PDF
    
    Args:
        file_path (str): Путь к Excel файлу
    
    Returns:
        pandas.DataFrame: Загруженные данные
    """
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"Загружаю данные из файла: {file_path}")
        
        # Проверяем существование файла
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Файл не найден: {file_path}")
        
        # Пытаемся загрузить данные из разных листов
        excel_file = pd.ExcelFile(file_path)
        logger.info(f"Доступные листы: {excel_file.sheet_names}")
        
        # Ищем подходящий лист
        sheet_name = None
        for sheet in excel_file.sheet_names:
            if any(keyword in str(sheet).lower() for keyword in ['pdf', 'файл', 'переименование', 'список', 'iso']):
                sheet_name = sheet
                break
        
        if sheet_name is None:
            # Если не нашли подходящий лист, берем первый
            sheet_name = excel_file.sheet_names[0]
        
        logger.info(f"Используем лист: {sheet_name}")
        
        # Загружаем данные из Excel
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        
        logger.info(f"Успешно загружено {len(df)} строк и {len(df.columns)} столбцов")
        logger.info(f"Столбцы: {list(df.columns)}")
        
        # Выводим первые несколько строк для проверки
        logger.info("Первые 5 строк данных:")
        logger.info(df.head().to_string())
        
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных из Excel: {e}")
        raise

def clean_data(df):
    """
    Очищает и подготавливает данные для переименования
    
    Args:
        df (pandas.DataFrame): Исходные данные
    
    Returns:
        pandas.DataFrame: Очищенные данные
    """
    logger = logging.getLogger(__name__)
    
    logger.info("Начинаю очистку данных...")
    
    # Удаляем полностью пустые строки
    initial_rows = len(df)
    df = df.dropna(how='all')
    logger.info(f"Удалено {initial_rows - len(df)} полностью пустых строк")
    
    # Удаляем полностью пустые столбцы
    initial_cols = len(df.columns)
    df = df.dropna(axis=1, how='all')
    logger.info(f"Удалено {initial_cols - len(df.columns)} полностью пустых столбцов")
    
    # Очищаем названия столбцов
    df.columns = df.columns.str.strip()
    
    # Заполняем NaN значения пустыми строками для текстовых столбцов
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].fillna('')
    
    logger.info(f"После очистки: {len(df)} строк и {len(df.columns)} столбцов")
    logger.info(f"Столбцы после очистки: {list(df.columns)}")
    
    return df

def find_pdf_files(pdf_directory):
    """
    Находит все PDF файлы в указанной директории
    
    Args:
        pdf_directory (str): Путь к директории с PDF файлами
    
    Returns:
        list: Список путей к PDF файлам
    """
    logger = logging.getLogger(__name__)
    
    if not os.path.exists(pdf_directory):
        logger.error(f"Директория не найдена: {pdf_directory}")
        return []
    
    pdf_files = []
    for file in os.listdir(pdf_directory):
        if file.lower().endswith('.pdf'):
            pdf_files.append(os.path.join(pdf_directory, file))
    
    logger.info(f"Найдено {len(pdf_files)} PDF файлов в директории {pdf_directory}")
    return pdf_files

def create_mapping_from_excel(df):
    """
    Создает словарь соответствия старых и новых имен файлов из Excel данных
    
    Args:
        df (pandas.DataFrame): Данные из Excel
    
    Returns:
        dict: Словарь {старое_имя: новое_имя}
    """
    logger = logging.getLogger(__name__)
    
    mapping = {}
    
    # Ищем столбец "имя_новое"
    new_name_col = None
    for col in df.columns:
        col_lower = str(col).lower()
        if 'имя_новое' in col_lower or 'новое_имя' in col_lower or 'new_name' in col_lower:
            new_name_col = col
            break
    
    if new_name_col is None:
        logger.error("Столбец 'имя_новое' не найден в Excel файле")
        logger.info(f"Доступные столбцы: {list(df.columns)}")
        return mapping
    
    # Используем первый столбец как старое имя, столбец "имя_новое" как новое имя
    old_name_col = df.columns[0]
    
    logger.info(f"Используем столбцы: {old_name_col} -> {new_name_col}")
    
    for idx, row in df.iterrows():
        old_name = str(row[old_name_col]).strip()
        new_name = str(row[new_name_col]).strip()
        
        if old_name and new_name and old_name != 'nan' and new_name != 'nan':
            # Убираем расширение .pdf если есть
            if old_name.lower().endswith('.pdf'):
                old_name = old_name[:-4]
            if new_name.lower().endswith('.pdf'):
                new_name = new_name[:-4]
            
            mapping[old_name] = new_name
            logger.info(f"Добавлено соответствие: {old_name} -> {new_name}")
    
    logger.info(f"Создано {len(mapping)} соответствий для переименования")
    return mapping

def rename_pdf_files(pdf_directory, mapping, backup=True):
    """
    Переименовывает PDF файлы согласно словарю соответствий
    
    Args:
        pdf_directory (str): Путь к директории с PDF файлами
        mapping (dict): Словарь соответствия {старое_имя: новое_имя}
        backup (bool): Создавать ли резервную копию
    
    Returns:
        tuple: (успешно_переименовано, ошибки)
    """
    logger = logging.getLogger(__name__)
    
    if backup:
        backup_dir = os.path.join(pdf_directory, f"backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}")
        os.makedirs(backup_dir, exist_ok=True)
        logger.info(f"Создана резервная копия в: {backup_dir}")
    
    pdf_files = find_pdf_files(pdf_directory)
    renamed_count = 0
    errors = []
    
    for pdf_file in pdf_files:
        try:
            # Получаем имя файла без пути и расширения
            file_name = os.path.basename(pdf_file)
            file_name_without_ext = os.path.splitext(file_name)[0]
            
            # Ищем соответствие в словаре
            if file_name_without_ext in mapping:
                new_name = mapping[file_name_without_ext]
                new_file_path = os.path.join(pdf_directory, f"{new_name}.pdf")
                
                # Проверяем, не существует ли уже файл с таким именем
                if os.path.exists(new_file_path):
                    logger.warning(f"Файл {new_file_path} уже существует, пропускаем {pdf_file}")
                    errors.append(f"Файл {new_file_path} уже существует")
                    continue
                
                # Создаем резервную копию
                if backup:
                    backup_file = os.path.join(backup_dir, file_name)
                    shutil.copy2(pdf_file, backup_file)
                
                # Переименовываем файл
                os.rename(pdf_file, new_file_path)
                logger.info(f"Переименован: {file_name} -> {new_name}.pdf")
                renamed_count += 1
                
            else:
                logger.warning(f"Не найдено соответствие для файла: {file_name}")
                errors.append(f"Не найдено соответствие для {file_name}")
                
        except Exception as e:
            logger.error(f"Ошибка при переименовании {pdf_file}: {e}")
            errors.append(f"Ошибка при переименовании {pdf_file}: {e}")
    
    logger.info(f"Переименовано {renamed_count} файлов")
    if errors:
        logger.warning(f"Найдено {len(errors)} ошибок")
    
    return renamed_count, errors

def main():
    """Основная функция скрипта"""
    logger = setup_logging()
    
    logger.info("=== ЗАПУСК СКРИПТА ПЕРЕИМЕНОВАНИЯ PDF ФАЙЛОВ (ISO) ===")
    
    try:
        # Пути к файлам
        current_dir = os.path.dirname(os.path.abspath(__file__))
        excel_file = os.path.join(current_dir, "преименовать_pdf_iso.xlsx")
        pdf_directory = os.path.join(current_dir, "raspulovka_pdfs")
        
        # Проверяем существование файлов
        if not os.path.exists(excel_file):
            logger.error(f"Excel файл не найден: {excel_file}")
            return
        
        if not os.path.exists(pdf_directory):
            logger.error(f"Директория с PDF файлами не найдена: {pdf_directory}")
            return
        
        # Загружаем данные из Excel
        df = load_excel_data(excel_file)
        
        # Очищаем данные
        df = clean_data(df)
        
        # Создаем словарь соответствий
        mapping = create_mapping_from_excel(df)
        
        if not mapping:
            logger.error("Не удалось создать словарь соответствий для переименования")
            return
        
        # Переименовываем файлы
        renamed_count, errors = rename_pdf_files(pdf_directory, mapping, backup=True)
        
        logger.info("=== РЕЗУЛЬТАТЫ ПЕРЕИМЕНОВАНИЯ ===")
        logger.info(f"Успешно переименовано: {renamed_count} файлов")
        
        if errors:
            logger.info("Ошибки:")
            for error in errors:
                logger.info(f"  - {error}")
        
        logger.info("Скрипт завершен успешно!")
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise

if __name__ == "__main__":
    main() 