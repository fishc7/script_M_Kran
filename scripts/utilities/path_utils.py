"""
Утилиты для работы с путями в проекте МК_Кран
Обеспечивает корректную работу путей при запуске из EXE и из Python
"""

import os
from pathlib import Path

def get_project_root():
    """
    Получает корневую директорию проекта МК_Кран
    """
    # Проверяем переменную окружения (устанавливается при запуске через GUI)
    if 'PROJECT_ROOT' in os.environ:
        return os.environ['PROJECT_ROOT']
    
    # Получаем директорию текущего скрипта
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Корневая директория проекта (на два уровня выше от utilities)
    # utilities -> scripts -> script_M_Kran (корень проекта)
    project_root = os.path.dirname(os.path.dirname(current_script_dir))
    
    return project_root

def get_mk_kran_kingesepp_path():
    """
    Получает путь к директории МК_Кран_Кингесеп
    """
    # Получаем корневую директорию проекта
    project_root = get_project_root()
    
    # Папка МК_Кран_Кингесеп находится на один уровень выше от script_M_Kran
    mk_kran_root = os.path.dirname(project_root)
    
    return os.path.join(mk_kran_root, "МК_Кран_Кингесеп")

def get_database_path():
    """
    Получает путь к базе данных SQLite
    """
    project_root = get_project_root()
    db_path = os.path.join(project_root, "database", "BD_Kingisepp", "M_Kran_Kingesepp.db")
    
    # Приводим путь к абсолютному виду с правильным регистром буквы диска
    db_path = os.path.abspath(db_path)
    
    # Убеждаемся, что путь использует правильный регистр буквы диска
    if db_path.startswith('d:'):
        db_path = 'D:' + db_path[2:]
    
    return db_path

def get_excel_paths():
    """
    Получает основные пути к Excel файлам
    """
    mk_kran_path = get_mk_kran_kingesepp_path()
    
    paths = {
        'smr_svarka': os.path.join(mk_kran_path, "СМР", "отчет_площадка", "сварка"),
        'pto': os.path.join(mk_kran_path, "ПТО"),
        'nk': os.path.join(mk_kran_path, "НК", "Заявки_НК", "Заявки_excel"),
        'ogs': os.path.join(mk_kran_path, "ОГС"),
        'nk_journal': os.path.join(mk_kran_path, "НК", "Журнал"),
        'nk_aks': os.path.join(mk_kran_path, "НК_АКС"),
        'nk_register': os.path.join(mk_kran_path, "НК", "Реестр_передачи_заключений"),
        'ogs_dl': os.path.join(mk_kran_path, "ОГС", "ДЛ"),
        'ogs_tks': os.path.join(mk_kran_path, "ОГС", "ТКС"),
        'ogs_tests': os.path.join(mk_kran_path, "ОГС", "Испытания"),
        'smr_rasstanovka_12460': os.path.join(mk_kran_path, "СМР", "отчет_площадка", "растановка 12460"),
        'smr_rasstanovka_12470': os.path.join(mk_kran_path, "СМР", "отчет_площадка", "растановка 12470"),
        'pto_iso': os.path.join(mk_kran_path, "ПТО", "номерация стыков по iso"),
        'ogs_weld_volume': os.path.join(mk_kran_path, "ОГС"),
        'ogs_journals': os.path.join(mk_kran_path, "ОГС", "Журналы"),
    }
    
    return paths

def validate_path(path, description=""):
    """
    Проверяет существование пути и возвращает информацию о нем
    
    Args:
        path: Путь для проверки
        description: Описание пути для логирования
        
    Returns:
        tuple: (exists, path, error_message)
    """
    if not path:
        return False, path, f"Путь не указан: {description}"
    
    if not os.path.exists(path):
        return False, path, f"Путь не существует: {path} ({description})"
    
    return True, path, ""

def get_script_log_path(script_name):
    """
    Получает путь для лог-файла скрипта
    
    Args:
        script_name: Имя скрипта (например, 'load_wl_report_smr')
        
    Returns:
        str: Полный путь к лог-файлу
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, f"{script_name}.log")

def get_relative_path_from_project_root(target_path):
    """
    Получает относительный путь от корня проекта
    
    Args:
        target_path: Целевой путь
        
    Returns:
        str: Относительный путь от корня проекта
    """
    project_root = get_project_root()
    
    try:
        # Пытаемся получить относительный путь
        relative_path = os.path.relpath(target_path, project_root)
        return relative_path
    except ValueError:
        # Если пути на разных дисках, возвращаем абсолютный путь
        return target_path

def get_log_path(script_name):
    """
    Получает правильный путь для лог-файла скрипта
    
    Args:
        script_name: Имя скрипта (например, 'load_ndt_findings_transmission_register')
        
    Returns:
        str: Полный путь к лог-файлу в директории logs проекта
    """
    project_root = get_project_root()
    log_dir = os.path.join(project_root, 'logs')
    
    # Создаем директорию для логов, если она не существует
    os.makedirs(log_dir, exist_ok=True)
    
    return os.path.join(log_dir, f'{script_name}.log') 