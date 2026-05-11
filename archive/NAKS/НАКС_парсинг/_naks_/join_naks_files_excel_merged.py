import pandas as pd
import numpy as np
import os

def join_excel_files():
    # Получаем путь к директории, где находится скрипт
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Вычисляем корень проекта (поднимаемся на 4 уровня вверх: _naks_ -> НАКС_парсинг -> NAKS -> archive -> корень)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(current_dir))))
    
    # Проверяем оба возможных пути к файлам (где они реально сохраняются)
    possible_naks_dirs = [
        os.path.join(project_root, 'NAKS', 'НАКС_парсинг'),  # Новые данные (где сохраняет naks_search.py)
        os.path.join(project_root, 'archive', 'NAKS', 'НАКС_парсинг'),  # Старые данные
        os.path.dirname(current_dir),  # На один уровень выше от скрипта (для обратной совместимости)
    ]
    
    # Ищем директорию, где есть оба файла
    naks_dir = None
    for dir_path in possible_naks_dirs:
        main_file = os.path.join(dir_path, 'naks_главное.xlsx')
        details_file = os.path.join(dir_path, 'naks_подробнее.xlsx')
        if os.path.exists(main_file) and os.path.exists(details_file):
            naks_dir = dir_path
            print(f"Найдены файлы в директории: {naks_dir}")
            break
    
    if not naks_dir:
        # Если не нашли, используем первый путь по умолчанию
        naks_dir = possible_naks_dirs[0]
        print(f"Используем директорию по умолчанию: {naks_dir}")
    
    # Читаем Excel файлы с правильными путями
    main_file_path = os.path.join(naks_dir, 'naks_главное.xlsx')
    details_file_path = os.path.join(naks_dir, 'naks_подробнее.xlsx')
    
    if not os.path.exists(main_file_path):
        raise FileNotFoundError(f"Файл не найден: {main_file_path}")
    if not os.path.exists(details_file_path):
        raise FileNotFoundError(f"Файл не найден: {details_file_path}")
    
    print(f"Читаем файлы:")
    print(f"  Основной файл: {main_file_path}")
    print(f"  Детальный файл: {details_file_path}")
    
    df_main = pd.read_excel(main_file_path)
    df_details = pd.read_excel(details_file_path)
    
    # Выполняем левое объединение по индексу строк
    merged_df = pd.merge(df_main, df_details, 
                        how='left', 
                        left_index=True,
                        right_index=True)
    
    # Проверяем результаты объединения
    print("\nКоличество записей:")
    print(f"naks_главное.xlsx: {len(df_main)}")
    print(f"naks_подробнее.xlsx: {len(df_details)}")
    print(f"Объединенный файл: {len(merged_df)}")
    
    # Показываем первые строки объединённого файла
    print("\nПервые строки объединённого файла:")
    print(merged_df.head())
    
    # Сохраняем результат в ту же папку, где находятся исходные файлы
    output_path = os.path.join(naks_dir, 'naks_merged.xlsx')
    merged_df.to_excel(output_path, index=False, engine='openpyxl')
    print(f"\n✅ Файлы успешно объединены и сохранены как '{output_path}'")
    print(f"   Объединено {len(merged_df)} записей из {len(df_main)} основных и {len(df_details)} детальных")

if __name__ == "__main__":
    try:
        join_excel_files()
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}") 