import pandas as pd
import numpy as np
import os

def join_excel_files():
    # Получаем путь к директории, где находится скрипт
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Путь к директории NAKS\НАКС_парсинг (на один уровень выше)
    naks_dir = os.path.dirname(current_dir)
    
    # Читаем Excel файлы с правильными путями
    df_main = pd.read_excel(os.path.join(naks_dir, 'naks_главное.xlsx'))
    df_details = pd.read_excel(os.path.join(naks_dir, 'naks_подробнее.xlsx'))
    
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
    
    # Сохраняем результат в папку NAKS
    output_path = os.path.join(naks_dir, 'naks_merged.xlsx')
    merged_df.to_excel(output_path, index=False)
    print(f"\nФайлы успешно объединены и сохранены как '{output_path}'")

if __name__ == "__main__":
    try:
        join_excel_files()
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}") 