import pandas as pd

def check_excel_structure():
    """
    Проверяет структуру Excel файла и столбца диаметр
    """
    
    print("=== ПРОВЕРКА СТРУКТУРЫ EXCEL ФАЙЛА ===")
    
    # Загружаем данные из Excel (лист свод)
    print("\n1. Загружаем данные из Excel (лист свод)...")
    df_excel = pd.read_excel('D:/МК_Кран/МК_Кран_Кингесеп/ОГС/Переодика/переодика.xlsx', sheet_name='свод')
    
    print(f"Загружено {len(df_excel)} строк из Excel")
    print("Столбцы Excel:", df_excel.columns.tolist())
    
    # Анализируем столбец диаметр
    print("\n2. Анализ столбца 'диаметр':")
    print("Уникальные значения диаметра:")
    unique_diameters = df_excel['диаметр'].unique()
    unique_diameters_sorted = sorted(unique_diameters, key=lambda x: float(x) if str(x).replace('.', '').replace(',', '').isdigit() else 0)
    
    for i, diameter in enumerate(unique_diameters_sorted[:20], 1):  # Показываем первые 20
        print(f"  {i}. {diameter}")
    
    if len(unique_diameters_sorted) > 20:
        print(f"  ... и еще {len(unique_diameters_sorted) - 20} значений")
    
    # Статистика по диаметрам
    print(f"\n3. Статистика по диаметрам:")
    print(f"   Всего уникальных диаметров: {len(unique_diameters_sorted)}")
    
    # Проверяем диапазоны
    numeric_diameters = []
    for diameter in unique_diameters:
        try:
            # Заменяем запятую на точку для корректного преобразования
            diameter_str = str(diameter).replace(',', '.')
            numeric_diameters.append(float(diameter_str))
        except:
            continue
    
    if numeric_diameters:
        min_dn = min(numeric_diameters)
        max_dn = max(numeric_diameters)
        print(f"   Минимальный диаметр: {min_dn}")
        print(f"   Максимальный диаметр: {max_dn}")
        
        # Группируем по диапазонам
        dn_0_75_to_6 = [d for d in numeric_diameters if 0.75 <= d <= 6]
        dn_above_6 = [d for d in numeric_diameters if d > 6]
        
        print(f"   DN 0,75 ≤ 6: {len(dn_0_75_to_6)} значений")
        print(f"   DN > 6: {len(dn_above_6)} значений")
    
    # Показываем примеры данных
    print(f"\n4. Примеры данных:")
    print(df_excel[['материал', 'клеймо', 'способ_сварки', 'диаметр', 'Дата', 'Кол_во']].head(10))

if __name__ == "__main__":
    check_excel_structure()

