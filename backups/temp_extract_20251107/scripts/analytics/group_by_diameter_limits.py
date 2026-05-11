import pandas as pd
import os
from datetime import datetime

def group_by_diameter_limits():
    """
    Группирует данные по материалам, сварщикам, способам сварки
    с учетом лимитов по диаметрам:
    - DN 0,75 ≤ 6: каждые 100 стыков
    - DN > 6: каждые 50 стыков
    """
    
    print("=== ГРУППИРОВКА ПО ДИАМЕТРАМ С ЛИМИТАМИ ===")
    
    # Загружаем данные из Excel (лист свод)
    print("\n1. Загружаем данные из Excel (лист свод)...")
    df_excel = pd.read_excel('D:/МК_Кран/МК_Кран_Кингесеп/ОГС/Переодика/переодика.xlsx', sheet_name='свод')
    
    print(f"Загружено {len(df_excel)} строк из Excel")
    
    # Определяем группы диаметров
    print("\n2. Определяем группы диаметров...")
    
    def get_diameter_group(diameter):
        """Определяет группу диаметра"""
        try:
            dn = float(diameter)
            if 0.75 <= dn <= 6:
                return 'DN_0.75_to_6'
            elif dn > 6:
                return 'DN_above_6'
            else:
                return 'DN_other'
        except:
            return 'DN_other'
    
    df_excel['Группа_диаметра'] = df_excel['диаметр'].apply(get_diameter_group)
    
    # Показываем распределение по группам диаметров
    diameter_distribution = df_excel['Группа_диаметра'].value_counts()
    print("Распределение по группам диаметров:")
    for group, count in diameter_distribution.items():
        print(f"  {group}: {count} записей")
    
    # Группируем данные по материалам, сварщикам, способам сварки и группам диаметров
    print("\n3. Группируем данные...")
    
    grouped = df_excel.groupby([
        'материал', 
        'клеймо', 
        'способ_сварки', 
        'Группа_диаметра'
    ]).agg({
        'Кол_во': 'sum',
        'диаметр': 'nunique',
        'Дата': ['min', 'max']
    }).reset_index()
    
    # Переименовываем столбцы
    grouped.columns = [
        'материал', 'клеймо', 'способ_сварки', 'Группа_диаметра',
        'Общее_количество', 'Количество_диаметров', 'Дата_начала', 'Дата_окончания'
    ]
    
    # Определяем лимиты для каждой группы
    def get_limit_info(row):
        """Определяет лимит для группы диаметров"""
        diameter_group = row['Группа_диаметра']
        count = row['Общее_количество']
        
        if diameter_group == 'DN_0.75_to_6':
            limit = 100
            if count > limit:
                return f"ПРЕВЫШЕН ЛИМИТ: {count} > {limit} (DN 0,75-6)"
            else:
                return f"В пределах лимита: {count}/{limit} (DN 0,75-6)"
        elif diameter_group == 'DN_above_6':
            limit = 50
            if count > limit:
                return f"ПРЕВЫШЕН ЛИМИТ: {count} > {limit} (DN >6)"
            else:
                return f"В пределах лимита: {count}/{limit} (DN >6)"
        else:
            return "Другой диаметр"
    
    grouped['Статус_лимитов'] = grouped.apply(get_limit_info, axis=1)
    
    # Рассчитываем количество партий для каждой группы
    def calculate_batches(row):
        """Рассчитывает количество партий"""
        diameter_group = row['Группа_диаметра']
        count = row['Общее_количество']
        
        if diameter_group == 'DN_0.75_to_6':
            limit = 100
            batches = (count - 1) // limit + 1
            return f"{batches} партий по {limit} стыков"
        elif diameter_group == 'DN_above_6':
            limit = 50
            batches = (count - 1) // limit + 1
            return f"{batches} партий по {limit} стыков"
        else:
            return "1 партия"
    
    grouped['Информация_о_партиях'] = grouped.apply(calculate_batches, axis=1)
    
    # Сортируем результаты
    grouped = grouped.sort_values(['материал', 'клеймо', 'способ_сварки', 'Группа_диаметра'])
    
    # Создаем папку results если её нет
    os.makedirs('results', exist_ok=True)
    
    # Сохраняем результат в Excel
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f'results/grouped_by_diameter_limits_{timestamp}.xlsx'
    print(f"\n4. Сохраняем результат в {output_file}")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Основной лист с группированными данными
        grouped.to_excel(writer, sheet_name='Группировка по диаметрам', index=False)
        
        # Лист с исходными данными
        df_excel.to_excel(writer, sheet_name='Исходные данные', index=False)
        
        # Сводка по материалам
        material_summary = grouped.groupby('материал').agg({
            'Общее_количество': 'sum',
            'клеймо': 'nunique',
            'способ_сварки': 'nunique'
        }).reset_index()
        material_summary.columns = ['Материал', 'Общее количество', 'Количество сварщиков', 'Количество способов сварки']
        material_summary.to_excel(writer, sheet_name='Сводка по материалам', index=False)
        
        # Сводка по сварщикам
        welder_summary = grouped.groupby(['материал', 'клеймо']).agg({
            'Общее_количество': 'sum',
            'способ_сварки': 'nunique',
            'Группа_диаметра': 'nunique'
        }).reset_index()
        welder_summary.columns = ['Материал', 'Клеймо сварщика', 'Общее количество', 'Количество способов сварки', 'Количество групп диаметров']
        welder_summary.to_excel(writer, sheet_name='Сводка по сварщикам', index=False)
        
        # Анализ лимитов
        limits_analysis = grouped.groupby('Статус_лимитов').agg({
            'Общее_количество': 'sum',
            'материал': 'nunique',
            'клеймо': 'nunique'
        }).reset_index()
        limits_analysis.columns = ['Статус лимитов', 'Общее количество', 'Количество материалов', 'Количество сварщиков']
        limits_analysis.to_excel(writer, sheet_name='Анализ лимитов', index=False)
    
    print("✅ Готово! Файл сохранен.")
    
    # Показываем примеры данных
    print("\n5. Примеры группированных данных:")
    print(grouped[['материал', 'клеймо', 'способ_сварки', 'Группа_диаметра', 'Общее_количество', 'Статус_лимитов', 'Информация_о_партиях']].head(10))
    
    # Статистика
    print(f"\n6. Статистика:")
    print(f"   Всего уникальных материалов: {grouped['материал'].nunique()}")
    print(f"   Всего уникальных сварщиков: {grouped['клеймо'].nunique()}")
    print(f"   Всего уникальных способов сварки: {grouped['способ_сварки'].nunique()}")
    print(f"   Общее количество сварных соединений: {grouped['Общее_количество'].sum()}")
    
    # Анализ лимитов
    exceeded_limits = grouped[grouped['Статус_лимитов'].str.contains('ПРЕВЫШЕН ЛИМИТ')]
    print(f"   Групп с превышением лимитов: {len(exceeded_limits)}")
    
    # Анализ по группам диаметров
    dn_0_75_to_6_groups = grouped[grouped['Группа_диаметра'] == 'DN_0.75_to_6']
    dn_above_6_groups = grouped[grouped['Группа_диаметра'] == 'DN_above_6']
    print(f"   Групп DN 0,75-6 (лимит 100): {len(dn_0_75_to_6_groups)}")
    print(f"   Групп DN >6 (лимит 50): {len(dn_above_6_groups)}")
    
    return grouped

if __name__ == "__main__":
    group_by_diameter_limits()

