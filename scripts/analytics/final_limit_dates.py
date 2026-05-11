import pandas as pd
import os
from datetime import datetime

def find_limit_dates():
    """
    Находит даты приближения к лимитам для групп с превышением
    """
    
    print("=== ПОИСК ДАТ ПРИБЛИЖЕНИЯ К ЛИМИТАМ ===")
    
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
    
    # Конвертируем даты
    df_excel['Дата'] = pd.to_datetime(df_excel['Дата'], errors='coerce')
    df_excel = df_excel.dropna(subset=['Дата'])
    
    # Сортируем по датам
    df_excel = df_excel.sort_values(['материал', 'клеймо', 'способ_сварки', 'Группа_диаметра', 'Дата'])
    
    # Создаем накопительную сумму для каждой группы
    print("\n3. Создаем накопительную сумму...")
    
    def find_limit_reach_date(group_data):
        """Находит дату достижения лимита"""
        material = group_data['материал'].iloc[0]
        welder = group_data['клеймо'].iloc[0]
        method = group_data['способ_сварки'].iloc[0]
        diameter_group = group_data['Группа_диаметра'].iloc[0]
        
        # Определяем лимит
        if diameter_group == 'DN_0.75_to_6':
            limit = 100
        elif diameter_group == 'DN_above_6':
            limit = 50
        else:
            return None
        
        # Создаем накопительную сумму
        cumulative_sum = 0
        limit_reach_date = None
        limit_reach_count = 0
        
        # Проходим по всем записям группы
        for _, row in group_data.iterrows():
            cumulative_sum += row['Кол_во']
            
            # Если достигли лимита и еще не записали дату
            if cumulative_sum >= limit and limit_reach_date is None:
                # Берем предыдущую дату (если есть) или текущую
                if len(group_data) > 1:
                    # Ищем запись с количеством меньше лимита
                    temp_sum = 0
                    for _, temp_row in group_data.iterrows():
                        temp_sum += temp_row['Кол_во']
                        if temp_sum >= limit:
                            # Берем предыдущую запись
                            if temp_row.name > group_data.index[0]:
                                prev_idx = group_data.index[group_data.index.get_loc(temp_row.name) - 1]
                                limit_reach_date = group_data.loc[prev_idx, 'Дата']
                                limit_reach_count = temp_sum - temp_row['Кол_во']
                            else:
                                limit_reach_date = temp_row['Дата']
                                limit_reach_count = temp_sum
                            break
                else:
                    limit_reach_date = row['Дата']
                    limit_reach_count = cumulative_sum
        
        return {
            'материал': material,
            'клеймо': welder,
            'способ_сварки': method,
            'Группа_диаметра': diameter_group,
            'Лимит': limit,
            'Дата_достижения_лимита': limit_reach_date,
            'Количество_на_дату_лимита': limit_reach_count,
            'Общее_количество': cumulative_sum,
            'Превышение': cumulative_sum - limit if cumulative_sum > limit else 0
        }
    
    # Группируем данные и находим даты достижения лимитов
    print("\n4. Анализируем группы...")
    
    limit_analysis = []
    
    for name, group in df_excel.groupby(['материал', 'клеймо', 'способ_сварки', 'Группа_диаметра']):
        result = find_limit_reach_date(group)
        if result:
            limit_analysis.append(result)
    
    df_limit_analysis = pd.DataFrame(limit_analysis)
    
    # Определяем статус для каждой группы
    def get_limit_status(row):
        """Определяет статус лимита"""
        if row['Превышение'] > 0:
            return f"ПРЕВЫШЕН ЛИМИТ: {row['Общее_количество']} > {row['Лимит']} (превышение: {row['Превышение']})"
        else:
            return f"В пределах лимита: {row['Общее_количество']}/{row['Лимит']}"
    
    df_limit_analysis['Статус_лимитов'] = df_limit_analysis.apply(get_limit_status, axis=1)
    
    # Сортируем результаты
    df_limit_analysis = df_limit_analysis.sort_values(['материал', 'клеймо', 'способ_сварки', 'Группа_диаметра'])
    
    # Создаем папку results если её нет
    os.makedirs('results', exist_ok=True)
    
    # Сохраняем результат в Excel
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f'results/limit_dates_analysis_{timestamp}.xlsx'
    print(f"\n5. Сохраняем результат в {output_file}")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Основной лист с анализом лимитов
        df_limit_analysis.to_excel(writer, sheet_name='Анализ дат лимитов', index=False)
        
        # Лист с исходными данными
        df_excel.to_excel(writer, sheet_name='Исходные данные', index=False)
        
        # Группы с превышением лимитов
        exceeded_limits = df_limit_analysis[df_limit_analysis['Превышение'] > 0]
        exceeded_limits.to_excel(writer, sheet_name='Превышение лимитов', index=False)
        
        # Группы в пределах лимитов
        within_limits = df_limit_analysis[df_limit_analysis['Превышение'] == 0]
        within_limits.to_excel(writer, sheet_name='В пределах лимитов', index=False)
    
    print("✅ Готово! Файл сохранен.")
    
    # Показываем примеры данных
    print("\n6. Примеры анализа лимитов:")
    print(df_limit_analysis[['материал', 'клеймо', 'способ_сварки', 'Группа_диаметра', 'Лимит', 'Дата_достижения_лимита', 'Количество_на_дату_лимита', 'Общее_количество', 'Превышение', 'Статус_лимитов']].head(10))
    
    # Статистика
    print(f"\n7. Статистика:")
    print(f"   Всего групп: {len(df_limit_analysis)}")
    print(f"   Групп с превышением лимитов: {len(exceeded_limits)}")
    print(f"   Групп в пределах лимитов: {len(within_limits)}")
    print(f"   Общее количество стыков: {df_limit_analysis['Общее_количество'].sum()}")
    print(f"   Общее превышение лимитов: {df_limit_analysis['Превышение'].sum()}")
    
    return df_limit_analysis

if __name__ == "__main__":
    find_limit_dates()

