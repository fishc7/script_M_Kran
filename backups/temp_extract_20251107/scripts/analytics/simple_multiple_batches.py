import pandas as pd
import os
from datetime import datetime

def calculate_multiple_batches():
    """
    Рассчитывает несколько партий для групп с превышением лимита
    """
    
    print("=== РАСЧЕТ НЕСКОЛЬКИХ ПАРТИЙ ДЛЯ ПРЕВЫШЕНИЯ ЛИМИТОВ ===")
    
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
    print("\n3. Создаем накопительную сумму и считаем партии...")
    
    def calculate_batches_for_group(group_data):
        """Рассчитывает несколько партий для группы"""
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
        batches = []
        batch_number = 1
        
        # Проходим по всем записям группы
        for _, row in group_data.iterrows():
            cumulative_sum += row['Кол_во']
            
            # Если достигли лимита для текущей партии
            if cumulative_sum >= limit * batch_number:
                # Находим дату, где количество было меньше лимита для текущей партии
                temp_sum = 0
                limit_reach_date = None
                limit_reach_count = 0
                
                for _, temp_row in group_data.iterrows():
                    temp_sum += temp_row['Кол_во']
                    if temp_sum >= limit * batch_number:
                        # Берем предыдущую запись, где количество было меньше лимита
                        if temp_row.name > group_data.index[0]:
                            prev_idx = group_data.index[group_data.index.get_loc(temp_row.name) - 1]
                            limit_reach_date = group_data.loc[prev_idx, 'Дата']
                            limit_reach_count = temp_sum - temp_row['Кол_во']
                        else:
                            limit_reach_date = temp_row['Дата']
                            limit_reach_count = temp_sum
                        break
                
                # Добавляем информацию о партии
                batch_info = {
                    'материал': material,
                    'клеймо': welder,
                    'способ_сварки': method,
                    'Группа_диаметра': diameter_group,
                    'Номер_партии': batch_number,
                    'Лимит_партии': limit,
                    'Дата_достижения_лимита_партии': limit_reach_date,
                    'Количество_на_дату_лимита_партии': limit_reach_count,
                    'Количество_в_партии': limit,
                    'Накопительное_количество': cumulative_sum
                }
                batches.append(batch_info)
                batch_number += 1
        
        # Добавляем последнюю неполную партию (если есть)
        if cumulative_sum > limit * (batch_number - 1):
            remaining = cumulative_sum - limit * (batch_number - 1)
            if remaining > 0:
                batch_info = {
                    'материал': material,
                    'клеймо': welder,
                    'способ_сварки': method,
                    'Группа_диаметра': diameter_group,
                    'Номер_партии': batch_number,
                    'Лимит_партии': limit,
                    'Дата_достижения_лимита_партии': group_data.iloc[-1]['Дата'],
                    'Количество_на_дату_лимита_партии': cumulative_sum - remaining,
                    'Количество_в_партии': remaining,
                    'Накопительное_количество': cumulative_sum
                }
                batches.append(batch_info)
        
        return batches
    
    # Группируем данные и рассчитываем партии
    print("\n4. Анализируем группы...")
    
    all_batches = []
    
    for name, group in df_excel.groupby(['материал', 'клеймо', 'способ_сварки', 'Группа_диаметра']):
        batches = calculate_batches_for_group(group)
        if batches:
            all_batches.extend(batches)
    
    df_batches = pd.DataFrame(all_batches)
    
    # Определяем статус для каждой партии
    def get_batch_status(row):
        """Определяет статус партии"""
        if row['Количество_в_партии'] >= row['Лимит_партии']:
            return f"ПОЛНАЯ ПАРТИЯ: {row['Количество_в_партии']}/{row['Лимит_партии']}"
        else:
            return f"НЕПОЛНАЯ ПАРТИЯ: {row['Количество_в_партии']}/{row['Лимит_партии']}"
    
    df_batches['Статус_партии'] = df_batches.apply(get_batch_status, axis=1)
    
    # Сортируем результаты
    df_batches = df_batches.sort_values(['материал', 'клеймо', 'способ_сварки', 'Группа_диаметра', 'Номер_партии'])
    
    # Создаем папку results если её нет
    os.makedirs('results', exist_ok=True)
    
    # Сохраняем результат в Excel
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f'results/multiple_batches_analysis_{timestamp}.xlsx'
    print(f"\n5. Сохраняем результат в {output_file}")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Основной лист с анализом партий
        df_batches.to_excel(writer, sheet_name='Анализ партий', index=False)
        
        # Лист с исходными данными
        df_excel.to_excel(writer, sheet_name='Исходные данные', index=False)
        
        # Полные партии
        full_batches = df_batches[df_batches['Количество_в_партии'] >= df_batches['Лимит_партии']]
        full_batches.to_excel(writer, sheet_name='Полные партии', index=False)
        
        # Неполные партии
        incomplete_batches = df_batches[df_batches['Количество_в_партии'] < df_batches['Лимит_партии']]
        incomplete_batches.to_excel(writer, sheet_name='Неполные партии', index=False)
    
    print("✅ Готово! Файл сохранен.")
    
    # Показываем примеры данных
    print("\n6. Примеры анализа партий:")
    print(df_batches[['материал', 'клеймо', 'способ_сварки', 'Группа_диаметра', 'Номер_партии', 'Лимит_партии', 'Дата_достижения_лимита_партии', 'Количество_в_партии', 'Статус_партии']].head(15))
    
    # Статистика
    print(f"\n7. Статистика:")
    print(f"   Всего партий: {len(df_batches)}")
    print(f"   Полных партий: {len(full_batches)}")
    print(f"   Неполных партий: {len(incomplete_batches)}")
    
    return df_batches

if __name__ == "__main__":
    calculate_multiple_batches()

