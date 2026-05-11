import calendar
import pandas as pd
from datetime import datetime, date
import os

def create_calendar_dataframe(year):
    """Создает DataFrame с календарем за указанный год"""
    cal = calendar.Calendar()
    
    # Создаем список всех дат года
    dates = []
    for month in range(1, 13):
        for day in cal.itermonthdays(year, month):
            if day != 0:  # Пропускаем дни из других месяцев
                current_date = date(year, month, day)
                dates.append({
                    'Дата': current_date,
                    'День недели': current_date.strftime('%A'),
                    'День недели (рус)': get_russian_weekday(current_date.weekday()),
                    'Месяц': current_date.strftime('%B'),
                    'Месяц (рус)': get_russian_month(month),
                    'День': day,
                    'Год': year,
                    'Неделя': current_date.isocalendar()[1]
                })
    
    df = pd.DataFrame(dates)
    return df

def get_russian_weekday(weekday):
    """Возвращает русское название дня недели"""
    weekdays = {
        0: 'Понедельник',
        1: 'Вторник', 
        2: 'Среда',
        3: 'Четверг',
        4: 'Пятница',
        5: 'Суббота',
        6: 'Воскресенье'
    }
    return weekdays[weekday]

def get_russian_month(month):
    """Возвращает русское название месяца"""
    months = {
        1: 'Январь',
        2: 'Февраль',
        3: 'Март',
        4: 'Апрель',
        5: 'Май',
        6: 'Июнь',
        7: 'Июль',
        8: 'Август',
        9: 'Сентябрь',
        10: 'Октябрь',
        11: 'Ноябрь',
        12: 'Декабрь'
    }
    return months[month]

def create_monthly_calendar_table(year):
    """Создает таблицу календаря по месяцам"""
    cal = calendar.Calendar()
    
    # Создаем пустой DataFrame для календаря
    calendar_data = []
    
    for month in range(1, 13):
        month_name = get_russian_month(month)
        
        # Получаем дни месяца
        month_days = list(cal.itermonthdays(year, month))
        
        # Разбиваем на недели
        weeks = []
        week = []
        for day in month_days:
            week.append(day if day != 0 else '')
            if len(week) == 7:
                weeks.append(week)
                week = []
        
        # Добавляем последнюю неделю если она неполная
        if week:
            week.extend([''] * (7 - len(week)))
            weeks.append(week)
        
        # Добавляем данные в список
        for week_num, week_days in enumerate(weeks, 1):
            row = {
                'Год': year,
                'Месяц': month_name,
                'Неделя': week_num
            }
            for i, day in enumerate(week_days):
                row[f'День_{i+1}'] = day
            calendar_data.append(row)
    
    return pd.DataFrame(calendar_data)

def add_holidays(df, year):
    """Добавляет информацию о праздниках"""
    holidays = get_holidays(year)
    
    # Добавляем колонку с праздниками
    df['Праздник'] = ''
    
    for holiday_date, holiday_name in holidays.items():
        day, month_name = holiday_date.split()
        month_num = get_month_number(month_name)
        holiday_datetime = datetime(year, month_num, int(day))
        
        # Находим строку с этой датой и добавляем праздник
        mask = df['Дата'] == holiday_datetime.date()
        df.loc[mask, 'Праздник'] = holiday_name
    
    return df

def get_month_number(month_name):
    """Возвращает номер месяца по русскому названию"""
    months = {
        'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
        'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
        'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
    }
    return months[month_name.lower()]

def get_holidays(year):
    """Возвращает праздники за указанный год"""
    holidays = {
        "1 января": "Новый год",
        "7 января": "Рождество Христово",
        "23 февраля": "День защитника Отечества",
        "8 марта": "Международный женский день",
        "1 мая": "Праздник Весны и Труда",
        "9 мая": "День Победы",
        "12 июня": "День России",
        "4 ноября": "День народного единства"
    }
    return holidays

def save_calendar_to_excel():
    """Сохраняет календарь в Excel файл"""
    with pd.ExcelWriter('calendar_2024_2025.xlsx', engine='openpyxl') as writer:
        
        # Полный календарь 2024
        df_2024 = create_calendar_dataframe(2024)
        df_2024 = add_holidays(df_2024, 2024)
        df_2024.to_excel(writer, sheet_name='Календарь_2024', index=False)
        
        # Полный календарь 2025
        df_2025 = create_calendar_dataframe(2025)
        df_2025 = add_holidays(df_2025, 2025)
        df_2025.to_excel(writer, sheet_name='Календарь_2025', index=False)
        
        # Полный календарь 2026
        df_2026 = create_calendar_dataframe(2026)
        df_2026 = add_holidays(df_2026, 2026)
        df_2026.to_excel(writer, sheet_name='Календарь_2026', index=False)
        
        # Полный календарь 2027
        df_2027 = create_calendar_dataframe(2027)
        df_2027 = add_holidays(df_2027, 2027)
        df_2027.to_excel(writer, sheet_name='Календарь_2027', index=False)
        
        # Табличный календарь 2024
        table_2024 = create_monthly_calendar_table(2024)
        table_2024.to_excel(writer, sheet_name='Таблица_2024', index=False)
        
        # Табличный календарь 2025
        table_2025 = create_monthly_calendar_table(2025)
        table_2025.to_excel(writer, sheet_name='Таблица_2025', index=False)
        
        # Табличный календарь 2026
        table_2026 = create_monthly_calendar_table(2026)
        table_2026.to_excel(writer, sheet_name='Таблица_2026', index=False)
        
        # Табличный календарь 2027
        table_2027 = create_monthly_calendar_table(2027)
        table_2027.to_excel(writer, sheet_name='Таблица_2027', index=False)
        
        # Праздники
        holidays_data = []
        for year in [2024, 2025, 2026, 2027]:
            holidays = get_holidays(year)
            for date_str, holiday_name in holidays.items():
                holidays_data.append({
                    'Год': year,
                    'Дата': date_str,
                    'Праздник': holiday_name
                })
        
        holidays_df = pd.DataFrame(holidays_data)
        holidays_df.to_excel(writer, sheet_name='Праздники', index=False)

def get_database_path():
    """Получает путь к базе данных"""
    import sys
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'utilities'))
        from path_utils import get_database_path as get_path_from_utils
        return get_path_from_utils()
    except ImportError:
        # Если не удалось импортировать, используем относительный путь
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        return os.path.join(project_root, 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db')

def add_year_2026_to_database():
    """Добавляет только 2026 год в существующую таблицу Календарь базы данных (для обратной совместимости)"""
    return add_year_to_database(2026)

def add_year_to_database(year):
    """Добавляет указанный год в существующую таблицу Календарь базы данных"""
    import sqlite3
    
    db_path = get_database_path()
    
    if not os.path.exists(db_path):
        print(f"Ошибка: База данных не найдена по пути: {db_path}")
        return False
    
    try:
        # Создаем календарь для указанного года
        print(f"Создание календаря для {year} года...")
        df_year = create_calendar_dataframe(year)
        df_year = add_holidays(df_year, year)
        
        # Подключаемся к базе данных
        print(f"Подключение к базе данных: {db_path}")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Проверяем, существует ли таблица
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Календарь'")
        if not cursor.fetchone():
            print(f"Таблица Календарь не найдена. Создаем новую таблицу с данными за {year} год...")
            # Если таблицы нет, создаем календарь для указанного года
            df_year.to_sql('Календарь', conn, if_exists='replace', index=False)
            print(f"Создана таблица с {len(df_year)} записями")
        else:
            # Проверяем, есть ли уже данные за указанный год
            cursor.execute("SELECT COUNT(*) FROM `Календарь` WHERE Год = ?", (year,))
            count_year = cursor.fetchone()[0]
            
            if count_year > 0:
                print(f"В таблице уже есть {count_year} записей за {year} год.")
                response = input(f"Перезаписать данные за {year} год? (y/n): ")
                if response.lower() in ['y', 'yes', 'да', 'д']:
                    # Удаляем старые данные за указанный год
                    cursor.execute("DELETE FROM `Календарь` WHERE Год = ?", (year,))
                    conn.commit()
                    print(f"Старые данные за {year} год удалены.")
                else:
                    print("Операция отменена.")
                    conn.close()
                    return False
            
            # Добавляем данные за указанный год
            print(f"Добавление данных за {year} год в таблицу Календарь...")
            df_year.to_sql('Календарь', conn, if_exists='append', index=False)
            print(f"Успешно добавлено {len(df_year)} записей за {year} год")
        
        conn.commit()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Ошибка при добавлении данных в базу данных: {e}")
        import traceback
        traceback.print_exc()
        return False

def load_calendar_to_database():
    """Загружает объединенный календарь (2024, 2025, 2026, 2027) в таблицу Календарь базы данных"""
    import sqlite3
    
    db_path = get_database_path()
    
    if not os.path.exists(db_path):
        print(f"Ошибка: База данных не найдена по пути: {db_path}")
        return False
    
    try:
        # Создаем объединенный календарь для всех четырех лет
        print("Создание объединенного календаря для 2024, 2025, 2026 и 2027 годов...")
        df_2024 = create_calendar_dataframe(2024)
        df_2024 = add_holidays(df_2024, 2024)
        
        df_2025 = create_calendar_dataframe(2025)
        df_2025 = add_holidays(df_2025, 2025)
        
        df_2026 = create_calendar_dataframe(2026)
        df_2026 = add_holidays(df_2026, 2026)
        
        df_2027 = create_calendar_dataframe(2027)
        df_2027 = add_holidays(df_2027, 2027)
        
        # Объединяем все годы
        df_combined = pd.concat([df_2024, df_2025, df_2026, df_2027], ignore_index=True)
        
        # Подключаемся к базе данных
        print(f"Подключение к базе данных: {db_path}")
        conn = sqlite3.connect(db_path)
        
        # Загружаем данные в таблицу (заменяем существующие данные)
        print("Загрузка данных в таблицу Календарь...")
        df_combined.to_sql('Календарь', conn, if_exists='replace', index=False)
        
        conn.commit()
        conn.close()
        
        print(f"Успешно загружено {len(df_combined)} записей в таблицу Календарь")
        print(f"  - 2024 год: {len(df_2024)} записей")
        print(f"  - 2025 год: {len(df_2025)} записей")
        print(f"  - 2026 год: {len(df_2026)} записей")
        print(f"  - 2027 год: {len(df_2027)} записей")
        return True
        
    except Exception as e:
        print(f"Ошибка при загрузке данных в базу данных: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Основная функция"""
    print("Создание календаря в виде таблицы...")
    
    # Создаем и сохраняем календарь
    save_calendar_to_excel()
    
    print("Календарь сохранен в файл 'calendar_2024_2025.xlsx'")
    print("Файл содержит следующие листы:")
    print("- Календарь_2024: полный календарь 2024 года")
    print("- Календарь_2025: полный календарь 2025 года")
    print("- Календарь_2026: полный календарь 2026 года")
    print("- Календарь_2027: полный календарь 2027 года")
    print("- Таблица_2024: табличный формат 2024 года")
    print("- Таблица_2025: табличный формат 2025 года")
    print("- Таблица_2026: табличный формат 2026 года")
    print("- Таблица_2027: табличный формат 2027 года")
    print("- Праздники: список праздников за все четыре года")
    
    # Показываем пример данных
    print("\nПример данных календаря 2024:")
    df_2024 = create_calendar_dataframe(2024)
    df_2024 = add_holidays(df_2024, 2024)
    print(df_2024.head(10).to_string(index=False))
    
    # Предлагаем загрузить в базу данных
    print("\n" + "="*50)
    print("Опции загрузки в базу данных:")
    print("1. Добавить только 2027 год (сохранит существующие данные)")
    print("2. Перезаписать всю таблицу (2024, 2025, 2026, 2027)")
    print("3. Пропустить загрузку в базу данных")
    response = input("Выберите опцию (1/2/3): ")
    
    if response == '1':
        add_year_to_database(2027)
    elif response == '2':
        load_calendar_to_database()
    else:
        print("Загрузка в базу данных пропущена.")

if __name__ == "__main__":
    main()

