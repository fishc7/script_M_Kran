# Структура базы данных для статистики DN

## Структура базы данных: Отдельные таблицы для каждого типа статистики

### 1. Таблица `dn_statistics_daily` - Статистика по дням
```sql
CREATE TABLE IF NOT EXISTS dn_statistics_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    дата DATE NOT NULL UNIQUE,
    день_недели TEXT,
    количество_записей INTEGER NOT NULL,
    среднее_dn REAL,
    минимальное_dn REAL,
    максимальное_dn REAL,
    сумма_dn REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dn_statistics_daily_дата ON dn_statistics_daily(дата);
```

### 2. Таблица `dn_statistics_weekly` - Статистика по неделям
```sql
CREATE TABLE IF NOT EXISTS dn_statistics_weekly (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    год INTEGER NOT NULL,
    неделя TEXT NOT NULL,  -- Формат: '2026-W02'
    начало_недели DATE NOT NULL,
    конец_недели DATE NOT NULL,
    количество_записей INTEGER NOT NULL,
    среднее_dn REAL,
    минимальное_dn REAL,
    максимальное_dn REAL,
    сумма_dn REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(год, неделя)
);

CREATE INDEX idx_dn_statistics_weekly_период ON dn_statistics_weekly(год, неделя);
CREATE INDEX idx_dn_statistics_weekly_даты ON dn_statistics_weekly(начало_недели, конец_недели);
```

### 3. Таблица `dn_statistics_monthly` - Статистика по месяцам
```sql
CREATE TABLE IF NOT EXISTS dn_statistics_monthly (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    год INTEGER NOT NULL,
    месяц INTEGER NOT NULL,  -- 1-12
    месяц_название TEXT,  -- 'Январь', 'Февраль', etc.
    год_месяц TEXT NOT NULL,  -- Формат: '2026-01'
    количество_записей INTEGER NOT NULL,
    количество_дней INTEGER,
    среднее_dn REAL,
    минимальное_dn REAL,
    максимальное_dn REAL,
    сумма_dn REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(год, месяц)
);

CREATE INDEX idx_dn_statistics_monthly_период ON dn_statistics_monthly(год, месяц);
CREATE INDEX idx_dn_statistics_monthly_год_месяц ON dn_statistics_monthly(год_месяц);
```

### 4. Таблица `dn_statistics_yearly` - Статистика по годам
```sql
CREATE TABLE IF NOT EXISTS dn_statistics_yearly (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    год INTEGER NOT NULL UNIQUE,
    количество_записей INTEGER NOT NULL,
    количество_дней INTEGER,
    количество_месяцев INTEGER,
    среднее_dn REAL,
    минимальное_dn REAL,
    максимальное_dn REAL,
    сумма_dn REAL,
    первая_дата DATE,
    последняя_дата DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dn_statistics_yearly_год ON dn_statistics_yearly(год);
```

### 5. Таблица `dn_statistics_period` - Общая статистика за весь период
```sql
CREATE TABLE IF NOT EXISTS dn_statistics_period (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    общее_количество_записей INTEGER NOT NULL,
    количество_дней INTEGER,
    среднее_dn_за_период REAL,
    минимальное_dn REAL,
    максимальное_dn REAL,
    сумма_dn REAL,
    первая_дата DATE,
    последняя_дата DATE,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## Рекомендации по использованию

### Преимущества структуры:
- ✅ Более четкая структура данных
- ✅ Легче делать запросы для конкретного типа статистики
- ✅ Проще добавлять специфичные поля для каждого типа
- ✅ Лучшая производительность (меньше данных в каждой таблице)

### Стратегия обновления данных:

1. **При каждом запросе статистики** - обновлять только последние периоды
2. **По расписанию** - запускать скрипт обновления раз в день/неделю
3. **Гибридный подход** - проверять, есть ли данные за период, если нет - вычислять и сохранять

### Пример функции обновления:

```python
def update_dn_statistics_in_db(conn, stats_data):
    """
    Обновляет статистику DN в базе данных
    
    Args:
        conn: Подключение к БД
        stats_data: Словарь со статистикой из get_dn_statistics_data()
    """
    cursor = conn.cursor()
    
    # Обновление дневной статистики
    if 'last_daily' in stats_data:
        daily = stats_data['last_daily']
        cursor.execute('''
            INSERT OR REPLACE INTO dn_statistics_daily 
            (дата, день_недели, количество_записей, среднее_dn, минимальное_dn, максимальное_dn, сумма_dn, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (daily['дата'], daily['день_недели'], daily['количество_записей'], 
              daily['среднее_dn'], daily['минимальное_dn'], 
              daily['максимальное_dn'], daily['сумма_dn']))
    
    # Аналогично для недель, месяцев, годов, периода...
    
    conn.commit()
```

---

## Дополнительные возможности

### История изменений:
```sql
CREATE TABLE IF NOT EXISTS dn_statistics_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    тип_периода TEXT NOT NULL,
    период_идентификатор TEXT,
    старое_значение REAL,
    новое_значение REAL,
    изменено_в TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Кэширование последних значений:
Можно добавить отдельную таблицу для быстрого доступа к последним значениям:
```sql
CREATE TABLE IF NOT EXISTS dn_statistics_cache (
    ключ TEXT PRIMARY KEY,  -- 'last_daily', 'last_weekly', etc.
    данные_json TEXT,  -- JSON с данными
    обновлено_в TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

