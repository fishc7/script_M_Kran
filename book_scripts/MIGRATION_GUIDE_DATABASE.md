# 📘 Руководство по миграции: Единый модуль подключения к БД

**Дата создания**: Ноябрь 2025  
**Статус**: ✅ Реализовано  
**Приоритет**: 🔴 Критический

---

## 🎯 Цель рефакторинга

Устранить дублирование кода подключения к базе данных в **57+ файлах** проекта. Все скрипты теперь используют единый модуль `scripts.core.database`.

---

## ✅ Что было сделано

### 1. Создан единый модуль `scripts/core/database.py`

Модуль предоставляет:
- `get_database_path()` - автоматическое определение пути к БД
- `get_database_connection()` - создание подключения с оптимальными настройками
- `DatabaseConnection` - контекстный менеджер для безопасной работы с БД
- `database_transaction()` - контекстный менеджер для транзакций
- `test_connection()` - тестирование подключения

### 2. Обновлен `scripts/utilities/db_utils.py`

Теперь использует новый модуль, но сохраняет обратную совместимость.

### 3. Обновлены примеры скриптов

- `scripts/data_loaders/normalization_functions.py`
- `scripts/data_cleaners/analyze_vik_status.py`

---

## 📖 Как использовать новый модуль

### Вариант 1: Простое использование

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys

# Добавляем путь к core модулю
core_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'core')
if core_dir not in sys.path:
    sys.path.insert(0, core_dir)

# Импортируем единый модуль
from core.database import get_database_connection

# Используем
conn = get_database_connection()
cursor = conn.cursor()
cursor.execute("SELECT * FROM logs_lnk LIMIT 10")
rows = cursor.fetchall()
conn.close()
```

### Вариант 2: Использование контекстного менеджера (рекомендуется)

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys

core_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'core')
if core_dir not in sys.path:
    sys.path.insert(0, core_dir)

from core.database import DatabaseConnection

# Автоматическое закрытие соединения
with DatabaseConnection() as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM logs_lnk LIMIT 10")
    rows = cursor.fetchall()
    # Соединение автоматически закроется
```

### Вариант 3: Использование транзакций

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys

core_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'core')
if core_dir not in sys.path:
    sys.path.insert(0, core_dir)

from core.database import database_transaction

# Автоматический commit/rollback
with database_transaction() as conn:
    cursor = conn.cursor()
    cursor.execute("INSERT INTO table_name (col1) VALUES (?)", ('value',))
    # Изменения автоматически закоммитятся при успехе
    # или откатятся при ошибке
```

### Вариант 4: Использование через utilities (для обратной совместимости)

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from scripts.utilities.db_utils import get_database_connection

# Работает как раньше, но теперь использует новый модуль под капотом
conn = get_database_connection()
# ...
```

---

## 🔄 Как мигрировать существующий скрипт

### Шаг 1: Найдите старый код подключения

Обычно это выглядит так:

```python
def get_database_connection():
    current_dir = os.getcwd()
    possible_paths = [
        os.path.join(current_dir, 'database', 'BD_Kingisepp', 'M_Kran_Kingesepp.db'),
        # ... много других путей
    ]
    for path in possible_paths:
        # ...
    return sqlite3.connect(path)
```

### Шаг 2: Замените на импорт нового модуля

```python
import os
import sys

# Добавляем путь к core модулю
core_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'core')
if core_dir not in sys.path:
    sys.path.insert(0, core_dir)

from core.database import get_database_connection, DatabaseConnection
```

### Шаг 3: Удалите старую функцию `get_database_connection()`

Больше не нужна - используйте импортированную функцию.

### Шаг 4: Обновите использование

**Было:**
```python
conn = get_database_connection()  # Ваша локальная функция
cursor = conn.cursor()
# ...
conn.close()
```

**Стало:**
```python
# Вариант 1: Простое использование
from core.database import get_database_connection
conn = get_database_connection()  # Из нового модуля
cursor = conn.cursor()
# ...
conn.close()

# Вариант 2: С контекстным менеджером (лучше)
from core.database import DatabaseConnection
with DatabaseConnection() as conn:
    cursor = conn.cursor()
    # ...
    # Автоматическое закрытие
```

---

## 📋 Примеры миграции

### Пример 1: Простой скрипт чтения

**Было:**
```python
import sqlite3
import os

def get_db():
    current_dir = os.getcwd()
    # ... много кода определения пути
    return sqlite3.connect(path)

conn = get_db()
cursor = conn.cursor()
cursor.execute("SELECT * FROM table")
conn.close()
```

**Стало:**
```python
import os
import sys

core_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'core')
if core_dir not in sys.path:
    sys.path.insert(0, core_dir)

from core.database import DatabaseConnection

with DatabaseConnection() as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM table")
```

### Пример 2: Скрипт с записью данных

**Было:**
```python
conn = get_database_connection()
cursor = conn.cursor()
try:
    cursor.execute("INSERT INTO table VALUES (?)", (value,))
    conn.commit()
except:
    conn.rollback()
finally:
    conn.close()
```

**Стало:**
```python
from core.database import database_transaction

with database_transaction() as conn:
    cursor = conn.cursor()
    cursor.execute("INSERT INTO table VALUES (?)", (value,))
    # Автоматический commit при успехе или rollback при ошибке
```

---

## 🎯 Преимущества нового подхода

1. ✅ **Единая точка входа** - все скрипты используют один модуль
2. ✅ **Автоматическое определение пути** - работает из любой директории
3. ✅ **Оптимальные настройки БД** - WAL режим, кэширование и т.д.
4. ✅ **Безопасность** - контекстные менеджеры гарантируют закрытие соединений
5. ✅ **Транзакции** - автоматический commit/rollback
6. ✅ **Обратная совместимость** - старый код продолжает работать

---

## 📝 Чеклист миграции скрипта

- [ ] Удалить локальную функцию `get_database_connection()`
- [ ] Удалить локальную функцию `get_database_path()` (если есть)
- [ ] Добавить импорт нового модуля
- [ ] Заменить использование на новый модуль
- [ ] Использовать контекстные менеджеры где возможно
- [ ] Протестировать скрипт
- [ ] Обновить комментарии в коде

---

## 🔍 Проверка миграции

После миграции скрипта проверьте:

1. **Импорт работает:**
   ```python
   from core.database import get_database_connection
   ```

2. **Подключение устанавливается:**
   ```python
   conn = get_database_connection()
   assert conn is not None
   ```

3. **Скрипт выполняется без ошибок**

4. **Нет дублирования кода подключения**

---

## 📊 Статистика миграции

- **Всего файлов с дублированием**: 57+
- **Созданных модулей**: 1 (`scripts/core/database.py`)
- **Обновленных файлов**: 3 (примеры)
- **Осталось мигрировать**: ~54 файла

---

## 🚀 Следующие шаги

1. Постепенно мигрировать остальные скрипты
2. Удалить старые функции подключения после полной миграции
3. Добавить unit-тесты для модуля `database.py`
4. Документировать все функции модуля

---

## ❓ FAQ

**Q: Нужно ли мигрировать все скрипты сразу?**  
A: Нет, можно мигрировать постепенно. Старый код продолжит работать.

**Q: Что делать, если скрипт запускается из другой директории?**  
A: Новый модуль автоматически определяет путь к БД независимо от текущей директории.

**Q: Можно ли использовать старый способ?**  
A: Да, `scripts/utilities/db_utils.py` сохраняет обратную совместимость, но рекомендуется использовать новый модуль.

**Q: Как протестировать новый модуль?**  
A: Запустите `python scripts/core/database.py` - он выполнит самопроверку.

---

**Дата создания**: Ноябрь 2025  
**Версия**: 1.0  
**Статус**: ✅ Готово к использованию

















