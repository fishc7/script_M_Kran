# 📘 Руководство по улучшению качества кода

**Дата создания**: Ноябрь 2025  
**Версия**: 1.0  
**Статус**: ✅ Реализовано

---

## 🎯 Цель

Стандартизировать код проекта M_Kran, улучшить его качество и поддерживаемость.

---

## ✅ Что было сделано

### 1. Конфигурационные файлы

Созданы следующие конфигурационные файлы:

- **`.editorconfig`** - единые настройки редактора для всех разработчиков
- **`pyproject.toml`** - конфигурация для black, isort, pylint, mypy, pytest
- **`.flake8`** - настройки flake8 для проверки стиля кода
- **`.pylintrc`** - настройки pylint для статического анализа
- **`.pre-commit-config.yaml`** - автоматические проверки перед коммитом

### 2. Скрипт поиска мертвого кода

Создан `scripts/tools/find_dead_code.py` для поиска неиспользуемого кода.

---

## 🛠️ Установка инструментов

### Базовые инструменты

```bash
# Форматирование кода
pip install black

# Сортировка импортов
pip install isort

# Проверка стиля кода
pip install flake8

# Статический анализ
pip install pylint

# Проверка типов (опционально)
pip install mypy

# Поиск мертвого кода
pip install vulture

# Удаление неиспользуемых импортов
pip install autoflake

# Pre-commit hooks
pip install pre-commit
```

### Установка pre-commit hooks

```bash
# Установка hooks
pre-commit install

# Ручной запуск на всех файлах
pre-commit run --all-files
```

---

## 📖 Использование инструментов

### 1. Форматирование кода (Black)

```bash
# Проверить форматирование (без изменений)
black --check .

# Отформатировать все файлы
black .

# Отформатировать конкретный файл
black scripts/core/database.py
```

### 2. Сортировка импортов (isort)

```bash
# Проверить сортировку (без изменений)
isort --check-only .

# Отсортировать импорты
isort .

# Отсортировать конкретный файл
isort scripts/core/database.py
```

### 3. Проверка стиля кода (flake8)

```bash
# Проверить все файлы
flake8 .

# Проверить конкретную директорию
flake8 scripts/core/

# Показать статистику
flake8 --statistics .
```

### 4. Статический анализ (pylint)

```bash
# Проверить все файлы
pylint scripts/

# Проверить конкретный файл
pylint scripts/core/database.py

# С выводом в файл
pylint scripts/ > pylint_report.txt
```

### 5. Проверка типов (mypy)

```bash
# Проверить типы
mypy scripts/core/

# С более строгими настройками
mypy --strict scripts/core/
```

### 6. Поиск мертвого кода (vulture)

```bash
# Использовать готовый скрипт
python scripts/tools/find_dead_code.py

# Или напрямую через vulture
vulture . --min-confidence 80
```

### 7. Удаление неиспользуемых импортов (autoflake)

```bash
# Показать что будет удалено (без изменений)
autoflake --remove-all-unused-imports --recursive --check .

# Удалить неиспользуемые импорты
autoflake --remove-all-unused-imports --in-place --recursive .
```

---

## 🔄 Workflow разработки

### Рекомендуемый процесс

1. **Перед коммитом** (автоматически через pre-commit):
   ```bash
   # Pre-commit hooks запустятся автоматически
   git commit -m "Your message"
   ```

2. **Ручная проверка перед push**:
   ```bash
   # Форматирование
   black .
   isort .
   
   # Проверка
   flake8 .
   pylint scripts/
   ```

3. **Периодическая очистка**:
   ```bash
   # Поиск мертвого кода
   python scripts/tools/find_dead_code.py
   
   # Удаление неиспользуемых импортов
   autoflake --remove-all-unused-imports --in-place --recursive .
   ```

---

## 📝 Стандарты кодирования

### Стиль кода

- **Длина строки**: максимум 100 символов
- **Отступы**: 4 пробела
- **Кодировка**: UTF-8
- **Конец строки**: LF (Unix-style)

### Docstrings

Используйте Google стиль для docstrings:

```python
def function_name(param1: str, param2: int) -> bool:
    """
    Краткое описание функции.
    
    Более подробное описание функции, если необходимо.
    Может быть несколько строк.
    
    Args:
        param1: Описание первого параметра
        param2: Описание второго параметра
        
    Returns:
        Описание возвращаемого значения
        
    Raises:
        ValueError: Когда возникает ошибка
        
    Example:
        >>> function_name("test", 42)
        True
    """
    pass
```

### Импорты

Импорты должны быть отсортированы:
1. Стандартная библиотека
2. Сторонние библиотеки
3. Локальные импорты

```python
# Стандартная библиотека
import os
import sys
from typing import Optional

# Сторонние библиотеки
import pandas as pd
import sqlite3

# Локальные импорты
from scripts.core.database import get_database_connection
```

---

## 🎯 Целевые метрики качества

- **Покрытие docstrings**: 80%+ публичных функций
- **Pylint score**: 7.0+ (из 10)
- **Flake8 errors**: 0 критических ошибок
- **Мертвый код**: < 5% от общего объема

---

## 📊 Отчеты

После запуска инструментов отчеты сохраняются в:

- `results/pylint_report.txt` - отчет pylint
- `results/dead_code_report.txt` - отчет о мертвом коде
- `results/flake8_report.txt` - отчет flake8 (если настроен)

---

## 🔧 Настройка IDE

### VS Code

Добавьте в `.vscode/settings.json`:

```json
{
    "python.formatting.provider": "black",
    "python.linting.enabled": true,
    "python.linting.pylintEnabled": true,
    "python.linting.flake8Enabled": true,
    "editor.formatOnSave": true,
    "editor.codeActionsOnSave": {
        "source.organizeImports": true
    }
}
```

### PyCharm

1. Settings → Tools → Black → Enable Black
2. Settings → Tools → isort → Enable isort
3. Settings → Editor → Code Style → Python → Set from → Black

---

## ❓ FAQ

**Q: Нужно ли форматировать весь проект сразу?**  
A: Нет, можно форматировать постепенно. Black безопасен и не меняет логику.

**Q: Что делать с ошибками pylint?**  
A: Многие ошибки можно игнорировать через комментарии `# pylint: disable=...`

**Q: Как отключить pre-commit hooks?**  
A: Используйте `git commit --no-verify`, но не злоупотребляйте этим.

**Q: Нужно ли запускать все инструменты каждый раз?**  
A: Pre-commit hooks запускаются автоматически. Остальные можно запускать периодически.

---

## 📚 Дополнительные ресурсы

- [Black Documentation](https://black.readthedocs.io/)
- [Flake8 Documentation](https://flake8.pycqa.org/)
- [Pylint Documentation](https://pylint.pycqa.org/)
- [Pre-commit Documentation](https://pre-commit.com/)

---

**Дата создания**: Ноябрь 2025  
**Версия**: 1.0  
**Статус**: ✅ Готово к использованию

















