#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Минимальный тест для проверки синтаксиса app.py.
"""

from pathlib import Path
import ast


def check_syntax():
    """Проверяет синтаксис web/app/app.py."""
    project_root = Path(__file__).resolve().parents[2]
    file_path = project_root / "web" / "app" / "app.py"

    try:
        with file_path.open("r", encoding="utf-8") as f:
            content = f.read()

        ast.parse(content)
        print("SUCCESS: Синтаксис файла корректен")
        return True

    except SyntaxError as e:
        print(f"ERROR: Синтаксическая ошибка на строке {e.lineno}: {e.msg}")
        print(f"Текст: {e.text}")
        return False
    except Exception as e:
        print(f"ERROR: Ошибка при проверке синтаксиса: {e}")
        return False


if __name__ == "__main__":
    check_syntax()
