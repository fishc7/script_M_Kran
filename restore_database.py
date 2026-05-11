#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Совместимая точка входа для scripts/tools/restore_database.py."""

from pathlib import Path
import runpy


if __name__ == "__main__":
    target = Path(__file__).resolve().parent / "scripts" / "tools" / "restore_database.py"
    runpy.run_path(str(target), run_name="__main__")

