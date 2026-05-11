#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Совместимая точка входа для scripts/tools/load_naks_to_db.py."""

from pathlib import Path
import runpy


if __name__ == "__main__":
    target = Path(__file__).resolve().parent / "scripts" / "tools" / "load_naks_to_db.py"
    runpy.run_path(str(target), run_name="__main__")

