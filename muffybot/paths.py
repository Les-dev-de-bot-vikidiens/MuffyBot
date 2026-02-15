# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
ENVIKIDIA_DIR = ROOT_DIR / "envikidia"
LOG_DIR = ROOT_DIR / "logs"


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path
