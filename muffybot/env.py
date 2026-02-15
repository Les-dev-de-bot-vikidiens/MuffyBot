# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from pathlib import Path

from .paths import ROOT_DIR


def load_dotenv(path: Path | None = None) -> None:
    """Load a .env file without external dependencies."""
    env_path = path or (ROOT_DIR / ".env")
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def get_env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name, default)
