# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType

from .paths import ROOT_DIR


def _load_python_config(path: Path) -> ModuleType | None:
    if not path.exists():
        return None
    spec = spec_from_file_location("muffybot_runtime_config", path)
    if spec is None or spec.loader is None:
        return None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def load_dotenv(path: Path | None = None) -> None:
    """
    Load runtime configuration from `config.py` first, then `.env`.

    Kept as `load_dotenv` for backward compatibility with existing imports.
    """
    config_path = path or (ROOT_DIR / "config.py")
    module = _load_python_config(config_path)
    if module is not None:
        for attr_name in dir(module):
            if not attr_name.isupper():
                continue
            value = getattr(module, attr_name)
            if value is None:
                continue
            os.environ.setdefault(attr_name, str(value))

    env_path = ROOT_DIR / ".env"
    _load_env_file(env_path)


def load_config(path: Path | None = None) -> None:
    """Alias explicite pour les nouveaux appels."""
    load_dotenv(path)


def get_env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name, default)


def get_bool_env(name: str, default: bool = False) -> bool:
    raw = get_env(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def get_int_env(name: str, default: int = 0) -> int:
    raw = get_env(name)
    if raw is None:
        return default
    try:
        return int(str(raw))
    except (TypeError, ValueError):
        return default


def get_csv_env(name: str, default: list[str] | None = None) -> list[str]:
    raw = get_env(name)
    if raw is None:
        return list(default or [])
    return [part.strip() for part in str(raw).split(",") if part.strip()]
