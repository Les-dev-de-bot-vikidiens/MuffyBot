# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
ENVIKIDIA_DIR = ROOT_DIR / "envikidia"
LOG_DIR = ROOT_DIR / "logs"
BOT_LOG_FILE = ROOT_DIR / "bot.logs"
CONFIG_BACKUP_DIR = LOG_DIR / "config_backups"
CONTROL_DIR = ROOT_DIR / "control"
KILL_SWITCH_FILE = CONTROL_DIR / "kill.switch"
MAINTENANCE_FILE = CONTROL_DIR / "maintenance.mode"


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path
