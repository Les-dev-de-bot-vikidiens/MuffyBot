#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import fcntl
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from typing import TextIO

from . import config
from . import commands  # noqa: F401 - registers events/commands
from .runtime import load_token
from .storage import init_db


def configure_logging() -> None:
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")

    if not any(isinstance(handler, logging.StreamHandler) for handler in root.handlers):
        stream = logging.StreamHandler()
        stream.setFormatter(formatter)
        root.addHandler(stream)

    log_path = config.PYWIKIBOT_DIR / "bot.logs"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    if not any(isinstance(handler, TimedRotatingFileHandler) and handler.baseFilename == str(log_path) for handler in root.handlers):
        file_handler = TimedRotatingFileHandler(
            filename=str(log_path),
            when="midnight",
            interval=1,
            backupCount=14,
            encoding="utf-8",
            utc=True,
        )
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)


def acquire_instance_lock() -> TextIO:
    lock_path = config.BASE_DIR / "luffybot.instance.lock"
    handle = lock_path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.close()
        raise RuntimeError(f"Instance deja active (lock: {lock_path})") from exc

    handle.seek(0)
    handle.truncate()
    handle.write(f"{os.getpid()}\n")
    handle.flush()
    return handle


def main() -> None:
    configure_logging()
    init_db()
    lock_handle = acquire_instance_lock()
    token = load_token()
    try:
        config.bot.run(token)
    finally:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        finally:
            lock_handle.close()


if __name__ == "__main__":
    main()
