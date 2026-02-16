# -*- coding: utf-8 -*-
from __future__ import annotations

import fcntl
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .paths import LOG_DIR, ensure_dir


class LockUnavailableError(RuntimeError):
    """Raised when a lock cannot be acquired immediately."""


@contextmanager
def hold_lock(lock_name: str, lock_dir: Path | None = None) -> Iterator[Path]:
    """
    Acquire an exclusive non-blocking lock for the current process.

    The lock is released automatically when the context exits.
    """
    safe_name = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in lock_name).strip("._") or "task"
    directory = ensure_dir(lock_dir or (LOG_DIR / "locks"))
    lock_path = directory / f"{safe_name}.lock"

    lock_file = lock_path.open("a+", encoding="utf-8")
    try:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise LockUnavailableError(f"Lock already held: {lock_path}") from exc

        lock_file.seek(0)
        lock_file.truncate()
        lock_file.write(f"{os.getpid()}\n")
        lock_file.flush()
        yield lock_path
    finally:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        finally:
            lock_file.close()
