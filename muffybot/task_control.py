# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import time
from typing import Any

from muffybot.admin_ops import kill_switch_enabled, maintenance_mode_enabled
from muffybot.discord import log_server_action, send_task_report


def dry_run_enabled() -> bool:
    raw = os.getenv("MUFFYBOT_DRY_RUN", "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


class RunPausedError(RuntimeError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def ensure_runtime_allowed(script_name: str) -> None:
    if kill_switch_enabled():
        raise RunPausedError("Kill switch actif")
    if maintenance_mode_enabled():
        allow_during_maintenance = str(os.getenv("MUFFYBOT_ALLOW_DURING_MAINTENANCE", "")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        if not allow_during_maintenance:
            raise RunPausedError("Mode maintenance actif")


def save_page_or_dry_run(
    page: Any,
    *,
    script_name: str,
    summary: str,
    minor: bool,
    botflag: bool,
    context: dict[str, object] | None = None,
) -> bool:
    if dry_run_enabled():
        log_server_action(
            "dry_run_skip_save",
            script_name=script_name,
            level="WARNING",
            context={"summary": summary[:220], **(context or {})},
        )
        return False
    page.save(summary=summary, minor=minor, botflag=botflag)
    return True


def report_lock_unavailable(script_name: str, started_monotonic: float, lock_name: str) -> int:
    details = f"Execution ignoree: lock '{lock_name}' deja actif."
    duration = max(0.0, time.monotonic() - started_monotonic)
    log_server_action(
        "run_skipped_lock_held",
        script_name=script_name,
        level="WARNING",
        context={"lock_name": lock_name, "duration_seconds": round(duration, 3)},
    )
    send_task_report(
        script_name=script_name,
        status="WARNING",
        duration_seconds=duration,
        details=details,
        stats={"reason": "lock_held", "lock_name": lock_name},
        level="WARNING",
    )
    return 0
