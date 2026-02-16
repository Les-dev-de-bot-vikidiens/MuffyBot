# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from muffybot.discord import log_server_action, send_discord_webhook, send_task_report
from muffybot.env import get_bool_env, get_env, get_int_env, load_dotenv
from muffybot.files import read_json
from muffybot.paths import LOG_DIR, ROOT_DIR
from muffybot.wiki import prepare_runtime

LOGGER = logging.getLogger(__name__)

WEBHOOK_PREFIXES = (
    "https://discord.com/api/webhooks/",
    "https://ptb.discord.com/api/webhooks/",
    "https://canary.discord.com/api/webhooks/",
)


def _is_webhook_url(value: str | None) -> bool:
    if not value:
        return False
    text = value.strip().lower()
    return any(text.startswith(prefix) for prefix in WEBHOOK_PREFIXES)


def _check_writable(path: Path) -> bool:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8"):
            pass
        return True
    except Exception:
        return False


def _format_lines(lines: list[str], fallback: str = "Aucune") -> str:
    if not lines:
        return fallback
    return "\n".join(f"- {line}" for line in lines[:12])


def main() -> int:
    started = time.monotonic()
    script_name = "doctor.py"
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    prepare_runtime(ROOT_DIR)

    log_server_action("doctor_start", script_name=script_name, include_runtime=True)

    ok_checks: list[str] = []
    warning_checks: list[str] = []
    critical_checks: list[str] = []
    critical_count = 0
    warning_count = 0

    def add_check(name: str, ok: bool, *, detail: str, severity: str = "warning") -> None:
        nonlocal critical_count, warning_count
        if ok:
            ok_checks.append(f"{name}: {detail}")
            log_server_action(
                "doctor_check",
                script_name=script_name,
                level="SUCCESS",
                context={"check": name, "ok": True, "detail": detail[:220], "severity": severity},
            )
            return
        if severity == "critical":
            critical_count += 1
            critical_checks.append(f"{name}: {detail}")
            level = "ERROR"
        else:
            warning_count += 1
            warning_checks.append(f"{name}: {detail}")
            level = "WARNING"
        log_server_action(
            "doctor_check",
            script_name=script_name,
            level=level,
            context={"check": name, "ok": False, "detail": detail[:220], "severity": severity},
        )

    for key, required in (
        ("DISCORD_WEBHOOK_MAIN", True),
        ("DISCORD_WEBHOOK_SERVER_LOGS", True),
        ("DISCORD_WEBHOOK_ERRORS", False),
        ("DISCORD_WEBHOOK_VANDALISM", False),
    ):
        raw = get_env(key)
        valid = _is_webhook_url(raw)
        if required:
            add_check(key, valid, detail="webhook valide" if valid else "manquant ou invalide", severity="critical")
        else:
            add_check(key, valid or not raw, detail="configuré" if valid else "non défini (optionnel)", severity="warning")

    task_reports_file = Path(get_env("TASK_REPORTS_FILE", str(LOG_DIR / "task_reports.jsonl")) or str(LOG_DIR / "task_reports.jsonl"))
    server_actions_file = Path(get_env("SERVER_ACTIONS_FILE", str(LOG_DIR / "server_actions.jsonl")) or str(LOG_DIR / "server_actions.jsonl"))

    add_check("LOG_DIR", LOG_DIR.exists() and LOG_DIR.is_dir(), detail=str(LOG_DIR), severity="critical")
    add_check("TASK_REPORTS_FILE", _check_writable(task_reports_file), detail=str(task_reports_file), severity="warning")
    add_check("SERVER_ACTIONS_FILE", _check_writable(server_actions_file), detail=str(server_actions_file), severity="warning")

    queue_file = LOG_DIR / "discord_queue.json"
    queue_payload = read_json(queue_file, default=[])
    queue_depth = len(queue_payload) if isinstance(queue_payload, list) else 0
    queue_warn_threshold = max(get_int_env("DOCTOR_QUEUE_WARNING_THRESHOLD", 50), 1)
    add_check(
        "DISCORD_QUEUE_DEPTH",
        queue_depth < queue_warn_threshold,
        detail=f"queue={queue_depth}, seuil={queue_warn_threshold}",
        severity="warning",
    )

    add_check(
        "SERVER_LOG_EVERY_ACTION",
        get_bool_env("SERVER_LOG_EVERY_ACTION", True),
        detail="activé" if get_bool_env("SERVER_LOG_EVERY_ACTION", True) else "désactivé",
        severity="warning",
    )
    add_check(
        "SERVER_ACTION_LOG_TO_DISCORD",
        get_bool_env("SERVER_ACTION_LOG_TO_DISCORD", True),
        detail="activé" if get_bool_env("SERVER_ACTION_LOG_TO_DISCORD", True) else "désactivé (fichier local uniquement)",
        severity="warning",
    )

    if get_bool_env("DOCTOR_SEND_TEST_MESSAGES", False):
        ping_text = "MuffyBot doctor ping test"
        default_ok = send_discord_webhook(content=ping_text, level="INFO", script_name=script_name)
        server_ok = send_discord_webhook(content=ping_text, level="INFO", script_name=script_name, channel="server")
        add_check("DISCORD_PING_MAIN", default_ok, detail="test webhook principal", severity="warning")
        add_check("DISCORD_PING_SERVER", server_ok, detail="test webhook serveur", severity="warning")

    level = "CRITICAL" if critical_count > 0 else ("WARNING" if warning_count > 0 else "SUCCESS")
    color_map = {"SUCCESS": 5763719, "WARNING": 15105570, "CRITICAL": 15158332}
    now = datetime.now(timezone.utc)

    embed = {
        "title": "Diagnostic MuffyBot",
        "description": "Audit de configuration, logs et connectivité Discord.",
        "color": color_map[level],
        "fields": [
            {"name": "Checks critiques", "value": _format_lines(critical_checks, fallback="Aucun"), "inline": False},
            {"name": "Warnings", "value": _format_lines(warning_checks, fallback="Aucun"), "inline": False},
            {"name": "Checks OK", "value": _format_lines(ok_checks), "inline": False},
            {
                "name": "Résumé",
                "value": f"critical={critical_count} | warning={warning_count} | queue={queue_depth}",
                "inline": False,
            },
        ],
        "timestamp": now.isoformat().replace("+00:00", "Z"),
    }

    sent = send_discord_webhook(embed=embed, level=level, script_name=script_name, channel="server")
    if not sent:
        LOGGER.warning("Impossible d'envoyer le diagnostic doctor sur Discord")

    duration = time.monotonic() - started
    summary = f"Doctor terminé: critical={critical_count}, warning={warning_count}, queue={queue_depth}"
    send_task_report(
        script_name=script_name,
        status="FAILED" if critical_count > 0 else ("WARNING" if warning_count > 0 else "SUCCESS"),
        duration_seconds=duration,
        details=summary,
        stats={
            "critical": critical_count,
            "warning": warning_count,
            "queue_depth": queue_depth,
            "discord_sent": int(sent),
        },
        level=level,
        channel="server",
    )
    log_server_action(
        "doctor_end",
        script_name=script_name,
        level=level,
        context={"critical": critical_count, "warning": warning_count, "queue_depth": queue_depth, "duration_seconds": round(duration, 2)},
    )
    return 1 if critical_count > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
