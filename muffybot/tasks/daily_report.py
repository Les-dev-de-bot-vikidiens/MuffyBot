# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from muffybot.discord import send_discord_webhook, send_task_report
from muffybot.env import get_env, load_dotenv
from muffybot.files import read_json
from muffybot.paths import ENVIKIDIA_DIR, LOG_DIR, ROOT_DIR
from muffybot.wiki import prepare_runtime

LOGGER = logging.getLogger(__name__)

SCRIPT_NAME = "daily-report.py"
DEFAULT_WINDOW_HOURS = 24
TASK_REPORTS_FILE = LOG_DIR / "task_reports.jsonl"
DISCORD_QUEUE_FILE = LOG_DIR / "discord_queue.json"
FR_VANDALISM_DB = ROOT_DIR / "vandalism_db.json"
EN_VANDALISM_DB = ENVIKIDIA_DIR / "vandalism_db.json"


def _safe_float(value: object) -> float | None:
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _parse_timestamp(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    events.append(payload)
    except Exception as exc:
        LOGGER.warning("Impossible de lire %s: %s", path, exc)
    return events


def _short_list(lines: list[str], max_lines: int = 6, max_chars: int = 980) -> str:
    if not lines:
        return "Aucune donnée"
    output: list[str] = []
    current = 0
    for line in lines[:max_lines]:
        text = f"- {line}"
        if current + len(text) + 1 > max_chars:
            break
        output.append(text)
        current += len(text) + 1
    return "\n".join(output) if output else "Aucune donnée"


def _window_hours() -> int:
    raw = get_env("DAILY_REPORT_WINDOW_HOURS", str(DEFAULT_WINDOW_HOURS))
    try:
        value = int(str(raw))
    except (TypeError, ValueError):
        value = DEFAULT_WINDOW_HOURS
    return max(value, 1)


def _aggregate_task_reports(since: datetime) -> tuple[dict[str, dict[str, float]], dict[str, float]]:
    scripts: dict[str, dict[str, float]] = {}
    totals = {"runs": 0.0, "success": 0.0, "warning": 0.0, "error": 0.0, "failed": 0.0}

    for event in _read_jsonl(TASK_REPORTS_FILE):
        timestamp = _parse_timestamp(event.get("timestamp"))
        if timestamp is None or timestamp < since:
            continue

        script_name = str(event.get("script_name") or "unknown")
        status = str(event.get("status") or "UNKNOWN").upper()
        duration = _safe_float(event.get("duration_seconds"))

        bucket = scripts.setdefault(
            script_name,
            {
                "runs": 0.0,
                "success": 0.0,
                "warning": 0.0,
                "error": 0.0,
                "failed": 0.0,
                "duration_sum": 0.0,
                "duration_count": 0.0,
            },
        )

        bucket["runs"] += 1
        totals["runs"] += 1

        if status in {"SUCCESS", "WARNING", "ERROR", "FAILED"}:
            key = status.lower()
            bucket[key] += 1
            totals[key] += 1

        if duration is not None and duration >= 0:
            bucket["duration_sum"] += duration
            bucket["duration_count"] += 1

    return scripts, totals


def _aggregate_reverts(since: datetime) -> dict[str, object]:
    result: dict[str, object] = {
        "fr": 0,
        "en": 0,
        "total": 0,
        "avg_confidence": 0.0,
        "top_pages": [],
    }

    confidence_sum = 0.0
    confidence_count = 0
    page_counter: Counter[str] = Counter()

    for lang, db_file in (("fr", FR_VANDALISM_DB), ("en", EN_VANDALISM_DB)):
        payload = read_json(db_file, default={})
        if not isinstance(payload, dict):
            continue
        for item in payload.values():
            if not isinstance(item, dict):
                continue
            timestamp = _parse_timestamp(item.get("timestamp"))
            if timestamp is None or timestamp < since:
                continue

            result[lang] = int(result[lang]) + 1
            result["total"] = int(result["total"]) + 1

            title = str(item.get("title") or "Inconnu")
            page_counter[title] += 1

            confidence = _safe_float(item.get("confidence"))
            if confidence is not None:
                confidence_sum += confidence
                confidence_count += 1

    if confidence_count:
        result["avg_confidence"] = confidence_sum / confidence_count
    result["top_pages"] = page_counter.most_common(5)
    return result


def _discord_queue_depth() -> int:
    payload = read_json(DISCORD_QUEUE_FILE, default=[])
    if isinstance(payload, list):
        return len(payload)
    return 0


def run() -> int:
    started = time.monotonic()
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    prepare_runtime(ROOT_DIR)

    now = datetime.now(timezone.utc)
    hours = _window_hours()
    since = now - timedelta(hours=hours)

    scripts, totals = _aggregate_task_reports(since=since)
    reverts = _aggregate_reverts(since=since)
    queue_depth = _discord_queue_depth()

    top_scripts: list[str] = []
    for script_name, stats in sorted(scripts.items(), key=lambda item: (-item[1]["runs"], item[0]))[:8]:
        avg_duration = stats["duration_sum"] / stats["duration_count"] if stats["duration_count"] else 0.0
        errors = int(stats["error"] + stats["failed"])
        top_scripts.append(f"{script_name}: {int(stats['runs'])} runs, {errors} erreurs, {avg_duration:.1f}s moy")

    top_pages = [f"{title}: {count}" for title, count in reverts.get("top_pages", [])]
    avg_confidence = float(reverts.get("avg_confidence", 0.0))

    total_errors = int(totals["error"] + totals["failed"])
    color = 5763719 if total_errors == 0 and queue_depth == 0 else 15105570
    level = "INFO" if total_errors == 0 else "WARNING"

    embed = {
        "title": "Rapport quotidien MuffyBot",
        "description": (
            f"Période UTC: {since.strftime('%Y-%m-%d %H:%M')} -> "
            f"{now.strftime('%Y-%m-%d %H:%M')} ({hours}h)"
        ),
        "color": color,
        "fields": [
            {
                "name": "Exécutions",
                "value": (
                    f"Runs: {int(totals['runs'])}\n"
                    f"Success: {int(totals['success'])}\n"
                    f"Warning: {int(totals['warning'])}\n"
                    f"Errors: {total_errors}"
                ),
                "inline": True,
            },
            {
                "name": "Anti-vandalisme",
                "value": (
                    f"FR reverts: {int(reverts['fr'])}\n"
                    f"EN reverts: {int(reverts['en'])}\n"
                    f"Total: {int(reverts['total'])}\n"
                    f"Confiance moy: {avg_confidence * 100:.1f}%"
                ),
                "inline": True,
            },
            {
                "name": "File Discord",
                "value": f"{queue_depth} message(s) en attente",
                "inline": True,
            },
            {
                "name": "Top scripts",
                "value": _short_list(top_scripts),
                "inline": False,
            },
            {
                "name": "Top pages revertées",
                "value": _short_list(top_pages),
                "inline": False,
            },
        ],
        "timestamp": now.isoformat().replace("+00:00", "Z"),
    }

    sent = send_discord_webhook(embed=embed, level=level, script_name=SCRIPT_NAME)
    summary = (
        f"Rapport quotidien envoyé: runs={int(totals['runs'])}, "
        f"reverts={int(reverts['total'])}, errors={total_errors}, queue={queue_depth}"
    )
    if not sent:
        LOGGER.warning("Envoi Discord du rapport quotidien non confirmé")

    send_task_report(
        script_name=SCRIPT_NAME,
        status="SUCCESS" if sent else "WARNING",
        duration_seconds=time.monotonic() - started,
        details=summary,
        stats={
            "runs_total": int(totals["runs"]),
            "errors_total": total_errors,
            "reverts_total": int(reverts["total"]),
            "discord_queue_depth": queue_depth,
        },
        level=level if sent else "WARNING",
    )
    return 0


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
