# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from muffybot.discord import log_server_action, send_discord_webhook, send_task_report
from muffybot.env import get_env, load_dotenv
from muffybot.files import read_json
from muffybot.paths import ENVIKIDIA_DIR, LOG_DIR, ROOT_DIR
from muffybot.wiki import prepare_runtime

LOGGER = logging.getLogger(__name__)

TASK_REPORTS_FILE = LOG_DIR / "task_reports.jsonl"
DISCORD_QUEUE_FILE = LOG_DIR / "discord_queue.json"
FR_VANDALISM_DB = ROOT_DIR / "vandalism_db.json"
EN_VANDALISM_DB = ENVIKIDIA_DIR / "vandalism_db.json"


def _safe_float(value: object) -> float | None:
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _safe_int(value: object, default: int) -> int:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default


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


def _short_list(lines: list[str], max_lines: int = 7, max_chars: int = 980) -> str:
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


def _window_hours(period: str) -> int:
    period_norm = period.strip().lower()
    if period_norm == "weekly":
        return max(_safe_int(get_env("WEEKLY_REPORT_WINDOW_HOURS", "168"), 168), 1)
    if period_norm == "monthly":
        days = max(_safe_int(get_env("MONTHLY_REPORT_WINDOW_DAYS", "30"), 30), 1)
        return days * 24
    return max(_safe_int(get_env("DAILY_REPORT_WINDOW_HOURS", "24"), 24), 1)


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
        "top_users": [],
    }

    confidence_sum = 0.0
    confidence_count = 0
    page_counter: Counter[str] = Counter()
    user_counter: Counter[str] = Counter()

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
            creator = str(item.get("creator") or "Inconnu")
            page_counter[title] += 1
            user_counter[creator] += 1

            confidence = _safe_float(item.get("confidence"))
            if confidence is not None:
                confidence_sum += confidence
                confidence_count += 1

    if confidence_count:
        result["avg_confidence"] = confidence_sum / confidence_count
    result["top_pages"] = page_counter.most_common(6)
    result["top_users"] = user_counter.most_common(6)
    return result


def _discord_queue_depth() -> int:
    payload = read_json(DISCORD_QUEUE_FILE, default=[])
    if isinstance(payload, list):
        return len(payload)
    return 0


def _report_level(total_errors: int, queue_depth: int, reverts_total: int) -> str:
    critical_errors = max(_safe_int(get_env("REPORT_CRITICAL_ERROR_THRESHOLD", "5"), 5), 1)
    critical_queue = max(_safe_int(get_env("REPORT_CRITICAL_QUEUE_THRESHOLD", "20"), 20), 1)
    critical_reverts = max(_safe_int(get_env("REPORT_CRITICAL_REVERT_THRESHOLD", "80"), 80), 1)

    if total_errors >= critical_errors or queue_depth >= critical_queue or reverts_total >= critical_reverts:
        return "CRITICAL"
    if total_errors > 0 or queue_depth > 0:
        return "WARNING"
    return "INFO"


def _period_title(period: str) -> str:
    normalized = period.strip().lower()
    if normalized == "weekly":
        return "Rapport hebdomadaire consolidé"
    if normalized == "monthly":
        return "Rapport mensuel consolidé"
    return "Rapport quotidien consolidé"


def _script_name(period: str) -> str:
    normalized = period.strip().lower()
    if normalized == "weekly":
        return "weekly-report.py"
    if normalized == "monthly":
        return "monthly-report.py"
    return "daily-report.py"


def run(period: str = "daily") -> int:
    started = time.monotonic()
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    prepare_runtime(ROOT_DIR)
    report_script_name = _script_name(period)
    log_server_action("report_run_start", script_name=report_script_name, include_runtime=True, context={"period": period})

    now = datetime.now(timezone.utc)
    window_hours = _window_hours(period)
    since = now - timedelta(hours=window_hours)

    scripts, totals = _aggregate_task_reports(since=since)
    reverts = _aggregate_reverts(since=since)
    queue_depth = _discord_queue_depth()
    log_server_action(
        "report_aggregates_ready",
        script_name=report_script_name,
        context={
            "period": period,
            "window_hours": window_hours,
            "runs_total": int(totals["runs"]),
            "reverts_total": int(reverts["total"]),
            "queue_depth": queue_depth,
        },
    )

    top_scripts: list[str] = []
    for script_name, stats in sorted(scripts.items(), key=lambda item: (-item[1]["runs"], item[0]))[:10]:
        avg_duration = stats["duration_sum"] / stats["duration_count"] if stats["duration_count"] else 0.0
        errors = int(stats["error"] + stats["failed"])
        top_scripts.append(f"{script_name}: {int(stats['runs'])} runs, {errors} erreurs, {avg_duration:.1f}s moy")

    top_pages = [f"{title}: {count}" for title, count in reverts.get("top_pages", [])]
    top_users = [f"{creator}: {count}" for creator, count in reverts.get("top_users", [])]
    avg_confidence = float(reverts.get("avg_confidence", 0.0))

    total_errors = int(totals["error"] + totals["failed"])
    level = _report_level(total_errors=total_errors, queue_depth=queue_depth, reverts_total=int(reverts["total"]))
    color_map = {"INFO": 3447003, "WARNING": 15105570, "CRITICAL": 15158332}

    embed = {
        "title": _period_title(period),
        "description": (
            f"Période UTC: {since.strftime('%Y-%m-%d %H:%M')} -> "
            f"{now.strftime('%Y-%m-%d %H:%M')} ({window_hours}h)"
        ),
        "color": color_map.get(level, 3447003),
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
                "name": "Infra",
                "value": f"Queue Discord: {queue_depth}\nNiveau: {level}",
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
            {
                "name": "Top utilisateurs revertés",
                "value": _short_list(top_users),
                "inline": False,
            },
        ],
        "timestamp": now.isoformat().replace("+00:00", "Z"),
    }

    sent = send_discord_webhook(embed=embed, level=level, script_name=report_script_name)
    summary = (
        f"{period} report envoyé: runs={int(totals['runs'])}, "
        f"reverts={int(reverts['total'])}, errors={total_errors}, queue={queue_depth}, level={level}"
    )
    if not sent:
        LOGGER.warning("Envoi Discord du rapport %s non confirmé", period)
        log_server_action("report_send_failed", script_name=report_script_name, level="WARNING", context={"period": period, "level": level})
    else:
        log_server_action("report_send_success", script_name=report_script_name, level=level, context={"period": period, "level": level})

    send_task_report(
        script_name=report_script_name,
        status="SUCCESS" if sent else "WARNING",
        duration_seconds=time.monotonic() - started,
        details=summary,
        stats={
            "period": period,
            "window_hours": window_hours,
            "runs_total": int(totals["runs"]),
            "errors_total": total_errors,
            "reverts_total": int(reverts["total"]),
            "discord_queue_depth": queue_depth,
            "level": level,
        },
        level=level if sent else "WARNING",
        channel="server",
    )
    return 0


def main() -> int:
    return run("daily")


def main_weekly() -> int:
    return run("weekly")


def main_monthly() -> int:
    return run("monthly")


if __name__ == "__main__":
    raise SystemExit(main())
