#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .config import DB_BACKUP_DIR, DB_PATH, RUN_LOG_DIR, SERVER_ACTIONS_LOG
from .utils import redact_sensitive, utc_now_iso

SETTINGS_CACHE: dict[str, str] = {}


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)
    DB_BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    with db_connect() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS script_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                script_key TEXT NOT NULL,
                requester_id INTEGER NOT NULL,
                requester_tag TEXT NOT NULL,
                public_request INTEGER NOT NULL,
                command_json TEXT NOT NULL,
                status TEXT NOT NULL,
                return_code INTEGER,
                note TEXT,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                duration_seconds REAL,
                log_path TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS op_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                actor_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                target TEXT,
                details TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS server_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                level TEXT NOT NULL,
                event TEXT NOT NULL,
                actor_id INTEGER,
                guild_id INTEGER,
                channel_id INTEGER,
                details TEXT
            )
            """
        )

        conn.execute("CREATE INDEX IF NOT EXISTS idx_script_runs_script ON script_runs(script_key)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_script_runs_started ON script_runs(started_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_script_runs_status ON script_runs(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_server_logs_ts ON server_logs(ts)")

        defaults = {
            "maintenance_mode": "0",
            "public_start_enabled": "1",
            "dry_run_mode": "0",
            "max_parallel_runs": "4",
            "public_cooldown_seconds": "120",
            "public_panel_channel_id": "",
            "public_panel_message_id": "",
            "public_channel_whitelist": "",
            "max_auto_retries": "1",
            "retry_backoff_seconds": "45",
            "max_system_ram_percent": "92",
            "max_process_ram_mb": "1400",
            "max_load_per_cpu_x10": "30",
            "min_free_disk_gb": "2",
            "startup_pressure_ram_percent": "95",
            "startup_pressure_load_per_cpu_x10": "45",
            "startup_pressure_min_free_disk_gb": "1",
            "log_retention_days": "14",
            "presence_state": "online",
            "presence_mode": "watching",
            "presence_text": "Vikidia scripts | run:{running} queue:{queue}",
            "digest_channel_id": "1427596219676495904",
            "critical_mention_user_id": "1424064908244422668",
            "last_daily_digest_date": "",
            "last_weekly_digest_key": "",
            "last_monthly_digest_key": "",
            "supervision_verbose": "1",
            "undo_approved_discord_ids": "",
            "undo_max_edits_per_run": "30",
        }
        for key, value in defaults.items():
            conn.execute("INSERT OR IGNORE INTO settings(key, value) VALUES(?, ?)", (key, value))

        rows = conn.execute("SELECT key, value FROM settings").fetchall()
        SETTINGS_CACHE.clear()
        SETTINGS_CACHE.update({str(row["key"]): str(row["value"]) for row in rows})


def get_setting(key: str, default: str) -> str:
    return SETTINGS_CACHE.get(key, default)


def set_setting(key: str, value: str) -> None:
    SETTINGS_CACHE[key] = value
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO settings(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )


def get_setting_bool(key: str, default: bool) -> bool:
    raw = get_setting(key, "1" if default else "0").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def get_setting_int(key: str, default: int, min_value: int = 0, max_value: int = 100000) -> int:
    raw = get_setting(key, str(default))
    try:
        value = int(raw)
    except ValueError:
        value = default
    return max(min(value, max_value), min_value)


def get_setting_int_optional(key: str) -> int | None:
    raw = get_setting(key, "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def set_public_panel_location(channel_id: int, message_id: int) -> None:
    set_setting("public_panel_channel_id", str(channel_id))
    set_setting("public_panel_message_id", str(message_id))


def get_public_panel_location() -> tuple[int | None, int | None]:
    return get_setting_int_optional("public_panel_channel_id"), get_setting_int_optional("public_panel_message_id")


def clear_public_panel_location() -> None:
    set_setting("public_panel_channel_id", "")
    set_setting("public_panel_message_id", "")


def write_action_log(payload: dict[str, Any]) -> None:
    try:
        RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)
        with SERVER_ACTIONS_LOG.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass


def server_log(
    *,
    level: str,
    event: str,
    actor_id: int | None = None,
    guild_id: int | None = None,
    channel_id: int | None = None,
    details: str = "",
) -> None:
    ts = utc_now_iso()
    payload = {
        "ts": ts,
        "level": level,
        "event": event,
        "actor_id": actor_id,
        "guild_id": guild_id,
        "channel_id": channel_id,
        "details": details,
    }
    write_action_log(payload)
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO server_logs(ts, level, event, actor_id, guild_id, channel_id, details)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            (ts, level[:20], event[:200], actor_id, guild_id, channel_id, details[:3000]),
        )


def audit(
    actor_id: int,
    action: str,
    target: str = "",
    details: str = "",
    *,
    guild_id: int | None = None,
    channel_id: int | None = None,
) -> None:
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO op_audit(ts, actor_id, action, target, details) VALUES(?, ?, ?, ?, ?)",
            (utc_now_iso(), actor_id, action[:200], target[:200], details[:2000]),
        )
    server_log(
        level="info",
        event=action,
        actor_id=actor_id,
        guild_id=guild_id,
        channel_id=channel_id,
        details=f"target={target} details={details}",
    )


def insert_run(
    *,
    script_key: str,
    requester_id: int,
    requester_tag: str,
    public_request: bool,
    command: list[str],
    started_at: str,
    log_path: Path,
) -> int:
    with db_connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO script_runs(
                script_key, requester_id, requester_tag, public_request,
                command_json, status, started_at, log_path
            ) VALUES(?, ?, ?, ?, ?, 'running', ?, ?)
            """,
            (
                script_key,
                requester_id,
                requester_tag,
                1 if public_request else 0,
                json.dumps(command, ensure_ascii=False),
                started_at,
                str(log_path),
            ),
        )
        return int(cursor.lastrowid)


def finalize_run(
    run_id: int,
    *,
    status: str,
    return_code: int | None,
    note: str,
    ended_at: str,
    duration_seconds: float,
) -> None:
    with db_connect() as conn:
        conn.execute(
            """
            UPDATE script_runs
            SET status = ?, return_code = ?, note = ?, ended_at = ?, duration_seconds = ?
            WHERE id = ?
            """,
            (status, return_code, note[:2000], ended_at, duration_seconds, run_id),
        )


def last_runs(script_key: str | None = None, limit: int = 10) -> list[sqlite3.Row]:
    limit = max(min(limit, 200), 1)
    with db_connect() as conn:
        if script_key:
            rows = conn.execute(
                """
                SELECT * FROM script_runs
                WHERE script_key = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (script_key, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM script_runs
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
    return list(rows)


def filtered_runs(
    *,
    script_key: str | None,
    status: str | None,
    limit: int,
    offset: int,
) -> tuple[list[sqlite3.Row], int]:
    limit = max(min(limit, 50), 1)
    offset = max(offset, 0)
    clauses: list[str] = []
    params: list[Any] = []

    if script_key:
        clauses.append("script_key = ?")
        params.append(script_key)
    if status:
        clauses.append("status = ?")
        params.append(status)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    with db_connect() as conn:
        rows = conn.execute(
            f"""
            SELECT * FROM script_runs
            {where_sql}
            ORDER BY id DESC
            LIMIT ? OFFSET ?
            """,
            (*params, limit, offset),
        ).fetchall()
        total = int(
            conn.execute(
                f"SELECT COUNT(*) FROM script_runs {where_sql}",
                params,
            ).fetchone()[0]
        )
    return list(rows), total


def last_failed_run() -> sqlite3.Row | None:
    with db_connect() as conn:
        row = conn.execute(
            """
            SELECT * FROM script_runs
            WHERE status IN ('failed','timed_out','killed_resource','killed')
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    return row


def summarize_runs(start_iso: str, end_iso: str) -> dict[str, Any]:
    with db_connect() as conn:
        totals = conn.execute(
            """
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_count,
                   SUM(CASE WHEN status != 'success' THEN 1 ELSE 0 END) AS failure_count,
                   AVG(duration_seconds) AS avg_duration
            FROM script_runs
            WHERE started_at >= ? AND started_at < ?
            """,
            (start_iso, end_iso),
        ).fetchone()

        by_status = conn.execute(
            """
            SELECT status, COUNT(*) AS c
            FROM script_runs
            WHERE started_at >= ? AND started_at < ?
            GROUP BY status
            ORDER BY c DESC
            """,
            (start_iso, end_iso),
        ).fetchall()

        by_script = conn.execute(
            """
            SELECT script_key, COUNT(*) AS c
            FROM script_runs
            WHERE started_at >= ? AND started_at < ?
            GROUP BY script_key
            ORDER BY c DESC
            LIMIT 8
            """,
            (start_iso, end_iso),
        ).fetchall()

        failed_by_script = conn.execute(
            """
            SELECT script_key, COUNT(*) AS c
            FROM script_runs
            WHERE started_at >= ? AND started_at < ? AND status != 'success'
            GROUP BY script_key
            ORDER BY c DESC
            LIMIT 8
            """,
            (start_iso, end_iso),
        ).fetchall()

    total = int(totals["total"] or 0)
    success_count = int(totals["success_count"] or 0)
    failure_count = int(totals["failure_count"] or 0)
    avg_duration = float(totals["avg_duration"] or 0.0)
    success_rate = (success_count / total * 100.0) if total else 0.0

    return {
        "total": total,
        "success_count": success_count,
        "failure_count": failure_count,
        "success_rate": success_rate,
        "avg_duration": avg_duration,
        "by_status": [(str(row["status"]), int(row["c"])) for row in by_status],
        "by_script": [(str(row["script_key"]), int(row["c"])) for row in by_script],
        "by_script_failed": [(str(row["script_key"]), int(row["c"])) for row in failed_by_script],
    }


def search_logs(log_paths: list[tuple[int, str, str]], needle: str, max_lines: int) -> str:
    query = (needle or "").strip().lower()
    if not query:
        return "Pattern vide."

    max_lines = max(min(max_lines, 200), 1)
    matches: list[str] = []

    for run_id, script_key, path_raw in log_paths:
        path = Path(path_raw)
        if not path.exists():
            continue
        try:
            content = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue

        for line in content.splitlines()[-4000:]:
            if query in line.lower():
                matches.append(f"#{run_id} {script_key} | {line[:250]}")
                if len(matches) >= max_lines:
                    return redact_sensitive("\n".join(matches))

    return redact_sensitive("\n".join(matches) if matches else "Aucune correspondance trouvee.")
