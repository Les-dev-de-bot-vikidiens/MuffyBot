# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
import logging
import os
import platform
import socket
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any

import requests

from .env import get_bool_env, get_csv_env, get_env, load_dotenv
from .paths import LOG_DIR

load_dotenv()

LOGGER = logging.getLogger(__name__)

MAX_CONTENT_LENGTH = 2000
MAX_EMBED_DESCRIPTION = 4096
MAX_EMBED_TITLE = 256
MAX_EMBED_FIELDS = 25
MAX_FIELD_NAME = 256
MAX_FIELD_VALUE = 1024
MAX_REPORT_DETAIL = 1600
MAX_REPORT_STATS_KEYS = 30
MAX_REPORT_STAT_VALUE = 300
MAX_SERVER_CONTEXT_SIZE = 7000
MAX_ACTION_CONTEXT_DEPTH = 4
MAX_ACTION_CONTEXT_KEYS = 80
MAX_ACTION_CONTEXT_ITEMS = 30
MAX_ACTION_CONTEXT_TEXT = 700
TASK_REPORTS_FILE = Path(get_env("TASK_REPORTS_FILE", str(LOG_DIR / "task_reports.jsonl")) or str(LOG_DIR / "task_reports.jsonl"))
_REPORT_LOCK = Lock()
SERVER_ACTIONS_FILE = Path(get_env("SERVER_ACTIONS_FILE", str(LOG_DIR / "server_actions.jsonl")) or str(LOG_DIR / "server_actions.jsonl"))
_SERVER_ACTION_FILE_LOCK = Lock()
_SERVER_ACTION_SEQ_LOCK = Lock()
_SERVER_ACTION_SEQ = 0
_SERVER_ACTION_SESSION_ID = f"{socket.gethostname()}:{os.getpid()}:{int(time.time())}"


def _safe_int(value: str | None, default: int) -> int:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _truncate(value: object, max_size: int) -> str:
    text = str(value or "")
    return text[:max_size]


def _critical_levels() -> set[str]:
    return {item.upper() for item in get_csv_env("DISCORD_CRITICAL_LEVELS", ["CRITICAL", "FAILED"])}


def _critical_user_mention() -> str | None:
    user_id = (get_env("DISCORD_CRITICAL_USER_ID") or "").strip()
    if user_id:
        return f"<@{user_id}>"
    username = (get_env("DISCORD_CRITICAL_USERNAME") or "").strip()
    if username:
        return f"@{username}"
    return None


def _prepend_critical_mention(content: str | None, level: str) -> str | None:
    if not get_bool_env("DISCORD_MENTION_ON_CRITICAL", True):
        return content
    normalized = (level or "INFO").upper()
    if normalized not in _critical_levels():
        return content
    mention = _critical_user_mention()
    if not mention:
        return content
    text = (content or "").strip()
    if mention in text:
        return text
    return mention if not text else f"{mention} {text}"


def _runtime_snapshot(include_secrets: bool) -> dict[str, object]:
    snapshot: dict[str, object] = {
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "pid": os.getpid(),
        "cwd": str(Path.cwd()),
        "utc": _utc_now_iso(),
    }
    if include_secrets:
        sensitive_values: dict[str, str] = {}
        for key in sorted(os.environ.keys()):
            upper = key.upper()
            if any(token in upper for token in ("KEY", "TOKEN", "SECRET", "WEBHOOK", "PASSWORD")):
                sensitive_values[key] = _truncate(os.environ.get(key, ""), 400)
        snapshot["sensitive_env"] = sensitive_values
    return snapshot


def _json_safe(value: object) -> object:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return _truncate(value, MAX_REPORT_STAT_VALUE)


def _normalize_stats(stats: dict[str, object] | None) -> dict[str, object]:
    if not stats:
        return {}
    normalized: dict[str, object] = {}
    for key, value in list(stats.items())[:MAX_REPORT_STATS_KEYS]:
        normalized[_truncate(key, 120)] = _json_safe(value)
    return normalized


def _normalize_action_context(value: object, depth: int = 0) -> object:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return _truncate(value, MAX_ACTION_CONTEXT_TEXT)
    if depth >= MAX_ACTION_CONTEXT_DEPTH:
        return _truncate(repr(value), MAX_ACTION_CONTEXT_TEXT)
    if isinstance(value, dict):
        normalized: dict[str, object] = {}
        for key, item in list(value.items())[:MAX_ACTION_CONTEXT_KEYS]:
            normalized[_truncate(key, 140)] = _normalize_action_context(item, depth + 1)
        return normalized
    if isinstance(value, (list, tuple, set)):
        return [_normalize_action_context(item, depth + 1) for item in list(value)[:MAX_ACTION_CONTEXT_ITEMS]]
    return _truncate(repr(value), MAX_ACTION_CONTEXT_TEXT)


def _next_server_action_sequence() -> int:
    global _SERVER_ACTION_SEQ
    with _SERVER_ACTION_SEQ_LOCK:
        _SERVER_ACTION_SEQ += 1
        return _SERVER_ACTION_SEQ


def _rotate_file(path: Path, max_size_bytes: int, backups: int) -> None:
    if max_size_bytes <= 0 or backups <= 0:
        return
    if not path.exists():
        return
    try:
        if path.stat().st_size < max_size_bytes:
            return
    except OSError:
        return

    for index in range(backups - 1, 0, -1):
        source = path.with_name(f"{path.name}.{index}")
        target = path.with_name(f"{path.name}.{index + 1}")
        if source.exists():
            if target.exists():
                target.unlink()
            source.replace(target)

    first_backup = path.with_name(f"{path.name}.1")
    if first_backup.exists():
        first_backup.unlink()
    path.replace(first_backup)


def _record_server_action_event(payload: dict[str, object]) -> None:
    max_size_mb = max(_safe_int(get_env("SERVER_ACTION_LOG_MAX_MB", "25"), 25), 1)
    backups = max(_safe_int(get_env("SERVER_ACTION_LOG_BACKUPS", "4"), 4), 1)
    try:
        with _SERVER_ACTION_FILE_LOCK:
            SERVER_ACTIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
            _rotate_file(SERVER_ACTIONS_FILE, max_size_mb * 1024 * 1024, backups)
            with SERVER_ACTIONS_FILE.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception as exc:
        LOGGER.warning("Unable to record server action event: %s", exc)


def _record_task_report_event(
    *,
    timestamp: str,
    script_name: str,
    status: str,
    level: str,
    duration_seconds: float | None,
    stats: dict[str, object],
    details: str | None,
) -> None:
    event = {
        "timestamp": timestamp,
        "script_name": script_name,
        "status": status,
        "level": level,
        "duration_seconds": round(float(duration_seconds), 4) if duration_seconds is not None else None,
        "stats": stats,
        "details": _truncate(details, MAX_REPORT_DETAIL) if details else "",
    }
    try:
        with _REPORT_LOCK:
            TASK_REPORTS_FILE.parent.mkdir(parents=True, exist_ok=True)
            with TASK_REPORTS_FILE.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(event, ensure_ascii=False) + "\n")
    except Exception as exc:
        LOGGER.warning("Unable to record task report event: %s", exc)


def _is_webhook_placeholder(url: str) -> bool:
    marker = url.strip().lower()
    return any(token in marker for token in ("votre_webhook_ici", "your_webhook_here", "<webhook>", "example.com"))


def _is_webhook_valid(url: str) -> bool:
    lowered = url.strip().lower()
    if not lowered:
        return False
    prefixes = (
        "https://discord.com/api/webhooks/",
        "https://ptb.discord.com/api/webhooks/",
        "https://canary.discord.com/api/webhooks/",
    )
    return lowered.startswith(prefixes)


def _clean_webhook(raw_value: str | None, field_name: str) -> str | None:
    if not raw_value:
        return None
    value = raw_value.strip()
    if not value:
        return None
    if _is_webhook_placeholder(value):
        LOGGER.warning("%s is still a placeholder and will be ignored", field_name)
        return None
    if not _is_webhook_valid(value):
        LOGGER.warning("%s is not a valid Discord webhook URL and will be ignored", field_name)
        return None
    return value


def _chunk_content(content: str) -> list[str]:
    text = str(content or "").strip()
    if not text:
        return []
    if len(text) <= MAX_CONTENT_LENGTH:
        return [text]

    chunks: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= MAX_CONTENT_LENGTH:
            chunks.append(remaining)
            break

        split_at = remaining.rfind("\n", 0, MAX_CONTENT_LENGTH)
        if split_at < MAX_CONTENT_LENGTH // 2:
            split_at = MAX_CONTENT_LENGTH

        chunk = remaining[:split_at].rstrip()
        if not chunk:
            chunk = remaining[:MAX_CONTENT_LENGTH]
            split_at = MAX_CONTENT_LENGTH

        chunks.append(chunk)
        remaining = remaining[split_at:].lstrip()

    return chunks


def _normalize_timestamp(raw: object) -> str:
    text = str(raw or "").strip()
    if not text:
        return _utc_now_iso()
    if text.endswith("Z") or "+" in text[10:]:
        return text
    return f"{text}Z"


def _normalize_embed(embed: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(embed)
    if "title" in normalized:
        normalized["title"] = _truncate(normalized.get("title"), MAX_EMBED_TITLE)
    if "description" in normalized:
        normalized["description"] = _truncate(normalized.get("description"), MAX_EMBED_DESCRIPTION)
    normalized["timestamp"] = _normalize_timestamp(normalized.get("timestamp"))

    fields = normalized.get("fields")
    if isinstance(fields, list):
        safe_fields: list[dict[str, Any]] = []
        for field in fields[:MAX_EMBED_FIELDS]:
            if not isinstance(field, dict):
                continue
            safe_fields.append(
                {
                    "name": _truncate(field.get("name"), MAX_FIELD_NAME) or "Info",
                    "value": _truncate(field.get("value"), MAX_FIELD_VALUE) or "-",
                    "inline": bool(field.get("inline", False)),
                }
            )
        normalized["fields"] = safe_fields
    return normalized


def _payload_fingerprint(webhook: str, payload: dict[str, Any]) -> str:
    normalized_payload = dict(payload)
    embeds = normalized_payload.get("embeds")
    if isinstance(embeds, list):
        clean_embeds: list[dict[str, Any]] = []
        for item in embeds:
            if isinstance(item, dict):
                cloned = dict(item)
                cloned.pop("timestamp", None)
                clean_embeds.append(cloned)
        normalized_payload["embeds"] = clean_embeds
    blob = json.dumps({"webhook": webhook, "payload": normalized_payload}, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


class DiscordNotifier:
    def __init__(self) -> None:
        self.main = _clean_webhook(
            get_env("DISCORD_WEBHOOK_MAIN") or get_env("DISCORD_WEBHOOK") or get_env("DISCORD_WEBHOOK_URL"),
            "DISCORD_WEBHOOK_MAIN",
        )
        self.errors = _clean_webhook(
            get_env("DISCORD_WEBHOOK_ERRORS") or get_env("DISCORD_WEBHOOK_ERRORS_URL"),
            "DISCORD_WEBHOOK_ERRORS",
        )
        self.vandalism = _clean_webhook(
            get_env("DISCORD_WEBHOOK_VANDALISM") or get_env("DISCORD_WEBHOOK_VANDALISM_URL"),
            "DISCORD_WEBHOOK_VANDALISM",
        )
        self.server_logs = _clean_webhook(
            get_env("DISCORD_WEBHOOK_SERVER_LOGS") or get_env("DISCORD_WEBHOOK_SERVER"),
            "DISCORD_WEBHOOK_SERVER_LOGS",
        )
        self.request_timeout = max(_safe_int(get_env("DISCORD_TIMEOUT_SECONDS", "12"), 12), 3)
        self.max_retries = max(_safe_int(get_env("DISCORD_MAX_RETRIES", "3"), 3), 0)
        self.dedupe_window_seconds = max(_safe_int(get_env("DISCORD_DEDUP_WINDOW_SECONDS", "45"), 45), 0)
        self.max_queue_size = max(_safe_int(get_env("DISCORD_MAX_QUEUE_SIZE", "200"), 200), 10)
        self.queue_file = Path(get_env("DISCORD_QUEUE_FILE", str(LOG_DIR / "discord_queue.json")) or str(LOG_DIR / "discord_queue.json"))
        self._dedupe_cache: dict[str, float] = {}
        self._lock = Lock()

    def _pick_webhook(self, level: str, script_name: str | None = None, channel: str | None = None) -> str | None:
        normalized = (level or "INFO").upper()
        if (channel or "").lower() == "server":
            return self.server_logs or self.errors or self.main or self.vandalism
        if script_name and "vandal" in script_name.lower() and self.vandalism:
            return self.vandalism
        if normalized in {"ERROR", "CRITICAL"} and self.errors:
            return self.errors
        return self.main or self.errors or self.vandalism

    def _load_queue(self) -> list[dict[str, Any]]:
        try:
            if not self.queue_file.exists():
                return []
            payload = json.loads(self.queue_file.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                return [item for item in payload if isinstance(item, dict)]
        except Exception:
            LOGGER.warning("Unable to read Discord queue file: %s", self.queue_file)
        return []

    def _save_queue(self, queue: list[dict[str, Any]]) -> None:
        try:
            self.queue_file.parent.mkdir(parents=True, exist_ok=True)
            self.queue_file.write_text(
                json.dumps(queue[-self.max_queue_size :], ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            LOGGER.warning("Unable to save Discord queue file: %s", exc)

    def _enqueue(
        self,
        webhook: str,
        payload: dict[str, Any],
        level: str,
        script_name: str | None,
        error: str,
        channel: str | None = None,
    ) -> None:
        with self._lock:
            queue = self._load_queue()
            queue.append(
                {
                    "queued_at": _utc_now_iso(),
                    "webhook": webhook,
                    "payload": payload,
                    "level": level,
                    "script_name": script_name or "bot",
                    "channel": channel or "",
                    "error": error[:500],
                }
            )
            self._save_queue(queue)

    def _should_drop_duplicate(self, webhook: str, payload: dict[str, Any]) -> bool:
        if self.dedupe_window_seconds <= 0:
            return False
        now = time.time()
        fingerprint = _payload_fingerprint(webhook, payload)

        expired = [key for key, expires_at in self._dedupe_cache.items() if expires_at <= now]
        for key in expired:
            self._dedupe_cache.pop(key, None)

        if self._dedupe_cache.get(fingerprint, 0.0) > now:
            return True

        self._dedupe_cache[fingerprint] = now + self.dedupe_window_seconds
        return False

    def _extract_retry_after(self, response: requests.Response) -> float | None:
        try:
            payload = response.json()
        except Exception:
            payload = {}
        retry_after = payload.get("retry_after")
        try:
            return max(float(retry_after), 0.5)
        except (TypeError, ValueError):
            return None

    def _send_payload(self, webhook: str, payload: dict[str, Any]) -> tuple[bool, str]:
        last_error = "unknown_error"
        for attempt in range(self.max_retries + 1):
            try:
                response = requests.post(webhook, json=payload, timeout=self.request_timeout)
            except Exception as exc:
                last_error = f"request_error: {exc}"
                if attempt < self.max_retries:
                    time.sleep(min(1.5**attempt, 8.0))
                    continue
                return False, last_error

            if 200 <= response.status_code < 300:
                return True, ""

            body_preview = _truncate((response.text or "").replace("\n", " "), 300)
            last_error = f"http_{response.status_code}: {body_preview}"

            retryable = response.status_code in {429, 500, 502, 503, 504}
            if retryable and attempt < self.max_retries:
                retry_after = self._extract_retry_after(response)
                wait_for = retry_after if retry_after is not None else min(1.5**attempt, 8.0)
                time.sleep(wait_for)
                continue

            return False, last_error

        return False, last_error

    def _build_payloads(self, content: str | None, embed: dict[str, Any] | None) -> list[dict[str, Any]]:
        chunks = _chunk_content(content or "")
        normalized_embed = _normalize_embed(embed) if embed else None
        payloads: list[dict[str, Any]] = []

        if normalized_embed and not chunks:
            payloads.append({"embeds": [normalized_embed]})
            return payloads

        if chunks:
            first: dict[str, Any] = {"content": chunks[0]}
            if normalized_embed:
                first["embeds"] = [normalized_embed]
            payloads.append(first)
            for extra in chunks[1:]:
                payloads.append({"content": extra})
            return payloads

        return payloads

    def flush_queue(self, max_items: int = 30) -> int:
        sent = 0
        with self._lock:
            queue = self._load_queue()
            if not queue:
                return 0

            remaining: list[dict[str, Any]] = []
            for index, item in enumerate(queue):
                if index >= max_items:
                    remaining.extend(queue[index:])
                    break

                webhook = item.get("webhook")
                payload = item.get("payload")
                if not isinstance(webhook, str) or not isinstance(payload, dict):
                    continue

                ok, _error = self._send_payload(webhook, payload)
                if ok:
                    sent += 1
                else:
                    remaining.append(item)

            self._save_queue(remaining)
        if sent:
            LOGGER.info("Flushed %s queued Discord message(s)", sent)
        return sent

    def send(
        self,
        content: str | None = None,
        embed: dict[str, Any] | None = None,
        level: str = "INFO",
        script_name: str | None = None,
        channel: str | None = None,
    ) -> bool:
        webhook = self._pick_webhook(level=level, script_name=script_name, channel=channel)
        if not webhook:
            LOGGER.warning("Discord webhook missing for %s (%s, channel=%s)", script_name or "bot", level, channel or "default")
            return False

        payloads = self._build_payloads(content=content, embed=embed)
        if not payloads:
            return False

        self.flush_queue(max_items=10)

        success = True
        for payload in payloads:
            if self._should_drop_duplicate(webhook, payload):
                continue

            ok, error = self._send_payload(webhook, payload)
            if ok:
                continue

            success = False
            LOGGER.warning("Discord webhook failed for %s (%s): %s", script_name or "bot", level, error)
            self._enqueue(
                webhook=webhook,
                payload=payload,
                level=level,
                script_name=script_name,
                error=error,
                channel=channel,
            )
        return success


NOTIFIER = DiscordNotifier()


def log_to_discord(message: str, level: str = "INFO", script_name: str | None = None, **_: Any) -> None:
    script = script_name or "bot"
    normalized = (level or "INFO").upper()
    color_map = {
        "INFO": 3447003,
        "SUCCESS": 5763719,
        "WARNING": 15105570,
        "ERROR": 15158332,
        "CRITICAL": 15158332,
    }
    embed = {
        "title": f"{script} | {normalized}",
        "description": _truncate(message, MAX_EMBED_DESCRIPTION),
        "color": color_map.get(normalized, 3447003),
        "timestamp": _utc_now_iso(),
    }
    content = _prepend_critical_mention(None, normalized)
    NOTIFIER.send(content=content, embed=embed, level=normalized, script_name=script)


def send_task_report(
    script_name: str,
    status: str,
    duration_seconds: float | None = None,
    stats: dict[str, object] | None = None,
    details: str | None = None,
    level: str | None = None,
    channel: str | None = None,
) -> bool:
    normalized_status = (status or "INFO").upper()
    inferred_level = (level or ("ERROR" if normalized_status in {"ERROR", "FAILED"} else "INFO")).upper()
    event_timestamp = _utc_now_iso()
    normalized_stats = _normalize_stats(stats)
    _record_task_report_event(
        timestamp=event_timestamp,
        script_name=script_name,
        status=normalized_status,
        level=inferred_level,
        duration_seconds=duration_seconds,
        stats=normalized_stats,
        details=details,
    )
    color_map = {
        "SUCCESS": 5763719,
        "INFO": 3447003,
        "WARNING": 15105570,
        "ERROR": 15158332,
        "FAILED": 15158332,
    }

    fields: list[dict[str, Any]] = []
    if duration_seconds is not None:
        fields.append({"name": "Duree", "value": f"{duration_seconds:.1f}s", "inline": True})

    if normalized_stats:
        for key, value in list(normalized_stats.items())[:10]:
            fields.append({"name": _truncate(key, MAX_FIELD_NAME), "value": _truncate(value, MAX_FIELD_VALUE), "inline": True})

    embed = {
        "title": f"{script_name} | RUN {normalized_status}",
        "description": _truncate(details or "Rapport automatique", MAX_EMBED_DESCRIPTION),
        "color": color_map.get(normalized_status, 3447003),
        "fields": fields,
        "timestamp": event_timestamp,
    }
    content = _prepend_critical_mention(None, inferred_level)
    sent = NOTIFIER.send(content=content, embed=embed, level=inferred_level, script_name=script_name, channel=channel)

    if get_bool_env("DISCORD_SERVER_LOG_EVERY_REPORT", True):
        raw = {
            "timestamp": event_timestamp,
            "script_name": script_name,
            "status": normalized_status,
            "level": inferred_level,
            "duration_seconds": duration_seconds,
            "stats": normalized_stats,
            "details": _truncate(details, MAX_SERVER_CONTEXT_SIZE),
        }
        payload_text = _truncate(json.dumps(raw, ensure_ascii=False, indent=2), MAX_SERVER_CONTEXT_SIZE)
        NOTIFIER.send(
            content=f"```json\n{payload_text}\n```",
            level=inferred_level,
            script_name=script_name,
            channel="server",
        )

    return sent


def send_discord_webhook(
    content: str | None = None,
    embed: dict[str, Any] | None = None,
    level: str = "INFO",
    script_name: str | None = None,
    channel: str | None = None,
) -> bool:
    normalized = (level or "INFO").upper()
    final_content = _prepend_critical_mention(content, normalized)
    return NOTIFIER.send(content=final_content, embed=embed, level=normalized, script_name=script_name, channel=channel)


def log_server_diagnostic(
    message: str,
    level: str = "ERROR",
    script_name: str | None = None,
    context: dict[str, object] | None = None,
    exception: BaseException | None = None,
) -> None:
    normalized = (level or "ERROR").upper()
    include_secrets = get_bool_env("SERVER_LOG_INCLUDE_SECRETS", True)
    server_context: dict[str, object] = {
        "runtime": _runtime_snapshot(include_secrets=include_secrets),
        "context": _normalize_stats(context or {}),
    }
    if exception is not None:
        server_context["exception"] = _truncate(repr(exception), 1200)
    trace_text = traceback.format_exc()
    if trace_text and trace_text.strip() and trace_text.strip() != "NoneType: None":
        server_context["traceback"] = _truncate(trace_text, 3500)

    description = _truncate(message, 1000)
    raw_diagnostics = _truncate(json.dumps(server_context, ensure_ascii=False, indent=2), MAX_SERVER_CONTEXT_SIZE)
    diagnostic_content = f"```json\n{raw_diagnostics}\n```"

    embed = {
        "title": f"{script_name or 'server'} | {normalized} | SERVER LOG",
        "description": description,
        "color": 15158332 if normalized in {"ERROR", "CRITICAL", "FAILED"} else 15105570,
        "timestamp": _utc_now_iso(),
    }
    send_discord_webhook(
        content=diagnostic_content,
        embed=embed,
        level=normalized,
        script_name=script_name or "server",
        channel="server",
    )


def log_server_action(
    action: str,
    *,
    script_name: str,
    level: str = "INFO",
    context: dict[str, object] | None = None,
    include_runtime: bool = False,
) -> None:
    if not get_bool_env("SERVER_LOG_EVERY_ACTION", True):
        return

    normalized = (level or "INFO").upper()
    sequence = _next_server_action_sequence()
    payload: dict[str, object] = {
        "timestamp": _utc_now_iso(),
        "action": _truncate(action, 240),
        "script_name": script_name,
        "level": normalized,
        "session_id": _SERVER_ACTION_SESSION_ID,
        "sequence": sequence,
        "context": _normalize_action_context(context or {}),
    }
    if include_runtime:
        payload["runtime"] = _runtime_snapshot(include_secrets=get_bool_env("SERVER_LOG_INCLUDE_SECRETS", True))

    _record_server_action_event(payload)
    if not get_bool_env("SERVER_ACTION_LOG_TO_DISCORD", True):
        return

    text = _truncate(json.dumps(payload, ensure_ascii=False, indent=2), MAX_SERVER_CONTEXT_SIZE)
    send_discord_webhook(
        content=f"```json\n{text}\n```",
        level=normalized,
        script_name=script_name,
        channel="server",
    )


def flush_logs() -> None:
    NOTIFIER.flush_queue(max_items=200)
