# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any

import requests

from .env import get_env, load_dotenv
from .paths import LOG_DIR

load_dotenv()

LOGGER = logging.getLogger(__name__)

MAX_CONTENT_LENGTH = 2000
MAX_EMBED_DESCRIPTION = 4096
MAX_EMBED_TITLE = 256
MAX_EMBED_FIELDS = 25
MAX_FIELD_NAME = 256
MAX_FIELD_VALUE = 1024


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
        self.request_timeout = max(_safe_int(get_env("DISCORD_TIMEOUT_SECONDS", "12"), 12), 3)
        self.max_retries = max(_safe_int(get_env("DISCORD_MAX_RETRIES", "3"), 3), 0)
        self.dedupe_window_seconds = max(_safe_int(get_env("DISCORD_DEDUP_WINDOW_SECONDS", "45"), 45), 0)
        self.max_queue_size = max(_safe_int(get_env("DISCORD_MAX_QUEUE_SIZE", "200"), 200), 10)
        self.queue_file = Path(get_env("DISCORD_QUEUE_FILE", str(LOG_DIR / "discord_queue.json")) or str(LOG_DIR / "discord_queue.json"))
        self._dedupe_cache: dict[str, float] = {}
        self._lock = Lock()

    def _pick_webhook(self, level: str, script_name: str | None = None) -> str | None:
        normalized = (level or "INFO").upper()
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

    def _enqueue(self, webhook: str, payload: dict[str, Any], level: str, script_name: str | None, error: str) -> None:
        with self._lock:
            queue = self._load_queue()
            queue.append(
                {
                    "queued_at": _utc_now_iso(),
                    "webhook": webhook,
                    "payload": payload,
                    "level": level,
                    "script_name": script_name or "bot",
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
    ) -> bool:
        webhook = self._pick_webhook(level=level, script_name=script_name)
        if not webhook:
            LOGGER.warning("Discord webhook missing for %s (%s)", script_name or "bot", level)
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
            self._enqueue(webhook=webhook, payload=payload, level=level, script_name=script_name, error=error)
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
    NOTIFIER.send(embed=embed, level=normalized, script_name=script)


def send_task_report(
    script_name: str,
    status: str,
    duration_seconds: float | None = None,
    stats: dict[str, object] | None = None,
    details: str | None = None,
    level: str | None = None,
) -> bool:
    normalized_status = (status or "INFO").upper()
    inferred_level = (level or ("ERROR" if normalized_status in {"ERROR", "FAILED"} else "INFO")).upper()
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

    if stats:
        for key, value in list(stats.items())[:10]:
            fields.append({"name": _truncate(key, MAX_FIELD_NAME), "value": _truncate(value, MAX_FIELD_VALUE), "inline": True})

    embed = {
        "title": f"{script_name} | RUN {normalized_status}",
        "description": _truncate(details or "Rapport automatique", MAX_EMBED_DESCRIPTION),
        "color": color_map.get(normalized_status, 3447003),
        "fields": fields,
        "timestamp": _utc_now_iso(),
    }
    return NOTIFIER.send(embed=embed, level=inferred_level, script_name=script_name)


def send_discord_webhook(
    content: str | None = None,
    embed: dict[str, Any] | None = None,
    level: str = "INFO",
    script_name: str | None = None,
) -> bool:
    return NOTIFIER.send(content=content, embed=embed, level=level, script_name=script_name)


def flush_logs() -> None:
    NOTIFIER.flush_queue(max_items=200)
