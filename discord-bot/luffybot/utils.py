#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import contextlib
import datetime as dt
import os
from pathlib import Path


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_iso(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def fmt_duration(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    total = int(max(seconds, 0))
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def memory_stats_mb() -> tuple[int, int]:
    total_kib = 0
    available_kib = 0
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("MemTotal:"):
                    total_kib = int(line.split()[1])
                elif line.startswith("MemAvailable:"):
                    available_kib = int(line.split()[1])
    except Exception:
        return 0, 0
    used_kib = max(total_kib - available_kib, 0)
    return used_kib // 1024, total_kib // 1024


def memory_used_percent() -> float:
    used_mb, total_mb = memory_stats_mb()
    if total_mb <= 0:
        return 0.0
    return (used_mb / total_mb) * 100.0


def load_per_cpu() -> float:
    try:
        load1 = os.getloadavg()[0]
    except Exception:
        return 0.0
    cpu = os.cpu_count() or 1
    return load1 / max(cpu, 1)


def process_rss_mb(pid: int) -> int:
    status_path = Path(f"/proc/{pid}/status")
    if not status_path.exists():
        return 0
    try:
        for line in status_path.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("VmRSS:"):
                kib = int(line.split()[1])
                return kib // 1024
    except Exception:
        return 0
    return 0


def redact_sensitive(text: str) -> str:
    redacted = text
    redacted = redacted.replace("https://discord.com/api/webhooks/", "https://discord.com/api/webhooks/[REDACTED]/")
    redacted = redacted.replace("https://ptb.discord.com/api/webhooks/", "https://ptb.discord.com/api/webhooks/[REDACTED]/")
    redacted = redacted.replace("https://canary.discord.com/api/webhooks/", "https://canary.discord.com/api/webhooks/[REDACTED]/")
    for marker in (
        "TOKEN=",
        "DISCORD_TOKEN=",
        "MISTRAL_API_KEY=",
        "WIKIBOT_PASSWORD=",
    ):
        if marker in redacted:
            redacted = redacted.replace(marker, f"{marker}[REDACTED]")
    return redacted


def read_tail(path: Path, lines: int = 80, max_chars: int = 3500) -> str:
    if not path.exists():
        return "Log introuvable."
    try:
        content = path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        return f"Impossible de lire le log: {exc}"
    snippet = "\n".join(content.splitlines()[-max(lines, 1) :])
    snippet = redact_sensitive(snippet)
    return snippet[-max_chars:]


def parse_int_csv(raw: str) -> list[int]:
    values: list[int] = []
    for part in (raw or "").replace(";", ",").split(","):
        token = part.strip()
        if not token:
            continue
        with contextlib.suppress(ValueError):
            values.append(int(token))

    seen: set[int] = set()
    ordered: list[int] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered
