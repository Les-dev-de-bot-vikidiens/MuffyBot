# -*- coding: utf-8 -*-
from __future__ import annotations

import difflib
import json
import logging
import re
import sqlite3
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import pywikibot
import requests

from muffybot.discord import log_server_action, log_server_diagnostic, log_to_discord, send_discord_webhook, send_task_report
from muffybot.env import get_bool_env, get_env, get_float_env, get_int_env, load_dotenv
from muffybot.files import read_json, write_json
from muffybot.logging_setup import configure_root_logging
from muffybot.locking import LockUnavailableError, hold_lock
from muffybot.ml.predictor import load_predictor
from muffybot.paths import ENVIKIDIA_DIR, ROOT_DIR
from muffybot.task_control import dry_run_enabled, report_lock_unavailable, save_page_or_dry_run
from muffybot.tasks.vandalism_shared import normalize_detection_text
from muffybot.wiki import connect_site, prepare_runtime

LOGGER = logging.getLogger(__name__)
MISTRAL_API_URL = "https://api.mistral.ai/v1/chat/completions"
COMMON_PATTERNS_FILE = ROOT_DIR / "vandalism_common_patterns.txt"
DETECTION_REGEX_FILE = ROOT_DIR / "vandalism_detection_regex.txt"
FALSE_POSITIVE_WHITELIST_FILE = ROOT_DIR / "vandalism_false_positive_whitelist.json"
INTEL_DB_FILE = ROOT_DIR / "vandalism_intel.sqlite3"
SENSITIVE_TITLES_FILE = ROOT_DIR / "vandalism_sensitive_titles.txt"
URL_RE = re.compile(r"https?://[^\s<>\"']+|www\.[^\s<>\"']+", flags=re.IGNORECASE)
SHORTENER_DOMAINS = {
    "bit.ly",
    "tinyurl.com",
    "goo.gl",
    "t.co",
    "ow.ly",
    "is.gd",
    "cutt.ly",
    "rb.gy",
}
DEFAULT_SENSITIVE_TITLE_TOKENS = {
    "obama",
    "macron",
    "trump",
    "hitler",
    "allah",
    "israel",
    "palestine",
    "ukraine",
    "russie",
    "poutine",
    "gaza",
    "nazisme",
    "religion",
    "racisme",
    "sexe",
    "porn",
    "sexualite",
}


@dataclass(frozen=True)
class VandalismConfig:
    script_name: str
    lang: str
    workdir: Path
    wiki_base_url: str
    log_page: str
    max_changes: int = 100
    instant_revert_threshold: float = 0.97
    ai_revert_threshold: float = 0.93
    review_threshold: float = 0.75
    mistral_model: str = "mistral-large-latest"

    @property
    def processed_file(self) -> Path:
        return self.workdir / "processed_pages.json"

    @property
    def metrics_file(self) -> Path:
        return self.workdir / "metrics.json"

    @property
    def db_file(self) -> Path:
        return self.workdir / "vandalism_db.json"

    @property
    def checkpoint_file(self) -> Path:
        return self.workdir / "vandalism_checkpoint.json"

    @property
    def quarantine_file(self) -> Path:
        return self.workdir / "vandalism_quarantine.json"


@dataclass(frozen=True)
class DynamicRule:
    pattern: re.Pattern[str]
    weight: float
    label: str
    status: str = "active"
    support: int = 0
    precision: float = 0.0


@dataclass
class HealthState:
    script_name: str
    lang: str
    started_monotonic: float = field(default_factory=time.monotonic)
    run_started_monotonic: float = field(default_factory=time.monotonic)
    running: bool = True
    last_status: str = "running"
    last_error: str = ""
    last_changes_prefetched: int = 0
    state_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def set_prefetched(self, total: int) -> None:
        with self.state_lock:
            self.last_changes_prefetched = max(0, int(total))

    def finish(self, *, status: str, error: str = "") -> None:
        with self.state_lock:
            self.running = False
            self.last_status = status
            self.last_error = error[:500]

    def snapshot(self) -> dict[str, object]:
        with self.state_lock:
            return {
                "status": self.last_status,
                "running": self.running,
                "script_name": self.script_name,
                "lang": self.lang,
                "last_error": self.last_error,
                "last_changes_prefetched": self.last_changes_prefetched,
                "uptime_seconds": round(time.monotonic() - self.started_monotonic, 3),
                "run_elapsed_seconds": round(time.monotonic() - self.run_started_monotonic, 3),
            }


class _HealthHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, host: str, port: int, state: HealthState) -> None:
        super().__init__((host, port), _HealthHandler)
        self.state = state


class _HealthHandler(BaseHTTPRequestHandler):
    server: _HealthHTTPServer

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path not in {"/healthz", "/health"}:
            self.send_error(404, "Not found")
            return

        payload = self.server.state.snapshot()
        code = 200 if payload.get("status") in {"running", "success"} else 503
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt: str, *args: object) -> None:
        return


@dataclass
class HealthServer:
    server: _HealthHTTPServer
    thread: threading.Thread

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2.0)


def _health_enabled(config: VandalismConfig) -> bool:
    raw = (get_env("PYWIKIBOT_HEALTH_ENABLE") or "").strip().lower()
    if raw:
        return raw in {"1", "true", "yes", "on"}
    return config.lang == "fr"


def _start_health_server(config: VandalismConfig, state: HealthState) -> HealthServer | None:
    if not _health_enabled(config):
        return None
    host = (get_env("PYWIKIBOT_HEALTH_HOST", "127.0.0.1") or "127.0.0.1").strip() or "127.0.0.1"
    port = max(1, min(get_int_env("PYWIKIBOT_HEALTH_PORT", 8798), 65535))
    server = _HealthHTTPServer(host, port, state)
    thread = threading.Thread(target=server.serve_forever, name=f"vandalism-health-{config.lang}", daemon=True)
    thread.start()
    LOGGER.info("Healthcheck Pywikibot actif: http://%s:%s/healthz", host, port)
    return HealthServer(server=server, thread=thread)


FR_CONFIG = VandalismConfig(
    script_name="vandalism.py",
    lang="fr",
    workdir=ROOT_DIR,
    wiki_base_url="https://fr.vikidia.org/wiki/",
    log_page="Utilisateur:MuffyBot/Journaux",
)

EN_CONFIG = VandalismConfig(
    script_name="envikidia/vandalism.py",
    lang="en",
    workdir=ENVIKIDIA_DIR,
    wiki_base_url="https://en.vikidia.org/wiki/",
    log_page="User:MuffyBot/Logs",
    instant_revert_threshold=0.98,
    ai_revert_threshold=0.95,
    review_threshold=0.8,
)


def _normalize_username(name: str) -> str:
    return (name or "").strip().replace("_", " ").casefold()


def _parse_revid(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _extract_change_revision_ids(change: dict[object, object]) -> tuple[int | None, int | None]:
    new_revid = _parse_revid(change.get("revid") or change.get("new_revid"))
    old_revid = _parse_revid(change.get("old_revid"))
    return new_revid, old_revid


def _parse_change_timestamp(change: dict[object, object]) -> datetime | None:
    raw = change.get("timestamp")
    if raw is None:
        return None
    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            return raw.replace(tzinfo=timezone.utc)
        return raw.astimezone(timezone.utc)
    text = str(raw).strip()
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


def _get_revision_text(page: pywikibot.Page, revid: int | None) -> str | None:
    if not revid:
        return None
    try:
        return page.getOldVersion(revid)
    except Exception:
        return None


def _extract_changed_text(old_text: str | None, new_text: str) -> tuple[str, str]:
    if old_text is None:
        return new_text.strip(), ""

    diff = difflib.unified_diff(old_text.splitlines(), new_text.splitlines(), lineterm="")
    additions: list[str] = []
    deletions: list[str] = []

    for line in diff:
        if line.startswith("+++") or line.startswith("---"):
            continue
        if line.startswith("+"):
            additions.append(line[1:])
        elif line.startswith("-"):
            deletions.append(line[1:])

    return "\n".join(additions).strip(), "\n".join(deletions).strip()


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return default


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default


def _load_disabled_rule_labels(path: Path = FALSE_POSITIVE_WHITELIST_FILE) -> set[str]:
    payload = read_json(path, default={})
    if isinstance(payload, dict):
        raw_labels = payload.get("disabled_rule_labels", [])
    elif isinstance(payload, list):
        raw_labels = payload
    else:
        raw_labels = []
    labels = {str(item).strip() for item in raw_labels if str(item).strip()}
    return labels


def _load_sensitive_title_tokens() -> set[str]:
    tokens = {token.strip().casefold() for token in DEFAULT_SENSITIVE_TITLE_TOKENS}
    from_env = get_env("VANDALISM_SENSITIVE_TITLE_KEYWORDS", "")
    for token in str(from_env).split(","):
        clean = token.strip().casefold()
        if clean:
            tokens.add(clean)
    if SENSITIVE_TITLES_FILE.exists():
        try:
            for raw_line in SENSITIVE_TITLES_FILE.read_text(encoding="utf-8").splitlines():
                line = raw_line.strip().casefold()
                if not line or line.startswith("#"):
                    continue
                tokens.add(line)
        except Exception:
            pass
    return tokens


def _is_sensitive_title(title: str, sensitive_tokens: set[str]) -> bool:
    norm_title = normalize_detection_text(title.replace("_", " "))
    return any(token in norm_title for token in sensitive_tokens)


def _count_link_signals(text: str) -> tuple[int, int]:
    if not text:
        return 0, 0
    links = [match.group(0) for match in URL_RE.finditer(text)]
    shorteners = 0
    for link in links:
        parsed = urlparse(link if link.startswith("http") else f"http://{link}")
        host = (parsed.netloc or "").casefold().lstrip("www.")
        if host in SHORTENER_DOMAINS:
            shorteners += 1
    return len(links), shorteners


def _symbol_ratio(text: str) -> float:
    if not text:
        return 0.0
    considered = [ch for ch in text if not ch.isspace()]
    if not considered:
        return 0.0
    symbols = sum(1 for ch in considered if not ch.isalnum())
    return symbols / max(len(considered), 1)


def _uppercase_ratio(text: str) -> float:
    letters = [ch for ch in text if ch.isalpha()]
    if not letters:
        return 0.0
    upper = sum(1 for ch in letters if ch.isupper())
    return upper / max(len(letters), 1)


def _compute_user_burst_map(
    changes: list[dict[object, object]],
    *,
    window_minutes: int,
) -> dict[str, int]:
    per_user_windows: dict[str, deque[datetime]] = defaultdict(deque)
    burst_map: dict[str, int] = {}
    timeline: list[tuple[datetime, str, str]] = []
    window_seconds = max(window_minutes, 1) * 60

    for change in changes:
        change_id = str(change.get("rcid") or change.get("revid") or "")
        user = str(change.get("user") or "")
        ts = _parse_change_timestamp(change)
        if not change_id or not user or ts is None:
            continue
        timeline.append((ts, _normalize_username(user), change_id))

    timeline.sort(key=lambda item: item[0])
    for ts, user_key, change_id in timeline:
        q = per_user_windows[user_key]
        while q and (ts - q[0]).total_seconds() > window_seconds:
            q.popleft()
        q.append(ts)
        burst_map[change_id] = len(q)
    return burst_map


def _open_intel_db(path: Path = INTEL_DB_FILE) -> sqlite3.Connection | None:
    try:
        conn = sqlite3.connect(path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS change_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_utc TEXT NOT NULL,
                lang TEXT NOT NULL,
                change_id TEXT NOT NULL,
                title TEXT NOT NULL,
                creator TEXT NOT NULL,
                action TEXT NOT NULL,
                score REAL NOT NULL,
                reason TEXT NOT NULL,
                matched_patterns TEXT NOT NULL,
                dynamic_rule_labels TEXT NOT NULL,
                url_count INTEGER NOT NULL,
                shortener_count INTEGER NOT NULL,
                symbol_ratio REAL NOT NULL,
                uppercase_ratio REAL NOT NULL,
                burst_count INTEGER NOT NULL,
                title_sensitive INTEGER NOT NULL,
                added_len INTEGER NOT NULL,
                removed_len INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS rule_hits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_utc TEXT NOT NULL,
                lang TEXT NOT NULL,
                change_id TEXT NOT NULL,
                title TEXT NOT NULL,
                creator TEXT NOT NULL,
                rule_label TEXT NOT NULL,
                rule_status TEXT NOT NULL,
                rule_weight REAL NOT NULL,
                action TEXT NOT NULL,
                score REAL NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_change_events_ts ON change_events(ts_utc)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_change_events_lang ON change_events(lang)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_rule_hits_ts ON rule_hits(ts_utc)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_rule_hits_label ON rule_hits(rule_label)")
        return conn
    except Exception:
        return None


def _record_intel_change_event(
    conn: sqlite3.Connection | None,
    *,
    config: VandalismConfig,
    change_id: str,
    title: str,
    creator: str,
    action: str,
    score: float,
    reason: str,
    matched_patterns: list[str],
    dynamic_rule_labels: list[str],
    feature_stats: dict[str, float | int],
    added_len: int,
    removed_len: int,
) -> None:
    if conn is None:
        return
    try:
        conn.execute(
            """
            INSERT INTO change_events (
                ts_utc, lang, change_id, title, creator, action, score, reason,
                matched_patterns, dynamic_rule_labels, url_count, shortener_count,
                symbol_ratio, uppercase_ratio, burst_count, title_sensitive, added_len, removed_len
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.utcnow().isoformat() + "Z",
                config.lang,
                change_id,
                title,
                creator,
                action,
                float(score),
                reason[:260],
                json.dumps(sorted(set(matched_patterns)), ensure_ascii=False),
                json.dumps(sorted(set(dynamic_rule_labels)), ensure_ascii=False),
                int(feature_stats.get("url_count", 0)),
                int(feature_stats.get("shortener_count", 0)),
                float(feature_stats.get("symbol_ratio", 0.0)),
                float(feature_stats.get("uppercase_ratio", 0.0)),
                int(feature_stats.get("burst_count", 1)),
                int(feature_stats.get("title_sensitive", 0)),
                int(added_len),
                int(removed_len),
            ),
        )
        conn.commit()
    except Exception:
        return


def _record_intel_rule_hits(
    conn: sqlite3.Connection | None,
    *,
    config: VandalismConfig,
    change_id: str,
    title: str,
    creator: str,
    action: str,
    score: float,
    rules: list[DynamicRule],
) -> None:
    if conn is None or not rules:
        return
    now = datetime.utcnow().isoformat() + "Z"
    try:
        conn.executemany(
            """
            INSERT INTO rule_hits (
                ts_utc, lang, change_id, title, creator, rule_label, rule_status, rule_weight, action, score
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    now,
                    config.lang,
                    change_id,
                    title,
                    creator,
                    rule.label,
                    rule.status,
                    float(rule.weight),
                    action,
                    float(score),
                )
                for rule in rules
            ],
        )
        conn.commit()
    except Exception:
        return


def _looks_constructive(added_text: str, summary: str) -> bool:
    if not added_text:
        return False

    summary_lower = (summary or "").lower()
    constructive_markers = (
        "orthographe",
        "typo",
        "wikif",
        "référence",
        "source",
        "catégorie",
        "correction",
        "grammar",
        "spelling",
        "format",
        "source",
    )
    if any(marker in summary_lower for marker in constructive_markers):
        return True

    letter_count = len(re.findall(r"[A-Za-zÀ-ÖØ-öø-ÿ]", added_text))
    structural_markup = any(token in added_text for token in ("[[", "]]", "{{", "}}", "==", "<ref", "</ref>"))
    return letter_count >= 60 and structural_markup


def _calculate_vandalism_score(
    added_text: str,
    new_text: str,
    old_text: str | None,
    dynamic_rules: list[DynamicRule] | None = None,
) -> tuple[float, list[str], list[DynamicRule], dict[str, float | int]]:
    focus_raw = added_text if added_text else new_text
    focus = normalize_detection_text(focus_raw)

    if not new_text.strip():
        return 0.88, ["blanking"], [], {"url_count": 0, "shortener_count": 0, "symbol_ratio": 0.0, "uppercase_ratio": 0.0}

    patterns = [
        (r"(.)\1{14,}", 0.98, "char_repetition"),
        (r"^\s*(test|asdf|qwerty|azerty|lol|mdr|bonjour)\s*$", 0.93, "test_word"),
        (r"(fuck|shit|bitch|merde|putain|connard|salope|encul[ée]|nique\s*ta\s*m[eè]re)", 0.98, "insult"),
        (r"(viagra|casino|pariez|bit\.ly|tinyurl)", 0.94, "spam"),
        (r"^[^a-zà-öø-ÿ]{12,}$", 0.9, "symbol_spam"),
        (r"([!?*._#@])\1{8,}", 0.95, "symbol_flood"),
        (r"\b(?:lol|mdr|ptdr|xd){5,}\b", 0.89, "token_flood"),
    ]

    score = 0.0
    matches: list[str] = []
    hit_rules: list[DynamicRule] = []

    for pattern, weight, label in patterns:
        if re.search(pattern, focus):
            score = max(score, weight)
            matches.append(label)

    url_count, shortener_count = _count_link_signals(focus_raw)
    if shortener_count > 0:
        score = max(score, 0.95)
        matches.append("shortener_link")
    elif url_count >= 3:
        score = max(score, 0.91)
        matches.append("link_flood")

    symbol_ratio = _symbol_ratio(focus_raw)
    if len(focus_raw) >= 25 and symbol_ratio >= 0.6:
        score = max(score, 0.9)
        matches.append("high_symbol_ratio")

    uppercase_ratio = _uppercase_ratio(focus_raw)
    if len(focus_raw) >= 35 and uppercase_ratio >= 0.75:
        score = max(score, 0.83)
        matches.append("high_uppercase_ratio")

    if old_text and len(old_text) > 200:
        ratio = len(new_text) / max(len(old_text), 1)
        if ratio < 0.2:
            score = max(score, 0.9)
            matches.append("massive_deletion")
        elif ratio < 0.35:
            score = max(score, 0.75)
            matches.append("large_deletion")

    if dynamic_rules:
        score, matches, hit_rules = _apply_dynamic_rules(focus, score, matches, dynamic_rules)

    feature_stats: dict[str, float | int] = {
        "url_count": url_count,
        "shortener_count": shortener_count,
        "symbol_ratio": round(symbol_ratio, 4),
        "uppercase_ratio": round(uppercase_ratio, 4),
    }
    return score, matches, hit_rules, feature_stats


def _load_dynamic_regex_rules(
    path: Path = DETECTION_REGEX_FILE,
    *,
    disabled_labels: set[str] | None = None,
) -> list[DynamicRule]:
    if not path.exists():
        return []

    rules: list[DynamicRule] = []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return rules

    review_factor = max(min(get_float_env("VANDALISM_REVIEW_RULE_WEIGHT_FACTOR", 0.65), 1.0), 0.1)
    blocked = disabled_labels or set()

    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        parts = [part.strip() for part in line.split("\t")]
        if not parts:
            continue

        pattern = parts[0]
        if not pattern or len(pattern) > 500:
            continue

        weight = 0.84
        if len(parts) >= 2:
            try:
                weight = float(parts[1])
            except (TypeError, ValueError):
                weight = 0.84
        weight = max(min(weight, 0.999), 0.50)

        label = parts[2] if len(parts) >= 3 and parts[2] else "dynamic_regex"
        if label in blocked:
            continue
        support = _safe_int(parts[3], 0) if len(parts) >= 4 else 0
        precision = _safe_float(parts[5], 0.0) if len(parts) >= 6 else 0.0
        status = (parts[6].strip().casefold() if len(parts) >= 7 else "active")
        if status not in {"active", "review"}:
            continue
        if status == "review":
            weight = max(0.5, min(weight * review_factor, 0.95))
        try:
            compiled = re.compile(pattern, flags=re.IGNORECASE)
        except re.error:
            continue
        rules.append(
            DynamicRule(
                pattern=compiled,
                weight=weight,
                label=label[:80],
                status=status,
                support=support,
                precision=precision,
            )
        )
        if len(rules) >= 300:
            break

    return rules


def _apply_dynamic_rules(
    focus: str,
    current_score: float,
    matches: list[str],
    dynamic_rules: list[DynamicRule],
) -> tuple[float, list[str], list[DynamicRule]]:
    score = current_score
    hit_rules: list[DynamicRule] = []
    for rule in dynamic_rules:
        try:
            if rule.pattern.search(focus):
                score = max(score, rule.weight)
                prefix = "dyn" if rule.status == "active" else "dyn_review"
                matches.append(f"{prefix}:{rule.label}")
                hit_rules.append(rule)
        except Exception:
            continue
    return score, matches, hit_rules


def _analyze_diff_for_ai(old_text: str | None, new_text: str) -> str:
    if old_text is None:
        return f"Added:\n{new_text[:700]}\n\nRemoved:\n"

    added, removed = _extract_changed_text(old_text, new_text)
    return f"Added:\n{added[:700]}\n\nRemoved:\n{removed[:700]}"


def _call_mistral(
    config: VandalismConfig,
    page_title: str,
    new_text: str,
    old_text: str | None,
    edit_summary: str,
) -> tuple[float, str, str]:
    api_key = get_env("MISTRAL_API_KEY")
    if not api_key:
        return 0.0, "MISTRAL_API_KEY absent", "NO_AI"

    diff_excerpt = _analyze_diff_for_ai(old_text, new_text)
    prompt = (
        "You classify a single wiki edit for vandalism.\n"
        "Return exactly:\n"
        "VANDALISM_CONFIDENCE: <0.0-1.0>\n"
        "REASON: <short reason>\n"
        "CATEGORY: <EVIDENT|PROBABLE|DOUBTFUL|LEGIT>\n\n"
        "Rules:\n"
        "- Favor LEGIT when uncertain.\n"
        "- Do not mark educational biology words as insults.\n"
        "- Distinguish vandalism from formatting fixes.\n\n"
        f"Title: {page_title}\n"
        f"Edit summary: {edit_summary[:500]}\n"
        f"Diff:\n{diff_excerpt}\n"
        f"Current content excerpt:\n{new_text[:1200]}\n"
    )

    payload = {
        "model": config.mistral_model,
        "messages": [
            {"role": "system", "content": "You are a conservative anti-vandalism assistant."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,
        "max_tokens": 220,
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    try:
        response = requests.post(MISTRAL_API_URL, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]

        confidence = 0.0
        reason = "No reason"
        category = "UNKNOWN"

        for line in content.splitlines():
            line = line.strip()
            if line.startswith("VANDALISM_CONFIDENCE:"):
                try:
                    confidence = float(line.split(":", 1)[1].strip())
                except ValueError:
                    confidence = 0.0
            elif line.startswith("REASON:"):
                reason = line.split(":", 1)[1].strip() or reason
            elif line.startswith("CATEGORY:"):
                category = line.split(":", 1)[1].strip() or category

        return confidence, reason, category
    except Exception as exc:
        return 0.0, f"Erreur IA: {exc}", "ERROR"


def _is_privileged_user(site: pywikibot.Site, username: str, cache: dict[str, bool]) -> bool:
    key = _normalize_username(username)
    if key in cache:
        return cache[key]

    try:
        user = pywikibot.User(site, username)
        groups = {group.casefold() for group in user.groups()}
    except Exception:
        # En cas de doute sur les groupes utilisateur, on n'annule pas.
        cache[key] = True
        return True

    privileged = {"autopatrol", "sysop", "bot", "bureaucrat", "oversight", "interface-admin"}
    is_privileged = bool(groups & privileged)
    cache[key] = is_privileged
    return is_privileged


def _revert_target_revision(
    page: pywikibot.Page,
    target_revid: int,
    old_revid: int | None,
    expected_user: str,
    reason: str,
    confidence: float | None = None,
) -> tuple[bool, str]:
    try:
        latest_revision = next(page.revisions(total=1))
    except Exception as exc:
        return False, f"latest_revision_error: {exc}"

    latest_revid = _parse_revid(getattr(latest_revision, "revid", None))
    latest_user = getattr(latest_revision, "user", "")

    if latest_revid != target_revid:
        return False, "skip_not_latest_revision"

    if _normalize_username(latest_user) != _normalize_username(expected_user):
        return False, "skip_latest_user_mismatch"

    previous_text = _get_revision_text(page, old_revid)
    if previous_text is None:
        try:
            revisions = list(page.revisions(total=2))
            if len(revisions) < 2:
                return False, "skip_no_previous_revision"
            previous_text = page.getOldVersion(revisions[1].revid)
        except Exception as exc:
            return False, f"previous_revision_error: {exc}"

    page.text = previous_text
    summary = "Bot: annulation de modification non constructive"
    if reason:
        summary += f" - {reason[:120]}"
    if confidence is not None:
        summary += f" ({confidence * 100:.1f}%)"

    try:
        saved = save_page_or_dry_run(
            page,
            script_name="vandalism.py",
            summary=summary,
            minor=False,
            botflag=False,
            context={"title": page.title(), "target_revid": target_revid},
        )
        if saved:
            return True, "reverted"
        return True, "reverted_dry_run"
    except Exception as exc:
        return False, f"save_error: {exc}"


def _append_wiki_log(
    site: pywikibot.Site,
    config: VandalismConfig,
    page_title: str,
    creator: str,
    reason: str,
    confidence: float,
    *,
    change_id: str,
    revid: int | None,
    old_revid: int | None,
    comment: str,
) -> None:
    log_page = pywikibot.Page(site, config.log_page)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    user_ns = "Utilisateur" if config.lang == "fr" else "User"
    header = "== Journaux de reversion ==" if config.lang == "fr" else "== Reversion Logs =="
    safe_comment = (comment or "").replace("\n", " ")[:160]
    line = (
        f"* [{now}] [[{page_title}]] by [[{user_ns}:{creator}|{creator}]] "
        f"| rcid={change_id} | revid={revid or 'n/a'} | oldrevid={old_revid or 'n/a'} "
        f"| confidence={confidence * 100:.1f}% | reason={reason[:220]} | summary={safe_comment}\n"
    )

    text = log_page.text if log_page.exists() else ""
    if not text:
        text = f"{header}\n"
    log_page.text = line + text
    save_page_or_dry_run(
        log_page,
        script_name=config.script_name,
        summary="Bot: ajout journal anti-vandalisme",
        minor=True,
        botflag=True,
        context={"log_page": config.log_page, "change_id": change_id},
    )


def _default_metrics() -> dict[str, object]:
    return {
        "total_analyzed": 0,
        "reverted": 0,
        "skipped": 0,
        "errors": 0,
        "confidences": [],
    }


def _update_metrics(metrics: dict[str, object], action: str, confidence: float | None = None) -> None:
    metrics["total_analyzed"] = int(metrics.get("total_analyzed", 0)) + 1
    if action == "reverted":
        metrics["reverted"] = int(metrics.get("reverted", 0)) + 1
        if confidence is not None:
            confidences = list(metrics.get("confidences", []))
            confidences.append(confidence)
            metrics["confidences"] = confidences[-500:]
    elif action == "error":
        metrics["errors"] = int(metrics.get("errors", 0)) + 1
    else:
        metrics["skipped"] = int(metrics.get("skipped", 0)) + 1


def _should_skip_change(
    site: pywikibot.Site,
    creator: str,
    change: dict[object, object],
    group_cache: dict[str, bool],
) -> tuple[bool, str]:
    site_user = site.user() or ""
    if _normalize_username(creator) == _normalize_username(site_user):
        return True, "self_edit"

    if bool(change.get("bot")):
        return True, "bot_flag"

    if creator.strip().lower().endswith("bot"):
        return True, "bot_like_username"

    if _is_privileged_user(site, creator, group_cache):
        return True, "privileged_user"

    return False, ""


def run(config: VandalismConfig) -> int:
    started = time.monotonic()
    health_state = HealthState(script_name=config.script_name, lang=config.lang)
    load_dotenv()
    configure_root_logging(logger_name=config.script_name)
    health_server = _start_health_server(config, health_state)
    lock_name = f"vandalism-{config.lang}"
    try:
        with hold_lock(lock_name):
            prepare_runtime(config.workdir)
            site = connect_site(lang=config.lang, family="vikidia")

            processed_data = read_json(config.processed_file, default={"ids": []})
            if not isinstance(processed_data, dict):
                processed_data = {"ids": []}

            processed_ids = {str(item) for item in processed_data.get("ids", [])}
            processed_ids.update(str(item) for item in processed_data.get("pages", []))

            metrics = read_json(config.metrics_file, default=_default_metrics())
            if not isinstance(metrics, dict):
                metrics = _default_metrics()

            db = read_json(config.db_file, default={})
            if not isinstance(db, dict):
                db = {}

            checkpoint = read_json(config.checkpoint_file, default={})
            if not isinstance(checkpoint, dict):
                checkpoint = {}
            checkpoint_last_ts = _parse_change_timestamp({"timestamp": checkpoint.get("last_timestamp")})
            checkpoint_last_revid = _parse_revid(checkpoint.get("last_revid")) or 0

            quarantine = read_json(config.quarantine_file, default=[])
            if not isinstance(quarantine, list):
                quarantine = []

            group_cache: dict[str, bool] = {}
            reverted_this_run = 0
            dry_run_revert_candidates = 0
            quarantined_this_run = 0
            skipped_checkpoint = 0
            burst_window_minutes = max(get_int_env("VANDALISM_BURST_WINDOW_MINUTES", 12), 1)
            burst_threshold = max(get_int_env("VANDALISM_BURST_THRESHOLD", 3), 2)
            burst_score_boost = max(min(get_float_env("VANDALISM_BURST_SCORE_BOOST", 0.08), 0.3), 0.01)
            sensitive_title_boost = max(min(get_float_env("VANDALISM_SENSITIVE_TITLE_BOOST", 0.08), 0.3), 0.0)
            ml_enabled = get_bool_env("ML_ENABLE", True)
            ml_assist_weight = max(min(get_float_env("ML_ASSIST_WEIGHT", 0.25), 1.0), 0.0)
            ml_predictor = load_predictor() if ml_enabled else None
            disabled_rule_labels = _load_disabled_rule_labels()
            sensitive_title_tokens = _load_sensitive_title_tokens()
            dynamic_rules = _load_dynamic_regex_rules(disabled_labels=disabled_rule_labels)
            active_dynamic_rules = sum(1 for rule in dynamic_rules if rule.status == "active")
            review_dynamic_rules = len(dynamic_rules) - active_dynamic_rules
            intel_conn = _open_intel_db()
            changes = list(site.recentchanges(total=config.max_changes, changetype="edit"))
            health_state.set_prefetched(len(changes))
            user_burst_map = _compute_user_burst_map(changes, window_minutes=burst_window_minutes)

            max_seen_ts = checkpoint_last_ts
            max_seen_revid = checkpoint_last_revid

            log_to_discord("Démarrage du scan anti-vandalisme", level="INFO", script_name=config.script_name)
            log_server_action(
                "run_start",
                script_name=config.script_name,
                include_runtime=True,
                context={
                    "lang": config.lang,
                    "max_changes": config.max_changes,
                    "processed_ids_size": len(processed_ids),
                    "dry_run": int(dry_run_enabled()),
                    "checkpoint_last_ts": checkpoint.get("last_timestamp", ""),
                    "checkpoint_last_revid": checkpoint_last_revid,
                    "dynamic_regex_rules": len(dynamic_rules),
                    "dynamic_rules_active": active_dynamic_rules,
                    "dynamic_rules_review": review_dynamic_rules,
                    "dynamic_regex_path": str(DETECTION_REGEX_FILE),
                    "disabled_rule_labels": len(disabled_rule_labels),
                    "sensitive_title_tokens": len(sensitive_title_tokens),
                    "burst_window_minutes": burst_window_minutes,
                    "burst_threshold": burst_threshold,
                    "changes_prefetched": len(changes),
                },
            )

            for change in changes:
                title = str(change.get("title") or "")
                creator = str(change.get("user") or "")
                comment = str(change.get("comment") or "")
                change_id = str(change.get("rcid") or change.get("revid") or f"{title}:{change.get('timestamp')}")

                target_revid, old_revid = _extract_change_revision_ids(change)
                change_ts = _parse_change_timestamp(change)

                if change_ts is not None:
                    if (
                        checkpoint_last_ts is not None
                        and change_ts < checkpoint_last_ts
                    ) or (
                        checkpoint_last_ts is not None
                        and change_ts == checkpoint_last_ts
                        and target_revid is not None
                        and target_revid <= checkpoint_last_revid
                    ):
                        skipped_checkpoint += 1
                        log_server_action(
                            "skip_checkpoint_old",
                            script_name=config.script_name,
                            context={"change_id": change_id, "title": title, "revid": target_revid, "timestamp": str(change_ts)},
                        )
                        continue

                    if max_seen_ts is None or change_ts > max_seen_ts:
                        max_seen_ts = change_ts
                        max_seen_revid = target_revid or max_seen_revid
                    elif change_ts == max_seen_ts and (target_revid or 0) > max_seen_revid:
                        max_seen_revid = target_revid or max_seen_revid

                if not title:
                    log_server_action("skip_missing_title", script_name=config.script_name, context={"change_id": change_id})
                    continue
                if not creator:
                    log_server_action("skip_missing_creator", script_name=config.script_name, context={"change_id": change_id, "title": title})
                    continue
                if change_id in processed_ids:
                    log_server_action("skip_already_processed", script_name=config.script_name, context={"change_id": change_id, "title": title, "creator": creator})
                    continue

                action = "skipped"
                confidence: float | None = None
                confidence_ml: float | None = None
                ml_label = ""
                ml_top_features: list[str] = []
                ml_model_version = ""
                ml_assist_applied = 0
                reason = "No pattern"
                score = 0.0
                matched_patterns: list[str] = []
                dynamic_hit_rules: list[DynamicRule] = []
                feature_stats: dict[str, float | int] = {
                    "url_count": 0,
                    "shortener_count": 0,
                    "symbol_ratio": 0.0,
                    "uppercase_ratio": 0.0,
                    "burst_count": 1,
                    "title_sensitive": 0,
                }
                added_text = ""
                removed_text = ""

                try:
                    log_server_action(
                        "inspect_change",
                        script_name=config.script_name,
                        context={"change_id": change_id, "title": title, "creator": creator, "comment": comment[:180]},
                    )
                    page = pywikibot.Page(site, title)

                    if page.namespace() == 2 or title.endswith((".js", ".css")):
                        _update_metrics(metrics, "skipped")
                        processed_ids.add(change_id)
                        log_server_action(
                            "skip_user_or_code_namespace",
                            script_name=config.script_name,
                            context={"change_id": change_id, "title": title, "namespace": page.namespace()},
                        )
                        continue

                    skip, skip_reason = _should_skip_change(site, creator, change, group_cache)
                    if skip:
                        LOGGER.debug("Skip %s (%s)", title, skip_reason)
                        _update_metrics(metrics, "skipped")
                        processed_ids.add(change_id)
                        log_server_action(
                            "skip_change",
                            script_name=config.script_name,
                            context={"change_id": change_id, "title": title, "creator": creator, "reason": skip_reason},
                        )
                        continue

                    if target_revid is None:
                        LOGGER.debug("Skip %s: no target revision id", title)
                        _update_metrics(metrics, "skipped")
                        processed_ids.add(change_id)
                        log_server_action("skip_no_target_revid", script_name=config.script_name, context={"change_id": change_id, "title": title})
                        continue

                    new_text = _get_revision_text(page, target_revid)
                    if new_text is None:
                        LOGGER.debug("Skip %s: target revision content unavailable", title)
                        _update_metrics(metrics, "skipped")
                        processed_ids.add(change_id)
                        log_server_action("skip_unavailable_target_text", script_name=config.script_name, context={"change_id": change_id, "title": title, "revid": target_revid})
                        continue

                    old_text = _get_revision_text(page, old_revid)
                    added_text, removed_text = _extract_changed_text(old_text, new_text)
                    dynamic_hit_rules: list[DynamicRule] = []
                    score, matched_patterns, dynamic_hit_rules, feature_stats = _calculate_vandalism_score(
                        added_text,
                        new_text,
                        old_text,
                        dynamic_rules=dynamic_rules,
                    )

                    burst_count = user_burst_map.get(change_id, 1)
                    if burst_count >= burst_threshold:
                        burst_bonus = min((burst_count - burst_threshold + 1) * burst_score_boost, 0.3)
                        score = min(0.999, max(score, score + burst_bonus))
                        matched_patterns.append(f"user_burst_{burst_count}")
                    feature_stats["burst_count"] = burst_count

                    title_sensitive = 1 if _is_sensitive_title(title, sensitive_title_tokens) else 0
                    feature_stats["title_sensitive"] = title_sensitive
                    if title_sensitive:
                        has_risky_signal = (
                            bool(feature_stats.get("url_count", 0))
                            or bool(feature_stats.get("shortener_count", 0))
                            or float(feature_stats.get("symbol_ratio", 0.0)) > 0.35
                            or "massive_deletion" in matched_patterns
                        )
                        if has_risky_signal:
                            score = min(0.999, max(score, score + sensitive_title_boost))
                            matched_patterns.append("sensitive_title_risk")

                    if _looks_constructive(added_text, comment) and score < 0.98:
                        score = min(score, 0.45)
                        matched_patterns.append("constructive_guard")

                    reason = ", ".join(sorted(set(matched_patterns))) or "No pattern"
                    log_server_action(
                        "score_computed",
                        script_name=config.script_name,
                        context={
                            "change_id": change_id,
                            "title": title,
                            "score": round(score, 4),
                            "reason": reason[:200],
                            "instant_threshold": config.instant_revert_threshold,
                            "ai_threshold": config.ai_revert_threshold,
                            "dynamic_rule_hits": len(dynamic_hit_rules),
                            "url_count": int(feature_stats.get("url_count", 0)),
                            "symbol_ratio": float(feature_stats.get("symbol_ratio", 0.0)),
                            "burst_count": int(feature_stats.get("burst_count", 1)),
                            "title_sensitive": int(feature_stats.get("title_sensitive", 0)),
                        },
                    )

                    if score >= config.instant_revert_threshold:
                        reverted, revert_status = _revert_target_revision(
                            page=page,
                            target_revid=target_revid,
                            old_revid=old_revid,
                            expected_user=creator,
                            reason=f"Score critique: {reason}",
                            confidence=score,
                        )
                        if reverted and revert_status == "reverted":
                            action = "reverted"
                            confidence = score
                            reverted_this_run += 1
                            log_server_action(
                                "instant_revert_success",
                                script_name=config.script_name,
                                level="WARNING",
                                context={
                                    "change_id": change_id,
                                    "title": title,
                                    "creator": creator,
                                    "score": round(score, 4),
                                    "reason": reason[:220],
                                },
                            )
                        elif reverted and revert_status == "reverted_dry_run":
                            action = "dry_run_revert"
                            confidence = score
                            dry_run_revert_candidates += 1
                            log_server_action(
                                "instant_revert_dry_run",
                                script_name=config.script_name,
                                level="WARNING",
                                context={"change_id": change_id, "title": title, "creator": creator, "score": round(score, 4), "reason": reason[:220]},
                            )
                        else:
                            LOGGER.debug("Skip revert on %s: %s", title, revert_status)
                            log_server_action(
                                "instant_revert_skipped",
                                script_name=config.script_name,
                                context={"change_id": change_id, "title": title, "status": revert_status},
                            )
                    elif score >= 0.5:
                        ai_confidence, ai_reason, ai_category = _call_mistral(
                            config=config,
                            page_title=title,
                            new_text=new_text,
                            old_text=old_text,
                            edit_summary=comment,
                        )
                        log_server_action(
                            "ai_assessment",
                            script_name=config.script_name,
                            context={
                                "change_id": change_id,
                                "title": title,
                                "ai_confidence": round(ai_confidence, 4),
                                "ai_category": ai_category,
                                "ai_reason": ai_reason[:220],
                            },
                        )

                        if ai_category.upper() in {"LEGIT", "DOUBTFUL"}:
                            ai_confidence = min(ai_confidence, 0.49)

                        if ai_confidence >= config.ai_revert_threshold:
                            reverted, revert_status = _revert_target_revision(
                                page=page,
                                target_revid=target_revid,
                                old_revid=old_revid,
                                expected_user=creator,
                                reason=ai_reason,
                                confidence=ai_confidence,
                            )
                            if reverted and revert_status == "reverted":
                                action = "reverted"
                                reason = ai_reason
                                confidence = ai_confidence
                                reverted_this_run += 1
                                log_server_action(
                                    "ai_revert_success",
                                    script_name=config.script_name,
                                    level="WARNING",
                                    context={
                                        "change_id": change_id,
                                        "title": title,
                                        "creator": creator,
                                        "confidence": round(ai_confidence, 4),
                                        "reason": ai_reason[:220],
                                        "category": ai_category,
                                    },
                                )
                            elif reverted and revert_status == "reverted_dry_run":
                                action = "dry_run_revert"
                                reason = ai_reason
                                confidence = ai_confidence
                                dry_run_revert_candidates += 1
                                log_server_action(
                                    "ai_revert_dry_run",
                                    script_name=config.script_name,
                                    level="WARNING",
                                    context={"change_id": change_id, "title": title, "creator": creator, "confidence": round(ai_confidence, 4), "reason": ai_reason[:220], "category": ai_category},
                                )
                            else:
                                LOGGER.debug("Skip AI revert on %s: %s", title, revert_status)
                                log_server_action(
                                    "ai_revert_skipped",
                                    script_name=config.script_name,
                                    context={"change_id": change_id, "title": title, "status": revert_status},
                                )
                        elif ai_confidence >= config.review_threshold:
                            reason = f"review_needed: {ai_reason}"
                            quarantine.append(
                                {
                                    "change_id": change_id,
                                    "title": title,
                                    "creator": creator,
                                    "comment": comment[:220],
                                    "ai_confidence": round(ai_confidence, 4),
                                    "ai_category": ai_category,
                                    "reason": ai_reason[:500],
                                    "revid": target_revid,
                                    "old_revid": old_revid,
                                    "timestamp": datetime.utcnow().isoformat(),
                                }
                            )
                            quarantine = quarantine[-20000:]
                            quarantined_this_run += 1
                            log_server_action(
                                "review_needed",
                                script_name=config.script_name,
                                level="WARNING",
                                context={
                                    "change_id": change_id,
                                    "title": title,
                                    "ai_confidence": round(ai_confidence, 4),
                                    "category": ai_category,
                                    "reason": ai_reason[:220],
                                    "quarantine_size": len(quarantine),
                                },
                            )

                    _update_metrics(metrics, action, confidence if action == "reverted" else None)
                    if action != "reverted":
                        log_server_action(
                            "change_finalized",
                            script_name=config.script_name,
                            context={"change_id": change_id, "title": title, "creator": creator, "action": action, "reason": reason[:220]},
                        )

                    if action == "reverted":
                        db[change_id] = {
                            "title": title,
                            "creator": creator,
                            "reason": reason,
                            "confidence": round(float(confidence or 0.0), 4),
                            "revid": target_revid,
                            "old_revid": old_revid,
                            "comment": comment[:280],
                            "added_text": (added_text or "")[:5000],
                            "removed_text": (removed_text or "")[:5000],
                            "matched_patterns": sorted(set(matched_patterns)),
                            "dynamic_rule_labels": sorted({rule.label for rule in dynamic_hit_rules}),
                            "feature_stats": {
                                "url_count": int(feature_stats.get("url_count", 0)),
                                "shortener_count": int(feature_stats.get("shortener_count", 0)),
                                "symbol_ratio": float(feature_stats.get("symbol_ratio", 0.0)),
                                "uppercase_ratio": float(feature_stats.get("uppercase_ratio", 0.0)),
                                "burst_count": int(feature_stats.get("burst_count", 1)),
                                "title_sensitive": int(feature_stats.get("title_sensitive", 0)),
                            },
                            "timestamp": datetime.utcnow().isoformat(),
                        }

                        embed = {
                            "title": "Modification non constructive annulée",
                            "description": f"[{title}]({config.wiki_base_url}{title.replace(' ', '_')})",
                            "color": 15158332,
                            "fields": [
                                {"name": "Utilisateur", "value": creator, "inline": True},
                                {"name": "Raison", "value": reason[:1024], "inline": False},
                                {"name": "Confiance", "value": f"{(confidence or 0.0) * 100:.1f}%", "inline": True},
                            ],
                            "timestamp": datetime.utcnow().isoformat(),
                        }
                        send_discord_webhook(embed=embed, level="WARNING", script_name=config.script_name)
                        log_server_action(
                            "revert_notified_discord",
                            script_name=config.script_name,
                            level="WARNING",
                            context={"change_id": change_id, "title": title, "creator": creator, "reason": reason[:220]},
                        )

                        try:
                            _append_wiki_log(
                                site,
                                config,
                                title,
                                creator,
                                reason,
                                float(confidence or 0.0),
                                change_id=change_id,
                                revid=target_revid,
                                old_revid=old_revid,
                                comment=comment,
                            )
                            log_server_action(
                                "revert_logged_on_wiki",
                                script_name=config.script_name,
                                level="SUCCESS",
                                context={"change_id": change_id, "title": title, "revid": target_revid, "old_revid": old_revid, "dry_run": int(dry_run_enabled())},
                            )
                        except Exception as exc:
                            LOGGER.debug("Wiki log failed: %s", exc)
                            log_server_action(
                                "revert_wiki_log_failed",
                                script_name=config.script_name,
                                level="ERROR",
                                context={"change_id": change_id, "title": title, "error": str(exc)[:220]},
                            )

                    dynamic_rule_labels = [rule.label for rule in dynamic_hit_rules]
                    _record_intel_change_event(
                        intel_conn,
                        config=config,
                        change_id=change_id,
                        title=title,
                        creator=creator,
                        action=action,
                        score=score,
                        reason=reason,
                        matched_patterns=matched_patterns,
                        dynamic_rule_labels=dynamic_rule_labels,
                        feature_stats=feature_stats,
                        added_len=len(added_text),
                        removed_len=len(removed_text),
                    )
                    _record_intel_rule_hits(
                        intel_conn,
                        config=config,
                        change_id=change_id,
                        title=title,
                        creator=creator,
                        action=action,
                        score=score,
                        rules=dynamic_hit_rules,
                    )

                    processed_ids.add(change_id)
                except Exception as exc:
                    _update_metrics(metrics, "error")
                    processed_ids.add(change_id)
                    log_to_discord(f"Erreur sur {title}: {exc}", level="ERROR", script_name=config.script_name)
                    log_server_action(
                        "change_processing_error",
                        script_name=config.script_name,
                        level="ERROR",
                        context={"change_id": change_id, "title": title, "creator": creator, "error": str(exc)[:220]},
                    )
                    log_server_diagnostic(
                        message=f"Erreur anti-vandalisme sur {title}",
                        level="ERROR",
                        script_name=config.script_name,
                        context={"change_id": change_id, "title": title, "creator": creator, "comment": comment[:200]},
                        exception=exc,
                    )

            processed_data["ids"] = list(processed_ids)[-20000:]

            write_json(config.processed_file, processed_data)
            write_json(config.metrics_file, metrics)
            write_json(config.db_file, db)
            write_json(config.quarantine_file, quarantine)

            checkpoint_payload = {
                "last_timestamp": (max_seen_ts.isoformat().replace("+00:00", "Z") if max_seen_ts else ""),
                "last_revid": max_seen_revid,
                "updated_at": datetime.utcnow().isoformat().replace("+00:00", "Z"),
                "skipped_checkpoint_this_run": skipped_checkpoint,
            }
            write_json(config.checkpoint_file, checkpoint_payload)

            confidences = metrics.get("confidences", [])
            average_confidence = sum(confidences) / len(confidences) if confidences else 0.0

            summary = (
                f"Analyse terminée - reverts session: {reverted_this_run} | "
                f"dry_run_reverts: {dry_run_revert_candidates} | "
                f"review_quarantine: {quarantined_this_run} | "
                f"total: {metrics.get('total_analyzed', 0)} | "
                f"reverts cumulés: {metrics.get('reverted', 0)} | "
                f"erreurs: {metrics.get('errors', 0)} | "
                f"confiance moyenne: {average_confidence * 100:.1f}%"
            )
            log_to_discord(summary, level="INFO", script_name=config.script_name)
            log_server_action(
                "run_end",
                script_name=config.script_name,
                level="SUCCESS",
                context={
                    "reverts_session": reverted_this_run,
                    "dry_run_revert_candidates": dry_run_revert_candidates,
                    "quarantined_this_run": quarantined_this_run,
                    "skipped_checkpoint": skipped_checkpoint,
                    "total_analyzed": int(metrics.get("total_analyzed", 0)),
                    "reverts_total": int(metrics.get("reverted", 0)),
                    "errors": int(metrics.get("errors", 0)),
                    "avg_confidence": round(average_confidence, 4),
                    "duration_seconds": round(time.monotonic() - started, 2),
                },
            )
            send_task_report(
                script_name=config.script_name,
                status="SUCCESS",
                duration_seconds=time.monotonic() - started,
                details=summary,
                stats={
                    "reverts_session": reverted_this_run,
                    "dry_run_revert_candidates": dry_run_revert_candidates,
                    "quarantined_this_run": quarantined_this_run,
                    "skipped_checkpoint": skipped_checkpoint,
                    "total_analyzed": int(metrics.get("total_analyzed", 0)),
                    "reverts_total": int(metrics.get("reverted", 0)),
                    "errors": int(metrics.get("errors", 0)),
                },
            )
            if intel_conn is not None:
                intel_conn.close()
            health_state.finish(status="success")
            return 0
    except LockUnavailableError:
        health_state.finish(status="lock_unavailable", error="lock_unavailable")
        return report_lock_unavailable(config.script_name, started, lock_name)
    except Exception as exc:
        health_state.finish(status="failed", error=str(exc))
        raise
    finally:
        if health_server is not None:
            health_server.stop()


def main_fr() -> int:
    return run(FR_CONFIG)


def main_en() -> int:
    return run(EN_CONFIG)


if __name__ == "__main__":
    raise SystemExit(main_fr())
