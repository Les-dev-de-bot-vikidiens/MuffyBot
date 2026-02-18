# -*- coding: utf-8 -*-
from __future__ import annotations

import difflib
import json
import logging
import re
import sqlite3
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pywikibot
from pywikibot.data.api import Request

from muffybot.discord import log_server_action, log_server_diagnostic, log_to_discord, send_task_report
from muffybot.env import get_float_env, get_int_env, load_dotenv
from muffybot.files import read_json
from muffybot.locking import LockUnavailableError, hold_lock
from muffybot.paths import ENVIKIDIA_DIR, ROOT_DIR
from muffybot.task_control import report_lock_unavailable
from muffybot.tasks.vandalism_shared import holdout_bucket, normalize_detection_text, tokenize_training_text
from muffybot.wiki import connect_site, prepare_runtime

LOGGER = logging.getLogger(__name__)

FR_DB = ROOT_DIR / "vandalism_db.json"
EN_DB = ENVIKIDIA_DIR / "vandalism_db.json"
COMMON_PATTERNS_FILE = ROOT_DIR / "vandalism_common_patterns.txt"
REGEX_PATTERNS_FILE = ROOT_DIR / "vandalism_detection_regex.txt"
HUMAN_CORPUS_FILE = ROOT_DIR / "human_reverts_corpus.jsonl"

TOKEN_RE = re.compile(r"[a-z0-9à-öø-ÿ_'-]{4,40}", flags=re.IGNORECASE)
SPACE_RE = re.compile(r"\s+")
ISOW_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T")
INTEL_DB_FILE = ROOT_DIR / "vandalism_intel.sqlite3"
RULE_STATS_FILE = ROOT_DIR / "vandalism_rule_stats.json"
RULE_DRIFT_REPORT_FILE = ROOT_DIR / "vandalism_rule_drift_report.txt"
VALIDATION_REPORT_FILE = ROOT_DIR / "vandalism_pattern_validation.json"
FALSE_POSITIVE_WHITELIST_FILE = ROOT_DIR / "vandalism_false_positive_whitelist.json"

TRUSTED_GROUPS = {
    "sysop",
    "patroller",
    "autopatrol",
    "rollbacker",
    "bureaucrat",
}
REVERT_TAGS = {
    "mw-rollback",
    "mw-undo",
    "mw-manual-revert",
    "mw-reverted",
}
REVERT_KEYWORDS = (
    " rv ",
    "rvv",
    "revert",
    "reverted",
    "undo",
    "undid",
    "annulation",
    "annule",
    "vandalisme",
    "vandalism",
    "rollback",
)
STOPWORDS = {
    "dans",
    "avec",
    "pour",
    "this",
    "that",
    "from",
    "sans",
    "mais",
    "http",
    "https",
    "wiki",
    "vikidia",
    "page",
    "comment",
    "reason",
    "title",
    "user",
}


def _normalize(text: str) -> str:
    return normalize_detection_text(str(text or ""))


def _parse_iso(value: str | None) -> datetime | None:
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


def _utc_iso(dt_obj: datetime) -> str:
    return dt_obj.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_jsonl_read(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
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
                    rows.append(payload)
    except Exception:
        return rows
    return rows


def _safe_jsonl_write(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


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


def _get_revision_text(page: pywikibot.Page, revid: int | None) -> str | None:
    if not revid:
        return None
    try:
        return page.getOldVersion(revid)
    except Exception:
        return None


def _is_bot_like_user(username: str) -> bool:
    normalized = _normalize(username)
    return normalized.endswith("bot")


def _is_human_revert_event(tags: list[str], comment: str) -> bool:
    low_tags = {str(tag).strip().lower() for tag in tags if str(tag).strip()}
    if low_tags & REVERT_TAGS:
        return True
    low_comment = f" {_normalize(comment)} "
    return any(keyword in low_comment for keyword in REVERT_KEYWORDS)


def _is_trusted_reverter(site: pywikibot.Site, username: str, cache: dict[str, bool]) -> bool:
    key = _normalize(username)
    if key in cache:
        return cache[key]
    try:
        groups = {group.strip().lower() for group in pywikibot.User(site, username).groups()}
    except Exception:
        cache[key] = False
        return False
    ok = bool(groups & TRUSTED_GROUPS)
    cache[key] = ok
    return ok


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _fetch_recent_changes(
    site: pywikibot.Site,
    *,
    since_utc: datetime,
    max_items: int,
) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    start_text = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_text = since_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    per_page = max(10, min(max_items, 200))

    params: dict[str, str] = {
        "action": "query",
        "list": "recentchanges",
        "rctype": "edit",
        "rcprop": "title|ids|user|comment|flags|tags|timestamp",
        "rcdir": "older",
        "rclimit": str(per_page),
        "rcstart": start_text,
        "rcend": end_text,
    }

    items: list[dict[str, Any]] = []
    rccontinue: str | None = None
    pages_fetched = 0
    max_pages = max(1, min((max_items // max(per_page, 1)) + 4, 120))
    while True:
        if len(items) >= max_items or pages_fetched >= max_pages:
            break
        query_params = dict(params)
        if rccontinue:
            query_params["rccontinue"] = rccontinue
        try:
            data = Request(site=site, parameters=query_params).submit()
        except Exception as exc:
            LOGGER.warning("recentchanges fetch failed for %s: %s", site.code, exc)
            break

        chunk = data.get("query", {}).get("recentchanges", [])
        if isinstance(chunk, list):
            for entry in chunk:
                if isinstance(entry, dict):
                    items.append(entry)
                    if len(items) >= max_items:
                        return items

        pages_fetched += 1
        rccontinue = data.get("continue", {}).get("rccontinue")
        if not rccontinue:
            break
    return items


def _collect_human_reverts_for_site(
    site: pywikibot.Site,
    *,
    lang: str,
    since_days: int,
    max_rc_items: int,
    max_diffs: int,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    since_utc = datetime.now(timezone.utc) - timedelta(days=since_days)
    recent_changes = _fetch_recent_changes(site, since_utc=since_utc, max_items=max_rc_items)

    groups_cache: dict[str, bool] = {}
    rows: list[dict[str, Any]] = []
    stats = {
        "rc_scanned": len(recent_changes),
        "trusted_revert_events": 0,
        "diff_extracted": 0,
        "skipped_untrusted": 0,
        "skipped_not_revert": 0,
        "skipped_missing_ids": 0,
        "skipped_bot_like": 0,
        "skipped_namespace": 0,
        "stopped_by_max_diffs": 0,
        "errors": 0,
    }

    for change in recent_changes:
        if stats["diff_extracted"] >= max_diffs:
            stats["stopped_by_max_diffs"] = 1
            break

        if _to_int(change.get("ns"), -1) != 0:
            stats["skipped_namespace"] += 1
            continue

        user = str(change.get("user") or "")
        if not user or bool(change.get("bot")) or _is_bot_like_user(user):
            stats["skipped_bot_like"] += 1
            continue

        tags_raw = change.get("tags")
        tags = [str(tag) for tag in tags_raw] if isinstance(tags_raw, list) else []
        comment = str(change.get("comment") or "")
        if not _is_human_revert_event(tags, comment):
            stats["skipped_not_revert"] += 1
            continue

        if not _is_trusted_reverter(site, user, groups_cache):
            stats["skipped_untrusted"] += 1
            continue

        stats["trusted_revert_events"] += 1
        title = str(change.get("title") or "")
        rcid = _to_int(change.get("rcid"), 0)
        revid = _to_int(change.get("revid"), 0)
        old_revid = _to_int(change.get("old_revid"), 0)
        if not title or revid <= 0 or old_revid <= 0:
            stats["skipped_missing_ids"] += 1
            continue

        try:
            page = pywikibot.Page(site, title)
            new_text = _get_revision_text(page, revid) or ""
            old_text = _get_revision_text(page, old_revid)
            if old_text is None:
                stats["skipped_missing_ids"] += 1
                continue
            added_text, removed_text = _extract_changed_text(old_text, new_text)
        except Exception:
            stats["errors"] += 1
            continue

        if not added_text and not removed_text:
            continue

        stats["diff_extracted"] += 1
        rows.append(
            {
                "source": "human_revert",
                "lang": lang,
                "timestamp": str(change.get("timestamp") or ""),
                "rcid": rcid,
                "title": title,
                "reverter": user,
                "comment": comment[:300],
                "tags": tags[:10],
                "revert_revid": revid,
                "prev_revid": old_revid,
                "added_text": added_text[:5000],
                "removed_text": removed_text[:5000],
            }
        )

    return rows, stats


def _merge_human_corpus(
    existing: list[dict[str, Any]],
    new_rows: list[dict[str, Any]],
    *,
    retention_days: int,
    max_entries: int,
) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=retention_days)

    for row in existing + new_rows:
        lang = str(row.get("lang") or "")
        revid = int(row.get("revert_revid") or 0)
        rcid = int(row.get("rcid") or 0)
        key = f"{lang}:{revid}" if revid > 0 else f"{lang}:rcid:{rcid}"

        ts = _parse_iso(str(row.get("timestamp") or ""))
        if ts is not None and ts < cutoff:
            continue
        by_key[key] = row

    merged = list(by_key.values())
    merged.sort(key=lambda row: _parse_iso(str(row.get("timestamp") or "")) or datetime(1970, 1, 1, tzinfo=timezone.utc), reverse=True)
    return merged[:max_entries]


def _iter_bot_reverts(path: Path, lang: str) -> list[dict[str, str]]:
    payload = read_json(path, default={})
    if not isinstance(payload, dict):
        return []

    rows: list[dict[str, str]] = []
    for change_id, raw in payload.items():
        if not isinstance(raw, dict):
            continue
        rows.append(
            {
                "change_id": str(change_id),
                "lang": lang,
                "title": str(raw.get("title") or ""),
                "reason": str(raw.get("reason") or ""),
                "comment": str(raw.get("comment") or ""),
                "added_text": str(raw.get("added_text") or ""),
                "removed_text": str(raw.get("removed_text") or ""),
            }
        )
    return rows


def _tokenize(text: str) -> list[str]:
    tokens: list[str] = []
    for token in tokenize_training_text(text):
        if token in STOPWORDS or token.isdigit():
            continue
        tokens.append(token)
    return tokens


def _open_intel_db(path: Path = INTEL_DB_FILE) -> sqlite3.Connection | None:
    if not path.exists():
        return None
    try:
        conn = sqlite3.connect(path, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception:
        return None


def _load_rule_performance(
    conn: sqlite3.Connection | None,
    *,
    window_days: int,
) -> dict[str, dict[str, float]]:
    if conn is None:
        return {}
    since = (datetime.now(timezone.utc) - timedelta(days=max(window_days, 1))).isoformat().replace("+00:00", "Z")
    try:
        rows = conn.execute(
            """
            SELECT
                rule_label,
                COUNT(*) AS hits,
                SUM(CASE WHEN action = 'reverted' THEN 1 ELSE 0 END) AS reverted_hits
            FROM rule_hits
            WHERE ts_utc >= ?
            GROUP BY rule_label
            """,
            (since,),
        ).fetchall()
    except Exception:
        return {}

    stats: dict[str, dict[str, float]] = {}
    for row in rows:
        label = str(row["rule_label"] or "").strip()
        if not label:
            continue
        hits = int(row["hits"] or 0)
        reverted_hits = int(row["reverted_hits"] or 0)
        precision = reverted_hits / hits if hits else 0.0
        stats[label] = {
            "hits": float(hits),
            "reverted_hits": float(reverted_hits),
            "precision": float(precision),
        }
    return stats


def _build_cluster_candidates(token_counter: Counter[str]) -> list[tuple[str, str, int]]:
    clusters: dict[str, list[tuple[str, int]]] = {}
    for token, count in token_counter.items():
        if len(token) < 5:
            continue
        stem = token[:4]
        clusters.setdefault(stem, []).append((token, count))

    candidates: list[tuple[str, str, int]] = []
    for stem, items in clusters.items():
        items.sort(key=lambda x: x[1], reverse=True)
        if len(items) < 3:
            continue
        total = sum(count for _, count in items[:6])
        if total < 8:
            continue
        variants = [re.escape(token) for token, _ in items[:6]]
        pattern = rf"\b(?:{'|'.join(variants)})\b"
        label = f"auto_cluster_{stem}"
        candidates.append((pattern, label, total))
    return candidates


def _split_holdout(texts: list[str], *, ratio: int) -> tuple[list[str], list[str]]:
    train: list[str] = []
    holdout: list[str] = []
    for text in texts:
        bucket = holdout_bucket(_normalize(text))
        if bucket < ratio:
            holdout.append(text)
        else:
            train.append(text)
    if not train:
        train = list(texts)
    return train, holdout


def _collect_training_texts(
    bot_rows: list[dict[str, str]],
    human_rows: list[dict[str, Any]],
) -> tuple[list[str], list[str]]:
    positives: list[str] = []
    negatives: list[str] = []

    for row in bot_rows:
        pos = _normalize(str(row.get("added_text") or ""))
        if len(pos) >= 4:
            positives.append(pos)
        else:
            fallback = _normalize(" ".join([str(row.get("reason") or ""), str(row.get("comment") or "")]).strip())
            if len(fallback) >= 4:
                positives.append(fallback)

        neg = _normalize(str(row.get("removed_text") or ""))
        if len(neg) >= 4:
            negatives.append(neg)

    for row in human_rows:
        pos = _normalize(str(row.get("removed_text") or ""))
        if len(pos) >= 4:
            positives.append(pos)
        neg = _normalize(str(row.get("added_text") or ""))
        if len(neg) >= 4:
            negatives.append(neg)

    # Deduplicate while keeping stable order.
    def _dedupe(values: list[str]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for item in values:
            norm = _normalize(item)
            if not norm or norm in seen:
                continue
            seen.add(norm)
            out.append(item)
        return out

    return _dedupe(positives), _dedupe(negatives)


def _count_candidates(positive_texts: list[str]) -> tuple[Counter[str], Counter[str]]:
    token_counter: Counter[str] = Counter()
    phrase_counter: Counter[str] = Counter()

    for text in positive_texts:
        tokens = _tokenize(text)
        if not tokens:
            continue
        token_counter.update(tokens)
        for idx in range(len(tokens) - 1):
            a = tokens[idx]
            b = tokens[idx + 1]
            if a == b:
                continue
            phrase_counter[f"{a} {b}"] += 1

    return token_counter, phrase_counter


def _evaluate_pattern(pattern: str, positives: list[str], negatives: list[str]) -> tuple[int, int, float]:
    try:
        compiled = re.compile(pattern, flags=re.IGNORECASE)
    except re.error:
        return 0, 0, 0.0

    support = sum(1 for text in positives if compiled.search(text))
    false_hits = sum(1 for text in negatives if compiled.search(text))
    precision = support / (support + false_hits) if (support + false_hits) else 0.0
    return support, false_hits, precision


def _build_regex_rules(
    token_counter: Counter[str],
    phrase_counter: Counter[str],
    *,
    positives: list[str],
    negatives: list[str],
    rule_performance: dict[str, dict[str, float]],
    min_token_hits: int,
    min_phrase_hits: int,
    min_support: int,
    min_precision: float,
    min_live_hits: int,
    review_support_threshold: int,
    expire_hits_threshold: int,
    expire_precision_threshold: float,
    max_rules: int,
) -> tuple[list[dict[str, Any]], set[str]]:
    candidates: list[tuple[str, str, int]] = []

    for token, count in token_counter.most_common(1200):
        if count < min_token_hits:
            break
        pattern = rf"\b{re.escape(token)}\b"
        candidates.append((pattern, f"auto_tok_{token[:24]}", count))

    for phrase, count in phrase_counter.most_common(900):
        if count < min_phrase_hits:
            break
        words = [w for w in phrase.split(" ") if w]
        if len(words) != 2:
            continue
        pattern = rf"\b{re.escape(words[0])}\W+{re.escape(words[1])}\b"
        candidates.append((pattern, f"auto_phrase_{words[0][:12]}_{words[1][:12]}", count))

    candidates.extend(_build_cluster_candidates(token_counter))

    # Extra robust generic candidates.
    candidates.extend(
        [
            (r"(.)\1{12,}", "auto_repeat_chars", 999),
            (r"^[^A-Za-zÀ-ÖØ-öø-ÿ]{12,}$", "auto_symbol_spam", 999),
        ]
    )

    seen: set[str] = set()
    rules: list[dict[str, Any]] = []
    disabled_labels: set[str] = set()
    for pattern, label, raw_count in candidates:
        if pattern in seen:
            continue
        seen.add(pattern)

        support, false_hits, precision = _evaluate_pattern(pattern, positives, negatives)
        if support < min_support:
            continue
        if precision < min_precision:
            continue

        weight = min(0.995, 0.58 + (precision * 0.34) + (min(support, 25) * 0.006))
        status = "active"
        perf = rule_performance.get(label)
        if perf:
            rolling_hits = int(perf.get("hits", 0))
            rolling_precision = float(perf.get("precision", precision))
            weight = min(0.995, max(0.5, (weight * 0.6) + (rolling_precision * 0.4)))
            if rolling_hits >= expire_hits_threshold and rolling_precision < expire_precision_threshold:
                disabled_labels.add(label)
                continue
            if rolling_hits < min_live_hits or rolling_precision < min_precision:
                status = "review"
        elif support < review_support_threshold:
            status = "review"

        rules.append(
            {
                "pattern": pattern,
                "weight": round(weight, 3),
                "label": label,
                "support": support,
                "false_hits": false_hits,
                "precision": round(precision, 4),
                "raw_count": raw_count,
                "status": status,
            }
        )

    rules.sort(key=lambda r: (r["precision"], r["support"], -r["false_hits"], r["weight"]), reverse=True)
    return rules[:max_rules], disabled_labels


def _write_common_patterns(
    path: Path,
    token_counter: Counter[str],
    phrase_counter: Counter[str],
    *,
    positives_count: int,
    negatives_count: int,
    bot_entries_count: int,
    human_entries_count: int,
) -> None:
    now = _utc_iso(datetime.now(timezone.utc))
    lines: list[str] = [
        f"# generated_at_utc: {now}",
        f"# positive_texts: {positives_count}",
        f"# negative_texts: {negatives_count}",
        f"# bot_reverts: {bot_entries_count}",
        f"# human_reverts: {human_entries_count}",
        "# format: token<TAB>count",
        "",
    ]
    for token, count in token_counter.most_common(280):
        lines.append(f"{token}\t{count}")

    lines.extend(["", "# top_phrases format: phrase<TAB>count", ""])
    for phrase, count in phrase_counter.most_common(220):
        lines.append(f"{phrase}\t{count}")

    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def _write_regex_patterns(
    path: Path,
    rules: list[dict[str, Any]],
    *,
    positives_count: int,
    negatives_count: int,
) -> None:
    now = _utc_iso(datetime.now(timezone.utc))
    lines: list[str] = [
        f"# generated_at_utc: {now}",
        f"# positive_texts: {positives_count}",
        f"# negative_texts: {negatives_count}",
        "# format: regex<TAB>weight<TAB>label<TAB>support<TAB>false_hits<TAB>precision<TAB>status",
        "",
    ]
    for rule in rules:
        lines.append(
            f"{rule['pattern']}\t{rule['weight']:.3f}\t{rule['label']}\t"
            f"{rule['support']}\t{rule['false_hits']}\t{rule['precision']:.4f}\t{rule.get('status', 'active')}"
        )
    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def _evaluate_ruleset(
    rules: list[dict[str, Any]],
    positives: list[str],
    negatives: list[str],
) -> dict[str, float]:
    compiled: list[re.Pattern[str]] = []
    for rule in rules:
        try:
            compiled.append(re.compile(str(rule.get("pattern", "")), flags=re.IGNORECASE))
        except re.error:
            continue
    if not compiled:
        return {"precision": 0.0, "recall": 0.0, "fpr": 0.0}

    tp = sum(1 for text in positives if any(regex.search(text) for regex in compiled))
    fp = sum(1 for text in negatives if any(regex.search(text) for regex in compiled))
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / len(positives) if positives else 0.0
    fpr = fp / len(negatives) if negatives else 0.0
    return {"precision": round(precision, 4), "recall": round(recall, 4), "fpr": round(fpr, 4)}


def _write_validation_report(
    path: Path,
    *,
    holdout_positive_count: int,
    holdout_negative_count: int,
    metrics: dict[str, float],
) -> None:
    payload = {
        "generated_at_utc": _utc_iso(datetime.now(timezone.utc)),
        "holdout_positive_count": holdout_positive_count,
        "holdout_negative_count": holdout_negative_count,
        "precision": float(metrics.get("precision", 0.0)),
        "recall": float(metrics.get("recall", 0.0)),
        "false_positive_rate": float(metrics.get("fpr", 0.0)),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_rule_stats(
    path: Path,
    *,
    rules: list[dict[str, Any]],
    performance: dict[str, dict[str, float]],
) -> None:
    payload: dict[str, Any] = {
        "generated_at_utc": _utc_iso(datetime.now(timezone.utc)),
        "rules": [],
    }
    for rule in rules:
        label = str(rule.get("label", ""))
        perf = performance.get(label, {})
        payload["rules"].append(
            {
                "label": label,
                "status": str(rule.get("status", "active")),
                "support": int(rule.get("support", 0)),
                "false_hits": int(rule.get("false_hits", 0)),
                "training_precision": float(rule.get("precision", 0.0)),
                "rolling_hits": int(perf.get("hits", 0)),
                "rolling_precision": float(perf.get("precision", 0.0)),
            }
        )
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_false_positive_whitelist(
    path: Path,
    *,
    disabled_labels: set[str],
    performance: dict[str, dict[str, float]],
    min_hits: int,
    precision_threshold: float,
) -> list[str]:
    auto_disabled = set(disabled_labels)
    for label, perf in performance.items():
        hits = int(perf.get("hits", 0))
        precision = float(perf.get("precision", 0.0))
        if hits >= min_hits and precision < precision_threshold:
            auto_disabled.add(label)

    labels = sorted(auto_disabled)
    payload = {
        "generated_at_utc": _utc_iso(datetime.now(timezone.utc)),
        "disabled_rule_labels": labels,
        "min_hits": min_hits,
        "precision_threshold": precision_threshold,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return labels


def _write_drift_report(
    path: Path,
    *,
    rules: list[dict[str, Any]],
    performance: dict[str, dict[str, float]],
    validation: dict[str, float],
) -> None:
    now = _utc_iso(datetime.now(timezone.utc))
    top_active = [rule for rule in rules if str(rule.get("status")) == "active"][:20]
    low_perf = sorted(
        (
            (label, stats)
            for label, stats in performance.items()
            if int(stats.get("hits", 0)) >= 5
        ),
        key=lambda item: float(item[1].get("precision", 0.0)),
    )[:20]

    lines: list[str] = [
        f"# generated_at_utc: {now}",
        f"# validation_precision: {validation.get('precision', 0.0):.4f}",
        f"# validation_recall: {validation.get('recall', 0.0):.4f}",
        f"# validation_fpr: {validation.get('fpr', 0.0):.4f}",
        "",
        "## Top Active Rules",
    ]
    for rule in top_active:
        label = str(rule.get("label", ""))
        perf = performance.get(label, {})
        lines.append(
            f"- {label} | status={rule.get('status')} | support={rule.get('support')} "
            f"| train_precision={rule.get('precision')} | rolling_hits={int(perf.get('hits', 0))} "
            f"| rolling_precision={float(perf.get('precision', 0.0)):.3f}"
        )

    lines.extend(["", "## Low Rolling Precision"])
    for label, stats in low_perf:
        lines.append(
            f"- {label} | hits={int(stats.get('hits', 0))} | rolling_precision={float(stats.get('precision', 0.0)):.3f}"
        )

    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def run() -> int:
    started = time.monotonic()
    script_name = "vandalism_patterns.py"
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    prepare_runtime(ROOT_DIR)

    min_token_hits = max(get_int_env("VANDALISM_PATTERN_MIN_TOKEN_HITS", 2), 1)
    min_phrase_hits = max(get_int_env("VANDALISM_PATTERN_MIN_PHRASE_HITS", 3), 1)
    min_support = max(get_int_env("VANDALISM_PATTERN_MIN_SUPPORT", 2), 1)
    min_precision = max(min(get_float_env("VANDALISM_PATTERN_MIN_PRECISION", 0.78), 0.999), 0.0)
    max_rules = max(min(get_int_env("VANDALISM_PATTERN_MAX_REGEX_RULES", 180), 500), 10)
    rolling_window_days = max(get_int_env("VANDALISM_PATTERN_ROLLING_WINDOW_DAYS", 30), 3)
    rolling_min_live_hits = max(get_int_env("VANDALISM_PATTERN_MIN_LIVE_HITS", 6), 1)
    review_support_threshold = max(get_int_env("VANDALISM_PATTERN_REVIEW_SUPPORT_THRESHOLD", 5), 1)
    expire_hits_threshold = max(get_int_env("VANDALISM_PATTERN_EXPIRE_HITS", 20), 1)
    expire_precision_threshold = max(min(get_float_env("VANDALISM_PATTERN_EXPIRE_PRECISION", 0.35), 1.0), 0.0)
    fp_whitelist_min_hits = max(get_int_env("VANDALISM_FP_WHITELIST_MIN_HITS", 12), 1)
    fp_whitelist_precision_threshold = max(min(get_float_env("VANDALISM_FP_WHITELIST_PRECISION", 0.25), 1.0), 0.0)
    holdout_ratio = max(min(get_int_env("VANDALISM_PATTERN_VALIDATION_HOLDOUT_RATIO", 20), 50), 5)
    human_window_days = max(get_int_env("HUMAN_REVERT_WINDOW_DAYS", 14), 1)
    human_max_rc_per_lang = max(get_int_env("HUMAN_REVERT_MAX_RC_PER_LANG", 4000), 200)
    human_max_diffs_per_lang = max(get_int_env("HUMAN_REVERT_MAX_DIFFS_PER_LANG", 250), 20)
    human_max_corpus_entries = max(get_int_env("HUMAN_REVERT_MAX_CORPUS_ENTRIES", 50000), 500)
    human_retention_days = max(get_int_env("HUMAN_REVERT_CORPUS_RETENTION_DAYS", 120), 7)

    try:
        with hold_lock("vandalism-patterns"):
            # 1) Collect trusted human reverts from FR and EN.
            fr_rows: list[dict[str, Any]] = []
            en_rows: list[dict[str, Any]] = []
            fr_stats: dict[str, Any] = {"source": "fr", "available": 0}
            en_stats: dict[str, Any] = {"source": "en", "available": 0}

            try:
                fr_site = connect_site(lang="fr", family="vikidia")
                fr_rows, fr_stats = _collect_human_reverts_for_site(
                    fr_site,
                    lang="fr",
                    since_days=human_window_days,
                    max_rc_items=human_max_rc_per_lang,
                    max_diffs=human_max_diffs_per_lang,
                )
                fr_stats["source"] = "fr"
                fr_stats["available"] = 1
            except Exception as exc:
                fr_stats = {"source": "fr", "available": 0, "error": str(exc)[:300]}
                log_server_action(
                    "vandalism_patterns_source_unavailable",
                    script_name=script_name,
                    level="WARNING",
                    context={"lang": "fr", "error": str(exc)[:280]},
                )

            try:
                prepare_runtime(ENVIKIDIA_DIR)
                en_site = connect_site(lang="en", family="vikidia")
                en_rows, en_stats = _collect_human_reverts_for_site(
                    en_site,
                    lang="en",
                    since_days=human_window_days,
                    max_rc_items=human_max_rc_per_lang,
                    max_diffs=human_max_diffs_per_lang,
                )
                en_stats["source"] = "en"
                en_stats["available"] = 1
            except Exception as exc:
                en_stats = {"source": "en", "available": 0, "error": str(exc)[:300]}
                log_server_action(
                    "vandalism_patterns_source_unavailable",
                    script_name=script_name,
                    level="WARNING",
                    context={"lang": "en", "error": str(exc)[:280]},
                )
            finally:
                prepare_runtime(ROOT_DIR)

            existing_human = _safe_jsonl_read(HUMAN_CORPUS_FILE)
            merged_human = _merge_human_corpus(
                existing_human,
                fr_rows + en_rows,
                retention_days=human_retention_days,
                max_entries=human_max_corpus_entries,
            )
            _safe_jsonl_write(HUMAN_CORPUS_FILE, merged_human)

            # 2) Build training corpus (bot + human) and derive regex with support/precision.
            bot_rows = _iter_bot_reverts(FR_DB, "fr") + _iter_bot_reverts(EN_DB, "en")
            positives, negatives = _collect_training_texts(bot_rows, merged_human)
            train_positives, holdout_positives = _split_holdout(positives, ratio=holdout_ratio)
            train_negatives, holdout_negatives = _split_holdout(negatives, ratio=holdout_ratio)

            token_counter, phrase_counter = _count_candidates(train_positives)
            intel_conn = _open_intel_db()
            rule_performance = _load_rule_performance(intel_conn, window_days=rolling_window_days)
            rules, disabled_labels = _build_regex_rules(
                token_counter,
                phrase_counter,
                positives=train_positives,
                negatives=train_negatives,
                rule_performance=rule_performance,
                min_token_hits=min_token_hits,
                min_phrase_hits=min_phrase_hits,
                min_support=min_support,
                min_precision=min_precision,
                min_live_hits=rolling_min_live_hits,
                review_support_threshold=review_support_threshold,
                expire_hits_threshold=expire_hits_threshold,
                expire_precision_threshold=expire_precision_threshold,
                max_rules=max_rules,
            )
            if intel_conn is not None:
                intel_conn.close()

            validation = _evaluate_ruleset(rules, holdout_positives, holdout_negatives)
            _write_validation_report(
                VALIDATION_REPORT_FILE,
                holdout_positive_count=len(holdout_positives),
                holdout_negative_count=len(holdout_negatives),
                metrics=validation,
            )
            disabled_from_fp = _write_false_positive_whitelist(
                FALSE_POSITIVE_WHITELIST_FILE,
                disabled_labels=disabled_labels,
                performance=rule_performance,
                min_hits=fp_whitelist_min_hits,
                precision_threshold=fp_whitelist_precision_threshold,
            )
            _write_rule_stats(RULE_STATS_FILE, rules=rules, performance=rule_performance)
            _write_drift_report(
                RULE_DRIFT_REPORT_FILE,
                rules=rules,
                performance=rule_performance,
                validation=validation,
            )

            _write_common_patterns(
                COMMON_PATTERNS_FILE,
                token_counter,
                phrase_counter,
                positives_count=len(train_positives),
                negatives_count=len(train_negatives),
                bot_entries_count=len(bot_rows),
                human_entries_count=len(merged_human),
            )
            _write_regex_patterns(
                REGEX_PATTERNS_FILE,
                rules,
                positives_count=len(train_positives),
                negatives_count=len(train_negatives),
            )

            top_precision = max((float(rule["precision"]) for rule in rules), default=0.0)
            summary = (
                f"Patterns generated: bot={len(bot_rows)} human={len(merged_human)} "
                f"positive={len(train_positives)} negative={len(train_negatives)} regex={len(rules)} "
                f"top_precision={top_precision:.3f} holdout_precision={validation.get('precision', 0.0):.3f}"
            )
            log_to_discord(summary, level="SUCCESS", script_name=script_name)
            log_server_action(
                "vandalism_patterns_generated",
                script_name=script_name,
                level="SUCCESS",
                context={
                    "bot_reverts": len(bot_rows),
                    "human_reverts": len(merged_human),
                    "positive_texts": len(train_positives),
                    "negative_texts": len(train_negatives),
                    "regex_rules": len(rules),
                    "regex_active_rules": sum(1 for rule in rules if str(rule.get("status")) == "active"),
                    "regex_review_rules": sum(1 for rule in rules if str(rule.get("status")) == "review"),
                    "top_precision": round(top_precision, 4),
                    "holdout_precision": validation.get("precision", 0.0),
                    "holdout_recall": validation.get("recall", 0.0),
                    "holdout_fpr": validation.get("fpr", 0.0),
                    "disabled_rule_labels": len(disabled_from_fp),
                    "common_patterns_file": str(COMMON_PATTERNS_FILE),
                    "regex_patterns_file": str(REGEX_PATTERNS_FILE),
                    "human_corpus_file": str(HUMAN_CORPUS_FILE),
                    "rule_stats_file": str(RULE_STATS_FILE),
                    "drift_report_file": str(RULE_DRIFT_REPORT_FILE),
                    "validation_report_file": str(VALIDATION_REPORT_FILE),
                    "false_positive_whitelist_file": str(FALSE_POSITIVE_WHITELIST_FILE),
                    "fr_stats": fr_stats,
                    "en_stats": en_stats,
                    "min_token_hits": min_token_hits,
                    "min_phrase_hits": min_phrase_hits,
                    "min_support": min_support,
                    "min_precision": min_precision,
                    "max_rules": max_rules,
                    "rolling_window_days": rolling_window_days,
                    "rolling_min_live_hits": rolling_min_live_hits,
                    "review_support_threshold": review_support_threshold,
                    "expire_hits_threshold": expire_hits_threshold,
                    "expire_precision_threshold": expire_precision_threshold,
                    "fp_whitelist_min_hits": fp_whitelist_min_hits,
                    "fp_whitelist_precision_threshold": fp_whitelist_precision_threshold,
                    "holdout_ratio": holdout_ratio,
                    "human_max_rc_per_lang": human_max_rc_per_lang,
                    "human_max_diffs_per_lang": human_max_diffs_per_lang,
                    "human_window_days": human_window_days,
                    "duration_seconds": round(time.monotonic() - started, 2),
                },
            )
            send_task_report(
                script_name=script_name,
                status="SUCCESS",
                duration_seconds=time.monotonic() - started,
                details=summary,
                stats={
                    "bot_reverts": len(bot_rows),
                    "human_reverts": len(merged_human),
                    "positive_texts": len(train_positives),
                    "negative_texts": len(train_negatives),
                    "regex_rules": len(rules),
                    "top_precision": round(top_precision, 4),
                    "holdout_precision": validation.get("precision", 0.0),
                    "holdout_recall": validation.get("recall", 0.0),
                },
                level="SUCCESS",
            )
            return 0
    except LockUnavailableError:
        return report_lock_unavailable(script_name, started, "vandalism-patterns")
    except Exception as exc:
        log_server_action(
            "vandalism_patterns_failed",
            script_name=script_name,
            level="ERROR",
            context={"error": str(exc)[:280]},
        )
        log_server_diagnostic(
            message="Erreur generation patterns anti-vandalisme",
            level="ERROR",
            script_name=script_name,
            exception=exc,
        )
        send_task_report(
            script_name=script_name,
            status="FAILED",
            duration_seconds=time.monotonic() - started,
            details=f"Echec generation patterns: {exc}",
            stats={"error": str(exc)[:300]},
            level="ERROR",
        )
        return 1


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
