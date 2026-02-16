# -*- coding: utf-8 -*-
from __future__ import annotations

import difflib
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pywikibot
import requests

from muffybot.discord import log_server_action, log_server_diagnostic, log_to_discord, send_discord_webhook, send_task_report
from muffybot.env import get_env, load_dotenv
from muffybot.files import read_json, write_json
from muffybot.paths import ENVIKIDIA_DIR, ROOT_DIR
from muffybot.wiki import connect_site, prepare_runtime

LOGGER = logging.getLogger(__name__)
MISTRAL_API_URL = "https://api.mistral.ai/v1/chat/completions"


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


def _calculate_vandalism_score(added_text: str, new_text: str, old_text: str | None) -> tuple[float, list[str]]:
    focus = added_text if added_text else new_text

    if not new_text.strip():
        return 0.88, ["blanking"]

    patterns = [
        (r"(.)\1{20,}", 0.98, "char_repetition"),
        (r"^\s*(test|asdf|qwerty|azerty|lol|mdr)\s*$", 0.93, "test_word"),
        (r"(fuck|shit|bitch|merde|putain|connard|salope|encul[ée]|nique\s*ta\s*m[eè]re)", 0.98, "insult"),
        (r"(viagra|casino|pariez|bit\.ly|tinyurl)", 0.94, "spam"),
        (r"^[^A-Za-zÀ-ÖØ-öø-ÿ]{16,}$", 0.9, "symbol_spam"),
        (r"[A-Z\s]{80,}", 0.82, "uppercase_block"),
    ]

    score = 0.0
    matches: list[str] = []

    for pattern, weight, label in patterns:
        if re.search(pattern, focus, flags=re.IGNORECASE):
            score = max(score, weight)
            matches.append(label)

    if old_text and len(old_text) > 200:
        ratio = len(new_text) / max(len(old_text), 1)
        if ratio < 0.2:
            score = max(score, 0.9)
            matches.append("massive_deletion")
        elif ratio < 0.35:
            score = max(score, 0.75)
            matches.append("large_deletion")

    return score, matches


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
        page.save(summary=summary, minor=False, botflag=True)
        return True, "reverted"
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
    log_page.save(summary="Bot: ajout journal anti-vandalisme", minor=True, botflag=True)


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
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

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

    group_cache: dict[str, bool] = {}
    reverted_this_run = 0

    log_to_discord("Démarrage du scan anti-vandalisme", level="INFO", script_name=config.script_name)
    log_server_action(
        "run_start",
        script_name=config.script_name,
        include_runtime=True,
        context={"lang": config.lang, "max_changes": config.max_changes, "processed_ids_size": len(processed_ids)},
    )

    for change in site.recentchanges(total=config.max_changes, changetype="edit"):
        title = str(change.get("title") or "")
        creator = str(change.get("user") or "")
        comment = str(change.get("comment") or "")

        change_id = str(change.get("rcid") or change.get("revid") or f"{title}:{change.get('timestamp')}")

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

            target_revid, old_revid = _extract_change_revision_ids(change)
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
            added_text, _removed_text = _extract_changed_text(old_text, new_text)

            score, matched_patterns = _calculate_vandalism_score(added_text, new_text, old_text)
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
                if reverted:
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
                    if reverted:
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
                    else:
                        LOGGER.debug("Skip AI revert on %s: %s", title, revert_status)
                        log_server_action(
                            "ai_revert_skipped",
                            script_name=config.script_name,
                            context={"change_id": change_id, "title": title, "status": revert_status},
                        )
                elif ai_confidence >= config.review_threshold:
                    reason = f"review_needed: {ai_reason}"
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
                        context={"change_id": change_id, "title": title, "revid": target_revid, "old_revid": old_revid},
                    )
                except Exception as exc:
                    LOGGER.debug("Wiki log failed: %s", exc)
                    log_server_action(
                        "revert_wiki_log_failed",
                        script_name=config.script_name,
                        level="ERROR",
                        context={"change_id": change_id, "title": title, "error": str(exc)[:220]},
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

    confidences = metrics.get("confidences", [])
    average_confidence = sum(confidences) / len(confidences) if confidences else 0.0

    summary = (
        f"Analyse terminée - reverts session: {reverted_this_run} | "
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
            "total_analyzed": int(metrics.get("total_analyzed", 0)),
            "reverts_total": int(metrics.get("reverted", 0)),
            "errors": int(metrics.get("errors", 0)),
        },
    )
    return 0


def main_fr() -> int:
    return run(FR_CONFIG)


def main_en() -> int:
    return run(EN_CONFIG)


if __name__ == "__main__":
    raise SystemExit(main_fr())
