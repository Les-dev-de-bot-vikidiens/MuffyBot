# -*- coding: utf-8 -*-
from __future__ import annotations

import ipaddress
import logging
import time

import pywikibot
from pywikibot.data.api import Request
from pywikibot.exceptions import Error as PywikibotError

from muffybot.discord import log_to_discord, send_task_report
from muffybot.paths import ROOT_DIR
from muffybot.wiki import connect_site, prepare_runtime

LOGGER = logging.getLogger(__name__)

WELCOME_MESSAGE = "== Bienvenue ==\n{{subst:Bienvenue}} ~~~~"
EDIT_SUMMARY = "MuffyBot - Bienvenue"
SLEEP_BETWEEN_EDITS = 10
RC_LIMIT = 500


def _is_ip(username: str) -> bool:
    try:
        ipaddress.ip_address(username)
        return True
    except Exception:
        return False


def _user_is_bot(site: pywikibot.Site, username: str) -> bool:
    try:
        request = Request(
            site=site,
            parameters={
                "action": "query",
                "list": "users",
                "ususers": username,
                "usprop": "groups",
            },
        )
        data = request.submit()
        users = data.get("query", {}).get("users", [])
        if not users:
            return False
        groups = {group.lower() for group in users[0].get("groups", [])}
        return "bot" in groups
    except Exception:
        return False


def _already_welcomed(site: pywikibot.Site, username: str) -> bool:
    talk_page = pywikibot.Page(site, f"User talk:{username}")
    return talk_page.exists()


def _has_viable_contribution(site: pywikibot.Site, username: str, limit: int = 20) -> bool:
    try:
        request = Request(
            site=site,
            parameters={
                "action": "query",
                "list": "usercontribs",
                "ucuser": username,
                "uclimit": str(limit),
                "ucprop": "ids|flags|comment|tags",
            },
        )
        data = request.submit()
        contributions = data.get("query", {}).get("usercontribs", [])
        for contribution in contributions:
            tags = {tag.lower() for tag in contribution.get("tags", [])}
            comment = (contribution.get("comment") or "").lower()
            if {"mw-rollback", "mw-undo", "mw-manual-revert"} & tags:
                continue
            if "revert" in comment or "révo" in comment or "annulation" in comment:
                continue
            return True
        return False
    except Exception:
        return False


def _post_welcome(site: pywikibot.Site, username: str) -> bool:
    if _already_welcomed(site, username):
        return False
    if not _has_viable_contribution(site, username):
        return False

    talk_page = pywikibot.Page(site, f"User talk:{username}")
    talk_page.text = WELCOME_MESSAGE
    try:
        talk_page.save(summary=EDIT_SUMMARY, minor=True, botflag=True)
        return True
    except PywikibotError:
        return False


def run() -> int:
    started = time.monotonic()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    prepare_runtime(ROOT_DIR)
    site = connect_site(lang="fr", family="vikidia")

    log_to_discord("Connexion réussie", level="INFO", script_name="welcome.py")

    welcomed = 0
    inspected_users: set[str] = set()

    for change in site.recentchanges(total=RC_LIMIT, changetype="edit"):
        user = change.get("user")
        if not user or user in inspected_users:
            continue
        inspected_users.add(user)

        if user == site.user() or _is_ip(user) or _user_is_bot(site, user):
            continue

        try:
            if _post_welcome(site, user):
                welcomed += 1
                LOGGER.info("Bienvenue postée à %s", user)
                time.sleep(SLEEP_BETWEEN_EDITS)
        except Exception as exc:
            log_to_discord(f"Erreur pour {user}: {exc}", level="ERROR", script_name="welcome.py")

    duration = time.monotonic() - started
    summary = f"Traitement terminé, {welcomed} nouveaux messages"
    log_to_discord(summary, level="SUCCESS", script_name="welcome.py")
    send_task_report(
        script_name="welcome.py",
        status="SUCCESS",
        duration_seconds=duration,
        details=summary,
        stats={"welcomed": welcomed, "users_scanned": len(inspected_users)},
    )
    return 0


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
