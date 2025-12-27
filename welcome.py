#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script Pywikibot — Welcome new users after their first non-reverted edit

Ce script s'exécute sans arguments, parfait pour cron.
"""

from __future__ import annotations
import logging
import time
import ipaddress
import pywikibot
from pywikibot.data.api import Request

WELCOME_MESSAGE = "== Bienvenue ==\n{{subst:Bienvenue}} ~~~~"
EDIT_SUMMARY = "MuffyBot - Bienvenue"
SLEEP_BETWEEN_EDITS = 10
RC_LIMIT = 500  # Nombre de changements récents à scanner

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def is_ip(name: str) -> bool:
    try:
        ipaddress.ip_address(name)
        return True
    except Exception:
        return False


def user_is_bot(site: pywikibot.Site, username: str) -> bool:
    try:
        req = Request(site=site, parameters={"action": "query", "list": "users", "ususers": username, "usprop": "groups"})
        data = req.submit()
        users = data.get("query", {}).get("users", [])
        if not users:
            return False
        groups = users[0].get("groups", [])
        return any(g.lower() == "bot" for g in groups)
    except Exception as e:
        logger.debug(f"Impossible de déterminer si {username} est bot: {e}")
        return False


def already_welcomed(site: pywikibot.Site, username: str) -> bool:
    talkpage = pywikibot.Page(site, f"User talk:{username}")
    return talkpage.exists()


def has_non_reverted_edit(site: pywikibot.Site, username: str) -> bool:
    try:
        req = Request(site=site, parameters={"action": "query", "list": "usercontribs", "ucuser": username, "uclimit": "10", "ucprop": "ids|flags"})
        data = req.submit()
        contribs = data.get("query", {}).get("usercontribs", [])
        for c in contribs:
            if not c.get("flags") or "reverted" not in c.get("flags"):
                return True
        return False
    except Exception as e:
        logger.debug(f"Erreur lors de la vérification des contributions non révoquées pour {username}: {e}")
        return False


def post_welcome(site: pywikibot.Site, username: str) -> bool:
    talkpage = pywikibot.Page(site, f"User talk:{username}")

    if already_welcomed(site, username):
        logger.info(f"{username} a déjà une talkpage — saut.")
        return False

    if not has_non_reverted_edit(site, username):
        logger.debug(f"{username} n'a pas encore d'édition valide — saut.")
        return False

    message = f"{WELCOME_MESSAGE}"

    try:
        talkpage.text = message
        talkpage.save(summary=EDIT_SUMMARY, minor=True, botflag=True)
        logger.info(f"Message de bienvenue posté sur {talkpage.title()}")
        return True
    except pywikibot.Error as e:
        logger.error(f"Échec lors de l'édition de {talkpage.title()}: {e}")
        return False


def main():
    site = pywikibot.Site()
    site.login()
    logger.info(f"Connecté à {site.hostname()} en tant que {site.user()}")

    rc_gen = site.recentchanges(total=RC_LIMIT, changetype="edit")
    processed_users = set()

    for rc in rc_gen:
        try:
            user = rc.get("user")
            if not user or user in processed_users or is_ip(user) or user == site.user():
                continue
            processed_users.add(user)

            if user_is_bot(site, user):
                continue

            post_welcome(site, user)
            time.sleep(SLEEP_BETWEEN_EDITS)

        except Exception as e:
            logger.exception(f"Erreur en traitant une modification récente: {e}")
            continue

    logger.info("Traitement terminé.")


if __name__ == "__main__":
    main()
