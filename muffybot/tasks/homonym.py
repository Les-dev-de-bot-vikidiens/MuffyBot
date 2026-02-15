# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import time

import mwparserfromhell
import pywikibot

from muffybot.discord import log_to_discord, send_task_report
from muffybot.paths import ROOT_DIR
from muffybot.wiki import connect_site, load_ignore_titles, prepare_runtime

LOGGER = logging.getLogger(__name__)
IGNORE_PAGE = "Utilisateur:MuffyBot/Ignore"


def _normalize_template_name(name: str) -> str:
    return str(name).strip().lower().replace("_", " ")


def _has_homonymie_template(wikicode: mwparserfromhell.wikicode.Wikicode) -> bool:
    return any(_normalize_template_name(tpl.name) == "homonymie" for tpl in wikicode.filter_templates())


def _remove_portail_templates(wikicode: mwparserfromhell.wikicode.Wikicode) -> int:
    removed = 0
    for tpl in list(wikicode.filter_templates()):
        if _normalize_template_name(tpl.name) == "portail":
            wikicode.remove(tpl)
            removed += 1
    return removed


def run() -> int:
    started = time.monotonic()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    prepare_runtime(ROOT_DIR)
    site = connect_site(lang="fr", family="vikidia")
    ignored = load_ignore_titles(site, IGNORE_PAGE)

    updated_pages = 0
    removed_templates = 0

    for page in site.allpages(namespace=0):
        title = page.title()

        if page.isRedirectPage() or title in ignored:
            continue

        try:
            wikicode = mwparserfromhell.parse(page.text)
        except Exception as exc:
            log_to_discord(f"Erreur lecture {title}: {exc}", level="ERROR", script_name="homonym.py")
            continue

        if not _has_homonymie_template(wikicode):
            continue

        removed = _remove_portail_templates(wikicode)
        if not removed:
            continue

        page.text = str(wikicode)
        try:
            page.save(summary="Retrait du modèle Portail sur page d'homonymie", minor=True, botflag=True)
            updated_pages += 1
            removed_templates += removed
            LOGGER.info("Portails supprimés sur %s", title)
        except Exception as exc:
            log_to_discord(f"Erreur sauvegarde {title}: {exc}", level="ERROR", script_name="homonym.py")

    summary = f"Traitement terminé: {updated_pages} pages, {removed_templates} modèles retirés"
    log_to_discord(summary, level="SUCCESS", script_name="homonym.py")
    send_task_report(
        script_name="homonym.py",
        status="SUCCESS",
        duration_seconds=time.monotonic() - started,
        details=summary,
        stats={"pages_updated": updated_pages, "templates_removed": removed_templates},
    )
    return 0


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
