# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import re
import threading
import time
from pathlib import Path

import pywikibot
import requests

from muffybot.discord import log_to_discord, send_task_report
from muffybot.env import get_env, load_dotenv
from muffybot.files import read_line_set, write_lines
from muffybot.paths import ROOT_DIR
from muffybot.wiki import connect_site, load_ignore_titles, prepare_runtime

LOGGER = logging.getLogger(__name__)

IGNORE_PAGE = "Utilisateur:MuffyBot/Ignore"
PROCESSED_FILE = ROOT_DIR / "pages_traitees.txt"
PING_URL = get_env("STATUS_URL", "https://bothulkvikidia.pythonanywhere.com/status")
ENABLE_PING = (get_env("ENABLE_STATUS_PING", "0") or "0") == "1"


def _start_status_ping() -> None:
    if not ENABLE_PING:
        return

    def _loop() -> None:
        while True:
            try:
                requests.get(PING_URL, timeout=10)
            except Exception:
                pass
            time.sleep(60)

    thread = threading.Thread(target=_loop, daemon=True)
    thread.start()


def _remove_nonexistent_categories(page: pywikibot.Page) -> list[str]:
    original_text = page.text
    updated_text = original_text
    removed_categories: list[str] = []

    for category in page.categories():
        if category.exists():
            continue

        category_title = category.title(with_ns=True)
        escaped = re.escape(category_title).replace(r"\ ", r"[ _]")
        pattern = re.compile(rf"\[\[\s*{escaped}\s*(?:\|[^\]]*)?\]\]\s*", flags=re.IGNORECASE)
        updated_text, replaced = pattern.subn("", updated_text)
        if replaced:
            removed_categories.append(category_title)

    if updated_text != original_text:
        page.text = updated_text
    return removed_categories


def run() -> int:
    started = time.monotonic()
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    prepare_runtime(ROOT_DIR)
    _start_status_ping()

    site = connect_site(lang="fr", family="vikidia")
    ignored_pages = load_ignore_titles(site, IGNORE_PAGE)
    processed_pages = read_line_set(PROCESSED_FILE)

    newly_processed: set[str] = set()
    changed_pages = 0

    for page in site.allpages(namespace=0):
        title = page.title()

        if title in ignored_pages or title in processed_pages:
            continue

        try:
            removed_categories = _remove_nonexistent_categories(page)
            if removed_categories:
                summary = "Suppression de catégories inexistantes"
                page.save(summary=summary, minor=True, botflag=True)
                changed_pages += 1
                log_to_discord(
                    f"{title}: {len(removed_categories)} catégories retirées",
                    level="SUCCESS",
                    script_name="categinex.py",
                )
            newly_processed.add(title)
        except Exception as exc:
            log_to_discord(f"Erreur sur {title}: {exc}", level="ERROR", script_name="categinex.py")

    if newly_processed:
        all_processed = sorted(processed_pages | newly_processed)
        write_lines(PROCESSED_FILE, all_processed)

    inspected_count = len(newly_processed)
    summary = f"Traitement terminé: {changed_pages} pages modifiées, {inspected_count} pages inspectées"
    LOGGER.info("Analyse terminée. Pages modifiées: %s", changed_pages)
    log_to_discord(summary, level="INFO", script_name="categinex.py")
    send_task_report(
        script_name="categinex.py",
        status="SUCCESS",
        duration_seconds=time.monotonic() - started,
        details=summary,
        stats={"pages_changed": changed_pages, "pages_inspected": inspected_count},
    )
    return 0


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
