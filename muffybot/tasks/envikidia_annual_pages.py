# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import time
from datetime import datetime

import pywikibot

from muffybot.discord import log_to_discord, send_task_report
from muffybot.paths import ENVIKIDIA_DIR
from muffybot.wiki import connect_site, prepare_runtime

LOGGER = logging.getLogger(__name__)

ANNUAL_PAGES = [
    ("Vikidia:Requests/Administrators", "Vikidia:Requests/Administrators/Header", "Requests for administrators"),
    ("Vikidia:The Scholar", "Vikidia:The Scholar/Header", "The Scholar"),
    ("Vikidia:Requests/Bureaucrats", "Vikidia:Requests/Bureaucrats/Header", "Requests for bureaucrats"),
    ("Vikidia:Requests/CheckUsers", "Vikidia:Requests/CheckUsers/Header", "Requests for CheckUsers"),
    ("Vikidia:Requests/Bots", "Vikidia:Requests/Bots/Header", "Requests for bots"),
]


def _create_annual_page(site: pywikibot.Site, base_page: str, header_template: str, category_name: str, year: int) -> bool:
    page_title = f"{base_page}/{year}"
    page = pywikibot.Page(site, page_title)
    if page.exists():
        return False

    page.text = f"<noinclude>{{{{{header_template}}}}}\n[[Category:{category_name}|{{{{SUBPAGENAME}}}}]]</noinclude>"
    page.save(summary=f"Bot: create annual page {page_title}", minor=True, botflag=True)
    return True


def run() -> int:
    started = time.monotonic()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    prepare_runtime(ENVIKIDIA_DIR)
    site = connect_site(lang="en", family="vikidia")

    year = datetime.utcnow().year
    created = 0

    for base_page, header_template, category_name in ANNUAL_PAGES:
        try:
            if _create_annual_page(site, base_page, header_template, category_name, year):
                created += 1
                LOGGER.info("Created %s/%s", base_page, year)
        except Exception as exc:
            log_to_discord(f"Erreur annual page {base_page}: {exc}", level="ERROR", script_name="envikidia/main.py")

    summary = f"Annual pages check terminé, créations: {created}"
    log_to_discord(summary, level="INFO", script_name="envikidia/main.py")
    send_task_report(
        script_name="envikidia/main.py",
        status="SUCCESS",
        duration_seconds=time.monotonic() - started,
        details=summary,
        stats={"pages_created": created, "year": year},
    )
    return 0


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
