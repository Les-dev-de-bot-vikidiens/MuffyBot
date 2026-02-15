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


def run() -> int:
    started = time.monotonic()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    prepare_runtime(ENVIKIDIA_DIR)
    site = connect_site(lang="en", family="vikidia")

    now = datetime.utcnow()
    year, week_number, _ = now.isocalendar()

    page_title = f"Vikidia:Talk/{year}/{week_number:02d}"
    page = pywikibot.Page(site, page_title)

    if page.exists():
        LOGGER.info("%s already exists", page_title)
        send_task_report(
            script_name="envikidia/semaine.py",
            status="INFO",
            duration_seconds=time.monotonic() - started,
            details=f"{page_title} already exists",
        )
        return 0

    page.text = "<noinclude> {{Vikidia:Talk/Head}} </noinclude>"
    page.save(summary=f"Bot: create weekly Vikidia:Talk page for week {week_number}", minor=True, botflag=True)

    details = f"Page créée: {page_title}"
    log_to_discord(details, level="SUCCESS", script_name="envikidia/semaine.py")
    send_task_report(
        script_name="envikidia/semaine.py",
        status="SUCCESS",
        duration_seconds=time.monotonic() - started,
        details=details,
        stats={"year": year, "week": week_number},
    )
    return 0


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
