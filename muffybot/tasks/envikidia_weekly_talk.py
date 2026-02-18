# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import time
from datetime import datetime

import pywikibot

from muffybot.discord import log_server_action, log_to_discord, send_task_report
from muffybot.locking import LockUnavailableError, hold_lock
from muffybot.paths import ENVIKIDIA_DIR
from muffybot.task_control import report_lock_unavailable, save_page_or_dry_run
from muffybot.wiki import connect_site, prepare_runtime

LOGGER = logging.getLogger(__name__)


def run() -> int:
    started = time.monotonic()
    script_name = "envikidia/semaine.py"
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    prepare_runtime(ENVIKIDIA_DIR)
    try:
        with hold_lock("envikidia-weekly-talk"):
            site = connect_site(lang="en", family="vikidia")

            now = datetime.utcnow()
            year, week_number, _ = now.isocalendar()

            page_title = f"Vikidia:Talk/{year}/{week_number:02d}"
            page = pywikibot.Page(site, page_title)
            log_server_action("run_start", script_name=script_name, include_runtime=True, context={"page_title": page_title, "year": year, "week": week_number})

            if page.exists():
                LOGGER.info("%s already exists", page_title)
                log_server_action("weekly_talk_exists", script_name=script_name, context={"page_title": page_title})
                send_task_report(
                    script_name=script_name,
                    status="INFO",
                    duration_seconds=time.monotonic() - started,
                    details=f"{page_title} already exists",
                )
                return 0

            page.text = "<noinclude> {{Vikidia:Talk/Head}} </noinclude>"
            saved = save_page_or_dry_run(
                page,
                script_name=script_name,
                summary=f"Bot: create weekly Vikidia:Talk page for week {week_number}",
                minor=True,
                botflag=True,
                context={"page_title": page_title, "year": year, "week": week_number},
            )

            details = f"Page créée: {page_title}" if saved else f"Création simulée (dry-run): {page_title}"
            log_to_discord(details, level="SUCCESS" if saved else "WARNING", script_name=script_name)
            log_server_action(
                "weekly_talk_created",
                script_name=script_name,
                level="SUCCESS" if saved else "WARNING",
                context={"page_title": page_title, "year": year, "week": week_number, "dry_run": int(not saved)},
            )
            send_task_report(
                script_name=script_name,
                status="SUCCESS" if saved else "WARNING",
                duration_seconds=time.monotonic() - started,
                details=details,
                stats={"year": year, "week": week_number, "dry_run": int(not saved)},
            )
            return 0
    except LockUnavailableError:
        return report_lock_unavailable(script_name, started, "envikidia-weekly-talk")


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
