# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

import pywikibot

from muffybot.discord import log_server_action, log_to_discord, send_task_report
from muffybot.locking import LockUnavailableError, hold_lock
from muffybot.paths import ENVIKIDIA_DIR
from muffybot.task_control import report_lock_unavailable, save_page_or_dry_run
from muffybot.wiki import connect_site, prepare_runtime

LOGGER = logging.getLogger(__name__)

PAGE_TITLE = "Vikidia:Sandbox"
RESET_CONTENT = "<!-- PLEASE DO NOT MODIFY THIS LINE -->{{/Header}}<!-- PLEASE DO NOT MODIFY THIS LINE -->"
DELAY_MINUTES = 3


def run() -> int:
    started = time.monotonic()
    script_name = "envikidia/sandboxreset.py"
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    prepare_runtime(ENVIKIDIA_DIR)
    try:
        with hold_lock("envikidia-sandboxreset"):
            site = connect_site(lang="en", family="vikidia")
            log_server_action("run_start", script_name=script_name, include_runtime=True, context={"page_title": PAGE_TITLE})

            page = pywikibot.Page(site, PAGE_TITLE)
            if not page.exists():
                details = "Sandbox inexistante"
                log_to_discord(details, level="WARNING", script_name=script_name)
                log_server_action("sandbox_missing", script_name=script_name, level="WARNING", context={"page_title": PAGE_TITLE})
                send_task_report(
                    script_name=script_name,
                    status="WARNING",
                    duration_seconds=time.monotonic() - started,
                    details=details,
                )
                return 0

            last_revision = next(page.revisions(total=1))
            now = datetime.now(timezone.utc)
            last_edit_time = last_revision.timestamp.replace(tzinfo=timezone.utc)

            if now - last_edit_time < timedelta(minutes=DELAY_MINUTES):
                LOGGER.info("Dernière édition trop récente, reset ignoré")
                log_server_action(
                    "sandbox_skip_recent_edit",
                    script_name=script_name,
                    context={"page_title": PAGE_TITLE, "minutes_since_last_edit": round((now - last_edit_time).total_seconds() / 60, 2)},
                )
                send_task_report(
                    script_name=script_name,
                    status="INFO",
                    duration_seconds=time.monotonic() - started,
                    details="Reset ignoré: dernière édition trop récente",
                )
                return 0

            if page.text.strip() == RESET_CONTENT:
                LOGGER.info("Sandbox déjà au contenu par défaut")
                log_server_action("sandbox_skip_already_default", script_name=script_name, context={"page_title": PAGE_TITLE})
                send_task_report(
                    script_name=script_name,
                    status="INFO",
                    duration_seconds=time.monotonic() - started,
                    details="Reset ignoré: sandbox déjà au contenu par défaut",
                )
                return 0

            page.text = RESET_CONTENT
            saved = save_page_or_dry_run(
                page,
                script_name=script_name,
                summary="Bot: reset sandbox to default content",
                minor=False,
                botflag=True,
                context={"page_title": PAGE_TITLE},
            )
            details = "Sandbox reset effectuée" if saved else "Sandbox reset simulé (dry-run)"
            log_to_discord(details, level="SUCCESS" if saved else "WARNING", script_name=script_name)
            log_server_action(
                "sandbox_reset_done",
                script_name=script_name,
                level="SUCCESS" if saved else "WARNING",
                context={"page_title": PAGE_TITLE, "dry_run": int(not saved)},
            )
            send_task_report(
                script_name=script_name,
                status="SUCCESS" if saved else "WARNING",
                duration_seconds=time.monotonic() - started,
                details=details,
                stats={"dry_run": int(not saved)},
            )
            return 0
    except LockUnavailableError:
        return report_lock_unavailable(script_name, started, "envikidia-sandboxreset")


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
