# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import time
from datetime import datetime

import pywikibot

from muffybot.discord import log_server_action, log_server_diagnostic, log_to_discord, send_task_report
from muffybot.locking import LockUnavailableError, hold_lock
from muffybot.paths import ENVIKIDIA_DIR
from muffybot.task_control import report_lock_unavailable, save_page_or_dry_run
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
    return save_page_or_dry_run(
        page,
        script_name="envikidia/main.py",
        summary=f"Bot: create annual page {page_title}",
        minor=True,
        botflag=True,
        context={"page_title": page_title, "year": year},
    )


def run() -> int:
    started = time.monotonic()
    script_name = "envikidia/main.py"
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    prepare_runtime(ENVIKIDIA_DIR)
    try:
        with hold_lock("envikidia-annual-pages"):
            site = connect_site(lang="en", family="vikidia")

            year = datetime.utcnow().year
            created = 0
            dry_run_candidates = 0
            log_server_action("run_start", script_name=script_name, include_runtime=True, context={"year": year, "jobs": len(ANNUAL_PAGES)})

            for base_page, header_template, category_name in ANNUAL_PAGES:
                try:
                    log_server_action("inspect_annual_page", script_name=script_name, context={"base_page": base_page, "year": year})
                    if _create_annual_page(site, base_page, header_template, category_name, year):
                        created += 1
                        LOGGER.info("Created %s/%s", base_page, year)
                        log_server_action("annual_page_created", script_name=script_name, level="SUCCESS", context={"base_page": base_page, "year": year})
                    else:
                        # Not created either because it exists, or because dry-run intercepted save.
                        page_title = f"{base_page}/{year}"
                        page = pywikibot.Page(site, page_title)
                        if page.exists():
                            log_server_action("annual_page_exists", script_name=script_name, context={"base_page": base_page, "year": year})
                        else:
                            dry_run_candidates += 1
                            log_server_action("annual_page_dry_run", script_name=script_name, level="WARNING", context={"base_page": base_page, "year": year})
                except Exception as exc:
                    log_to_discord(f"Erreur annual page {base_page}: {exc}", level="ERROR", script_name=script_name)
                    log_server_action("annual_page_error", script_name=script_name, level="ERROR", context={"base_page": base_page, "year": year})
                    log_server_diagnostic(
                        message=f"Erreur annual page {base_page}",
                        level="ERROR",
                        script_name=script_name,
                        context={"base_page": base_page, "year": year},
                        exception=exc,
                    )

            summary = f"Annual pages check terminé, créations: {created}, dry_run={dry_run_candidates}"
            log_to_discord(summary, level="INFO", script_name=script_name)
            log_server_action(
                "run_end",
                script_name=script_name,
                level="SUCCESS",
                context={"created": created, "dry_run_candidates": dry_run_candidates, "year": year, "duration_seconds": round(time.monotonic() - started, 2)},
            )
            send_task_report(
                script_name=script_name,
                status="SUCCESS",
                duration_seconds=time.monotonic() - started,
                details=summary,
                stats={"pages_created": created, "dry_run_candidates": dry_run_candidates, "year": year},
            )
            return 0
    except LockUnavailableError:
        return report_lock_unavailable(script_name, started, "envikidia-annual-pages")


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
