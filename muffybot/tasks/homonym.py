# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import time

import mwparserfromhell
import pywikibot

from muffybot.discord import log_server_action, log_server_diagnostic, log_to_discord, send_task_report
from muffybot.env import get_int_env, load_dotenv
from muffybot.logging_setup import configure_root_logging
from muffybot.locking import LockUnavailableError, hold_lock
from muffybot.paths import ROOT_DIR
from muffybot.task_control import report_lock_unavailable, save_page_or_dry_run
from muffybot.wiki import connect_site, load_ignore_titles, prepare_runtime

LOGGER = logging.getLogger(__name__)
IGNORE_PAGE = "Utilisateur:MuffyBot/Ignore"
HOMONYM_TEMPLATE_PAGE = "Modèle:Homonymie"


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
    script_name = "homonym.py"
    load_dotenv()
    configure_root_logging(logger_name=script_name)
    prepare_runtime(ROOT_DIR)
    max_pages = max(get_int_env("HOMONYM_MAX_PAGES_PER_RUN", 1500), 1)

    try:
        with hold_lock("homonym"):
            site = connect_site(lang="fr", family="vikidia")
            ignored = load_ignore_titles(site, IGNORE_PAGE)
            log_server_action(
                "run_start",
                script_name=script_name,
                include_runtime=True,
                context={"ignored_count": len(ignored), "max_pages": max_pages},
            )

            template_page = pywikibot.Page(site, HOMONYM_TEMPLATE_PAGE)
            if not template_page.exists():
                summary = f"Arrêt: template introuvable ({HOMONYM_TEMPLATE_PAGE})"
                log_to_discord(summary, level="ERROR", script_name=script_name)
                log_server_action(
                    "run_end_template_missing",
                    script_name=script_name,
                    level="ERROR",
                    context={"template_page": HOMONYM_TEMPLATE_PAGE},
                )
                send_task_report(
                    script_name=script_name,
                    status="FAILED",
                    duration_seconds=time.monotonic() - started,
                    details=summary,
                    stats={"template_page": HOMONYM_TEMPLATE_PAGE},
                    level="ERROR",
                )
                return 1

            updated_pages = 0
            removed_templates = 0
            inspected_pages = 0
            skipped_redirect = 0
            skipped_ignored = 0
            skipped_no_portail = 0
            parse_errors = 0
            save_errors = 0
            dry_run_candidates = 0

            for page in template_page.getReferences(
                only_template_inclusion=True,
                follow_redirects=False,
                with_template_inclusion=True,
                namespaces=[0],
                total=max_pages,
                content=True,
            ):
                title = page.title()
                inspected_pages += 1

                if page.isRedirectPage():
                    skipped_redirect += 1
                    continue
                if title in ignored:
                    skipped_ignored += 1
                    continue

                try:
                    wikicode = mwparserfromhell.parse(page.text)
                except Exception as exc:
                    parse_errors += 1
                    log_to_discord(f"Erreur lecture {title}: {exc}", level="ERROR", script_name=script_name)
                    log_server_action("parse_error", script_name=script_name, level="ERROR", context={"title": title})
                    log_server_diagnostic(
                        message=f"Erreur lecture homonymie: {title}",
                        level="ERROR",
                        script_name=script_name,
                        context={"title": title},
                        exception=exc,
                    )
                    continue

                if not _has_homonymie_template(wikicode):
                    continue

                removed = _remove_portail_templates(wikicode)
                if not removed:
                    skipped_no_portail += 1
                    continue

                page.text = str(wikicode)
                try:
                    saved = save_page_or_dry_run(
                        page,
                        script_name=script_name,
                        summary="Retrait du modèle Portail sur page d'homonymie",
                        minor=True,
                        botflag=True,
                        context={"title": title, "removed_templates": removed},
                    )
                    if saved:
                        updated_pages += 1
                        removed_templates += removed
                        LOGGER.info("Portails supprimés sur %s", title)
                        log_server_action(
                            "portail_removed",
                            script_name=script_name,
                            level="SUCCESS",
                            context={"title": title, "removed_templates": removed},
                        )
                    else:
                        dry_run_candidates += 1
                        removed_templates += removed
                        log_server_action(
                            "portail_remove_dry_run",
                            script_name=script_name,
                            level="WARNING",
                            context={"title": title, "removed_templates": removed},
                        )
                except Exception as exc:
                    save_errors += 1
                    log_to_discord(f"Erreur sauvegarde {title}: {exc}", level="ERROR", script_name=script_name)
                    log_server_action("save_error", script_name=script_name, level="ERROR", context={"title": title})
                    log_server_diagnostic(
                        message=f"Erreur sauvegarde homonymie: {title}",
                        level="ERROR",
                        script_name=script_name,
                        context={"title": title},
                        exception=exc,
                    )

            summary = (
                "Traitement terminé: "
                f"inspectées={inspected_pages}, modifiées={updated_pages}, "
                f"portails_retirés={removed_templates}, skip_redirect={skipped_redirect}, "
                f"skip_ignore={skipped_ignored}, skip_sans_portail={skipped_no_portail}, "
                f"erreurs_parse={parse_errors}, erreurs_save={save_errors}, dry_run={dry_run_candidates}"
            )
            status = "WARNING" if (parse_errors or save_errors) else "SUCCESS"
            level = "WARNING" if status == "WARNING" else "SUCCESS"
            log_to_discord(summary, level=level, script_name=script_name)
            log_server_action(
                "run_end",
                script_name=script_name,
                level=level,
                context={
                    "max_pages": max_pages,
                    "inspected_pages": inspected_pages,
                    "updated_pages": updated_pages,
                    "removed_templates": removed_templates,
                    "skipped_redirect": skipped_redirect,
                    "skipped_ignored": skipped_ignored,
                    "skipped_no_portail": skipped_no_portail,
                    "parse_errors": parse_errors,
                    "save_errors": save_errors,
                    "dry_run_candidates": dry_run_candidates,
                    "duration_seconds": round(time.monotonic() - started, 2),
                },
            )
            send_task_report(
                script_name=script_name,
                status=status,
                duration_seconds=time.monotonic() - started,
                details=summary,
                stats={
                    "max_pages": max_pages,
                    "inspected_pages": inspected_pages,
                    "pages_updated": updated_pages,
                    "templates_removed": removed_templates,
                    "parse_errors": parse_errors,
                    "save_errors": save_errors,
                    "dry_run_candidates": dry_run_candidates,
                },
                level=level,
            )
            return 0
    except LockUnavailableError:
        LOGGER.warning("Exécution ignorée: une autre instance homonym.py est déjà en cours.")
        return report_lock_unavailable(script_name, started, "homonym")


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
