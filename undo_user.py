#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import os
import sys
from typing import Any

import pywikibot


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Undo des contributions d'un utilisateur (MediaWiki undo).")
    parser.add_argument("username", help="Nom de l'utilisateur cible a undo")
    parser.add_argument("--lang", default=os.getenv("MUFFYBOT_UNDO_LANG", "fr"), help="Langue du wiki (defaut: fr)")
    parser.add_argument("--family", default=os.getenv("MUFFYBOT_UNDO_FAMILY", "vikidia"), help="Famille wiki (defaut: vikidia)")
    parser.add_argument(
        "--max-edits",
        type=int,
        default=max(1, min(int(os.getenv("MUFFYBOT_UNDO_MAX_EDITS", "30")), 200)),
        help="Nombre max d'edits a undo (defaut: 30)",
    )
    parser.add_argument(
        "--include-non-top",
        action="store_true",
        help="Inclure les edits qui ne sont plus la derniere revision de la page",
    )
    return parser


def undo_revision(
    site: pywikibot.Site,
    *,
    title: str,
    revid: int,
    summary: str,
    dry_run: bool,
) -> tuple[bool, str]:
    if dry_run:
        return True, "dry_run"

    try:
        request = site.simple_request(
            action="edit",
            title=title,
            undo=revid,
            summary=summary,
            bot="1",
            nocreate="1",
            token=site.tokens["csrf"],
        )
        data: dict[str, Any] = request.submit()
    except Exception as exc:
        return False, f"api_error:{exc}"

    if "error" in data:
        err = data.get("error") or {}
        return False, f"api_error:{err.get('code', 'unknown')}"

    if "edit" in data and str(data["edit"].get("result", "")).lower() == "success":
        return True, "ok"
    return False, "unknown_result"


def main() -> int:
    if not os.getenv("LUFFYBOT_RUN_ID"):
        print("Refus execution: ce script est reserve aux runs lances par le bot Discord (LUFFYBOT_RUN_ID manquant).")
        return 2

    args = build_parser().parse_args()
    username = args.username.strip()
    if not username:
        print("Username vide.")
        return 2

    max_edits = max(1, min(int(args.max_edits), 200))
    include_non_top = bool(args.include_non_top)
    dry_run = str(os.getenv("MUFFYBOT_DRY_RUN", "")).strip().lower() in {"1", "true", "yes", "on"}
    requester = str(os.getenv("MUFFYBOT_UNDO_REQUESTER_DISCORD_ID", "")).strip()

    site = pywikibot.Site(args.lang, args.family)
    site.login()

    summary = f"Undo automatique des modifications de {username} (commande Discord)"
    if requester:
        summary += f" - requester:{requester}"

    scanned = 0
    undone = 0
    skipped_non_top = 0
    failed = 0
    seen_revids: set[int] = set()

    for contrib in site.usercontribs(user=username, total=max_edits * 6):
        revid = int(contrib.get("revid") or 0)
        if revid <= 0 or revid in seen_revids:
            continue
        seen_revids.add(revid)

        scanned += 1
        is_top = bool(contrib.get("top"))
        if not include_non_top and not is_top:
            skipped_non_top += 1
            if scanned >= max_edits * 3:
                break
            continue

        title = str(contrib.get("title") or "").strip()
        if not title:
            failed += 1
            continue

        ok, detail = undo_revision(site, title=title, revid=revid, summary=summary, dry_run=dry_run)
        if ok:
            undone += 1
            print(f"[OK] undo rev={revid} page={title} ({detail})")
        else:
            failed += 1
            print(f"[FAIL] undo rev={revid} page={title} ({detail})")

        if undone >= max_edits:
            break

    print(
        f"Resultat undo username={username} scanned={scanned} undone={undone} "
        f"failed={failed} skipped_non_top={skipped_non_top} dry_run={int(dry_run)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
