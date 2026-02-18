# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from pathlib import Path

import pywikibot

from .healthcheck import ensure_started


def prepare_runtime(workdir: Path) -> None:
    workdir.mkdir(parents=True, exist_ok=True)
    os.chdir(workdir)
    ensure_started()


def connect_site(lang: str, family: str = "vikidia") -> pywikibot.Site:
    site = pywikibot.Site(lang, family)
    site.login()
    return site


def load_ignore_titles(site: pywikibot.Site, ignore_page_title: str) -> set[str]:
    page = pywikibot.Page(site, ignore_page_title)
    if not page.exists():
        return set()
    return {line.strip() for line in page.text.splitlines() if line.strip() and not line.strip().startswith("#")}
