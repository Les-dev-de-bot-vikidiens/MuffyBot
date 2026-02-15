#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse

from muffybot.tasks import categinex, homonym, welcome
from muffybot.tasks import envikidia_annual_pages, envikidia_sandboxreset, envikidia_weekly_talk
from muffybot.tasks.vandalism import main_en as vandalism_en_main
from muffybot.tasks.vandalism import main_fr as vandalism_fr_main

TASKS = {
    "welcome": welcome.main,
    "homonym": homonym.main,
    "categinex": categinex.main,
    "vandalism-fr": vandalism_fr_main,
    "vandalism-en": vandalism_en_main,
    "envikidia-annual": envikidia_annual_pages.main,
    "envikidia-sandboxreset": envikidia_sandboxreset.main,
    "envikidia-weekly-talk": envikidia_weekly_talk.main,
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a MuffyBot task")
    parser.add_argument("task", choices=sorted(TASKS.keys()), help="Task name to run")
    args = parser.parse_args()
    return TASKS[args.task]()


if __name__ == "__main__":
    raise SystemExit(main())
