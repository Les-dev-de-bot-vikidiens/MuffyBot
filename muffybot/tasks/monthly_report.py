# -*- coding: utf-8 -*-
from __future__ import annotations

from muffybot.tasks.daily_report import main_monthly


def main() -> int:
    return main_monthly()


if __name__ == "__main__":
    raise SystemExit(main())
