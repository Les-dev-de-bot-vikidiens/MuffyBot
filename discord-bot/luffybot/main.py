#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging

from . import config
from . import commands  # noqa: F401 - registers events/commands
from .runtime import load_token
from .storage import init_db


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")


def main() -> None:
    configure_logging()
    init_db()
    token = load_token()
    config.bot.run(token)


if __name__ == "__main__":
    main()
