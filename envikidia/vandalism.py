#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from muffybot.tasks.vandalism import main_en


if __name__ == "__main__":
    raise SystemExit(main_en())
