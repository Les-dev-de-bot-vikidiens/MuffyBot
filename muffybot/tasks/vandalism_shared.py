# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import re
import unicodedata

SPACE_RE = re.compile(r"\s+")
TOKEN_RE = re.compile(r"[a-z0-9à-öø-ÿ_'-]{4,40}", flags=re.IGNORECASE)

LEET_TRANSLATION = str.maketrans(
    {
        "0": "o",
        "1": "i",
        "2": "z",
        "3": "e",
        "4": "a",
        "5": "s",
        "6": "g",
        "7": "t",
        "8": "b",
        "9": "g",
        "@": "a",
        "$": "s",
        "!": "i",
    }
)

HOMOGLYPH_MAP = {
    "а": "a",
    "е": "e",
    "о": "o",
    "р": "p",
    "с": "c",
    "у": "y",
    "х": "x",
    "і": "i",
    "ј": "j",
    "ԁ": "d",
    "ｍ": "m",
    "ｏ": "o",
    "ｌ": "l",
    "ｉ": "i",
    "Ａ": "a",
    "Β": "b",
    "Ε": "e",
    "Ζ": "z",
    "Η": "h",
    "Ι": "i",
    "Κ": "k",
    "Μ": "m",
    "Ν": "n",
    "Ο": "o",
    "Ρ": "p",
    "Τ": "t",
    "Χ": "x",
    "Υ": "y",
}


def normalize_detection_text(text: str) -> str:
    raw = unicodedata.normalize("NFKC", str(text or ""))
    replaced = "".join(HOMOGLYPH_MAP.get(ch, ch) for ch in raw)
    replaced = replaced.translate(LEET_TRANSLATION)
    lowered = replaced.casefold()
    return SPACE_RE.sub(" ", lowered).strip()


def tokenize_training_text(text: str, *, min_len: int = 4) -> list[str]:
    normalized = normalize_detection_text(text)
    tokens: list[str] = []
    for match in TOKEN_RE.finditer(normalized):
        token = match.group(0)
        if len(token) < min_len:
            continue
        tokens.append(token)
    return tokens


def holdout_bucket(value: str) -> int:
    digest = hashlib.sha1(str(value).encode("utf-8")).hexdigest()[:8]
    return int(digest, 16) % 100

