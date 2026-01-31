from __future__ import annotations

from typing import Iterable

TYPE_MAP = {
    "currencies": "currency",
    "money market": "money_market",
    "indices": "index",
    "cryptocurrencies": "cryptocurrency",
    "bonds": "bond",
    "stooq stocks indices": "equity",
}

CANONICAL_PREFIX = {
    "currencies": "FX",
    "money market": "MM",
    "indices": "INDEX",
    "cryptocurrencies": "CRYPTO",
    "bonds": "BOND",
}

EXCHANGE_SUFFIX_MAP = {
    "US": "XNAS",
    "FT": "XLON",
    "L": "XLON",
    "DE": "XFRA",
    "HK": "XHKG",
    "F": "XPAR",
    "SW": "XSWX",
    "AS": "XASE",
    "B": "XLON",
    "V": "XSWX",
    "X": "XAMS",
    "ST": "XSTO",
}


def guess_type(parts: Iterable[str], ticker: str) -> str:
    if ticker.startswith("^"):
        return "synthetic"
    for part in parts:
        cleaned = part.replace("-", " ")
        if cleaned in TYPE_MAP:
            return TYPE_MAP[cleaned]
    return "unknown"


def canonical_instrument_id(ticker: str, parts: list[str]) -> str:
    if "." in ticker:
        base, suffix = ticker.split(".", 1)
        exchange = EXCHANGE_SUFFIX_MAP.get(suffix.upper(), suffix.upper())
        return f"{exchange}|{base}"

    for part in reversed(parts):
        if part in CANONICAL_PREFIX:
            prefix = CANONICAL_PREFIX[part]
            return f"{prefix}|{ticker}"
    return f"STOOQ|{ticker}"


def exchange_for_ticker(ticker: str, parts: Iterable[str]) -> str:
    for part in parts:
        if part == "cryptocurrencies":
            return "CRYPTO"
    if "." in ticker:
        _, suffix = ticker.split(".", 1)
        return EXCHANGE_SUFFIX_MAP.get(suffix.upper(), suffix.upper())
    for part in parts:
        if part in EXCHANGE_SUFFIX_MAP.values():
            return part
    return ""
