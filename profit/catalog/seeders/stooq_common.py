from __future__ import annotations

from typing import Iterable, TextIO

import csv
from datetime import datetime, timezone
from pathlib import Path

try:
    import pandas as pd
except ImportError:  # pragma: no cover - pandas optional
    pd = None

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
    # Crypto: prefer explicit category over suffix-based routing (e.g., BTC.V -> CRYPTO|BTC).
    if "cryptocurrencies" in parts:
        base = ticker.split(".", 1)[0]
        return f"CRYPTO|{base}"

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


STOOQ_COLUMNS = ["TICKER", "PER", "DATE", "TIME", "OPEN", "HIGH", "LOW", "CLOSE", "VOL", "OPENINT"]
NUMERIC_COLUMNS = ["OPEN", "HIGH", "LOW", "CLOSE", "VOL", "OPENINT"]


def iterate_stooq_rows(txt: Path) -> Iterable[dict]:
    if pd is not None:
        yield from _iter_with_pandas(txt)
    else:
        yield from _iter_with_csv(txt)


def iterate_stooq_rows_file(handle: TextIO, name: str = "") -> Iterable[dict]:
    """
    Parse Stooq CSV rows from an already-open text handle (e.g., inside a zip).
    """
    reader = csv.reader(handle)
    next(reader, None)
    for row in reader:
        if len(row) < 9:
            continue
        per = row[1]
        if per != "D":
            continue
        date_obj = _parse_date(row[2])
        if date_obj is None:
            continue
        try:
            open_val = float(row[4])
            high_val = float(row[5])
            low_val = float(row[6])
            close_val = float(row[7])
            vol_val = float(row[8])
            openint_val = float(row[9]) if len(row) > 9 and row[9] else 0.0
        except ValueError:
            continue
        yield {
            "ticker": row[0].upper(),
            "date": date_obj,
            "open": open_val,
            "high": high_val,
            "low": low_val,
            "close": close_val,
            "volume": vol_val,
            "openint": openint_val,
        }


def _iter_with_csv(txt: Path) -> Iterable[dict]:
    with txt.open("r", newline="") as fh:
        reader = csv.reader(fh)
        next(reader, None)
        for row in reader:
            if len(row) < 9:
                continue
            per = row[1]
            if per != "D":
                continue
            date_obj = _parse_date(row[2])
            if date_obj is None:
                continue
            try:
                open_val = float(row[4])
                high_val = float(row[5])
                low_val = float(row[6])
                close_val = float(row[7])
                vol_val = float(row[8])
                openint_val = float(row[9]) if len(row) > 9 and row[9] else 0.0
            except ValueError:
                continue
            yield {
                "ticker": row[0].upper(),
                "date": date_obj,
                "open": open_val,
                "high": high_val,
                "low": low_val,
                "close": close_val,
                "volume": vol_val,
                "openint": openint_val,
            }


def _iter_with_pandas(txt: Path) -> Iterable[dict]:
    df = pd.read_csv(txt, header=0, names=STOOQ_COLUMNS, usecols=STOOQ_COLUMNS, dtype=str)
    df = df[df["PER"] == "D"].copy()
    df["DATE"] = pd.to_datetime(df["DATE"], format="%Y%m%d", errors="coerce")
    df = df[df["DATE"].notna()]
    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["OPEN", "HIGH", "LOW", "CLOSE", "VOL", "DATE"])
    if "OPENINT" in df.columns:
        df["OPENINT"] = df["OPENINT"].fillna(0.0)
    for row in df.itertuples(index=False):
        date_val = row.DATE.to_pydatetime().replace(tzinfo=timezone.utc)
        yield {
            "ticker": row.TICKER.upper(),
            "date": date_val,
            "open": float(row.OPEN),
            "high": float(row.HIGH),
            "low": float(row.LOW),
            "close": float(row.CLOSE),
            "volume": float(row.VOL),
            "openint": float(row.OPENINT) if getattr(row, "OPENINT", None) is not None else 0.0,
        }


def _parse_date(value: str) -> datetime | None:
    try:
        return datetime.strptime(value, "%Y%m%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
