from __future__ import annotations

import re
from typing import Optional

from profit.catalog.entity_store import validate_entity_id

# Minimal, extendable mapping from yfinance exchange codes to ISO 10383 MICs.
_EXCHANGE_TO_MIC = {
    "nasdaq": "xnas",
    "nyse": "xnys",
    "nysearca": "arcx",
    "amex": "arcx",
    "bats": "bats",
    "tsx": "xtsx",
    "lse": "xlon",
    "asx": "xasx",
    "six": "xswx",
    "euronext": "xpar",  # default to Paris when yfinance only gives 'EURONEXT'
    "epa": "xpar",
    "xetra": "xetr",
    "fra": "xfra",
    "bom": "xbom",
    "nse": "xnse",
    "tse": "xtks",  # Tokyo
    "hkg": "xhkg",
    "sse": "xshg",  # Shanghai Stock Exchange main board
    "szse": "xshe",
}


_INVALID_SYMBOL_CHARS = re.compile(r"[^a-z0-9_-]")


def normalize_symbol(raw: str) -> str:
    """
    Normalize a ticker for slug use:
    - lowercase
    - convert '.' to '-'
    - strip spaces
    - drop characters outside [a-z0-9_-]
    """
    s = raw.strip().lower().replace(".", "-").replace(" ", "")
    s = _INVALID_SYMBOL_CHARS.sub("", s)
    if not s:
        raise ValueError(f"empty normalized symbol from {raw!r}")
    return s


def exchange_to_mic(exchange: str) -> Optional[str]:
    """
    Map yfinance exchange strings to MICs (best effort).
    Returns lowercase MIC or None when unknown.
    """
    code = exchange.strip().lower()
    return _EXCHANGE_TO_MIC.get(code)


def make_entity_id(exchange: str, symbol: str) -> str:
    """
    Build a deterministic, human-readable entity_id for yfinance equities.
    Shape: company:{mic}:{symbol_norm} when MIC is known; otherwise
    company:xref:{exchange_norm}:{symbol_norm}.
    """
    symbol_norm = normalize_symbol(symbol)
    mic = exchange_to_mic(exchange)
    if mic:
        entity_id = f"company:{mic}:{symbol_norm}"
    else:
        exchange_norm = normalize_symbol(exchange)
        entity_id = f"company:xref:{exchange_norm}:{symbol_norm}"
    validate_entity_id(entity_id)
    return entity_id


__all__ = ["normalize_symbol", "exchange_to_mic", "make_entity_id"]
