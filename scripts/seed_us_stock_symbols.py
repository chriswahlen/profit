#!/usr/bin/env python3
"""Seed US stock symbols from a local CSV into the entity store.

Expected CSV columns (case-insensitive subset):
- Symbol (ticker)
- Name (company/security name)
- Exchange (e.g., NASDAQ, NYSE, AMEX)

Canonical IDs created as `sec:<mic>:<symbol_lower>`, using MIC map:
  NASDAQ -> XNAS, NYSE -> XNYS, AMEX/NYSEMKT -> XASE.

Provider mappings:
- Maps the CSV ticker to provider `provider:us-stock-symbols`.
- Optionally also maps to `yfinance` when --map-yfinance is passed.

Usage:
  python scripts/seed_us_stock_symbols.py --csv path/to/us_symbols.csv [--map-yfinance]
"""
from __future__ import annotations

import argparse
import csv
import logging
from pathlib import Path
from typing import Optional

from config import Config
from data_sources.entity import Entity, EntityStore, EntityType

MIC_MAP = {
    "nasdaq": "xnas",
    "nasdaqgs": "xnas",
    "nasdaqcm": "xnas",
    "nasdaq global select": "xnas",
    "nasdaq global market": "xnas",
    "nasdaq capital market": "xnas",
    "nyse": "xnys",
    "nysemkt": "xase",
    "amex": "xase",
}


def canonical_id(symbol: str, exchange: str) -> str:
    mic = MIC_MAP.get(exchange.lower())
    if not mic:
        raise ValueError(f"Unknown exchange: {exchange}")
    return f"sec:{mic}:{symbol.lower()}"


def seed(csv_path: Path, *, map_yfinance: bool, store: EntityStore) -> None:
    provider_default = "provider:us-stock-symbols"
    store.upsert_provider(provider_default, description="US stock symbols from CSV", base_url=str(csv_path))
    if map_yfinance:
        store.upsert_provider("yfinance", description="Yahoo Finance")

    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        inserted = 0
        skipped = 0
        for row in reader:
            symbol = (row.get("Symbol") or row.get("symbol") or "").strip()
            name = (row.get("Name") or row.get("name") or "").strip()
            exch = (row.get("Exchange") or row.get("exchange") or "").strip()
            if not symbol or not name or not exch:
                skipped += 1
                continue
            try:
                cid = canonical_id(symbol, exch)
            except ValueError:
                skipped += 1
                continue

            ent = Entity(entity_id=cid, entity_type=EntityType.SECURITY, name=name)
            store.upsert_entity(ent)
            store.map_provider_entity(
                provider=provider_default,
                provider_entity_id=symbol.upper(),
                entity_id=cid,
                active_from=None,
            )
            if map_yfinance:
                store.map_provider_entity(
                    provider="yfinance",
                    provider_entity_id=symbol.upper(),
                    entity_id=cid,
                    active_from=None,
                )
            inserted += 1
    logging.info("Seeded %d symbols (skipped %d)", inserted, skipped)


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed US stock symbols from CSV")
    parser.add_argument("--csv", required=True, help="Path to CSV with Symbol/Name/Exchange columns")
    parser.add_argument("--map-yfinance", action="store_true", help="Also map tickers to yfinance provider")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    cfg = Config()
    store = EntityStore(cfg)
    try:
        seed(Path(args.csv), map_yfinance=args.map_yfinance, store=store)
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
