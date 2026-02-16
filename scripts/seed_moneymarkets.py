#!/usr/bin/env python3
"""Seed canonical money market fund entities from FinanceDatabase CSV exports."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
from pathlib import Path
from typing import Iterable

from config import Config
from data_sources.entity import Entity, EntityStore, EntityType
from data_sources.entities import Currency
from scripts.seed_equities import EXCHANGE_TO_MIC, UNKNOWN_EXCHANGE_CODE, infer_exchange_from_suffix

_PROVIDER = "provider:financedatabase"
_RELATION_LISTED_ON = "listed_on"
_PROGRESS_INTERVAL = 500
_METADATA_FIELDS = ("summary", "family")


def _sanitize_symbol(symbol: str) -> str | None:
    if not symbol:
        return None
    base = symbol.strip().upper()
    if base.startswith("^"):
        base = base[1:]
    if "-" in base:
        base = base.split("-", 1)[0]
    if "." in base:
        base = base.split(".", 1)[0]
    base = re.sub(r"[^A-Z0-9]+", "", base)
    return base.lower() or None


def _slugify(text: str) -> str:
    cleaned: list[str] = []
    last_dash = False
    for ch in text.lower():
        if ch.isalnum():
            cleaned.append(ch)
            last_dash = False
        elif not last_dash:
            cleaned.append("-")
            last_dash = True
    return "".join(cleaned).strip("-")


def fund_slug(row: dict[str, str]) -> str:
    symbol = _sanitize_symbol((row.get("symbol") or "").strip())
    if symbol:
        return symbol
    name = (row.get("name") or "").strip()
    if name:
        return _slugify(name)
    return "money-market"


def fund_metadata(row: dict[str, str]) -> str:
    payload: dict[str, str] = {}
    for field in _METADATA_FIELDS:
        value = (row.get(field) or "").strip()
        if value:
            payload[field] = value
    currency_val = (row.get("currency") or "").strip()
    if currency_val:
        try:
            payload["currency"] = Currency.from_code(currency_val).canonical_id
        except ValueError:
            logging.debug("Skipping invalid currency %s for money market %s", currency_val, row.get("symbol"))
    exchange_val = (row.get("exchange") or "").strip()
    if exchange_val:
        payload["exchange"] = exchange_val
    return json.dumps(payload, ensure_ascii=True) if payload else ""


def _resolve_mic(exchange: str | None, symbol: str) -> str:
    candidate = (exchange or "").strip().upper()
    mic = None
    if candidate:
        mic = EXCHANGE_TO_MIC.get(candidate)
        if not mic and len(candidate) <= 5 and candidate.isalpha():
            mic = candidate
    if not mic:
        mic = infer_exchange_from_suffix(symbol)
    if not mic:
        mic = UNKNOWN_EXCHANGE_CODE
    return mic


def rows_from_csv(path: Path, limit: int | None = None) -> Iterable[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for idx, row in enumerate(reader):
            yield row
            if limit and idx + 1 >= limit:
                break


def seed_rows(rows: Iterable[dict[str, str]], store: EntityStore, progress_interval: int = _PROGRESS_INTERVAL) -> tuple[int, int]:
    logging.debug("Ensuring money market metadata for %s", _PROVIDER)
    store.upsert_provider(_PROVIDER, description="FinanceDatabase money market symbols")
    store.ensure_relation_type(_RELATION_LISTED_ON, description="Entity listed on exchange")
    inserted = skipped = 0
    listed_relations: set[tuple[str, str]] = set()
    created_funds: set[str] = set()
    mapped_provider_symbols: set[str] = set()

    for idx, row in enumerate(rows, start=1):
        symbol = (row.get("symbol") or "").strip()
        if not symbol:
            skipped += 1
            continue

        slug = fund_slug(row)
        mic = _resolve_mic((row.get("exchange") or "").strip(), symbol)
        fund_id = f"fund:{mic.lower()}:{slug}"
        name = (row.get("name") or symbol).strip() or None
        metadata = fund_metadata(row)

        if fund_id not in created_funds:
            store.upsert_entity(Entity(entity_id=fund_id, entity_type=EntityType.FUND, name=name, metadata=metadata))
            created_funds.add(fund_id)
            inserted += 1
        else:
            logging.debug("Skipping duplicate fund insert for %s", fund_id)

        symbol_key = symbol.upper()
        if symbol_key not in mapped_provider_symbols:
            store.map_provider_entity(
                provider=_PROVIDER,
                provider_entity_id=symbol_key,
                entity_id=fund_id,
                active_from=None,
                active_to=None,
                metadata=None,
            )
            mapped_provider_symbols.add(symbol_key)

        exchange_id = f"mic:{mic.lower()}"
        rel_key = (fund_id, exchange_id)
        if rel_key not in listed_relations:
            if not store.entity_exists(exchange_id):
                store.upsert_entity(Entity(entity_id=exchange_id, entity_type=EntityType.MARKET_VENUE, name=mic.upper()))
            store.map_entity_relation(src_entity_id=fund_id, dst_entity_id=exchange_id, relation=_RELATION_LISTED_ON)
            listed_relations.add(rel_key)

        if idx % progress_interval == 0:
            logging.info("Seeded %d money market rows", idx)

    return inserted, skipped


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Seed FinanceDatabase money market funds")
    parser.add_argument("--csv", required=True, help="Path to FinanceDatabase money market CSV (e.g., moneymarkets.csv)")
    parser.add_argument("--limit", type=int, help="Optional row limit for testing")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    logging.info("Starting money market seed (csv=%s)", csv_path)
    cfg = Config()
    store = EntityStore(cfg)
    rows = rows_from_csv(csv_path, limit=args.limit)
    inserted, skipped = seed_rows(rows, store)
    logging.info("Finished money market seed: %d inserted, %d skipped", inserted, skipped)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
