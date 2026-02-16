#!/usr/bin/env python3
"""Seed canonical fund entities from FinanceDatabase CSV exports."""

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
from data_sources.entities import Company, Currency
from scripts.seed_equities import (
    DEFAULT_COMPANY_COUNTRY,
    EXCHANGE_TO_MIC,
    UNKNOWN_EXCHANGE_CODE,
    infer_exchange_from_suffix,
)

_PROVIDER = "provider:financedatabase"
_RELATION_LISTED_ON = "listed_on"
_RELATION_MANAGED_BY = "managed_by"
_PROGRESS_INTERVAL = 500
_METADATA_FIELDS = ("summary", "category_group", "category", "family")
_DEFAULT_FUNDS_CSV = Path("incoming/datasets/fdb/funds.csv")
_COMPANY_METADATA_SOURCE = "financedatabase"


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
    if slug := _sanitize_symbol((row.get("symbol") or "").strip()):
        return slug
    name = (row.get("name") or "").strip()
    family = (row.get("family") or "").strip()
    parts: list[str] = []
    if family:
        parts.append(_slugify(family))
    if name:
        parts.append(_slugify(name))
    if parts:
        return "-".join(part for part in parts if part)
    return "fund"


def fund_metadata(row: dict[str, str]) -> str:
    payload: dict[str, str] = {}
    for field in _METADATA_FIELDS:
        if (value := (row.get(field) or "").strip()):
            payload[field] = value
    currency_val = (row.get("currency") or "").strip()
    if currency_val:
        try:
            payload["currency"] = Currency.from_code(currency_val).canonical_id
        except ValueError:
            logging.debug("Skipping invalid currency %s for fund %s", currency_val, row.get("symbol"))
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
    logging.debug("Ensuring fund metadata for %s", _PROVIDER)
    store.upsert_provider(_PROVIDER, description="FinanceDatabase funds")
    store.ensure_relation_type(_RELATION_MANAGED_BY, description="Entity managed by fund family")
    store.ensure_relation_type(_RELATION_LISTED_ON, description="Entity listed on exchange")
    inserted = skipped = 0
    created_funds: set[str] = set()
    listed_relations: set[tuple[str, str]] = set()
    family_relations: set[tuple[str, str]] = set()
    mapped_provider_symbols: set[str] = set()
    created_companies: set[str] = set()

    for idx, row in enumerate(rows, start=1):
        symbol = (row.get("symbol") or "").strip()
        if not symbol:
            skipped += 1
            continue

        slug = fund_slug(row)
        mic = _resolve_mic((row.get("exchange") or "").strip(), symbol)
        fund_id = f"fund:{slug}"
        fund_name = (row.get("name") or symbol).strip() or None
        metadata = fund_metadata(row)

        if fund_id not in created_funds:
            store.upsert_entity(
                Entity(entity_id=fund_id, entity_type=EntityType.FUND, name=fund_name, metadata=metadata)
            )
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
        relation_key = (fund_id, exchange_id)
        if relation_key not in listed_relations:
            if not store.entity_exists(exchange_id):
                store.upsert_entity(
                    Entity(entity_id=exchange_id, entity_type=EntityType.MARKET_VENUE, name=mic.upper())
                )
            store.map_entity_relation(
                src_entity_id=fund_id,
                dst_entity_id=exchange_id,
                relation=_RELATION_LISTED_ON,
            )
            listed_relations.add(relation_key)

        family_name = (row.get("family") or "").strip()
        if family_name:
            try:
                company = Company.from_name(family_name, country_iso2=DEFAULT_COMPANY_COUNTRY)
                company_id = company.canonical_id
                if company_id not in created_companies:
                    metadata_payload = {"source": _COMPANY_METADATA_SOURCE, "family": family_name}
                    metadata = json.dumps(metadata_payload, ensure_ascii=True)
                    if store.entity_exists(company_id):
                        logging.info("Company %s already exists, skipping metadata insert", company_id)
                    else:
                        store.upsert_entity(
                            Entity(
                                entity_id=company_id,
                                entity_type=EntityType.COMPANY,
                                name=company.name,
                                metadata=metadata,
                            )
                        )
                    created_companies.add(company_id)
                family_key = (fund_id, company_id)
                if family_key not in family_relations:
                    logging.debug("Linking fund %s to family %s", fund_id, company_id)
                    store.map_entity_relation(
                        src_entity_id=fund_id,
                        dst_entity_id=company_id,
                        relation=_RELATION_MANAGED_BY,
                    )
                    family_relations.add(family_key)
            except ValueError:
                logging.debug("Skipping family for fund %s: %s", fund_id, family_name)

        if idx % progress_interval == 0:
            logging.info("Seeded %d fund rows", idx)

    return inserted, skipped


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Seed FinanceDatabase fund entities")
    parser.add_argument("--limit", type=int, help="Optional row limit for testing")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    csv_path = _DEFAULT_FUNDS_CSV
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    logging.info("Starting fund seed (csv=%s)", csv_path)
    cfg = Config()
    store = EntityStore(cfg)
    try:
        rows = rows_from_csv(csv_path, limit=args.limit)
        inserted, skipped = seed_rows(rows, store)
        logging.info("Finished fund seed: %d inserted, %d skipped", inserted, skipped)
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
