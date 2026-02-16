#!/usr/bin/env python3
"""Seed ETF fund entities from FinanceDatabase CSV exports.

Each row may represent the same ETF listed on multiple venues; this script
creates a single `etf:<slug>` entity per product, records the summary/
category metadata, links the fund to its managing company, and marks which
exchanges list the product via `listed_on` relations.
"""

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
from data_sources.entities import Company
from scripts.seed_equities import (
    DEFAULT_COMPANY_COUNTRY,
    EXCHANGE_TO_MIC,
    UNKNOWN_EXCHANGE_CODE,
    infer_exchange_from_suffix,
)

_METADATA_FIELDS = ("summary", "category_group", "category", "family")
_DEFAULT_ETF_CSV = Path("incoming/datasets/fdb/etfs.csv")
_PROGRESS_INTERVAL = 500
_RELATION_ETF_FAMILY = "managed_by"
_RELATION_LISTED_ON = "listed_on"
_COMPANY_METADATA_SOURCE = "financedatabase"


def _parse_metadata(value: str | None) -> dict[str, str]:
    if not value:
        return {}
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        logging.debug("Failed to parse metadata %r", value)
        return {}


def _merge_fund_metadata(store: EntityStore, fund_id: str, name: str | None, metadata: str) -> None:
    incoming = _parse_metadata(metadata)
    if not incoming and not name:
        return
    cur = store.connection.execute("SELECT metadata FROM entities WHERE entity_id = ?", (fund_id,))
    row = cur.fetchone()
    existing = _parse_metadata(row[0] if row and row[0] else None)
    merged = {**existing, **incoming}
    if merged == existing and not name:
        return
    metadata_payload = json.dumps(merged, ensure_ascii=True) if merged else ""
    entity = Entity(entity_id=fund_id, entity_type=EntityType.ETF, name=name, metadata=metadata_payload)
    store.upsert_entity(entity, overwrite=True)


def _metadata_matches(existing_name: str | None, existing_meta: dict[str, str], new_name: str | None, new_meta: dict[str, str]) -> bool:
    if existing_name and new_name and existing_name != new_name:
        return False
    for key in existing_meta.keys() & new_meta.keys():
        if existing_meta[key] != new_meta[key]:
            return False
    return True


def _get_entity_data(store: EntityStore, entity_id: str) -> tuple[str | None, dict[str, str]]:
    cur = store.connection.execute("SELECT name, metadata FROM entities WHERE entity_id = ?", (entity_id,))
    row = cur.fetchone()
    name = row[0] if row else None
    return name, _parse_metadata(row[1] if row and row[1] else None)


def _alternate_entity_id(slug: str, symbol: str) -> str:
    parts = symbol.lower().split(".", 1)
    base = _sanitize_symbol(parts[0]) or slug
    suffix = parts[1] if len(parts) > 1 else ""
    if suffix:
        suffix = suffix.replace(".", "-")
        return f"{base}.{suffix}"
    return base


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


def fund_metadata(row: dict[str, str]) -> str:
    payload: dict[str, str] = {}
    for field in _METADATA_FIELDS:
        if (value := (row.get(field) or "").strip()):
            payload[field] = value
    return json.dumps(payload, ensure_ascii=True) if payload else ""


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
    symbol_raw = (row.get("symbol") or "").strip()
    if symbol_raw:
        base_symbol = symbol_raw.split(".", 1)[0]
        if slug := _sanitize_symbol(base_symbol):
            return slug
    name = (row.get("name") or "").strip()
    family = (row.get("family") or "").strip()
    parts: list[str] = []
    if family:
        parts.append(_slugify(family))
    if name:
        parts.append(_slugify(name))
    slug = "-".join(part for part in parts if part)
    if not slug:
        slug = _sanitize_symbol(symbol_raw) or "etf"
    return slug


def rows_from_csv(path: Path, limit: int | None = None) -> Iterable[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for idx, row in enumerate(reader):
            yield row
            if limit and idx + 1 >= limit:
                return


def seed_rows(rows: Iterable[dict[str, str]], store: EntityStore, progress_interval: int = _PROGRESS_INTERVAL) -> tuple[int, int]:
    store.ensure_relation_type(_RELATION_ETF_FAMILY, description="Entity managed by fund family")
    store.ensure_relation_type(_RELATION_LISTED_ON, description="Entity listed on exchange")
    inserted = skipped = 0
    fund_created: set[str] = set()
    listed_relations: set[tuple[str, str]] = set()
    family_relations: set[tuple[str, str]] = set()
    created_companies: set[str] = set()

    for idx, row in enumerate(rows, start=1):
        symbol = (row.get("symbol") or "").strip()
        if not symbol:
            skipped += 1
            continue

        slug = fund_slug(row)
        fund_id = f"etf:{slug}"
        fund_name = (row.get("name") or symbol).strip() or None
        metadata_str = fund_metadata(row)
        metadata_dict = _parse_metadata(metadata_str)
        entity_id = fund_id
        if fund_id not in fund_created:
            fund_entity = Entity(
                entity_id=fund_id,
                entity_type=EntityType.ETF,
                name=fund_name,
                metadata=metadata_str,
            )
            store.upsert_entity(fund_entity)
            fund_created.add(fund_id)
        else:
            existing_name, existing_meta = _get_entity_data(store, fund_id)
            if _metadata_matches(existing_name, existing_meta, fund_name, metadata_dict):
                _merge_fund_metadata(store, fund_id, fund_name, metadata_str)
            else:
                alt_slug = _alternate_entity_id(slug, symbol)
                entity_id = f"etf:{alt_slug}"
                if entity_id not in fund_created:
                    alt_entity = Entity(
                        entity_id=entity_id,
                        entity_type=EntityType.ETF,
                        name=fund_name,
                        metadata=metadata_str,
                    )
                    store.upsert_entity(alt_entity)
                    fund_created.add(entity_id)
                else:
                    _merge_fund_metadata(store, entity_id, fund_name, metadata_str)

        mic = _resolve_mic(row.get("exchange"), symbol)
        exchange_id = f"mic:{mic.lower()}"
        listed_key = (entity_id, exchange_id)
        if listed_key not in listed_relations:
            if not store.entity_exists(exchange_id):
                store.upsert_entity(
                    Entity(
                        entity_id=exchange_id,
                        entity_type=EntityType.MARKET_VENUE,
                        name=mic.upper(),
                    )
                )
            relation_metadata = json.dumps({"symbol": symbol.strip()}, ensure_ascii=True)
            store.map_entity_relation(
                src_entity_id=entity_id,
                dst_entity_id=exchange_id,
                relation=_RELATION_LISTED_ON,
                metadata=relation_metadata,
            )
            listed_relations.add(listed_key)

        if family := (row.get("family") or "").strip():
            try:
                company = Company.from_name(family, country_iso2=DEFAULT_COMPANY_COUNTRY)
                company_id = company.canonical_id
                if company_id not in created_companies:
                    metadata_payload = {"source": _COMPANY_METADATA_SOURCE, "family": family}
                    metadata = json.dumps(metadata_payload, ensure_ascii=True)
                    if store.entity_exists(company_id):
                        logging.info("Company %s already exists, skipping metadata insert", company_id)
                    else:
                        company_entity = Entity(
                            entity_id=company_id,
                            entity_type=EntityType.COMPANY,
                            name=company.name,
                            metadata=metadata,
                        )
                        store.upsert_entity(company_entity)
                    created_companies.add(company_id)
                family_key = (entity_id, company_id)
                if family_key not in family_relations:
                    logging.debug("Linking ETF fund %s to family %s", fund_id, company_id)
                    store.map_entity_relation(
                        src_entity_id=fund_id,
                        dst_entity_id=company_id,
                        relation=_RELATION_ETF_FAMILY,
                    )
                    family_relations.add(family_key)
            except ValueError:
                logging.debug("Skipping family for ETF fund %s: %s", fund_id, family)

        inserted += 1
        if idx % progress_interval == 0:
            logging.info("Seeded %d ETF rows", idx)
    return inserted, skipped


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Seed ETF entities from FinanceDatabase CSV")
    parser.add_argument("--limit", type=int, help="Optional row limit for testing")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    csv_path = _DEFAULT_ETF_CSV
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    logging.info("Starting ETF seed (csv=%s)", csv_path)
    cfg = Config()
    store = EntityStore(cfg)
    try:
        rows = rows_from_csv(csv_path, limit=args.limit)
        inserted, skipped = seed_rows(rows, store)
        logging.info("Finished ETF seed: %d inserted, %d skipped", inserted, skipped)
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
