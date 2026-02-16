#!/usr/bin/env python3
"""Seed canonical index entities from FinanceDatabase CSV exports."""

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
_METADATA_FIELDS = ("summary", "category_group", "category")
_DEFAULT_INDICES_CSV = Path("incoming/datasets/fdb/indices.csv")


def _parse_metadata(value: str | None) -> dict[str, str]:
    if not value:
        return {}
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        logging.debug("Failed to parse metadata %r", value)
        return {}


def _merge_index_metadata(store: EntityStore, entity_id: str, name: str | None, metadata: str) -> None:
    incoming = _parse_metadata(metadata)
    if not incoming and not name:
        return
    cur = store.connection.execute("SELECT metadata FROM entities WHERE entity_id = ?", (entity_id,))
    row = cur.fetchone()
    existing = _parse_metadata(row[0] if row and row[0] else None)
    merged = {**existing, **incoming}
    if merged == existing and not name:
        return
    metadata_payload = json.dumps(merged, ensure_ascii=True) if merged else ""
    entity = Entity(entity_id=entity_id, entity_type=EntityType.INDEX, name=name, metadata=metadata_payload)
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


def _alternate_index_id(slug: str, symbol: str) -> str:
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


def _sanitize_symbol_with_suffix(symbol: str) -> str | None:
    if not symbol:
        return None
    base = symbol.strip().upper()
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


def index_slug(row: dict[str, str]) -> str:
    symbol_raw = (row.get("symbol") or "").strip()
    if symbol_raw:
        base_symbol = symbol_raw.split(".", 1)[0]
        if slug := _sanitize_symbol(base_symbol):
            return slug
        slug = _sanitize_symbol_with_suffix(symbol_raw)
        if slug:
            return slug
    name = (row.get("name") or "").strip()
    if name:
        return _slugify(name)
    return "index"


def index_metadata(row: dict[str, str]) -> str:
    payload: dict[str, str] = {}
    for field in _METADATA_FIELDS:
        value = (row.get(field) or "").strip()
        if value:
            payload[field] = value
    currency_val = (row.get("currency") or "").strip()
    if currency_val:
        try:
            currency = Currency.from_code(currency_val)
            payload["currency"] = currency.canonical_id
        except ValueError:
            logging.debug("Skipping invalid currency %s for index %s", currency_val, row.get("symbol"))
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
    logging.debug("Ensuring seed metadata for %s", _PROVIDER)
    store.upsert_provider(_PROVIDER, description="FinanceDatabase indexes")
    store.ensure_relation_type(_RELATION_LISTED_ON, description="Entity listed on exchange")
    inserted = skipped = 0
    listed_relations: set[tuple[str, str]] = set()
    created_indices: set[str] = set()
    mapped_provider_symbols: set[str] = set()

    for idx, row in enumerate(rows, start=1):
        symbol = (row.get("symbol") or "").strip()
        if not symbol:
            skipped += 1
            continue

        slug = index_slug(row)
        base_id = f"index:{slug}"
        name = (row.get("name") or symbol).strip() or None
        metadata_str = index_metadata(row)
        metadata_dict = _parse_metadata(metadata_str)

        entity_id = base_id
        if base_id not in created_indices:
            store.upsert_entity(
                Entity(entity_id=base_id, entity_type=EntityType.INDEX, name=name, metadata=metadata_str)
            )
            created_indices.add(base_id)
            inserted += 1
        else:
            existing_name, existing_meta = _get_entity_data(store, base_id)
            if _metadata_matches(existing_name, existing_meta, name, metadata_dict):
                _merge_index_metadata(store, base_id, name, metadata_str)
            else:
                alt_slug = _alternate_index_id(slug, symbol)
                entity_id = f"index:{alt_slug}"
                if entity_id not in created_indices:
                    store.upsert_entity(
                        Entity(entity_id=entity_id, entity_type=EntityType.INDEX, name=name, metadata=metadata_str)
                    )
                    created_indices.add(entity_id)
                    inserted += 1
                else:
                    _merge_index_metadata(store, entity_id, name, metadata_str)

        symbol_key = symbol.upper()
        if symbol_key not in mapped_provider_symbols:
            store.map_provider_entity(
                provider=_PROVIDER,
                provider_entity_id=symbol_key,
                entity_id=entity_id,
                active_from=None,
                active_to=None,
                metadata=None,
            )
            mapped_provider_symbols.add(symbol_key)

        exchange_key = (row.get("exchange") or "").strip()
        mic = _resolve_mic(exchange_key, symbol)
        exchange_id = f"mic:{mic.lower()}"
        relation_key = (entity_id, exchange_id)
        if relation_key not in listed_relations:
            if not store.entity_exists(exchange_id):
                store.upsert_entity(
                    Entity(entity_id=exchange_id, entity_type=EntityType.MARKET_VENUE, name=mic.upper())
                )
            relation_metadata = json.dumps({"symbol": symbol.upper()}, ensure_ascii=True)
            store.map_entity_relation(
                src_entity_id=entity_id,
                dst_entity_id=exchange_id,
                relation=_RELATION_LISTED_ON,
                metadata=relation_metadata,
            )
            listed_relations.add(relation_key)

        if idx % progress_interval == 0:
            logging.info("Seeded %d index rows", idx)
    return inserted, skipped


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Seed FinanceDatabase index entities")
    parser.add_argument("--limit", type=int, help="Optional row limit for testing")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    csv_path = _DEFAULT_INDICES_CSV
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    logging.info("Starting index seed (csv=%s)", csv_path)
    cfg = Config()
    store = EntityStore(cfg)
    try:
        rows = rows_from_csv(csv_path, limit=args.limit)
        inserted, skipped = seed_rows(rows, store)
        logging.info("Finished index seed: %d inserted, %d skipped", inserted, skipped)
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
