#!/usr/bin/env python3
"""Seed canonical crypto entities from FinanceDatabase CSV exports.

This script derives crypto canonical IDs from FinanceDatabase listings, records
provider mappings, and preserves the descriptive summary metadata for each asset.
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

PROVIDER = "provider:financedatabase"
_PROGRESS_INTERVAL = 500
_SUMMARY_NAME_RE = re.compile(r"^(?P<name>[^\(\n]+?)(?:\s*\(|\s+is\b|\s+has\b|,|$)", re.IGNORECASE)


def _normalize_asset_symbol(value: str) -> str | None:
    """Normalize a crypto ticker/slug for use in canonical IDs."""

    normalized = re.sub(r"[^a-z0-9]+", "-", value.strip().lower())
    normalized = normalized.strip("-")
    return normalized or None


def canonical_id_from_row(row: dict[str, str]) -> str | None:
    """Derive a canonical crypto ID from the row payload."""

    crypto = (row.get("cryptocurrency") or "").strip()
    if not crypto:
        symbol = (row.get("symbol") or "").strip()
        if "-" in symbol:
            crypto = symbol.split("-", 1)[0].strip()
        else:
            crypto = symbol
    if not crypto:
        return None
    normalized = _normalize_asset_symbol(crypto)
    if not normalized:
        return None
    return f"crypto:{normalized}"


def row_metadata(row: dict[str, str]) -> str:
    """Capture only the summary text in the metadata payload."""

    summary = (row.get("summary") or "").strip()
    return json.dumps({"summary": summary}, ensure_ascii=True) if summary else ""


def rows_from_csv(path: Path, limit: int | None = None) -> Iterable[dict[str, str]]:
    """Yield rows from a CSV file, honoring an optional row limit."""

    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for idx, row in enumerate(reader):
            yield row
            if limit and idx + 1 >= limit:
                return


def _extract_name_from_summary(summary: str) -> str | None:
    match = _SUMMARY_NAME_RE.match(summary)
    if match:
        candidate = match.group("name").strip()
        if candidate:
            return candidate
    return None


def descriptive_name(row: dict[str, str], symbol: str) -> str:
    """Derive a friendly crypto name by inspecting the summary/fields."""

    summary = (row.get("summary") or "").strip()
    if summary:
        if name := _extract_name_from_summary(summary):
            return name
    if name := (row.get("name") or "").strip():
        return name
    if name := (row.get("cryptocurrency") or "").strip():
        return name
    return symbol


def seed_rows(rows: Iterable[dict[str, str]], store: EntityStore, progress_interval: int = _PROGRESS_INTERVAL) -> tuple[int, int, int]:
    """Seed crypto rows and return (processed, skipped, unique_assets)."""

    store.upsert_provider(PROVIDER, description="FinanceDatabase crypto listings")
    processed = skipped = 0
    unique_ids: set[str] = set()

    for idx, row in enumerate(rows, start=1):
        cid = canonical_id_from_row(row)
        symbol = (row.get("symbol") or "").strip()
        if not cid or not symbol:
            skipped += 1
            continue

        unique_ids.add(cid)
        name = descriptive_name(row, symbol).strip()
        entity = Entity(
            entity_id=cid,
            entity_type=EntityType.CRYPTO,
            name=name or None,
            metadata=row_metadata(row),
        )
        store.upsert_entity(entity)
        processed += 1

        if idx % progress_interval == 0:
            logging.info("Processed %d crypto rows (%d unique assets)", idx, len(unique_ids))

    return processed, skipped, len(unique_ids)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Seed FinanceDatabase crypto metadata")
    parser.add_argument("--csv", required=True, help="Path to FinanceDatabase crypto CSV (e.g., cryptos.csv)")
    parser.add_argument("--limit", type=int, help="Optional row limit for testing")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    logging.info("Starting FinanceDatabase crypto seed (csv=%s)", csv_path)
    cfg = Config()
    store = EntityStore(cfg)
    rows = rows_from_csv(csv_path, limit=args.limit)
    processed, skipped, unique = seed_rows(rows, store)
    logging.info(
        "Finished FinanceDatabase crypto seed: %d rows processed, %d skipped, %d unique assets",
        processed,
        skipped,
        unique,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
