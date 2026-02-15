#!/usr/bin/env python3
"""Seed canonical entities from a plain-text ticker list.

Each line in the ticker file is treated as a single symbol. Blank lines and
whitespace-only lines are ignored. The script builds canonical IDs using the
provided MIC and entity type, then stores the entities and a provider mapping
so tickers can be resolved back to canonical IDs.

Usage:
  python scripts/seed_ticker_list.py --mic XNAS --entity-type security --tickers path/to/tickers.txt

Options:
  --provider   Optional provider name for the mapping table. Defaults to
               "provider:{mic}-tickers" (mic lowercased).
"""

from __future__ import annotations

import argparse
import logging
import urllib.request
from urllib.parse import urlparse
from pathlib import Path
from typing import Iterable, List

from config import Config
from data_sources.entity import Entity, EntityStore, EntityType


SUPPORTED_ENTITY_PREFIX = {
    EntityType.SECURITY: "sec",
    EntityType.FUND: "fund",
}


def build_entity_id(ticker: str, mic: str, entity_type: EntityType) -> str:
    """Return the canonical entity id for the ticker/mic pair.

    Raises ValueError for unsupported entity types or missing values.
    """

    ticker_clean = ticker.strip()
    mic_clean = mic.strip().lower()
    if not ticker_clean:
        raise ValueError("Ticker is required")
    if not mic_clean:
        raise ValueError("MIC is required")
    prefix = SUPPORTED_ENTITY_PREFIX.get(entity_type)
    if not prefix:
        raise ValueError(f"Unsupported entity type for ticker seeding: {entity_type}")
    return f"{prefix}:{mic_clean}:{ticker_clean.lower()}"


def _read_text_from_source(source: str) -> str:
    """Return text content from a local path or URL."""

    path = Path(source)
    if path.exists():
        logging.info("Loading tickers from file %s", path)
        return path.read_text()

    parsed = urlparse(source)
    if not parsed.scheme:
        raise FileNotFoundError(f"Ticker source not found: {source}")

    logging.info("Downloading tickers from %s", source)
    with urllib.request.urlopen(source, timeout=30) as resp:  # nosec B310 - caller controls URL
        status = getattr(resp, "status", None) or resp.getcode()
        if status and status >= 400:
            raise RuntimeError(f"Failed to download ticker list: HTTP {status}")
        return resp.read().decode("utf-8")


def load_tickers(source: str) -> List[str]:
    """Read tickers (one per line) from a text file or URL."""

    tickers: list[str] = []
    content = _read_text_from_source(source)
    for line in content.splitlines():
        symbol = line.strip()
        if symbol:
            tickers.append(symbol)
    return tickers


def seed(*, mic: str, entity_type: EntityType, tickers: Iterable[str], provider: str, store: EntityStore) -> None:
    """Insert entities for a set of tickers.

    Note: This intentionally does NOT write provider_entity_map rows; the caller
    can handle provider mappings separately if needed.
    """

    # Fail fast if the entity type is not supported for ticker-based seeding.
    if entity_type not in SUPPORTED_ENTITY_PREFIX:
        raise ValueError(f"Unsupported entity type for ticker seeding: {entity_type}")

    inserted = 0
    skipped = 0
    mic_clean = mic.strip()
    # Provider is accepted for compatibility, but we do not map tickers to it.

    for ticker in tickers:
        symbol = ticker.strip()
        if not symbol:
            skipped += 1
            continue

        try:
            canonical_id = build_entity_id(symbol, mic_clean, entity_type)
        except ValueError:
            skipped += 1
            continue

        entity = Entity(entity_id=canonical_id, entity_type=entity_type, name=symbol.upper())
        store.upsert_entity(entity)
        inserted += 1

    logging.info("Seeded %d tickers (skipped %d)", inserted, skipped)


def default_provider(mic: str) -> str:
    return f"provider:{mic.strip().lower()}-tickers"


def parse_entity_type(raw: str) -> EntityType:
    try:
        return EntityType(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid entity type: {raw}") from exc


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed entities from a ticker list")
    parser.add_argument("--mic", required=True, help="MIC for the tickers (e.g., XNAS)")
    parser.add_argument(
        "--entity-type",
        required=True,
        type=parse_entity_type,
        choices=sorted(SUPPORTED_ENTITY_PREFIX.keys(), key=lambda e: e.value),
        help="Entity type to create (supports SECURITY or FUND)",
    )
    parser.add_argument("--tickers", required=True, help="Path or URL to newline-delimited ticker file")
    parser.add_argument(
        "--provider",
        help="Provider name for mapping table (defaults to provider:{mic}-tickers)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    mic = args.mic
    provider = args.provider or default_provider(mic)

    cfg = Config()
    store = EntityStore(cfg)
    tickers = load_tickers(args.tickers)

    seed(
        mic=mic,
        entity_type=args.entity_type,
        tickers=tickers,
        provider=provider,
        store=store,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
