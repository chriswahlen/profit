#!/usr/bin/env python3
"""Seed canonical currencies into the entity store.

Data source: https://openexchangerates.org/api/currencies.json
"""

from __future__ import annotations

import json
import logging
from typing import Mapping
from urllib import request

from config import Config
from data_sources.entities import Currency
from data_sources.entity import Entity, EntityStore, EntityType

OXR_CURRENCY_URL = "https://openexchangerates.org/api/currencies.json"


def fetch_currency_definitions(url: str = OXR_CURRENCY_URL) -> Mapping[str, str]:
    """Fetch currency codes and names from Open Exchange Rates."""
    logging.info("Fetching currency definitions from %s", url)
    with request.urlopen(url, timeout=15) as resp:  # nosec: B310 - trusted static URL
        if resp.status != 200:
            raise RuntimeError(f"Failed to fetch currencies: HTTP {resp.status}")
        payload = resp.read()
    return json.loads(payload.decode("utf-8"))


def seed_currencies(
    *,
    config: Config,
    provider: str = "openexchangerates",
    currency_map: Mapping[str, str] | None = None,
) -> None:
    """Seed currencies into the entity store from a mapping of code->name."""
    entity_store = EntityStore(config)
    entity_store.upsert_provider(provider, description="Open Exchange Rates", base_url=OXR_CURRENCY_URL)

    definitions = currency_map or fetch_currency_definitions()
    inserted = failed = 0

    for code, name in sorted(definitions.items()):
        if not code or not code.strip():
            logging.debug("Skipping blank currency code entry")
            failed += 1
            continue

        try:
            currency = Currency.from_code(code)
            entity = Entity(
                entity_id=currency.canonical_id,
                entity_type=EntityType.CURRENCY,
                name=(name or code).strip(),
            )
            entity_store.upsert_entity(entity)
            entity_store.map_provider_entity(
                provider=provider,
                provider_entity_id=code,
                entity_id=currency.canonical_id,
                active_from="1970-01-01",
            )
            inserted += 1
        except Exception as exc:  # noqa: BLE001 - log and continue to next row
            logging.warning("Failed to insert currency %s: %s", code, exc)
            failed += 1

    logging.info("Seeded %d currencies (failed: %d)", inserted, failed)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    cfg = Config()
    seed_currencies(config=cfg)


if __name__ == "__main__":
    main()
