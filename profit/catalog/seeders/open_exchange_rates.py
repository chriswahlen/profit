from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

from profit.cache import FileCache
from profit.catalog import EntityIdentifierRecord, EntityRecord, EntityStore
from profit.seed_metadata import ensure_seed_metadata, read_seed_metadata, write_seed_metadata
from profit.utils.url_fetcher import fetch_url


OXR_CURRENCIES_URL = "https://openexchangerates.org/api/currencies.json"
OXR_PROVIDER_ID = "oxr"


@dataclass(frozen=True)
class SeedResult:
    entities_written: int
    identifiers_written: int


class OpenExchangeRatesCurrencySeeder:
    """
    Seed currency entities/identifiers from Open Exchange Rates currencies.json.

    - entity_id: ccy:<lowercase code>
    - entity_type: currency
    - identifiers:
        * scheme='iso:ccy' value=<CODE> provider_id='oxr'
    """

    def __init__(
        self,
        *,
        cache: FileCache,
        allow_network: bool = True,
        ttl: timedelta = timedelta(days=7),
        fetch_fn: Callable | None = None,
        force: bool = False,
    ) -> None:
        self.cache = cache
        self.allow_network = allow_network
        self.ttl = ttl
        self.fetch_fn = fetch_fn
        self.force = force

    def load_raw(self) -> dict:
        if self.fetch_fn is not None:
            resp = self.fetch_fn(OXR_CURRENCIES_URL, timeout=30.0, headers={"User-Agent": "profit-seeder"})
            body = resp.body if hasattr(resp, "body") else resp
            return json.loads(body)
        payload = fetch_url(
            OXR_CURRENCIES_URL,
            cache=self.cache,
            ttl=self.ttl,
            allow_network=self.allow_network,
            fetch_fn=None,
            headers={"User-Agent": "profit-seeder"},
        )
        return json.loads(payload)

    def seed(self, store: EntityStore) -> SeedResult:
        ensure_seed_metadata(store.conn)
        if not self.force and self._should_skip(store.conn):
            age = self._last_run_age(store.conn)
            remaining = max(self.ttl - age, timedelta(0))
            logging.info(
                "OXR seeder skipped: last_run_age=%s ttl=%s next_refresh_in=%s",
                age,
                self.ttl,
                remaining,
            )
            return SeedResult(entities_written=0, identifiers_written=0)

        raw = self.load_raw()
        store.upsert_providers([(OXR_PROVIDER_ID, "Open Exchange Rates", "OXR currency list")])

        entities: list[EntityRecord] = []
        identifiers: list[EntityIdentifierRecord] = []

        for code, name in raw.items():
            code_up = code.upper()
            eid = f"ccy:{code.lower()}"
            entities.append(
                EntityRecord(
                    entity_id=eid,
                    entity_type="currency",
                    name=name or code_up,
                    country_iso2=None,
                )
            )
            identifiers.append(
                EntityIdentifierRecord(
                    entity_id=eid,
                    scheme="iso:ccy",
                    value=code_up,
                    provider_id=OXR_PROVIDER_ID,
                )
            )

        entities_written = store.upsert_entities(entities)
        identifiers_written = store.upsert_identifiers(identifiers)
        self._bump_metadata(store.conn)
        return SeedResult(entities_written=entities_written, identifiers_written=identifiers_written)

    def _is_cache_fresh(self) -> bool:
        try:
            entry = self.cache.get(f"urlfetch::{OXR_CURRENCIES_URL}", ttl=self.ttl)
            return True
        except Exception:
            return False

    def _cache_age(self) -> timedelta:
        try:
            entry = self.cache.get(f"urlfetch::{OXR_CURRENCIES_URL}", ttl=None)
            return datetime.now(timezone.utc) - entry.created_at
        except Exception:
            return timedelta.max

    def _should_skip(self, conn: sqlite3.Connection) -> bool:
        last = read_seed_metadata(conn, "oxr_currencies")
        if last is None:
            return False
        return datetime.now(timezone.utc) - last < self.ttl

    def _last_run_age(self, conn: sqlite3.Connection) -> timedelta:
        last = read_seed_metadata(conn, "oxr_currencies")
        if last is None:
            return timedelta.max
        return datetime.now(timezone.utc) - last

    def _bump_metadata(self, conn: sqlite3.Connection) -> None:
        write_seed_metadata(conn, "oxr_currencies", datetime.now(timezone.utc))
