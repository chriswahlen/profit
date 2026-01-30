from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable

from profit.cache import FileCache
from profit.catalog import EntityIdentifierRecord, EntityRecord, EntityStore
from profit.utils.url_fetcher import fetch_url
from profit.config import get_setting


SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_PROVIDER_ID = "sec:edgar"
SEC_UA_ENV = "PROFIT_SEC_USER_AGENT"


@dataclass(frozen=True)
class SeedResult:
    entities_written: int
    identifiers_written: int


class SecCompanyTickerSeeder:
    """
    Seed `entity` and `entity_identifier` tables from SEC's company_tickers.json.

    - entity_id: cik:<10-digit>
    - entity_type: company
    - identifiers:
        * scheme='sec:cik' value=<10-digit CIK> provider_id='sec:edgar'
        * scheme='ticker:us' value=<ticker> provider_id='sec:edgar'
    """

    def __init__(
        self,
        *,
        cache: FileCache,
        allow_network: bool = True,
        ttl: timedelta = timedelta(days=7),
        fetch_fn: Callable | None = None,
        default_country: str = "US",
    ) -> None:
        self.cache = cache
        self.allow_network = allow_network
        self.ttl = ttl
        self.fetch_fn = fetch_fn
        self.default_country = default_country
        self.default_active_from = datetime(1970, 1, 1, tzinfo=timezone.utc)

    def load_raw(self) -> dict:
        user_agent = get_setting(SEC_UA_ENV)
        if not user_agent:
            raise RuntimeError(f"{SEC_UA_ENV} must be set for SEC requests")
        if self.fetch_fn is not None:
            resp = self.fetch_fn(SEC_TICKERS_URL, timeout=30.0, headers={"User-Agent": user_agent})
            body = resp.body if hasattr(resp, "body") else resp
            return json.loads(body)
        payload = fetch_url(
            SEC_TICKERS_URL,
            cache=self.cache,
            ttl=self.ttl,
            allow_network=self.allow_network,
            fetch_fn=None,
            headers={"User-Agent": user_agent},
        )
        return json.loads(payload)

    def seed(self, store: EntityStore) -> SeedResult:
        raw = self.load_raw()
        store.upsert_providers([(SEC_PROVIDER_ID, "SEC EDGAR", "SEC company tickers feed")])

        entities: list[EntityRecord] = []
        identifiers: list[EntityIdentifierRecord] = []
        current_tickers: set[tuple[str, str]] = set()

        for obj in raw.values():
            cik_str = int(obj["cik_str"])
            ticker = obj.get("ticker") or ""
            name = obj.get("title") or ""
            entity_id = f"cik:{cik_str:010d}"
            current_tickers.add((entity_id, ticker))

            entities.append(
                EntityRecord(
                    entity_id=entity_id,
                    entity_type="company",
                    name=name,
                    country_iso2=self.default_country,
                )
            )

            identifiers.extend(
                [
                    EntityIdentifierRecord(
                        entity_id=entity_id,
                        scheme="sec:cik",
                        value=f"{cik_str:010d}",
                        provider_id=SEC_PROVIDER_ID,
                        active_from=self.default_active_from,
                    ),
                    EntityIdentifierRecord(
                        entity_id=entity_id,
                        scheme="ticker:us",
                        value=ticker,
                        provider_id=SEC_PROVIDER_ID,
                        active_from=self.default_active_from,
                    ),
                ]
            )

        # Batch in a single fast transaction with lowered sync for speed.
        conn = store.conn
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("BEGIN IMMEDIATE")
        try:
            entities_written = store.upsert_entities(entities)
            identifiers_written = store.upsert_identifiers(identifiers)
            tombstoned = self._tombstone_missing_tickers(store, current_tickers)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.execute("PRAGMA synchronous=NORMAL")

        return SeedResult(entities_written=entities_written, identifiers_written=identifiers_written + tombstoned)

    def _tombstone_missing_tickers(self, store: EntityStore, current: set[tuple[str, str]]) -> int:
        """
        Set active_to for identifiers (scheme=ticker:us, provider_id=SEC_PROVIDER_ID)
        that are not present in the latest feed.
        """
        cur = store.conn.execute(
            """
            SELECT entity_id, scheme, value, provider_id, active_from, active_to
            FROM entity_identifier
            WHERE scheme = 'ticker:us' AND provider_id = ?
            """,
            (SEC_PROVIDER_ID,),
        )
        now_iso = datetime.now(timezone.utc).isoformat()
        rows = cur.fetchall()
        missing_updates = []
        for row in rows:
            tup = (row["entity_id"], row["value"])
            if tup not in current:
                missing_updates.append(
                    (
                        now_iso,
                        row["entity_id"],
                        row["scheme"],
                        row["value"],
                        row["provider_id"],
                    )
                )
        if not missing_updates:
            return 0
        store.conn.executemany(
            """
            UPDATE entity_identifier
            SET active_to = ?
            WHERE entity_id = ? AND scheme = ? AND value = ? AND provider_id = ? AND active_to IS NULL
            """,
            missing_updates,
        )
        store.conn.commit()
        return len(missing_updates)
