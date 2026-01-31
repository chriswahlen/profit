from __future__ import annotations

import json
import hashlib
import re
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

    - entity_id: friendly slug `us:com:<name>` (hash suffix on collision)
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
            raw_name = obj.get("title") or ""
            name = _strip_state_tags(raw_name)
            entity_id = self._resolve_entity_id(store, name, f"{cik_str:010d}")
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
                    ),
                    EntityIdentifierRecord(
                        entity_id=entity_id,
                        scheme="ticker:us",
                        value=ticker,
                        provider_id=SEC_PROVIDER_ID,
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

    # ------------------------------------------------------------------
    def _resolve_entity_id(self, store: EntityStore, name: str, cik: str) -> str:
        """
        Return a stable, friendly entity_id derived from name and CIK.
        - Reuse existing mapping if CIK already present.
        - Base slug: us:com:<slugified_name> (corporate suffix stripped).
        - On collision with another issuer, append -<hash8> derived from CIK.
        """
        existing = store.find_entity_by_identifier(scheme="sec:cik", value=cik)
        if existing:
            return existing

        slug = _slugify_company(name)
        base = f"us:com:{slug}"
        if not self._entity_exists(store, base):
            return base

        hash8 = hashlib.sha1(cik.encode("utf-8")).hexdigest()[:8]
        candidate = f"{base}-{hash8}"
        # If even candidate exists (extremely unlikely), keep adding hash chunks.
        suffix_iter = 1
        while self._entity_exists(store, candidate):
            suffix_iter += 1
            candidate = f"{base}-{hash8}{suffix_iter}"
        return candidate

    def _entity_exists(self, store: EntityStore, entity_id: str) -> bool:
        cur = store.conn.execute("SELECT 1 FROM entity WHERE entity_id = ? LIMIT 1", (entity_id,))
        return cur.fetchone() is not None


_SUFFIX_RE = re.compile(r"^(inc|incorporated|corp|corporation|co|company|ltd|llc|plc|sa|ag|nv|gmbh|lp|llp|adr)$", re.IGNORECASE)
_STATE_TAG_RE = re.compile(r"/(new|al|ak|az|ar|ca|co|ct|de|dc|fl|ga|hi|id|il|in|ia|ks|ky|la|me|md|ma|mi|mn|ms|mo|mt|ne|nv|nh|nj|nm|ny|nc|nd|oh|ok|or|pa|ri|sc|sd|tn|tx|ut|vt|va|wa|wv|wi|wy)$", re.IGNORECASE)
_CUSTOM_OVERRIDES = [
    (re.compile(r"^banco santander", re.IGNORECASE), "banco-santandar"),
    (re.compile(r"^spdr s&p 500", re.IGNORECASE), "sandp500"),
    (re.compile(r"^alibaba group holding", re.IGNORECASE), "alibaba"),
    (re.compile(r"^goldman sachs group", re.IGNORECASE), "goldman-sachs"),
    (re.compile(r"^1\s*800\s*flowers", re.IGNORECASE), "1800flowers"),
    (re.compile(r"^1895 bancorp of wisconsin", re.IGNORECASE), "1895-bancorp-of-wisconsin"),
    (re.compile(r"^3\s*e\s*network technology", re.IGNORECASE), "3-e-network-technology"),
    (re.compile(r"^aib group", re.IGNORECASE), "aib-group"),
    (re.compile(r"^ambitions enterprise management", re.IGNORECASE), "ambitions-enterprise-management"),
    (re.compile(r"^american exceptionalism acquisition", re.IGNORECASE), "american-exceptionalism-acquisition"),
    (re.compile(r"^pg&e", re.IGNORECASE), "pg-and-e"),
    (re.compile(r"^cheniere energy partners", re.IGNORECASE), "cheniere-energy-partners"),
]


def _slugify_company(name: str) -> str:
    """
    Slugify company name to a readable token:
    - lowercase
    - replace & with 'and'
    - strip punctuation
    - collapse whitespace/dashes to single dash
    - drop trailing corporate suffixes (iteratively) and share-class letters
    - apply known overrides for tricky names
    """
    if not name:
        return "unknown"
    for pattern, replacement in _CUSTOM_OVERRIDES:
        if pattern.match(name):
            return replacement
    n = name.replace("&", " and ")
    # strip trailing "& co" variations
    n = re.sub(r"\s+and\s+co\.?$", "", n, flags=re.IGNORECASE)
    # collapse dotted acronyms: a.b.c -> abc
    n = re.sub(r"\b([A-Za-z])(?:\.[A-Za-z])+\.?", lambda m: m.group(0).replace(".", ""), n)
    # normalize common punctuation to separators
    n = n.replace("n.v.", " nv ")
    n = n.replace("N.V.", " nv ")
    n = re.sub(r"[^A-Za-z0-9\s-]", " ", n)
    n = re.sub(r"\s+", " ", n).strip().lower()
    n = n.replace(" ", "-")
    parts = [p for p in n.split("-") if p]
    # drop trailing /XX or /NEW style tags
    if parts and _STATE_TAG_RE.search(name):
        parts = [p for p in parts if not _STATE_TAG_RE.match("/" + p)]
    # merge split legal-form tokens like s.a. -> sa, n.v. -> nv
    if len(parts) >= 2 and parts[-2:] == ["s", "a"]:
        parts = parts[:-2] + ["sa"]
    if len(parts) >= 2 and parts[-2:] == ["n", "v"]:
        parts = parts[:-2] + ["nv"]
    while parts and _SUFFIX_RE.match(parts[-1]):
        parts = parts[:-1]
    if parts and len(parts[-1]) == 1 and parts[-1].isalpha():
        parts = parts[:-1]
    if not parts:
        parts = ["unknown"]
    return "-".join(parts)


def _strip_state_tags(name: str) -> str:
    return _STATE_TAG_RE.sub("", name).strip()
