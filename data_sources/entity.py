from __future__ import annotations

import sqlite3
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from config import Config
from data_sources.data_store import DataSourceUpdateResults
from data_sources.sqlite_data_store import SqliteDataStore


FIXED_RELATION_TYPES: tuple[tuple[str, str], ...] = (
    ("traded_in", "Exchange trades in currency"),
    ("listed_on", "Security/ETF listed on exchange"),
    ("issued_security", "Company issues the security"),
    ("managed_by", "ETF or product managed by fund family"),
    ("belongs_to_sector", "Security belongs to sector"),
    ("belongs_to_industry", "Security belongs to industry"),
)


class EntityType(str, Enum):
    """Canonical entity types and expected ID formats.

    Defined now:
    - COMPANY:          country_code:com:company_name            (us:com:neo-aeronautics)
    - CURRENCY:         ccy:iso4217                              (ccy:usd)
    - REGION:           pipe hierarchy                           (country|us, metro|us|dallas_ft_worth, city|us|texas|dallas)
    - SECURITY:         mic:ticker or isin or cusip              (XNYS:AAPL, isin:US0378331005)
    - CRYPTO:           crypto:<slug>                           (crypto:btc)
    - ETF:              etf:<slug>                              (etf:spdr-s-p-500-etf-trust)
    - INDEX:            index:provider:code                      (index:spglobal:sp500)
    - FUND:             mic:ticker or isin                       (XNYS:SPY, isin:US78462F1030)
    - ECON_SERIES:      econ:provider:series_id                  (econ:fred:CPIAUCSL)
    - PERSON:           person:namespace:slug                    (person:wikidata:Q312)
    - PROPERTY:         property:country:scheme:id               (property:us:apn:123-456-789)
    - MARKET_VENUE:     mic:XXXX                                 (mic:XNAS)

    Future possibility (defined in the enum but semantics TBD):
    - ECON_EVENT:       econ_event:provider:event_id             (econ_event:ecb:2024-09-meeting)
    - INSTRUMENT_FAMILY: family:underlier:descriptor             (family:AAPL:opt-2025)
    - CORPORATE_ACTION: ca:provider:id                           (ca:sec:0000320193-24-000010-split)
    - NEWS_SOURCE:      news:source:slug                         (news:source:reuters)
    - NEWS_ARTICLE:     news:article:provider:id                 (news:article:reuters:2024-12-15-abc123)
    - SECTOR:           sector:gics:code                         (sector:gics:45101010)
    - INDUSTRY:         industry:naics:code                      (industry:naics:334220)
    - LEGAL_ENTITY:     lei:XXXX                                 (lei:5493001KJTIIGC8Y1R12)
    - DATA_PROVIDER:    provider:slug                            (provider:redfin)
    - CURRENCY_PAIR:    fx:base:quote                            (fx:usd:eur)
    - BENCHMARK_RATE:   rate:provider:code                       (rate:ice:libor-usd-3m)
    """

    COMPANY = "company"
    CURRENCY = "currency"
    REGION = "region"
    SECURITY = "security"
    CRYPTO = "crypto"
    ETF = "etf"
    INDEX = "index"
    FUND = "fund"
    ECON_SERIES = "econ_series"
    ECON_EVENT = "econ_event"
    PERSON = "person"
    PROPERTY = "property"
    INSTRUMENT_FAMILY = "instrument_family"
    CORPORATE_ACTION = "corporate_action"
    NEWS_SOURCE = "news_source"
    NEWS_ARTICLE = "news_article"
    SECTOR = "sector"
    INDUSTRY = "industry"
    LEGAL_ENTITY = "legal_entity"
    DATA_PROVIDER = "data_provider"
    CURRENCY_PAIR = "currency_pair"
    BENCHMARK_RATE = "benchmark_rate"
    MARKET_VENUE = "market_venue"


@dataclass
class Entity:
    """Canonical entity tracked across data providers."""

    entity_id: str  # canonical ID matching the chosen EntityType format
    entity_type: EntityType
    name: Optional[str] = None
    metadata: str = ""  # free-form JSON/string blob


class EntityStore(SqliteDataStore):
    """Tracks canonical entities and provider-specific identifiers."""

    def __init__(self, config: Config):
        super().__init__(
            db_name="entities.sqlite",
            config=config,
            summary="Canonical entities and provider ID mappings",
        )
        self._ensure_schema()

    def describe_brief(self) -> str:
        return f"- entities (sqlite://{self.db_path.name}): {self.summary}"

    def _ensure_schema(self) -> None:
        conn = self._ensure_conn()
        conn.executescript(
            """
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS entities (
                entity_id TEXT PRIMARY KEY,
                entity_type TEXT NOT NULL,
                name TEXT,
                metadata TEXT DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TRIGGER IF NOT EXISTS trg_entities_updated
            AFTER UPDATE ON entities
            BEGIN
                UPDATE entities SET updated_at = datetime('now') WHERE rowid = NEW.rowid;
            END;

            CREATE TABLE IF NOT EXISTS providers (
                provider TEXT PRIMARY KEY,
                description TEXT,
                base_url TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS provider_entity_map (
                provider TEXT NOT NULL REFERENCES providers(provider),
                provider_entity_id TEXT NOT NULL,
                entity_id TEXT NOT NULL REFERENCES entities(entity_id),
                active_from TEXT DEFAULT NULL,
                active_to TEXT,
                metadata TEXT,
                PRIMARY KEY (provider, provider_entity_id),
                UNIQUE (provider, entity_id, active_from)
            );

            CREATE INDEX IF NOT EXISTS idx_provider_map_entity ON provider_entity_map(entity_id);

            CREATE TABLE IF NOT EXISTS entity_entity_map (
                src_entity_id TEXT NOT NULL REFERENCES entities(entity_id),
                dst_entity_id TEXT NOT NULL REFERENCES entities(entity_id),
                relation TEXT NOT NULL REFERENCES entity_relation_types(relation),
                metadata TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (src_entity_id, dst_entity_id, relation)
            );

            CREATE INDEX IF NOT EXISTS idx_entity_entity_dst ON entity_entity_map(dst_entity_id);

            CREATE TABLE IF NOT EXISTS entity_relation_types (
                relation TEXT PRIMARY KEY,
                description TEXT
            );
            """
        )
        conn.commit()
        ins = conn.cursor()
        ins.executemany(
            """
            INSERT OR IGNORE INTO entity_relation_types (relation, description)
            VALUES (?, ?);
            """,
            FIXED_RELATION_TYPES,
        )
        conn.commit()

    def upsert_entity(self, entity: Entity) -> DataSourceUpdateResults:
        conn = self._ensure_conn()
        cur = conn.cursor()
        updated = failed = 0
        try:
            cur.execute(
                """
                INSERT INTO entities (entity_id, entity_type, name, metadata)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(entity_id) DO UPDATE SET
                  entity_type=excluded.entity_type,
                  name=excluded.name,
                  metadata=excluded.metadata;
                """,
                (entity.entity_id, entity.entity_type, entity.name, entity.metadata),
            )
            updated = 1
            conn.commit()
        except Exception:
            conn.rollback()
            failed = 1
            logging.debug("Relation insert failed: %s -> %s (%s)", src_entity_id, dst_entity_id, relation)
        return DataSourceUpdateResults(updated=updated, failed=failed)

    def upsert_provider(self, provider: str, description: Optional[str] = None, base_url: Optional[str] = None) -> None:
        conn = self._ensure_conn()
        conn.execute(
            """
            INSERT INTO providers (provider, description, base_url)
            VALUES (?, ?, ?)
            ON CONFLICT(provider) DO UPDATE SET
              description=excluded.description,
              base_url=excluded.base_url;
            """,
            (provider, description, base_url),
        )
        conn.commit()

    def map_provider_entity(
        self,
        *,
        provider: str,
        provider_entity_id: str,
        entity_id: str,
        active_from: Optional[str] = None,
        active_to: Optional[str] = None,
        metadata: Optional[str] = None,
    ) -> DataSourceUpdateResults:
        conn = self._ensure_conn()
        cur = conn.cursor()
        updated = failed = 0
        try:
            cur.execute(
                """
                INSERT INTO provider_entity_map (
                    provider, provider_entity_id, entity_id, active_from, active_to, metadata
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider, provider_entity_id) DO UPDATE SET
                    entity_id=excluded.entity_id,
                    active_from=excluded.active_from,
                    active_to=excluded.active_to,
                    metadata=excluded.metadata;
                """,
                (provider, provider_entity_id, entity_id, active_from, active_to, metadata),
            )
            updated = 1
            conn.commit()
        except Exception:
            failed = 1
        return DataSourceUpdateResults(updated=updated, failed=failed)

    def resolve_entity(self, provider: str, provider_entity_id: str) -> Optional[str]:
        """Return canonical entity_id for a provider-specific id."""
        conn = self._ensure_conn()
        cur = conn.execute(
            """
            SELECT entity_id FROM provider_entity_map
            WHERE provider = ? AND provider_entity_id = ?
              AND (active_to IS NULL OR active_to > datetime('now'))
            LIMIT 1;
            """,
            (provider, provider_entity_id),
        )
        row = cur.fetchone()
        return row[0] if row else None

    def provider_ids_for_entity(self, entity_id: str, provider: Optional[str] = None) -> list[tuple[str, str]]:
        """Return list of (provider, provider_entity_id) for the canonical entity."""
        conn = self._ensure_conn()
        if provider:
            cur = conn.execute(
                """
                SELECT provider, provider_entity_id FROM provider_entity_map
                WHERE entity_id = ? AND provider = ?
                  AND (active_to IS NULL OR active_to > datetime('now'));
                """,
                (entity_id, provider),
            )
        else:
            cur = conn.execute(
                """
                SELECT provider, provider_entity_id FROM provider_entity_map
                WHERE entity_id = ?
                  AND (active_to IS NULL OR active_to > datetime('now'));
                """,
                (entity_id,),
            )
        return [(row[0], row[1]) for row in cur.fetchall()]

    def entity_exists(self, entity_id: str) -> bool:
        conn = self._ensure_conn()
        cur = conn.execute("SELECT 1 FROM entities WHERE entity_id = ? LIMIT 1;", (entity_id,))
        return cur.fetchone() is not None

    def ensure_relation_type(self, relation: str, description: Optional[str] = None) -> None:
        conn = self._ensure_conn()
        cur = conn.execute("SELECT 1 FROM entity_relation_types WHERE relation = ?;", (relation,))
        if cur.fetchone() is None:
            raise ValueError(f"Unknown entity relation type: {relation!r}")

    def map_entity_relation(
        self,
        *,
        src_entity_id: str,
        dst_entity_id: str,
        relation: str,
        metadata: Optional[str] = None,
    ) -> DataSourceUpdateResults:
        """Link two canonical entities with a typed relation."""

        conn = self._ensure_conn()
        # Ensure relation type exists to satisfy FK and keep definitions consistent.
        self.ensure_relation_type(relation, description=None)
        cur = conn.cursor()
        updated = failed = 0
        try:
            cur.execute(
                """
                INSERT INTO entity_entity_map (src_entity_id, dst_entity_id, relation, metadata)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(src_entity_id, dst_entity_id, relation) DO UPDATE SET
                    metadata=excluded.metadata;
                """,
                (src_entity_id, dst_entity_id, relation, metadata),
            )
            updated = 1
            conn.commit()
        except Exception:
            failed = 1
        return DataSourceUpdateResults(updated=updated, failed=failed)
