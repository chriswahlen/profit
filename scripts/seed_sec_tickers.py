#!/usr/bin/env python3
"""Seed SEC company tickers into the canonical entity store.

- Creates/updates Entity records as companies (EntityType.COMPANY) using canonical id `Company.from_name` (us:com:slug).
- Maps SEC tickers to canonical ids in provider_entity_map with provider='sec:edgar'.
- Stores CIK in metadata on the entity row for reference.

Requirements:
- SEC requires a User-Agent. Set env SEC_USER_AGENT (e.g., "youremail@example.com").
- Internet access to https://www.sec.gov/files/company_tickers.json unless --local-path is provided.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib import request

from config import Config
from data_sources.entities import Company
from data_sources.entity import Entity, EntityStore, EntityType

# Relations
RELATION_LISTED_SECURITY = "listed_security"
US_EXCHANGE_MICS = {"XNAS", "XNYS", "XASE"}

SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_PROVIDER = "provider:edgar"
SEC_UA_ENV = "SEC_USER_AGENT"


@dataclass
class SecRow:
    cik: str
    ticker: str
    name: str


def fetch_sec_json(url: str, ua: str) -> dict:
    req = request.Request(url, headers={"User-Agent": ua})
    with request.urlopen(req, timeout=30) as resp:  # nosec B310
        if resp.status != 200:
            raise RuntimeError(f"SEC download failed HTTP {resp.status}")
        return json.loads(resp.read().decode("utf-8"))


def load_rows(*, local_path: Path | None, ua: str) -> list[SecRow]:
    if local_path:
        data = json.loads(local_path.read_text())
    else:
        data = fetch_sec_json(SEC_TICKERS_URL, ua)
    rows: list[SecRow] = []
    for obj in data.values():
        cik = f"{int(obj['cik_str']):010d}"
        ticker = (obj.get("ticker") or "").strip().upper()
        name = (obj.get("title") or "").strip()
        if not ticker or not name:
            continue
        rows.append(SecRow(cik=cik, ticker=ticker, name=name))
    return rows


def seed(rows: Iterable[SecRow], store: EntityStore) -> None:
    store.upsert_provider(provider=SEC_PROVIDER, description="SEC EDGAR", base_url=SEC_TICKERS_URL)
    store.ensure_relation_type(RELATION_LISTED_SECURITY, description="Company is listed security")
    entity_rows = []
    provider_maps = []
    relations = []
    for row in rows:
        comp = Company.from_name(row.name, country_iso2="US")
        entity = Entity(
            entity_id=comp.canonical_id,
            entity_type=EntityType.COMPANY,
            name=row.name,
            metadata=f"{{'cik':'{row.cik}'}}",
        )
        entity_rows.append(entity)
        provider_maps.append((SEC_PROVIDER, row.cik, comp.canonical_id, row.ticker))

        # If the SEC ticker already exists as a security on a US MIC, link company -> security.
        for mic in US_EXCHANGE_MICS:
            sec_entity_id = f"sec:{mic.lower()}:{row.ticker.lower()}"
            if store.entity_exists(sec_entity_id):
                relations.append((comp.canonical_id, sec_entity_id))
                break

    # Upsert entities
    for e in entity_rows:
        store.upsert_entity(e)
    for provider, provider_id, eid, ticker in provider_maps:
        store.map_provider_entity(
            provider=provider,
            provider_entity_id=provider_id,
            entity_id=eid,
            metadata=f'{{"ticker":"{ticker}"}}',
        )

    # Link companies to listed securities when found.
    for src, dst in relations:
        store.map_entity_relation(src_entity_id=src, dst_entity_id=dst, relation=RELATION_LISTED_SECURITY)

    logging.info("Seeded %d SEC companies; mapped %d tickers", len(entity_rows), len(provider_maps))


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    cfg = Config()
    ua = cfg.get_key(SEC_UA_ENV)
    if not ua:
        raise RuntimeError(f"{SEC_UA_ENV} must be set (env or config) with contact email per SEC policy")

    local_path_arg = None
    args = argv if argv is not None else sys.argv[1:]
    if args:
        local_path_arg = Path(args[0])
        if not local_path_arg.exists():
            print(f"Local path not found: {local_path_arg}", file=sys.stderr)
            return 2

    store = EntityStore(cfg)
    rows = load_rows(local_path=local_path_arg, ua=ua)
    seed(rows, store)
    return 0


if __name__ == "__main__":
    sys.exit(main())
