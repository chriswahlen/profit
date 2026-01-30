from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import requests

from profit.catalog.store import CatalogStore
from profit.catalog.types import InstrumentRecord

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _pad_cik(raw: str) -> str:
    return raw.zfill(10)


def _user_agent() -> str:
    return os.getenv("SEC_USER_AGENT") or os.getenv("PROFIT_SEC_USER_AGENT") or "profit-cli"


_ACTIVE_FROM_EPOCH = datetime(1900, 1, 1, tzinfo=timezone.utc)


class SecCompanyTickersRefresher:
    """
    CatalogRefresher that populates provider=sec instruments from SEC's company_tickers.json.
    """

    def __init__(self, store: CatalogStore) -> None:
        self.store = store

    def refresh(self, provider: str, *, allow_network: bool, use_cache_only: bool = False) -> None:
        if provider != "sec":
            return
        if not allow_network:
            logger.warning("sec catalog refresh skipped: network disabled")
            return

        url = "https://www.sec.gov/files/company_tickers.json"
        ua = _user_agent()
        logger.info("sec catalog: download company_tickers.json url=%s", url)
        resp = requests.get(url, headers={"User-Agent": ua}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        rows: list[InstrumentRecord] = []
        now = _now()
        for entry in data.values():
            cik_raw = str(entry.get("cik_str", "")).strip()
            ticker = (entry.get("ticker") or "").strip()
            name = (entry.get("title") or "").strip()
            if not cik_raw:
                continue
            cik = _pad_cik(cik_raw)
            instrument_id = f"equity:US:CIK:{cik}"
            attrs = {}
            if ticker:
                attrs["ticker"] = ticker
            if name:
                attrs["name"] = name
            rows.append(
                InstrumentRecord(
                    instrument_id=instrument_id,
                    instrument_type="equity",
                    provider="sec",
                    provider_code=cik,
                    mic=None,
                    currency=None,
                    active_from=_ACTIVE_FROM_EPOCH,
                    active_to=None,
                    attrs=attrs,
                )
            )
        written = self.store.upsert_instruments(rows)
        # write meta
        cur = self.store.conn.cursor()
        cur.execute(
            """
            INSERT INTO catalog_meta(provider, refreshed_at, source_version, row_count)
            VALUES(:provider, :refreshed_at, :source_version, :row_count)
            ON CONFLICT(provider) DO UPDATE SET
              refreshed_at=excluded.refreshed_at,
              source_version=excluded.source_version,
              row_count=excluded.row_count
            """,
            {
                "provider": "sec",
                "refreshed_at": _now().isoformat(),
                "source_version": None,
                "row_count": written,
            },
        )
        self.store.conn.commit()
        logger.info("sec catalog: refreshed rows=%s", written)
