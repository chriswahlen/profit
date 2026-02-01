from __future__ import annotations

"""
Helpers to fetch yfinance OHLCV data and write it into the columnar store.

This keeps CLI glue thin and lets tests exercise ingestion without touching
the network by swapping in a stubbed download_fn on the fetcher.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Iterable, Sequence, Tuple

import pandas as pd

from profit.cache import FileCache
from profit.cache.columnar_store import ColumnarSqliteStore
from profit.catalog.lifecycle import CatalogLifecycleReader
from profit.catalog.refresher import CatalogChecker
from profit.catalog.store import CatalogStore
from profit.catalog.types import InstrumentRecord
from profit.config import ProfitConfig
from profit.sources.yfinance import DATASET, FIELD_ORDER, PROVIDER, YFinanceFetcher, YFinanceRequest
from profit.stores.container import StoreContainer

logger = logging.getLogger(__name__)

DAY = timedelta(days=1)
STEP_US = int(86_400_000_000)  # daily
GRID_ORIGIN_US = int(datetime(1900, 1, 1, tzinfo=timezone.utc).timestamp() * 1_000_000)
WINDOW_POINTS = 1095  # align with Stooq seeders (3 years per slice)


def _parse_date(val: str) -> datetime:
    return datetime.fromisoformat(val).replace(tzinfo=timezone.utc) if "T" not in val else datetime.fromisoformat(val).astimezone(timezone.utc)


def _resolve_provider_codes(
    catalog: CatalogStore, instrument_ids: Sequence[str]
) -> list[Tuple[str, str, InstrumentRecord]]:
    """
    Given canonical instrument_ids, resolve to (instrument_id, provider_code, record).
    Raises if any mapping is missing.
    """
    out: list[Tuple[str, str, InstrumentRecord]] = []
    missing: list[str] = []
    for inst in instrument_ids:
        cur = catalog.conn.cursor()
        cur.execute(
            """
            SELECT provider_code
            FROM instrument_provider_map
            WHERE instrument_id = ? AND provider = ?
            LIMIT 1;
            """,
            (inst, PROVIDER),
        )
        row = cur.fetchone()
        provider_code = row[0] if row else None
        rec = catalog.get_instrument(provider=PROVIDER, provider_code=provider_code) if provider_code else None
        if rec is None:
            if "|" in inst:
                fallback = inst.split("|", 1)[1].upper()
                if provider_code != fallback:
                    cur2 = catalog.conn.cursor()
                    cur2.execute(
                        """
                        SELECT instrument_id
                        FROM instrument_provider_map
                        WHERE provider = ? AND provider_code = ?
                        LIMIT 1;
                        """,
                        (PROVIDER, fallback),
                    )
                    if cur2.fetchone():
                        rec = catalog.get_instrument(provider=PROVIDER, provider_code=fallback)
                        provider_code = fallback
        if rec is None or provider_code is None:
            missing.append(inst)
            continue
        out.append((inst, provider_code, rec))
    if missing:
        raise RuntimeError(
            "Missing yfinance provider mapping for instrument_id(s): " + ", ".join(sorted(set(missing)))
        )
    return out


def _ensure_catalog_meta(catalog: CatalogStore, provider: str, now: datetime) -> None:
    cur = catalog.conn.cursor()
    cur.execute(
        """
        INSERT INTO catalog_meta (provider, refreshed_at, source_version, row_count)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(provider) DO UPDATE SET refreshed_at=excluded.refreshed_at
        """,
        (provider, now.astimezone(timezone.utc).isoformat(), None, 0),
    )
    catalog.conn.commit()


class _AlwaysActiveLifecycleReader:
    def get_lifecycle(self, provider: str, provider_code: str):
        return datetime(1900, 1, 1, tzinfo=timezone.utc), None


class _NoopCatalogChecker:
    def ensure_fresh(self, provider: str):
        return

    def require_present(self, provider: str, provider_code: str):
        return


def _series_id(store: ColumnarSqliteStore, instrument_id: str, field: str) -> int:
    return store.get_or_create_series(
        instrument_id=instrument_id,
        dataset=DATASET,
        field=field,
        step_us=STEP_US,
        grid_origin_ts_us=GRID_ORIGIN_US,
        window_points=WINDOW_POINTS,
        compression="zlib",
        offsets_enabled=False,
        checksum_enabled=True,
        sentinel_f64=float("nan"),
    )


def _write_frame(store: ColumnarSqliteStore, instrument_id: str, frame: pd.DataFrame) -> None:
    if frame is None or frame.empty:
        return
    for field in FIELD_ORDER:
        sid = _series_id(store, instrument_id, field)
        pts = [(ts, float(val)) for ts, val in frame[field].items() if pd.notna(val)]
        if pts:
            store.write(sid, pts)
            last_ts = max(ts for ts, _ in pts)
            store.bump_high_water_ts_us(sid, int(last_ts.timestamp() * 1_000_000))


def fetch_and_store_yfinance(
    *,
    instrument_ids: Iterable[str],
    start: datetime,
    end: datetime,
    cfg: ProfitConfig,
    stores: StoreContainer,
    cache: FileCache,
    offline: bool = False,
    ttl: timedelta = timedelta(days=1),
    download_fn=None,
    dry_run: bool = False,
) -> None:
    """
    Fetch yfinance OHLCV for tickers in [start, end] and write to columnar store.

    Requires instruments to exist in the catalog under provider ``yfinance``.
    """
    start = start.astimezone(timezone.utc)
    end = end.astimezone(timezone.utc)
    if start > end:
        raise ValueError("start must be <= end")

    instrument_ids = [t.strip() for t in instrument_ids if t.strip()]
    if not instrument_ids:
        raise ValueError("at least one instrument_id is required")

    resolved = _resolve_provider_codes(stores.catalog, instrument_ids)
    records = {inst: rec for inst, _pc, rec in resolved}
    lifecycle = CatalogLifecycleReader(stores.catalog)
    _ensure_catalog_meta(stores.catalog, PROVIDER, datetime.now(timezone.utc))

    class _NoopRefresher:
        def refresh(self, provider: str, *, allow_network: bool, use_cache_only: bool = False) -> None:
            return

    catalog_checker = CatalogChecker(
        store=stores.catalog,
        refresher=_NoopRefresher(),
        max_age=ttl,
        allow_network=not offline,
        use_cache_only=offline,
    )

    fetcher = YFinanceFetcher(
        cfg=cfg,
        cache=cache,
        ttl=ttl,
        offline=offline,
        lifecycle=lifecycle,
        catalog_checker=catalog_checker,
        download_fn=download_fn,
    )

    requests = [YFinanceRequest(ticker=pc, provider_code=pc) for _inst, pc, _rec in resolved]
    frames = fetcher.timeseries_fetch_many(requests, start, end)

    for (instrument_id, pc, _rec), frame in zip(resolved, frames):
        if dry_run:
            logger.info(
                "yfinance dry-run ticker=%s instrument_id=%s points=%s",
                pc,
                instrument_id,
                len(frame.index),
            )
            continue
        _write_frame(stores.columnar, instrument_id, frame)
        logger.info(
            "yfinance stored ticker=%s instrument_id=%s points=%s",
            pc,
            instrument_id,
            len(frame.index),
        )
