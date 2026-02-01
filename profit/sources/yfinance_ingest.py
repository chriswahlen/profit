from __future__ import annotations

"""
Helpers to fetch yfinance OHLCV data and write it into the columnar store.

This keeps CLI glue thin and lets tests exercise ingestion without touching
the network by swapping in a stubbed download_fn on the fetcher.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Sequence

import pandas as pd

from profit.cache import FileCache
from profit.cache.columnar_store import ColumnarSqliteStore
from profit.catalog.lifecycle import CatalogLifecycleReader
from profit.catalog.refresher import CatalogChecker
from profit.catalog.store import CatalogStore
from profit.catalog.types import InstrumentRecord
from profit.config import ProfitConfig
from profit.sources.yfinance import FIELD_ORDER, PROVIDER, YFinanceFetcher, YFinanceRequest
from profit.stores.container import StoreContainer
from profit.utils.market_calendar import adjust_end_to_last_session

logger = logging.getLogger(__name__)

DAY = timedelta(days=1)
STEP_US = int(86_400_000_000)  # daily
GRID_ORIGIN_US = int(datetime(1900, 1, 1, tzinfo=timezone.utc).timestamp() * 1_000_000)
WINDOW_POINTS = 1095  # align with Stooq seeders (3 years per slice)
US_MARKET_PREFIXES = ("XNAS|", "XNYS|")


@dataclass(frozen=True)
class ResolvedInstrument:
    instrument_id: str
    provider_code: str
    derived: bool


def _parse_date(val: str) -> datetime:
    return datetime.fromisoformat(val).replace(tzinfo=timezone.utc) if "T" not in val else datetime.fromisoformat(val).astimezone(timezone.utc)


def _derive_provider_code(instrument_id: str) -> str | None:
    if "|" not in instrument_id:
        return None
    return instrument_id.split("|", 1)[1].upper()


def _ensure_derived_placeholder(
    catalog: CatalogStore,
    resolved_inst: ResolvedInstrument,
    start: datetime,
) -> None:
    if not resolved_inst.derived:
        return
    inst_id = resolved_inst.instrument_id
    provider_code = resolved_inst.provider_code
    inst_exists = catalog.conn.execute(
        "SELECT 1 FROM instrument WHERE instrument_id = ? LIMIT 1", (inst_id,)
    ).fetchone()
    if inst_exists is None:
        prefix = inst_id.split("|", 1)[0] if "|" in inst_id else None
        instr = InstrumentRecord(
            instrument_id=inst_id,
            instrument_type="equity",
            provider=PROVIDER,
            provider_code=provider_code,
            mic=prefix,
            currency=None,
            active_from=start,
            active_to=None,
            attrs={"derived_temp": True},
        )
        catalog.upsert_instruments([instr])
        return
    catalog.upsert_provider_mapping(
        instrument_id=inst_id,
        provider=PROVIDER,
        provider_code=provider_code,
        active_from=start,
        attrs={"derived_temp": True},
    )


def _resolve_provider_codes(
    catalog: CatalogStore, instrument_ids: Sequence[str]
) -> list[ResolvedInstrument]:
    """
    Given canonical instrument_ids, resolve to provider codes.
    Raises if a provider mapping cannot be derived.
    """
    out: list[ResolvedInstrument] = []
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
        derived = False
        if provider_code is None:
            fallback = _derive_provider_code(inst)
            if fallback:
                provider_code = fallback
                derived = True
        if provider_code is None:
            missing.append(inst)
            continue
        out.append(ResolvedInstrument(inst, provider_code, derived))
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
        field=field,
        provider_id=PROVIDER,
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


def _catchup_window(
    store: ColumnarSqliteStore,
    instrument_id: str,
    default_start: datetime,
    now: datetime,
    end_adjust_fn=None,
) -> tuple[datetime, datetime, int]:
    """Return the window for catch-up fetches.

    The start date is the last recorded day in the columnar store for the
    instrument across any provider. If no data exists, raise instead of
    guessing, so callers can decide how to seed history. End always uses ``now``.
    """

    latest_ts_us: int | None = None
    cur = store._cursor("catchup_high_water")  # re-use connection
    cur.execute(
        """
        SELECT MAX(high_water_ts_us)
        FROM __col_series__
        WHERE instrument_id = ? AND step_us = ?
        """,
        (instrument_id, STEP_US),
    )
    row = cur.fetchone()
    if row:
        latest_ts_us = row[0]

    if latest_ts_us is None:
        raise RuntimeError(
            f"catch-up requires existing data for instrument_id={instrument_id}; none found"
        )

    adjusted_end = end_adjust_fn(now) if end_adjust_fn else now
    latest_dt = datetime.fromtimestamp(latest_ts_us / 1_000_000, tz=timezone.utc)
    start_day = (latest_dt + DAY).date()
    end_day = adjusted_end.date()
    if start_day > end_day:
        # Already up to date; keep start=end to avoid invalid window.
        start = datetime.combine(end_day, datetime.min.time(), tzinfo=timezone.utc)
    else:
        start = datetime.combine(start_day, datetime.min.time(), tzinfo=timezone.utc)
    end = datetime.combine(end_day, datetime.min.time(), tzinfo=timezone.utc)
    return start, end, latest_ts_us


def _adjust_end_for_us_market(end: datetime) -> datetime:
    # Avoid requesting future/closed market days for US equities.
    return adjust_end_to_last_session(end, calendar_name="NYSE")


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
    catch_up: bool = False,
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

    # Allow per-instrument catch-up windows.
    now = datetime.now(timezone.utc)
    requests = [YFinanceRequest(ticker=inst.provider_code, provider_code=inst.provider_code) for inst in resolved]
    derived_codes = {inst.provider_code for inst in resolved if inst.derived}
    for inst in resolved:
        _ensure_derived_placeholder(stores.catalog, inst, start)
    pending_derived = set(derived_codes)

    if catch_up:
        window_groups: dict[tuple[datetime, datetime], list[tuple[ResolvedInstrument, YFinanceRequest, int]]] = {}
        try:
            for inst in resolved:
                end_adjust = _adjust_end_for_us_market if inst.instrument_id.startswith(US_MARKET_PREFIXES) else None
                start_i, end_i, last_ts_us = _catchup_window(
                    stores.columnar,
                    inst.instrument_id,
                    start,
                    now,
                    end_adjust_fn=end_adjust,
                )
                req = YFinanceRequest(ticker=inst.provider_code, provider_code=inst.provider_code)
                logger.info(
                    "yfinance catch-up planning ticker=%s instrument_id=%s last_day=%s start=%s end=%s",
                    inst.provider_code,
                    inst.instrument_id,
                    datetime.fromtimestamp(last_ts_us / 1_000_000, tz=timezone.utc).date().isoformat(),
                    start_i.date().isoformat(),
                    end_i.date().isoformat(),
                )
                window_groups.setdefault((start_i, end_i), []).append((inst, req, last_ts_us))

            for (start_i, end_i), group in window_groups.items():
                requests_group = [g[1] for g in group]
                try:
                    frames = fetcher.timeseries_fetch_many(requests_group, start_i, end_i)
                except Exception:
                    for code in list(pending_derived):
                        stores.catalog.remove_provider_mapping(provider=PROVIDER, provider_code=code)
                    raise
                for (inst, _req, last_ts_us), frame in zip(group, frames):
                    if frame is not None and not frame.empty:
                        last_dt = datetime.fromtimestamp(last_ts_us / 1_000_000, tz=timezone.utc)
                        newest_dt = frame.index.max().to_pydatetime()
                        logger.info(
                            "yfinance catch-up fetched ticker=%s instrument_id=%s points=%s last_seen=%s newest_point=%s",
                            inst.provider_code,
                            inst.instrument_id,
                            len(frame.index),
                            last_dt.isoformat(),
                            newest_dt.isoformat(),
                        )
                    _consume_frame(
                        stores=stores,
                        resolved_inst=inst,
                        frame=frame,
                        start=start_i,
                        dry_run=dry_run,
                        pending_derived=pending_derived,
                    )
        except Exception:
            # ensure pending derived cleaned on error
            for code in list(pending_derived):
                stores.catalog.remove_provider_mapping(provider=PROVIDER, provider_code=code)
            raise
    else:
        try:
            frames = fetcher.timeseries_fetch_many(requests, start, end)
        except Exception:
            for code in list(pending_derived):
                stores.catalog.remove_provider_mapping(provider=PROVIDER, provider_code=code)
            raise
        for resolved_inst, frame in zip(resolved, frames):
            _consume_frame(
                stores=stores,
                resolved_inst=resolved_inst,
                frame=frame,
                start=start,
                dry_run=dry_run,
                pending_derived=pending_derived,
            )

    for code in list(pending_derived):
        stores.catalog.remove_provider_mapping(provider=PROVIDER, provider_code=code)


def _consume_frame(
    *,
    stores: StoreContainer,
    resolved_inst: ResolvedInstrument,
    frame: pd.DataFrame,
    start: datetime,
    dry_run: bool,
    pending_derived: set[str],
) -> None:
    instrument_id = resolved_inst.instrument_id
    if dry_run:
        logger.info(
            "yfinance dry-run ticker=%s instrument_id=%s points=%s",
            resolved_inst.provider_code,
            instrument_id,
            len(frame.index) if frame is not None else 0,
        )
        if resolved_inst.derived:
            stores.catalog.remove_provider_mapping(provider=PROVIDER, provider_code=resolved_inst.provider_code)
            pending_derived.discard(resolved_inst.provider_code)
        return

    if frame is None:
        frame = pd.DataFrame()
    if frame.empty:
        if resolved_inst.derived:
            stores.catalog.remove_provider_mapping(provider=PROVIDER, provider_code=resolved_inst.provider_code)
            pending_derived.discard(resolved_inst.provider_code)
        return

    _write_frame(stores.columnar, instrument_id, frame)
    logger.info(
        "yfinance stored ticker=%s instrument_id=%s points=%s",
        resolved_inst.provider_code,
        instrument_id,
        len(frame.index),
    )
    if resolved_inst.derived:
        stores.catalog.upsert_provider_mapping(
            instrument_id=instrument_id,
            provider=PROVIDER,
            provider_code=resolved_inst.provider_code,
            active_from=start,
        )
        pending_derived.discard(resolved_inst.provider_code)
