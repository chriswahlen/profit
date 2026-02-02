from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import re
import logging
import json
from pathlib import Path
from typing import Iterable, Sequence

from profit.agent.types import RetrievalPlan, RetrievedData
from profit.cache.columnar_store import ColumnarSqliteStore
from profit.sources.yfinance import FIELD_ORDER, PROVIDER as YF_PROVIDER
from profit.sources.yfinance_ingest import GRID_ORIGIN_US, STEP_US, WINDOW_POINTS
from profit.config import ProfitConfig
from profit.stores.redfin_store import RedfinStore
from profit.catalog.store import CatalogStore
from profit.catalog.entity_store import EntityStore
from profit.agent.edgar_loader import load_chunks
from profit.sources.edgar.sec_edgar import SEC_PROVIDER_ID

logger = logging.getLogger(__name__)

class RetrieverError(RuntimeError):
    pass


def fetch(
    plan: RetrievalPlan,
    *,
    columnar_store: ColumnarSqliteStore | None = None,
    catalog_db_path: Path | None = None,
    redfin_db_path: Path | None = None,
    edgar_docs_path: Path | None = None,
    entity_store_path: Path | None = None,
) -> RetrievedData:
    """
    Dispatch to a source-specific retriever.
    This is intentionally minimal; future work will call into stores.
    """
    if plan.source == "prices":
        payload = fetch_prices(plan, store=columnar_store, catalog_db_path=catalog_db_path)
    elif plan.source == "redfin":
        payload = fetch_redfin(plan, db_path=redfin_db_path)
    elif plan.source == "edgar":
        payload = fetch_edgar(plan, docs_path=edgar_docs_path, store_path=entity_store_path)
    else:
        payload = None
    logger.info("retriever payload source=%s summary=%s", plan.source, _summarize(payload))
    return RetrievedData(source=plan.source, payload=payload, start=plan.start, end=plan.end, metadata={})


def fetch_prices(
    plan: RetrievalPlan,
    *,
    store: ColumnarSqliteStore | None = None,
    catalog_db_path: Path | None = None,
) -> dict:
    """
    Read OHLCV slices from the columnar store for the given instruments.
    Falls back to a 365-day window ending today if no dates are provided.
    """
    instruments, resolved_inputs = _resolve_instruments(plan.instruments, catalog_db_path)
    window_start, window_end = _resolve_window(plan.start, plan.end)
    if store is None:
        store = ColumnarSqliteStore()
        logger.info("prices: opening columnar store path=%s", store.db_path)
    else:
        logger.info("prices: using provided columnar store path=%s", store.db_path)

    providers_priority = [YF_PROVIDER, "stooq"]
    instrument_rows = []
    for instrument_id in instruments:
        instrument_rows.append(
            _read_instrument_series_multi(
                store=store,
                instrument_id=instrument_id,
                providers=providers_priority,
                start_dt=window_start,
                end_dt=window_end,
            )
        )

    return {
        "provider": "multi",
        "field_order": FIELD_ORDER,
        "window": {"start": window_start.date().isoformat(), "end": window_end.date().isoformat()},
        "instruments": instrument_rows,
        "unresolved": [code for code in plan.instruments if code not in resolved_inputs],
    }


def fetch_redfin(plan: RetrievalPlan, *, db_path: Path | None = None) -> dict:
    """
    Read Redfin market metrics from the RedfinStore (profit.sqlite by default).
    """
    window_start, window_end = _resolve_window(plan.start, plan.end)
    db_path = db_path or ProfitConfig.resolve_columnar_db_path(filename="profit.sqlite")
    logger.info("redfin: opening db=%s", db_path)
    store = RedfinStore(db_path, readonly=True)
    regions = _resolve_regions(store, plan.regions)
    metrics = _load_market_metrics(store, regions, window_start.date(), window_end.date())
    return {
        "provider": "redfin",
        "window": {"start": window_start.date().isoformat(), "end": window_end.date().isoformat()},
        "regions": metrics,
        "unresolved_regions": [r for r in plan.regions if r not in {reg['name'] for reg in regions}],
    }


def fetch_edgar(plan: RetrievalPlan, *, docs_path: Path | None = None, store_path: Path | None = None) -> dict:
    """
    Minimal EDGAR retriever: loads local markdown/HTML files and returns paragraph chunks.
    """
    window_start, window_end = _resolve_window(plan.start, plan.end)
    docs_path = docs_path or (Path(ProfitConfig.resolve_data_root()) / "edgar")
    store_path = store_path or ProfitConfig.resolve_columnar_db_path(filename="profit.sqlite")
    logger.info("edgar: docs_path=%s store_path=%s", docs_path, store_path)
    keywords = _keywords(plan)
    chunks = load_chunks(docs_path, keywords=keywords, max_chars_per_chunk=1200)
    facts = _load_finance_facts(store_path, plan.filings, window_start.date(), window_end.date())
    return {
        "provider": "edgar",
        "window": {"start": window_start.date().isoformat(), "end": window_end.date().isoformat()},
        "filings": list(plan.filings),
        "chunks": [c.__dict__ for c in chunks[:100]],
        "facts": facts,
        "unresolved_filings": [f for f in plan.filings if f not in {entry['cik'] for entry in facts}],
    }


def _resolve_window(start: date | None, end: date | None) -> tuple[datetime, datetime]:
    today = datetime.now(timezone.utc).date()
    resolved_end = end or today
    resolved_start = start or (resolved_end - timedelta(days=365))
    if resolved_start > resolved_end:
        resolved_start, resolved_end = resolved_end, resolved_start
    # Inclusive end-of-day for end date to catch the final bar.
    start_dt = datetime.combine(resolved_start, datetime.min.time(), tzinfo=timezone.utc)
    end_dt = datetime.combine(resolved_end, datetime.max.time(), tzinfo=timezone.utc)
    return start_dt, end_dt


def _read_instrument_series_multi(
    *,
    store: ColumnarSqliteStore,
    instrument_id: str,
    providers: list[str],
    start_dt: datetime,
    end_dt: datetime,
) -> dict:
    field_points: dict[str, dict[str, tuple[float, str]]] = {f: {} for f in FIELD_ORDER}
    high_water_iso: str | None = None
    providers_used: set[str] = set()

    for provider_id in providers:
        for field in FIELD_ORDER:
            sid = store.get_series_id(
                instrument_id=instrument_id,
                field=field,
                step_us=STEP_US,
                provider_id=provider_id,
            )
            if sid is None:
                continue
            points = store.read_points(sid, start=start_dt, end=end_dt, include_sentinel=False)
            if not points:
                continue
            providers_used.add(provider_id)
            for pt_dt, val in points:
                ts = pt_dt.date().isoformat()
                # keep first (higher-priority) provider's value
                if ts not in field_points[field]:
                    field_points[field][ts] = (float(val), provider_id)
            hw_us = store.get_high_water_ts_us(sid)
            if hw_us:
                hw_iso = datetime.fromtimestamp(hw_us / 1_000_000, tz=timezone.utc).isoformat()
                if high_water_iso is None or hw_iso > high_water_iso:
                    high_water_iso = hw_iso

    series_data: dict[str, list[dict[str, float | str]]] = {}
    missing_fields: list[str] = []
    for field in FIELD_ORDER:
        pts = field_points[field]
        if not pts:
            missing_fields.append(field)
            series_data[field] = []
            continue
        series_data[field] = [
            {"ts": ts, "value": value, "provider": provider} for ts, (value, provider) in sorted(pts.items())
        ]

    return {
        "instrument_id": instrument_id,
        "fields": series_data,
        "missing_fields": missing_fields,
        "high_water_utc": high_water_iso,
        "providers_used": sorted(providers_used),
    }


def _resolve_instruments(codes: Sequence[str], catalog_db_path: Path | None) -> tuple[tuple[str, ...], set[str]]:
    """
    Map user-entered tickers/provider codes to canonical instrument_ids via CatalogStore.
    If a code already looks like an instrument_id (contains '|'), pass through.
    """
    if not codes:
        return (), set()
    passthrough = [c for c in codes if "|" in c]
    to_resolve = [c for c in codes if "|" not in c]
    if not to_resolve:
        return tuple(sorted(set(passthrough))), set(passthrough)
    db_path = catalog_db_path or ProfitConfig.resolve_columnar_db_path(filename="profit.sqlite")
    logger.info("prices: opening catalog db=%s", db_path)
    store = CatalogStore(db_path, readonly=True)
    out: list[str] = []
    resolved_inputs: set[str] = set(passthrough)
    for code in to_resolve:
        row = store.conn.execute(
            """
            SELECT instrument_id
            FROM instrument_provider_map
            WHERE provider = ? AND provider_code = ?
            LIMIT 1;
            """,
            (YF_PROVIDER, code.upper()),
        ).fetchone()
        if row:
            out.append(row[0])
            resolved_inputs.add(code)
    out.extend(passthrough)
    return tuple(sorted(set(out))), resolved_inputs


def _keywords(plan: RetrievalPlan) -> set[str]:
    words = set()
    fields = []
    fields.extend(plan.filings)
    fields.extend(plan.instruments)
    fields.extend(plan.regions)
    if plan.notes:
        fields.append(plan.notes)
    for field in fields:
        for token in re.findall(r"[a-zA-Z]{4,}", str(field)):
            words.add(token.lower())
    return words


def _normalize_cik(value: str) -> str:
    digits = re.sub(r"\D", "", value or "")
    return digits.zfill(10)


def _resolve_entities(store_path: Path, filings: Sequence[str]) -> list[tuple[str, str]]:
    """
    Resolve input filings (CIKs or tickers) to (entity_id, cik) tuples using EntityStore.
    """
    if not filings:
        return []
    store = EntityStore(store_path, readonly=True)
    out: list[tuple[str, str]] = []
    for token in filings:
        entity_id = None
        cik = None
        if token.isdigit() or re.fullmatch(r"\d{1,10}", token):
            cik = _normalize_cik(token)
            entity_id = store.find_entity_by_identifier(scheme="sec:cik", value=cik)
        if entity_id is None:
            # Try ticker:us mapping
            entity_id = store.find_entity_by_identifier(scheme="ticker:us", value=token.upper())
        if entity_id:
            if cik is None:
                # fetch any cik identifier
                row = store.conn.execute(
                    """
                    SELECT value FROM entity_identifier
                    WHERE entity_id=? AND scheme='sec:cik'
                    ORDER BY last_seen DESC LIMIT 1
                    """,
                    (entity_id,),
                ).fetchone()
                cik = row["value"] if row else None
            if cik:
                out.append((entity_id, cik))
    return out


def _load_finance_facts(store_path: Path, filings: Sequence[str], start_date: date, end_date: date) -> list[dict]:
    entities = _resolve_entities(store_path, filings)
    if not entities:
        return []
    ids = [e[0] for e in entities]
    cik_map = dict(entities)
    store = EntityStore(store_path, readonly=True)
    placeholders = ",".join("?" for _ in ids)
    cur = store.conn.cursor()
    params = [SEC_PROVIDER_ID, *[cik_map[eid] for eid in ids], start_date.isoformat(), end_date.isoformat()]
    cur.execute(
        f"""
        SELECT f.entity_id, f.provider_entity_id, f.report_id, f.report_key, f.period_start, f.period_end,
               f.value, f.units, f.asof, f.filed_at, f.decimals, f.is_consolidated, f.amendment_flag
        FROM company_finance_fact f
        WHERE f.provider_id = ?
          AND f.provider_entity_id IN ({placeholders})
          AND f.period_end BETWEEN ? AND ?
        ORDER BY f.period_end DESC, f.report_id, f.report_key
        LIMIT 500
        """,
        params,
    )
    rows = cur.fetchall()
    names = {
        row["entity_id"]: row["name"]
        for row in store.conn.execute(
            f"SELECT entity_id, name FROM entity WHERE entity_id IN ({placeholders})", tuple(ids)
        )
    }
    grouped: dict[str, list[dict]] = {eid: [] for eid in ids}
    for row in rows:
        grouped[row["entity_id"]].append(
            {
                "cik": row["provider_entity_id"],
                "report_id": row["report_id"],
                "report_key": row["report_key"],
                "period_start": row["period_start"],
                "period_end": row["period_end"],
                "value": row["value"],
                "units": row["units"],
                "asof": row["asof"],
                "filed_at": row["filed_at"],
                "decimals": row["decimals"],
                "is_consolidated": row["is_consolidated"],
                "amendment_flag": row["amendment_flag"],
            }
        )
    return [
        {
            "entity_id": eid,
            "name": names.get(eid),
            "cik": cik_map[eid],
            "facts": grouped.get(eid, []),
        }
        for eid in ids
    ]


def _summarize(payload, max_len: int = 800) -> str:
    try:
        raw = json.dumps(payload, default=str)
    except TypeError:
        raw = str(payload)
    if len(raw) > max_len:
        return f"[len={len(raw)} truncated->{max_len}] " + raw[: max_len - 3] + "..."
    return raw


def _resolve_regions(store: RedfinStore, region_terms: Sequence[str]) -> list[dict]:
    if not region_terms:
        return []
    regions: list[dict] = []
    cur = store.conn.cursor()
    for term in region_terms:
        cur.execute(
            """
            SELECT region_id, name, region_type, canonical_code
            FROM regions
            WHERE name = ? COLLATE NOCASE
               OR canonical_code = ?
            LIMIT 1;
            """,
            (term, term),
        )
        row = cur.fetchone()
        if row:
            regions.append(dict(row))
            continue
        # Try provider map
        cur.execute(
            """
            SELECT r.region_id, r.name, r.region_type, r.canonical_code
            FROM region_provider_map rpm
            JOIN regions r ON r.region_id = rpm.region_id
            WHERE rpm.provider_region_id = ?
            LIMIT 1;
            """,
            (term,),
        )
        row = cur.fetchone()
        if row:
            regions.append(dict(row))
    return regions


def _load_market_metrics(
    store: RedfinStore,
    regions: list[dict],
    start_date: date,
    end_date: date,
) -> list[dict]:
    if not regions:
        return []
    region_ids = tuple(r["region_id"] for r in regions)
    cur = store.conn.cursor()
    placeholders = ",".join("?" for _ in region_ids)
    cur.execute(
        f"""
        SELECT *
        FROM market_metrics
        WHERE region_id IN ({placeholders})
          AND period_start_date BETWEEN ? AND ?
        ORDER BY region_id, period_start_date;
        """,
        (*region_ids, start_date.isoformat(), end_date.isoformat()),
    )
    rows = cur.fetchall()
    metrics_by_region: dict[str, list[dict]] = {r["region_id"]: [] for r in regions}
    for row in rows:
        metrics_by_region[row["region_id"]].append(dict(row))
    output = []
    for r in regions:
        output.append(
            {
                "region_id": r["region_id"],
                "name": r["name"],
                "region_type": r["region_type"],
                "canonical_code": r["canonical_code"],
                "metrics": metrics_by_region.get(r["region_id"], []),
            }
        )
    return output
