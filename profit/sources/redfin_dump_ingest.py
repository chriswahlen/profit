from __future__ import annotations

import csv
import gzip
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Iterator, Mapping, Sequence

import sqlite3

logger = logging.getLogger(__name__)

METRIC_COLUMN_CANDIDATES: dict[str, tuple[str, ...]] = {
    "median_sale_price": ("median_sale_price", "median_price"),
    "median_list_price": ("median_list_price", "list_price"),
    "homes_sold": ("homes_sold", "closed_sales"),
    "new_listings": ("new_listings", "new_listing_count"),
    "inventory": ("inventory", "active_listings"),
    "median_dom": ("median_dom", "median_days_on_market"),
    "sale_to_list_ratio": ("sale_to_list_ratio", "avg_sale_to_list", "sale_to_list"),
    "price_drops_pct": ("price_drops_pct", "price_drop_pct", "price_drops", "sold_above_list"),
    "pending_sales": ("pending_sales",),
    "months_supply": ("months_supply", "months_of_supply"),
    "avg_ppsf": ("avg_ppsf", "median_ppsf"),
}

METRIC_COLUMN_ORDER: tuple[str, ...] = (
    "region_id",
    "period_start_date",
    "period_granularity",
    "data_revision",
    "source_provider",
    "median_sale_price",
    "median_list_price",
    "homes_sold",
    "new_listings",
    "inventory",
    "median_dom",
    "sale_to_list_ratio",
    "price_drops_pct",
    "pending_sales",
    "months_supply",
    "avg_ppsf",
)

METRIC_INSERT_SQL = f"""
INSERT OR REPLACE INTO market_metrics ({', '.join(METRIC_COLUMN_ORDER)})
VALUES ({', '.join('?' for _ in METRIC_COLUMN_ORDER)})
"""

REGION_ID_COLUMNS = ("regionid", "region_id", "region_code", "redfin_region_id")
REGION_NAME_COLUMNS = ("regionname", "region_name", "name", "regionlabel")
REGION_TYPE_COLUMNS = ("regiontype", "region_type", "region_type_display", "region_type_label")
CANONICAL_CODE_COLUMNS = ("canonical_code", "canonicalid", "region_code", "regionid")
COUNTRY_COLUMNS = ("country_iso2", "country_code", "country")
PERIOD_COLUMNS = ("period_begin", "period_start", "week_beginning", "period", "snapshot_date", "date")
DATA_REVISION_COLUMNS = ("data_revision", "revision", "revision_id", "run_revision")
PARENT_REGION_COLUMNS = ("stateid", "state_id", "countyid", "county_id", "metro_id", "metroid")
METADATA_COLUMNS = ("state", "state_name", "state_code", "county", "county_name", "metro", "metro_name", "city", "zip", "zip_code")
CODE_MAP_COLUMNS: dict[str, tuple[str, ...]] = {
    "state": ("state", "state_name", "state_code"),
    "county": ("county", "county_name"),
    "metro": ("metro", "metro_name"),
    "city": ("city", "city_name"),
    "zip": ("zip", "zip_code", "zip_code_plain"),
}

FALLBACK_REGION_COLUMNS = ("region", "metro", "city", "state_code", "state", "parent_metro_region")


def _normalize_row(row: Mapping[str, str | None]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in row.items():
        if not key:
            continue
        cleaned = value.strip() if value is not None else ""
        if not cleaned:
            continue
        normalized[key.lower().strip()] = cleaned
    return normalized


def _find_first(row: Mapping[str, str], candidates: Sequence[str]) -> str | None:
    for cand in candidates:
        val = row.get(cand)
        if val is not None:
            return val
    return None


def _parse_period_start(value: str) -> str | None:
    if not value:
        return None
    text = value.split()[0]
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text).date().isoformat()
    except ValueError:
        return None


def _parse_number(value: str | None) -> float | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    lower = text.lower()
    if lower in ("na", "n/a", "none", "null"):
        return None
    is_percent = text.endswith("%")
    cleaned = text.rstrip("%").replace(",", "")
    if not cleaned or cleaned in ("-", "--"):
        return None
    try:
        parsed = float(cleaned)
    except ValueError:
        return None
    if is_percent:
        parsed /= 100.0
    return parsed


def _parse_int(value: str | None) -> int | None:
    num = _parse_number(value)
    if num is None:
        return None
    return int(num)


def _slugify_for_id(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower())
    slug = slug.strip("_")
    return slug or value.lower()


def _derive_fallback_region_id(normalized: Mapping[str, str]) -> str | None:
    candidate = _find_first(normalized, FALLBACK_REGION_COLUMNS)
    if not candidate:
        return None
    region_type = _find_first(normalized, REGION_TYPE_COLUMNS) or "region"
    return f"{region_type.lower()}|{_slugify_for_id(candidate)}"


def _flush_metric_buffer(cur: sqlite3.Cursor, conn: sqlite3.Connection, buffer: list[tuple[str | float | None, ...]]) -> None:
    if not buffer:
        return
    cur.executemany(METRIC_INSERT_SQL, buffer)
    buffer.clear()
    conn.commit()


@dataclass(frozen=True)
class RedfinIngestConfig:
    provider: str = "redfin"
    period_granularity: str = "week"
    country_iso2: str = "US"
    default_data_revision: int = 0
    batch_size: int = 1000
    source_url: str | None = None


@dataclass(frozen=True)
class RedfinIngestionStats:
    row_count: int
    regions: int
    metrics: int
    max_data_revision: int


def rows_from_dump(path: Path, *, delimiter: str = "\t", limit: int | None = None) -> Iterator[dict[str, str]]:
    is_gzip = path.suffix.lower() == ".gz"
    if is_gzip:
        fh_ctx = gzip.open(path, mode="rt", encoding="utf-8", newline="")
    else:
        fh_ctx = path.open(mode="r", encoding="utf-8", newline="")
    with fh_ctx as fh:
        reader = csv.DictReader(fh, delimiter=delimiter)
        count = 0
        try:
            for count, raw in enumerate(reader, start=1):
                yield _normalize_row(raw)
                if limit is not None and count >= limit:
                    break
        except EOFError as exc:
            logger.warning("gzip stream truncated (%s) after %s rows from %s", exc, count, path)
        except OSError as exc:
            logger.warning("error reading compressed dump (%s) after %s rows from %s", exc, count, path)


def ingest_redfin_rows(
    *,
    conn: sqlite3.Connection,
    rows: Iterable[Mapping[str, str]],
    config: RedfinIngestConfig,
    run_started_at: datetime,
) -> RedfinIngestionStats:
    cur = conn.cursor()
    logger.info(
        "ingest_redfin_rows provider=%s granularity=%s started_at=%s",
        config.provider,
        config.period_granularity,
        run_started_at.isoformat(),
    )
    seen_regions: set[str] = set()
    row_count = 0
    max_revision = config.default_data_revision
    metric_buffer: list[tuple[str | float | None, ...]] = []
    region_revision: dict[str, int] = {}
    rows_since_log = 0
    for raw in rows:
        normalized = _normalize_row(raw)
        region_id = _find_first(normalized, REGION_ID_COLUMNS) or _derive_fallback_region_id(normalized)
        region_name = _find_first(normalized, REGION_NAME_COLUMNS)
        region_type = _find_first(normalized, REGION_TYPE_COLUMNS)
        canonical_code = _find_first(normalized, CANONICAL_CODE_COLUMNS) or region_id
        period_start = None
        for cand in PERIOD_COLUMNS:
            period_candidate = normalized.get(cand)
            period_start = _parse_period_start(period_candidate or "")
            if period_start:
                break
        if not region_id or not period_start:
            logger.debug("skipping row missing required region or period data %s", normalized)
            continue

        row_count += 1
        rows_since_log += 1
        revision_val = _parse_int(_find_first(normalized, DATA_REVISION_COLUMNS)) or config.default_data_revision
        max_revision = max(max_revision, revision_val)

        country_iso = _find_first(normalized, COUNTRY_COLUMNS) or config.country_iso2
        parent_region = _find_first(normalized, PARENT_REGION_COLUMNS)
        metadata: dict[str, str] = {}
        for meta_key in METADATA_COLUMNS:
            meta_val = normalized.get(meta_key)
            if meta_val:
                metadata[meta_key] = meta_val

        current_revision = region_revision.get(region_id, -1)
        if revision_val >= current_revision:
            cur.execute(
                """
                INSERT INTO regions (
                    region_id, region_type, name, canonical_code, country_iso2,
                    parent_region_id, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(region_id) DO UPDATE SET
                    region_type=excluded.region_type,
                    name=excluded.name,
                    canonical_code=excluded.canonical_code,
                    country_iso2=excluded.country_iso2,
                    parent_region_id=COALESCE(excluded.parent_region_id, regions.parent_region_id),
                    metadata=excluded.metadata
                """,
                (
                    region_id,
                    region_type or "",
                    region_name or "",
                    canonical_code or region_id,
                    country_iso,
                    parent_region,
                    json.dumps(metadata, sort_keys=True),
                ),
            )
            cur.execute(
                """
                INSERT INTO region_provider_map (
                    provider, provider_region_id, region_id, provider_name, active_from, data_revision
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider, provider_region_id) DO UPDATE SET
                    region_id=excluded.region_id,
                    provider_name=COALESCE(excluded.provider_name, region_provider_map.provider_name),
                    active_from=MIN(region_provider_map.active_from, excluded.active_from),
                    data_revision=MAX(region_provider_map.data_revision, excluded.data_revision)
                """,
                (
                    config.provider,
                    region_id,
                    region_id,
                    region_name or "",
                    run_started_at.isoformat(),
                    revision_val,
                ),
            )
            region_revision[region_id] = revision_val
            seen_regions.add(region_id)
            logger.debug("upserted region %s period %s", region_id, period_start)
        else:
            logger.debug(
                "skipping provider update for %s revision=%s (current=%s)",
                region_id,
                revision_val,
                current_revision,
            )

        # Region code map (e.g., state/city/zip)
        for code_type, candidates in CODE_MAP_COLUMNS.items():
            code_value = _find_first(normalized, candidates)
            if code_value:
                cur.execute(
                    """
                    INSERT OR IGNORE INTO region_code_map (
                        region_id, code_type, code_value, active_from
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (region_id, code_type, code_value, run_started_at.isoformat()),
                )

        metric_values: dict[str, float | None] = {}
        for metric, candidates in METRIC_COLUMN_CANDIDATES.items():
            metric_values[metric] = _parse_number(_find_first(normalized, candidates))

        values: list[str | float | None] = [
            region_id,
            period_start,
            _find_first(normalized, ("period_granularity", "timeframe")) or config.period_granularity,
            revision_val,
            config.provider,
            metric_values["median_sale_price"],
            metric_values["median_list_price"],
            metric_values["homes_sold"],
            metric_values["new_listings"],
            metric_values["inventory"],
            metric_values["median_dom"],
            metric_values["sale_to_list_ratio"],
            metric_values["price_drops_pct"],
            metric_values["pending_sales"],
            metric_values["months_supply"],
            metric_values["avg_ppsf"],
        ]
        metric_buffer.append(tuple(values))
        if len(metric_buffer) >= config.batch_size:
            logger.debug("flushing %s metric rows", len(metric_buffer))
            _flush_metric_buffer(cur, conn, metric_buffer)
            logger.info("processed %s rows (total %s)", rows_since_log, row_count)
            rows_since_log = 0

    _flush_metric_buffer(cur, conn, metric_buffer)
    if rows_since_log:
        logger.info("processed %s rows (total %s)", rows_since_log, row_count)
    stats = RedfinIngestionStats(
        row_count=row_count,
        regions=len(seen_regions),
        metrics=row_count,
        max_data_revision=max_revision,
    )
    logger.info("ingest_redfin_rows finished rows=%s regions=%s max_revision=%s", stats.row_count, stats.regions, stats.max_data_revision)
    return stats


def record_ingestion_run(
    *,
    conn: sqlite3.Connection,
    run_id: str,
    provider: str,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    source_url: str | None,
    row_count: int,
    data_revision: int,
    notes: str | None = None,
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO ingestion_runs (
            run_id, provider, started_at, finished_at, status,
            source_url, row_count, data_revision, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            provider,
            started_at.isoformat(),
            finished_at.isoformat(),
            status,
            source_url,
            row_count,
            data_revision,
            notes,
        ),
    )
