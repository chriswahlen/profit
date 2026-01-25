# Initial schema sketch (global, provider-neutral)

Goal: support multi-provider economic/market/real-estate data with clear lineage and reproducible snapshots. All timestamps are UTC; keep calendar/venue metadata to interpret local times when needed.

## Reference dimensions
- **currency**: `code` (ISO4217, PK), `name`, `minor_units`.
- **country**: `iso2`, `iso3`, `name`, `region` (UN-style), `subregion`.
- **venue**: `venue_id` (PK), `mic` (nullable), `name`, `country_iso2`, `timezone`, `calendar_id`.
- **calendar**: `calendar_id` (PK), `name`, `tz`, `ruleset_version`. (Rules live externally; store version + source.)
- **instrument**: `instrument_id` (PK), `type` (equity, etf, fx_pair, future, bond, crypto, index, rate, real_estate_region, macro_series, custom), `currency`, `venue_id` (nullable), `country_iso2` (nullable), `status`, `attrs` (JSON for type-specific fields, e.g., `maturity`, `multiplier`).
- **identifier_map**: `instrument_id`, `scheme` (`isin`, `figi`, `cusip`, `ticker+mic`, `provider:code`, etc.), `value`, `active_from`, `active_to`, `source`.

## Market & macro time series
- **bar_ohlcv**: `instrument_id`, `ts_utc`, `freq` (1m/5m/1d/etc), `open`, `high`, `low`, `close`, `volume`, `vwap` (nullable), `currency`, `source`, `asof`, `version`.
- **fx_rate**: `base_ccy`, `quote_ccy`, `ts_utc`, `rate`, `source`, `asof`, `version`.
- **corporate_action**: `instrument_id`, `effective_date`, `action_type` (`split`, `dividend_cash`, `dividend_stock`, `symbol_change`, `merger`, etc.), `ratio` (for splits), `cash_amount`, `cash_currency`, `new_identifier` (for symbol change/merger), `source`, `asof`, `version`.
- **yield_curve_point**: `curve_id` (e.g., country+curve_type), `ts_utc`, `tenor` (P3M, P2Y, etc.), `rate`, `source`, `asof`, `version`.
- **macro_series_point**: `series_id`, `ts_utc` (or `period_end`), `value`, `unit`, `seasonal_adjustment`, `country_iso2` (nullable), `source`, `asof`, `version`.

## Real estate (stub for later)
- **re_region**: `region_id` (PK), `name`, `country_iso2`, `admin_level` (city/county/state/metro), `parent_region_id` (nullable), `geojson` (nullable).
- **re_metric**: `region_id`, `ts_utc`/`period_end`, `metric` (`price_index`, `median_price`, `dom`, `inventory`, etc.), `value`, `unit`, `source`, `asof`, `version`.

## Ingestion + lineage
- **ingest_run**: `run_id` (PK), `source`, `started_at`, `finished_at`, `status`, `params` (JSON), `error` (nullable).
- **dataset_version**: `dataset` (`bar_ohlcv`, `fx_rate`, etc.), `source`, `version`, `watermark_start`, `watermark_end`, `schema_hash`, `created_at`.
- **dataset_chunk**: `run_id`, `dataset`, `path` (e.g., Parquet file), `records`, `byte_size`, `checksum`, `partition_keys`, `created_at`.

## Storage and partitioning (initial recommendation)
- Use Parquet + DuckDB/PyArrow datasets.
- Partition primarily by `dataset/source/freq` then by `date_bucket` (e.g., `ts_utc` date) and, where high-cardinality, by `instrument_hash` to balance file sizes.
- Keep metadata tables (above) in DuckDB or SQLite; they reference on-disk Parquet paths via `dataset_chunk`.

## Validation essentials
- Key uniqueness: (`instrument_id`, `ts_utc`, `freq`, `source`, `version`) unique in `bar_ohlcv`.
- Monotonic time per instrument/freq; no duplicate bars per key.
- Corporate actions effective dates must align with instrument calendar when available.
- FX pairs enforce canonical ordering (e.g., `EUR/USD` as base/quote); store as two columns, not one string.

## Versioning & as-of
- Every mutable dataset carries `version` (schema or provider change) and `asof` (ingestion timestamp). Query APIs should allow “as-of” time travel.
- Breaking schema changes increment `version` and invalidate/segment caches accordingly.

