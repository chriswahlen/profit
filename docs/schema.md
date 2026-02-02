# Initial schema sketch (global, provider-neutral)

Goal: support multi-provider economic/market/real-estate data with clear lineage and reproducible snapshots. All timestamps are UTC; keep calendar/venue metadata to interpret local times when needed.

## Redfin schema (provider-specific component)

1. `regions` – canonical geography table keyed by `region_id`. Store `region_type`, `name`, a single `canonical_code` (ISO2/FIPS/CBSA/ZCTA/Geoname ID), `country_iso2`, optional `parent_region_id`, population, timezone, and free-form metadata. `created_at` defaults to `datetime('now')`.

2. `region_code_map` – alternate codes keyed by `(region_id, code_type, code_value, active_from)` to track ISO variants, FIPS slices, CBSA/CSA, and other external identifiers.

3. `region_provider_map` – maps provider identifiers (e.g., Redfin `region_id`) to our canonical `region_id`. Includes `provider_name`, active window, and `data_revision` so ingestion can detect remaps/backfills.

4. `market_metrics` – fact table at `(region_id, period_start_date, period_granularity)` with normalized price/inventory/DOM/sale-to-list/pending/price-drop metrics plus `source_provider`, `data_revision`, and `created_at`. Indexes cover region+period, period, and data revision.

5. `ingestion_runs` – audit trail per fetch run with `run_id`, provider metadata, `status`, optional `etag/last_modified`, row count, and `data_revision`.
