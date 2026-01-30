# EDGAR Corporate Fundamentals (SEC) - Schema + Design Notes (v1)

This document captures the initial design for a corporate fundamentals data source backed by SEC EDGAR filings.

The goal is to make fundamentals **provider-neutral, globally identifiable, and time-correct** while staying dependency-light and reproducible.

## Scope (v1)

- Source/provider: `sec` (EDGAR)
- Forms included:
  - Annual: `10-K`, `10-K/A`, `20-F`, `20-F/A`, `40-F`, `40-F/A`
  - Quarterly: `10-Q`, `10-Q/A`
- Facts included:
  - Numeric facts (stored as `REAL`)
  - Text facts (stored as gzipped blob with a preview)
- Dimensions:
  - Explicit dimensions are stored and queryable.
  - Typed dimensions are preserved in `attrs` but excluded from identity hashing (see "Dimensions").

Why:
- Including 20-F/40-F expands coverage to foreign private issuers (annual reporting) without pulling in 6-K noise.
- Keeping both numeric and text facts preserves disclosure content while keeping a path to efficient quant workflows.

## Storage: where data lives

All fundamentals datasets are stored in the same SQLite file used elsewhere in the repo (the `SqliteStore` DB path passed around in scripts).

Why:
- Reduces wiring complexity and keeps catalog + fundamentals co-located.
- SQLite remains dependency-light and deterministic for local workflows.

## Instrument identity

- Provider: `sec`
- Provider code: `CIK` as a **10-digit zero-padded** string.
- Internal instrument id: `equity:US:CIK:<cik10>`

Why:
- CIK is stable and globally unambiguous within EDGAR.
- Using an internal ID derived from CIK avoids coupling fundamentals to any market data provider (e.g., yfinance).

## Catalog dependency (v1)

The fetcher remains "blocked" behind the catalog:
- The instrument must exist in the catalog as `provider=sec, provider_code=<cik10>`.
- We seed the catalog from a repo CSV (`metadata/sec_ciks.csv`) rather than depending on yfinance.

Why:
- Keeps lifecycle enforcement consistent with the rest of the fetchers (fail loudly when unknown).
- Avoids accidental universe expansion / silent mismatch of identifiers.

## Ingestion unit and coverage model

Unit of ingestion is a single filing submission (accession).

Coverage is tracked by "have we ingested accession X already?" rather than by date ranges.

Why:
- EDGAR updates are event-based (new accessions appear; amendments are new accessions).
- Accession-level coverage is deterministic and naturally handles restatements.

## As-of semantics (time travel)

We store all versions of facts across filings and provide an `read_asof(..., asof=...)` helper that returns the best-known fact as of a given timestamp.

- `accepted_at`: SEC acceptance timestamp (preferred when available)
- `filed_at`: filing date/time
- `known_at = COALESCE(accepted_at, filed_at)` is the timestamp used for as-of selection.

As-of selection rule:
- For each "fact identity key" (below), choose the row with maximum `known_at` such that `known_at <= asof`.

Why:
- Enables time-correct backtests and avoids lookahead bias.
- Provides a reproducible snapshot of "what we knew then."

## Dimensions

Many XBRL facts are qualified by dimensions (axis/member pairs), e.g. segment/geography breakdowns.

We store explicit dimensions as:
- `dims_json`: canonical JSON array of objects like `{"axis": "<qname>", "member": "<qname>"}`, sorted for determinism
- `dims_key`: stable string representation for debugging/filtering (e.g. `axis=member|axis=member`)
- `dims_hash`: short hash of the canonical dims; totals have `dims_hash = ""` and `dims_json = "[]"`

Typed dimensions:
- Preserved in `attrs` for traceability.
- Excluded from `dims_hash` and from the fact identity key in v1.

Why:
- Explicit dims are common and useful for queryability.
- Typed dims can be high-cardinality and hard to canonicalize; excluding them from identity avoids key explosion while still retaining the raw content.

## Datasets (SqliteStore)

### 1) Filings (coverage + lineage)

Dataset name: `fundamentals_filing_sec_v1`

Primary key:
- `(provider, provider_code, accession)`

Columns (initial sketch):
- `provider TEXT` (always `sec`)
- `provider_code TEXT` (CIK10)
- `instrument_id TEXT` (`equity:US:CIK:<cik10>`)
- `accession TEXT`
- `form TEXT`
- `filed_at TIMESTAMP`
- `accepted_at TIMESTAMP` (nullable)
- `known_at TIMESTAMP` (materialized `COALESCE(accepted_at, filed_at)`)
- `report_period_end TIMESTAMP` (nullable; when derivable)
- `is_amendment INTEGER` (0/1)
- `asof TIMESTAMP` (ingest timestamp)
- `attrs TEXT/BLOB` (JSON; optional)

Why:
- Provides deterministic "seen set" for skipping already-ingested accessions.
- Makes it easy to explain exactly where any fact came from.

### 2) Facts (numbers + text, totals + dimensionalized)

Dataset name: `fundamentals_fact_sec_v1`

Fact identity key (for as-of selection):
- `(instrument_id, tag_qname, period_start, period_end, unit, dims_hash, value_kind)`

Primary key (storage):
- Includes `accession` for lineage and to avoid overwriting across filings.
- Expected PK shape:
  - `(instrument_id, accession, tag_qname, period_start, period_end, unit, dims_hash, value_kind)`

Columns (initial sketch):
- Identity/lineage:
  - `instrument_id TEXT`
  - `provider TEXT` (always `sec`)
  - `provider_code TEXT` (CIK10)
  - `accession TEXT`
  - `form TEXT`
  - `filed_at TIMESTAMP`
  - `accepted_at TIMESTAMP` (nullable)
  - `known_at TIMESTAMP`
  - `asof TIMESTAMP`
- Fact keys:
  - `tag_qname TEXT` (e.g., `us-gaap:Revenues`)
  - `period_start TIMESTAMP` (nullable for instant facts)
  - `period_end TIMESTAMP`
  - `unit TEXT` (e.g., `USD`, `shares`, `pure`)
  - `currency TEXT` (nullable; often redundant with `unit` but useful for consumers)
- Dimensions:
  - `dims_json TEXT` (canonical JSON)
  - `dims_key TEXT`
  - `dims_hash TEXT`
- Value:
  - `value_kind TEXT` (`number` or `text`)
  - `value_num REAL` (nullable; for `number`)
  - `value_text_preview TEXT` (nullable; for `text`, first 512 chars)
  - `value_text_gz BLOB` (nullable; for `text`, gzipped UTF-8)
  - `value_text_len INTEGER` (nullable; original length)
  - `value_text_truncated INTEGER` (nullable; 0/1)
- Optional enrichment:
  - `statement TEXT` (nullable; `is`/`bs`/`cf` when we can classify)
  - `line_item_code TEXT` (nullable; future canonical mapping)
  - `decimals INTEGER` (nullable; XBRL "decimals" attribute when present)
  - `attrs TEXT/BLOB` (JSON; includes typed dims, role/label metadata, parse notes)

Text storage policy:
- Cap uncompressed text at 256 KiB (262,144 characters), then gzip.
- Preview is always stored as plain text (512 chars) for quick inspection without decompression.

Why:
- One canonical tall fact table keeps semantics consistent across totals and dimensional breakdowns.
- Gzipped blobs control DB size while retaining full text content up to a bounded limit.
- Preview improves usability for ad-hoc inspection and debugging.

## Indexing (initial recommendation)

We expect queries like:
- "latest as-of totals for tag X over period range"
- "all facts from accession Y"
- "dimensioned facts for tag X in period P"

Suggested indexes:
- Facts:
  - `(instrument_id, known_at)`
  - `(instrument_id, tag_qname, period_end)`
  - `(instrument_id, tag_qname, period_start, period_end, unit, dims_hash, value_kind, known_at)`
  - `(accession)`
  - `(dims_hash)`
- Filings:
  - `(provider, provider_code, known_at)`

Why:
- Window-function `read_asof` queries benefit from a composite key + `known_at`.
- Dimension queries should not require full table scans.

## Logging and determinism requirements

- Network calls log: `provider`, `provider_code` (CIK), and the window / accession list being fetched; log `points=<count>` on success.
- "Empty" responses are logged at INFO (`empty series`) to diagnose coverage/cache issues.
- Rate limiting/backoff stays in `BaseFetcher`; fetcher-specific logs should stay structured and concise.

## Future work (explicitly deferred)

- XML fallback for accessions where JSON representation is unavailable.
- Robust statement classification and stable `line_item_code` mapping (provider-neutral tags).
- Rich identifier mapping (ticker, ISIN/FIGI) in catalog via `identifier_map`.
- Handling typed dimensions in `dims_hash` with a stable canonicalization strategy.
