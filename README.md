## CLI helper

### Catalog helpers

### Fetchers
- YFinance OHLCV: `profit.sources.yfinance.YFinanceFetcher` (daily bars via yfinance; cached and UTC-normalized).

### Data loaders
- Redfin dumps: `scripts/load_redfin_dump.py` parses a Redfin data center TSV/CSV (optionally compressed) and writes canonical regions, provider mappings, market metrics, and ingestion run metadata into the Redfin component tables (defaults to `data/profit.sqlite`, supports `--store-path`, `--redfin-db-path`, `--granularity`, `--country-iso2`, and `--limit` for local testing).
- Local Redfin fetch: `scripts/fetch_redfin_data.py` looks in `data/datasets/redfin`, selects the most recent TSV/CSV (or accepts `--dataset`), and runs the same ingestion logic so you can exercise the loader against downloaded dumps without hitting Redfin directly.

### Scripts
- `scripts/fetch_yfinance.py`: fetch daily OHLCV for tickers in a date window into the columnar store.
