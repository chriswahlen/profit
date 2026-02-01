## CLI helper

### Catalog helpers

### Fetchers
- YFinance OHLCV: `profit.sources.yfinance.YFinanceFetcher` (daily bars via yfinance; cached and UTC-normalized).

### Scripts
- `scripts/fetch_yfinance.py`: fetch daily OHLCV for tickers in a date window into the columnar store.
