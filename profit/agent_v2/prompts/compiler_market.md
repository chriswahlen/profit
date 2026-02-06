## Market requests (Step 2)

- When you need price/volume context, emit `type="market_ohlcv"` requests.
- Each request needs `params` containing `ticker`, `exchange_mic`, `start_utc`, `end_utc`, `bar_size`, and `fields`.
- Optional `params.adjust_splits`/`adjust_dividends` indicate adjustments, and `params.post_aggregations` can list derived windows (e.g., 21-day average).
- Set `timeout_ms` to an appropriate timeout (e.g., 30000).
- Example:
  ```json
  {
    "request_id": "mkt_capex_context",
    "type": "market_ohlcv",
    "params": {
      "ticker": "GOOG",
      "exchange_mic": "XNAS",
      "start_utc": "2024-01-01",
      "end_utc": "2024-06-01",
      "bar_size": "1d",
      "fields": ["close", "volume"],
      "adjust_splits": true,
      "adjust_dividends": false,
      "post_aggregations": []
    },
    "timeout_ms": 30000
  }
  ```
