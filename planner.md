The following are instructions on how 

## Canonical IDs & enums
- **Assets**: use provider-neutral IDs (`XNAS|AAPL`, `XNYS|SPY`, `Crypto|BTC`, `FX|EURUSD`, `INDEX|NASDAQ100`, `FUT|CL|202602`).
- **Regions**: use canonical region IDs (`metro|us|seattle_metro_area`, `county|us|washington|king`, `state|us|texas`, `postcode|us|20002`, `country|us`).
- **Aggregations** (case-sensitive, unlimited array length): `7d_avg`, `14d_avg`, `30d_avg`, `weekly_avg`, `monthly_avg`, `7d_median`, `14d_median`, `30d_median`, `weekly_median`, `monthly_median`, `monthly_max`, `monthly_min`, `weekly_max`, `weekly_min`.
- **Market fields**: `open`, `high`, `low`, `close`, `volume`, `adj_close`, `vwap`.
- **Derived metrics**: `pct_change`, `volume_delta`, `avg_spread`, `market_depth`, `trade_count`.
- **Company fields**: Base Schema keys such as `Revenues`, `NetIncome`, `EarningsPerShare`, `Assets`, `Liabilities`, `Equity`, `CashFlowsFromOperations`, `CapitalExpenditures`.
- **Dates**: UTC strings `YYYY-MM-DD` or JSON `null`; `end` is inclusive.
- **Snippets**: objects with `snippet_id`, `title`, `body` (array of strings), `tags`, optional `related_instruments`, optional `related_regions`, `source_provider`, `created_at` (UTC timestamp), optional `expires_at`; tags/instruments normalized to canonical forms.

## Response envelope
```
{
  "data_request": [
    {
      "type": "<market|real_estate|company_facts|snippet>",
      "notes": "any context for downstream fetchers",
      "request": { ... type-specific payload ... }
    }
  ],
  "agent_response": "final text or reasoning for next turn"
}
```
- `data_request` must contain ≥1 entry and obey the schema for its type. No shorthand IDs or extra keys.
- `agent_response` should explain what was requested, summarize retrieved data, or describe why the flow is done.

## Request schemas

### Market
```
{
  "instruments": ["XNAS|AAPL", "Crypto|BTC"],
  "fields": ["open", "close"],
  "start": "YYYY-MM-DD or null",
  "end": "YYYY-MM-DD or null",
  "aggregation": ["7d_avg", "30d_median"]
}
```
- Aggregations is a non-empty array of allowed values.
- Dates are UTC; use JSON `null` for open bounds.

### Real estate
```
{
  "regions": ["metro|us|seattle_metro_area"],
  "start": "YYYY-MM-DD or null",
  "end": "YYYY-MM-DD or null",
  "aggregation": ["7d_avg", "monthly_max"]
}
```
- Regions must follow canonical format.
- Aggregations use the same vocabulary as market.

### Company facts
```
{
  "companies": ["XNAS|AAPL"],
  "filings": ["10-K", "10-Q"],
  "start": "YYYY-MM-DD or null",
  "end": "YYYY-MM-DD or null",
  "fields": [
    { "key": "Revenues", "consolidated": true },
    { "key": "Assets", "consolidated": true }
  ]
}
```
- Companies accept canonical exchange|ticker or `CIK:<digits>`.

### Snippets
- **Store**
```
{
  "action": "store",
  "snippet": {
    "title": "Insight title",
    "body": ["sentence1", "sentence2"],
    "tags": ["theme", "XNAS|AAPL"],
    "related_instruments": ["XNAS|AAPL"],
    "related_regions": ["metro|us|seattle_metro_area"],
    "source_provider": "agent",
    "created_at": "2026-02-02T00:00:00Z",
    "expires_at": "2026-08-01T00:00:00Z"
  }
}
```
- **Lookup**
```
{
  "action": "lookup",
  "filters": {
    "tags": ["energy"],
    "related_instruments": ["XNAS|AAPL"],
    "active_at": "2026-02-01T00:00:00Z"
  },
  "limit": 5
}
```

## Errors & partials
- Unsupported instrument/field/window responses use structured errors:
```
{
  "error_code": "unsupported_field",
  "field": "avg_spread",
  "message": "Provider X lacks depth data"
}
```
- If only part of a request can be satisfied, it will be returned with available data plus warnings
about missing elements.
- Empty/zero-point fetches will still return a valid envelope.
