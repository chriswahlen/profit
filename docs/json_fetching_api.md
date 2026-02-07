# Overview

This API is the "language" the Agent can use to tell us what kind of data to fetch.

Note: A newer, independent agent API (v2) is documented in `docs/agent_v2/api.md` and implemented in `profit/agent_v2`.
This file remains the canonical contract for the legacy `profit/agent` runtime.

## Definitions

- Companies: Refer to by their exchange and ticker, e.g.:  "XNAS|AAPL"
- Crypto: e.g. "Crypto|BTC"
- Regions: We use a 'canonical' region id:
  - By metro:  metro|us|seattle_metro_area
  - By county: county|us|washington|king
  - By state/province:  state|us|texas
  - By postal code:  postcode|us|20002
  - By country: country|us
- Aggregation values: "7d_avg", "14d_avg", "30d_avg", "weekly_avg", "monthly_avg",
                      "7d_median", "14d_median", "30d_median", "weekly_median", "monthly_median",
                      "monthly_max", "monthly_min", "weekly_max", "weekly_min"

## Vocabulary Reference

### Instrument classes

| Class | Canonical ID pattern | Description | Notes |
| --- | --- | --- | --- |
| Equities & ETFs | `XNAS\|\<ticker\>`, `XNYS\|\<ticker\>`, `XNAS\|QQQ` | US-listed common stocks, ETFs, and index funds | Quote currency = USD; use EOD session hits (close) unless `session` is requested. |
| Indexes | `INDEX\|<symbol>` | Index-level data (S&P 500, Nasdaq 100) | No trading volume by default; set `fields` to `close`/`level`. |
| Crypto | `Crypto\|BTC`, `Crypto\|ETH` | Spot crypto asset quotes denominated in USD | Normalized to UTC close-of-calendar-day. |
| FX pairs | `FX\|EURUSD`, `FX\|USDJPY` | Spot forex quotes expressed as base/quote | Use contiguous pair format; quote currency is the second symbol. |
| Futures (when available) | `FUT\|CL\|202602`, `FUT\|ES\|202603` | Front-month and benchmark futures contracts | Include both `symbol` and `contract_month`; clarify settlement hours per provider. |

### Market field vocabulary

| Field | Description | Units | Notes |
| --- | --- | --- | --- |
| `open` | First trade price for the period/session | quote currency | UTC normalized; fallback to provider session start. |
| `high` | Maximum Price | quote currency | |
| `low` | Minimum Price | quote currency | |
| `close` | Last trade price | quote currency | |
| `adj_close` | Adjusted Close (splits/dividends) | quote currency | Available for equities/ETFs only. |
### Error handling expectations

- If an agent requests an unsupported instrument class or field, or if the requested window falls outside cached coverage, the retriever should raise a descriptive validation exception before making network calls. The exception should include the offending key (`instrument`, `field`, `region`, etc.); no silent fallbacks.
- When a provider returns empty or zero-point payloads, return an empty-but-valid response so the agent can decide whether to retry or move on.
- If partial data is available (some instruments/fields succeed, others fail), return success for the available subset and surface structured warnings identifying the missing segments so the agent can request replacements or drop them explicitly.
- Always surface structured errors (e.g., `{"error_code":"unsupported_field","field":"avg_spread","message":"Provider X does not support depth metrics"}`) back to the agent if data cannot be returned.

## Request Envelope

The Agent response can have one request envelope per response.

```json
{
    "data_request": [{   // A list of followup requests.
    "type": "...",  // one of: "market", "real_estate", "insight"
    "notes": "...",  // any context the Agent wants to keep around for this data
    "request": { }  // Contents of the request
  }],
  "agent_response": "..."   // either the final response, or context for the next set of data requests.
}
```

### Market Request

Use `type: "market"`. Example request:

```json
// stock/crypto/index fund/forex data request (type: "market")
{
  "instruments": ["XNAS|AAPL", "Crypto|BTC"],
  "fields": ["open", "close"],   // OHLCV fields to fetch for each instrument.
  "start": "YYYY-MM-DD",         // or null
  "end": "YYYY-MM-DD",           // or null; end date is inclusive; all timestamps are UTC
  "aggregation": ["7d_avg", "30d_median"]   // one or more aggregation types to return
}
```

### Real Estate Request

Use `type: "real_estate"`. Example request:

```json
// Real estate market data request (type: "real_estate")
{
  "regions": ["metro|us|seattle_metro_area"],  // regions to fetch
  "start": "YYYY-MM-DD",     // or null
  "end": "YYYY-MM-DD",       // or null; inclusive; UTC-normalized
  "aggregation": ["7d_avg", "monthly_max"]   // one or more aggregation types to return
}
```

Notes:
- Use JSON `null` (not the string `"null"`) when a bound is open-ended.
- End dates are inclusive; all times are normalized to UTC while preserving original timezone metadata in downstream payloads where available.
- Aggregations accept an array of keywords (see Definitions); responses should make clear which aggregation produced each value (e.g., separate columns or keyed objects).
- When you change anything in this document (fields, schemas, insight behavior, etc.), also update `planner.md` so the agent prompt stays current.

## Explicit data needs

Agents can signal missing or desirable datasets alongside their responses. Include an optional
`data_needs` array in the envelope if the agent would benefit from data we currently lack:

```json
{
  "data_needs": [
    {
      "name": "FX|EURUSD intraday ticks",
      "provider": "provider-slug",
      "reason": "Needed to analyze hourly carry and mid-price drift for the exposure.",
      "criticality": "high"   // enum: high | medium | low
    }
  ]
}
```

Use `name` to describe the dataset, `provider` if a particular source owns it, `reason` to describe why it matters, and `criticality` so humans can prioritize fulfilment. When the dataset exists but is empty for the requested window, capture that via the standard structured error path (see error expectations). This helps us decide whether to add new fetchers or expose a provider-specific backfill.

## Research insights

Agents can optionally read from and write to a persistent research-insight store. Insights let the Agent reuse prior context without re-deriving the same findings.

### Insight schema

| Property | Type | Description |
| --- | --- | --- |
| `insight_id` | string | Canonical identifier (UUID4 or prefixed slug). |
| `title` | string | Human-readable short summary (max ~120 chars). |
| `body` | array of string | List of bullet/sentence strings describing the insight. |
| `tags` | array of string | Themes/sectors/instrument classes (e.g., `["tech", "xnas|aapl"]`). |
| `related_instruments` | array of canonical IDs | Optional assets the insight references. |
| `related_regions` | array of canonical region ids | Optional regions. |
| `source_provider` | string | Where the insight originated (e.g., `agent` or specific fetcher). |
| `created_at` | string (UTC timestamp) | Time the insight was stored. |
| `expires_at` | string (UTC timestamp) | Optional TTL; insights past this timestamp should be disregarded unless refreshed. |

The store should normalize tags and instruments (canonical IDs) at write time and record metadata (`source_provider`, `related_*`, `created_at`) for discoverability.

### Insight requests

Agents can interact with the insight store using request type `insight`.

#### Create insight

```json
{
  "type": "insight",
  "notes": "final insight should be cached",
  "request": {
    "action": "store",
    "insight": {
      "title": "Energy sector rotation in Jan 2026",
      "body": [
        "Found strong inflows into XNYS|XOM and XNYS|CVX over the past 3 weeks.",
        "Macro PMI surprise triggered a 12% relative outperformance vs. broad energy ETF."
      ],
      "tags": ["energy", "sentiment_bullish"],
      "related_instruments": ["XNAS|AAPL"],
      "source_provider": "agent",
      "expires_at": "2026-08-01T00:00:00Z"
    }
  }
}
```

The response from the insight retriever should include the stored `insight_id` and any normalization notes (e.g., tag deduplication).

#### Lookup insight

```json
{
  "type": "insight",
  "notes": "see relevant context",
  "request": {
    "action": "lookup",
    "filters": {
      "tags": ["energy"],
      "related_instruments": ["XNAS|AAPL"],
      "active_at": "2026-02-01T00:00:00Z"
    },
    "limit": 5
  }
}
```

The retriever should return an array of matching insight summaries (`insight_id`, `title`, `body`, `created_at`), ordered by recency or relevance, and optionally include metadata about why they matched (tags/instruments).

When the agent loops, include any retrieved insight summaries (with IDs) in the next prompt so the LLM can cite or reuse them. 
