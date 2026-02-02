# Overview

This API is the "language" the Agent can use to tell us what kind of data to fetch.

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
| `volume` | Trading volume | shares/contracts | Provide provider's native unit; call out notional for futures. |
| `vwap` | Volume-weighted average price | quote currency | Optional; requires per-trade data. |

### Company facts field vocabulary

| Field | Description | Notes |
| --- | --- | --- |
| `Revenues`, `NetIncome`, `EarningsPerShare` | US GAAP Base Schema attributes | Use consolidated when `consolidated=true`; fields map 1:1 to normalized taxonomy. |
| `Assets`, `Liabilities`, `Equity` | Balance sheet components | Specify `period_type` (`Q`, `Y`, `TTM`) in downstream request metadata when relevant. |
| `CashFlowsFromOperations`, `CapitalExpenditures` | Cash-flow statement rows | Values may be as-reported or restated; denote `restated=true` when requested. |

For `company_facts` requests, provide the desired `filings` (e.g., `10-K`, `10-Q`) and period window. Fields should map to explicit Base Schema keys; missing fields should be treated as error responses unless explicitly allowed by the agent.

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
    "type": "...",  // one of: "market", "real_estate", "company_facts"
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

### Company Info Request

Use `type: "company_facts"`. Example request:

```json
{
  "companies": ["XNAS|AAPL"],   // use canonical id, or CIK
  "filings": ["10-K", "10-Q"],    // SEC filings
  "start": "YYYY-MM-DD",          // or null
  "end": "YYYY-MM-DD",            // or null; inclusive; UTC-normalized
  "fields": [{
    "key": "Revenues",    // Field to fetch; generally US GAAP Base Schema fields
    "consolidated": true,    // If true only returned the consolidated fields
  }],
}
```

Notes:
- Use JSON `null` (not the string `"null"`) when a bound is open-ended.
- End dates are inclusive; all times are normalized to UTC while preserving original timezone metadata in downstream payloads where available.
- Aggregations accept an array of keywords (see Definitions); responses should make clear which aggregation produced each value (e.g., separate columns or keyed objects).

## Research snippets

Agents can optionally read from and write to a persistent research-snippet store. Snippets let the Agent reuse prior insights without re-deriving the same context.

### Snippet schema

| Property | Type | Description |
| --- | --- | --- |
| `snippet_id` | string | Canonical identifier (UUID4 or prefixed slug). |
| `title` | string | Human-readable short summary (max ~120 chars). |
| `body` | array of string | List of bullet/sentence strings describing the insight. |
| `tags` | array of string | Themes/sectors/instrument classes (e.g., `["tech", "xnas|aapl"]`). |
| `related_instruments` | array of canonical IDs | Optional assets the insight references. |
| `related_regions` | array of canonical region ids | Optional regions. |
| `source_provider` | string | Where the snippet originated (e.g., `agent` or specific fetcher). |
| `created_at` | string (UTC timestamp) | Time the snippet was stored. |
| `expires_at` | string (UTC timestamp) | Optional TTL; snippets past this timestamp should be disregarded unless refreshed. |

The store should normalize tags and instruments (canonical IDs) at write time and record metadata (`source_provider`, `related_*`, `created_at`) for discoverability.

### Snippet requests

Agents can interact with the snippet store using request type `snippet`.

#### Create snippet

```json
{
  "type": "snippet",
  "notes": "final insight should be cached",
  "request": {
    "action": "store",
    "snippet": {
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

The response from the snippet retriever should include the stored `snippet_id` and any normalization notes (e.g., tag deduplication).

#### Lookup snippet

```json
{
  "type": "snippet",
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

The retriever should return an array of matching snippet summaries (`snippet_id`, `title`, `body`, `created_at`), ordered by recency or relevance, and optionally include metadata about why they matched (tags/instruments).

When the agent loops, include any retrieved snippet summaries (with IDs) in the next prompt so the LLM can cite or reuse them. 