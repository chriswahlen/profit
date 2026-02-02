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
