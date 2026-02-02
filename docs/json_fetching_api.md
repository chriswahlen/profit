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

## Request Envelope

The Agent response can have one request envelope per response.

```json
{
  "data_request": [{   // A list of followup requests.
    "type": "..."  // one of the data request types: "market", "real_estate", "company_facts"
    "notes": "..."  // any context the Agent wants to keep around for this data
    "request": { }  // Contents of the request
  }],
  "agent_response": "..."   // either the final response, or context for the next set of data requests.
}
```

### Market Request

Use request_type: "market". Example request:

```json
// stock/crypto/index fund/forex data request (type: "market")
{
  "instruments": ["AAPL", "Crypto|BTC"],
  "fields": ["open", "close"],   // one of the OHLCV fields to fetch for the instrument.
  "start": "YYYY-MM-DD|null",
  "end": "YYYY-MM-DD|null",
  "aggregation": "7d_avg"   // one or more of our aggregation types to return
}
```

### Real Estate Request

Use request_type: "real_estate". Example request:

```json
// Real estate market data request (type: "real_estate")
{
  "regions": ["us|metro:seattle_metro_area"],  // regions to fetch
  "start": "YYYY-MM-DD|null",
  "end": "YYYY-MM-DD|null",
  "aggregation": "7d_avg"   // one or more of our aggregation types to return
}
```

### Company Info Request

Use request_type: "company_facts". Example request:

```json
{
  "companies": ["XNAS:AAPL"],   // use canonical id, or CIK
  "filings": ["10-K", "10-Q"],    // SEC filings
  "start": "YYYY-MM-DD|null",
  "end": "YYYY-MM-DD|null",
  "fields": [{
    "key": "Revenues",    // Field to fetch; generally US GAAP Base Schema fields
    "consolidated": true,    // If true only returned the consolidated fields
  }],
}
```