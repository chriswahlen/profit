## Real estate requests (Step 2)

- For `real_estate_intent` anchors, emit `type="real_estate"` requests.
- Each request must include `params.geo_id`, `params.start_utc`, `params.end_utc`, `params.measures`, and `params.aggregation`.
- You may optionally set `timeout_ms` for longer queries.
- The runtime will translate this into a SQL query against the Redfin store (`market_metrics` and `regions`), filtering by the provided geographic scope and date window.
