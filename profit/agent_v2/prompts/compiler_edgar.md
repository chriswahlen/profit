## EDGAR requests (Step 2)

- Emit `type="edgar_xbrl"` requests when the anchor asks for fundamentals or filings.
- Each request specifies:
  - `params.cik`, `params.start_utc`, `params.end_utc`, `params.period_type` (`duration` for flows, `instant` for snapshots).
  - `params.concept_aliases`, the alias set you plan to filter on (from `METRIC_CONCEPT_ALIASES`).
  - `params.limit` to cap the rows and optional `timeout_ms`.
- The SQL query the runtime will build must:
  - JOIN `edgar_accession` so you can filter `a.cik = ?`.
  - JOIN `xbrl_fact → xbrl_concept → xbrl_context`.
  - Apply `c.qname IN (...)` using the alias list (without namespace prefixes).
  - Filter the date window by testing `start_date`, `end_date`, and `instant_date`.
- If you need multiple alias sets (e.g., CapEx plus PaymentsToAcquire…), list them all in `concept_aliases`, and note which alias you expect to satisfy.
