# General

- Use the first class entity helpers (like Region, Currency, Company) when possible.
- Always add unit tests
- Prefer to keep functions and methods small (< 100) lines. Abstract out helper methods to accomplish this goal.
- Be generous with comments especially with nested or complex code.
- DataSourceManager owns data store instances: construct shared stores in the manager and inject them into data sources/providers instead of letting each source open its own connection.
- Keep tests in directory structures that mirror source modules (e.g., `data_sources/market/...` -> `tests/data_source/market/...`).
- Canonical entity IDs must use the typed, colon-delimited pattern `[entity_type]:...` (e.g., `company:us:microsoft-corporation`, `sec:xnas:aapl`, `fx:usd:eur`, `index:spglobal:sp500`). Never use provider-prefixed IDs as canonical.
- For index entities specifically, embed the exchange MIC (or equivalent venue identifier) in the canonical ID (e.g., `index:xshg:shanghai-a50`), not the data provider name, so IDs stay stable across provider imports.

- For now, DO NOT worry about backwards compatibility. Assume we will start over with new data and that there are no legacy clients.

# Python
- Log significant events as INFO (opening a database, opening a file to import it, etc)
- Log minor events as DEBUG (inserting to the datbase, etc)
- Avoid importing modules inline; strongly prefer all imports at the beginning of .py files.
- Ingestions must record start/finish runs, batch inserts to avoid unbounded memory, log periodic progress, and mark partial/failure status with row counts.

# SQLite
- Always use WAL mode for databases.
