# General

- Use the first class entity helpers (like Region, Currency, Company) when possible.
- Always add unit tests
- Prefer to keep functions and methods small (< 100) lines. Abstract out helper methods to accomplish this goal.
- Be generous with comments especially with nested or complex code.
- Keep tests in directory structures that mirror source modules (e.g., `data_sources/market/...` -> `tests/data_source/market/...`).

# Python
- Log significant events as INFO (opening a database, opening a file to import it, etc)
- Log minor events as DEBUG (inserting to the datbase, etc)
- Avoid importing modules inline; strongly prefer all imports at the beginning of .py files.

# SQLite
- Always use WAL mode for databases.
