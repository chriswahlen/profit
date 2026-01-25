# Agent Guidelines (profit)

These guidelines define how changes should be made in this repository to keep the codebase maintainable, testable, and safe.

## Core principles
- Prefer small, reviewable changes that fix root causes.
- Keep modules cohesive: clear boundaries between data access, business logic, and presentation/CLI.
- Optimize for reproducibility: deterministic outputs given the same inputs and config.
- Avoid hidden state: no implicit globals, no “magic” side effects on import.
- Fail loudly on invalid inputs; validate at boundaries.

## Python standards
- Target Python 3.11+ unless the repo specifies otherwise.
- Prefer type hints on public APIs; use `mypy`-friendly patterns.
- Prefer dataclasses or Pydantic models for structured data; keep models immutable where practical.
- Keep dependencies minimal; don’t add heavyweight libs without a clear need.
- Errors: raise specific exceptions; don’t swallow exceptions without logging/context.

### Style
- Use `ruff` for linting/import sorting (preferred) and `black` (or `ruff format`) for formatting.
- Prefer explicit names over abbreviations; avoid one-letter names outside small scopes.
- Keep functions short; extract helpers instead of deep nesting.

## Testing (pytest)
- Write tests for new behavior and bug fixes.
- Prefer fast unit tests; isolate external services behind interfaces and mock/fake them.
- No network access in tests by default.
- Make tests deterministic (control randomness and time; avoid “today/now” unless injected).
- Use fixtures for reusable setup; keep fixtures local to the test module unless broadly shared.
- Add “contract tests” for interfaces (e.g., any downloader/source must satisfy a common behavior).

## Data, IO, and caching
- Never commit secrets (API keys, tokens). Use env vars and/or local config files excluded by `.gitignore`.
- Keep raw data and cached artifacts out of git; store them under a configurable data directory.
- When caching, define clear cache keys, TTLs, and versioning (schema/provider changes should invalidate caches).
- Use UTC timestamps internally; preserve original timezone metadata when needed.

## Project hygiene
- Keep public APIs documented with docstrings (especially inputs/outputs and units).
- Update README when adding a new subsystem, config knob, or workflow.
- Prefer adding or updating a small example script when introducing a new feature that’s hard to test via unit tests alone.

## When unsure
- Ask for clarification on requirements (data source, schema expectations, performance constraints).
- Default to the simplest solution that preserves future extensibility.

