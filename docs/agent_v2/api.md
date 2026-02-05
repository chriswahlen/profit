# Agent API v2

This API is independent of the existing v1 agent envelope (`docs/json_fetching_api.md`).

The v2 agent loop is:

1) **Step 1 (Planner)**: produce compact, anchored data needs (+ insight ops).
2) **Step 2 (Compiler)**: compile anchors into a (potentially large) retrieval plan.
3) Execute retrieval plan, feed **DATA** + **INSIGHTS** back into Step 1, repeat.

Both Step 1 and Step 2 outputs are **JSON only** (no surrounding prose).

## Step 1: Planner output (Anchors IR)

Schema: `docs/agent_v2/schemas/step1_anchors.schema.json`

Required high-level fields:
- `context.user_query`: the user question
- `context.approach`: the approach
- `data_needed_fluid[]`: human-readable, “fluid” data needs
- `anchors[]`: machine-anchored intents (compact)
- `insight_ops.search[]`: snippet tags to lookup
- `missing_sources[]`: wishlist only

Control fields:
- `needs_data`, `can_answer_now`
- `stop_reason`: `answered|need_more_data|insufficient_datasets|need_clarification`
- Optional: `final_answer` and `clarifying_questions[]` (when `need_clarification`)

Optional write-back:
- `insights_writeback[]`: snippet payloads to persist

## Step 2: Compiler output (Retrieval Plan)

Schema: `docs/agent_v2/schemas/step2_retrieval_plan.schema.json`

The retrieval plan is batched:
- `batches[]` (each has `requests[]`)
- optional `depends_on_batches[]` per batch

Supported request types:
- `market_ohlcv`: ticker+MIC OHLCV reads
- `sql`: read-only SQL against `edgar` or `real_estate`

Entity resolution:
- `entity_resolution_report[]` must include an entry for every EDGAR anchor (ticker+MIC → CIK resolution).

## Dataset notes

- **market_ohlcv**: uses series IDs with canonical instrument ID `EXCHANGEMIC|TICKER` (e.g., `XNAS|AAPL`).
- **edgar (sql)**: SQLite DB at `data/edgar.sqlite3` with tables defined in `profit/edgar/db.py`.
- **real_estate (sql)**: SQLite DB at `data/redfin.sqlite` with tables defined in `profit/stores/redfin_store.py`.

## Time semantics

All windows use **date-only UTC strings** (`YYYY-MM-DD`) with inclusive start and inclusive end.
