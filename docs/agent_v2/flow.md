# Agent Flow v2 (Fluid + Anchored)

This document defines an updated agent loop that separates:

1. **What data is needed** (compact, human-readable + machine-anchored intents)
2. **How to fetch it** (potentially large, executable retrieval instructions)
3. **Synthesis** (answer + optional snippet write-back)

The design goal is to keep planning **small and stable**, while allowing the retrieval plan to grow large without
making Step 1 brittle.

## Summary

- **Step 1 (Planner)**: outputs a compact JSON object with:
  - `context` (user query + approach)
  - `data_needed_fluid` (human-readable bullets)
  - `anchors` (machine-anchored retrieval intents)
  - `insight_ops` (snippet tags to lookup; store candidates for later)
  - `missing_sources` (future wishlist only)
  - Optional: `final_answer` / `clarifying_questions` / `insights_writeback`
- **Step 2 (Compiler)**: takes the Step 1 JSON and produces a runner-ready `RETRIEVAL_PLAN`.
- **Step 3 (Repeat/Synthesize)**: same as Step 1, but the prompt additionally includes retrieved `DATA` and `INSIGHTS`.
  The agent can now answer, request more, and/or write back snippets.

## Entity IDs

- Equities are identified by **ticker + MIC**:
  - Example: `{ "ticker": "GOOG", "exchange_mic": "XNAS" }`
- **MIC is required** everywhere (`exchange_mic`), pattern: `^[A-Z0-9]{4}$`.

## Time semantics (date-only)

- Step 1 uses date-only UTC strings: `YYYY-MM-DD`.
- Range semantics: **inclusive start, inclusive end** (matches the existing dataset conventions in this repo).
- Step 2 may normalize internally (e.g., `YYYY-MM-DDT00:00:00Z` boundaries), but should preserve the original date-only intent.

## EDGAR (XBRL) compilation notes

EDGAR facts in this repo are stored in SQLite tables defined in `profit/edgar/db.py`:

- `xbrl_fact` + `xbrl_concept` + `xbrl_context` are the core join path.
- Units/dimensions are optional via `xbrl_unit` and `context_dimension` (+ `dimension_axis`, `dimension_member`).

The v2 anchor for EDGAR should specify:

- which concepts are allowed (`metric.concept_qnames_allow`)
- period type (`instant` vs `duration`)
- grain (`quarterly|annual|ttm`)
- optional unit/dimension allowlists

Step 2 resolves `{ticker, exchange_mic} -> cik` using the system’s mapping and should report the resolved CIK for provenance.

## Schemas

- Step 1 anchors schema: `docs/agent_v2/schemas/step1_anchors.schema.json`
- Step 2 retrieval plan schema: `docs/agent_v2/schemas/step2_retrieval_plan.schema.json`
