from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Iterable

from agentapi.errors import CachedRetryable
from agentapi.plan import Run
from agentapi.runners import AgentTransformRunner
from agentapi.plan import Plan

logger = logging.getLogger(__name__)

# Stage names live here so the prompt + parser can refer to them consistently.
STAGE_INITIAL_PROMPT = "financial_adviser.initial_prompt"


@dataclass(frozen=True)
class SqlQuerySpec:
    """
    A single SQLite query request produced by the agent.

    We keep this intentionally small because it will flow through the state
    machine snapshot (user_context) and will be displayed in the AgentAPI UI.
    """

    key: str
    purpose: str
    sql: str


@dataclass(frozen=True)
class AdviserPlan:
    description: str
    instructions: str

    def to_dict(self) -> dict[str, str]:
        return {"description": self.description, "instructions": self.instructions}


def _coerce_plan(value: object) -> AdviserPlan | None:
    if isinstance(value, dict):
        description = value.get("description")
        instructions = value.get("instructions")
        if isinstance(description, str) and description.strip() and isinstance(instructions, str) and instructions.strip():
            return AdviserPlan(description=description.strip(), instructions=instructions.strip())
    if isinstance(value, str) and value.strip():
        # Back-compat: allow a single string; treat it as description with minimal instructions.
        return AdviserPlan(description=value.strip(), instructions="Use the database results to answer; ask for follow-up queries if needed.")
    return None


def _coerce_goal(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _unique_key(desired: str, used: set[str]) -> str:
    base = (desired or "").strip() or "q"
    key = base
    i = 2
    while key in used:
        key = f"{base}_{i}"
        i += 1
    used.add(key)
    return key


def _coerce_queries(payload: dict[str, Any]) -> list[SqlQuerySpec]:
    """
    Accept a few compatible response shapes from the agent.

    Supported:
      - {"query": "SELECT ..."}  (legacy, single query)
      - {"queries": ["SELECT ...", ...]} (legacy list)
      - {"queries": [{"key":"...", "purpose":"...", "sql":"SELECT ..."}, ...]}
      - {"action": "query", "queries": [...]}
    """

    used_keys: set[str] = set()

    if "query" in payload and isinstance(payload["query"], str):
        sql = payload["query"].strip()
        if not sql:
            return []
        return [SqlQuerySpec(key=_unique_key("q1", used_keys), purpose="(unspecified)", sql=sql)]

    queries = payload.get("queries")
    if isinstance(queries, list):
        out: list[SqlQuerySpec] = []
        for idx, item in enumerate(queries, start=1):
            if isinstance(item, str) and item.strip():
                out.append(SqlQuerySpec(key=_unique_key(f"q{idx}", used_keys), purpose="(unspecified)", sql=item.strip()))
                continue
            if isinstance(item, dict):
                key = item.get("key")
                purpose = item.get("purpose")
                sql = item.get("sql") or item.get("query")
                if not isinstance(sql, str) or not sql.strip():
                    continue
                if not isinstance(key, str) or not key.strip():
                    key = f"q{idx}"
                if not isinstance(purpose, str) or not purpose.strip():
                    purpose = "(unspecified)"
                out.append(SqlQuerySpec(key=_unique_key(key, used_keys), purpose=purpose.strip(), sql=sql.strip()))
        return out

    return []


def _safe_json(value: object) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except Exception:
        return "{}"

def _strip_code_fences(text: str) -> str:
    """
    Remove common markdown code-fence wrappers around JSON.

    The agent is instructed to output JSON only, but in practice models
    sometimes wrap the response as ```json ... ```.
    """

    s = text.strip()
    if not s.startswith("```"):
        return text
    lines = s.splitlines()
    if len(lines) < 3:
        return text
    if not lines[0].startswith("```"):
        return text
    if not lines[-1].startswith("```"):
        return text
    return "\n".join(lines[1:-1]).strip()


def _parse_json_object(text: str) -> dict[str, Any]:
    """
    Parse a JSON object from an LLM response with a small amount of tolerance.

    We accept:
      - Leading/trailing whitespace
      - Code fences
      - A single trailing '}' (common accidental extra brace)

    Anything beyond that remains a retryable error so we don't silently accept
    unintended extra output.
    """

    raw = _strip_code_fences(text).strip()
    start = raw.find("{")
    if start < 0:
        raise json.JSONDecodeError("missing object", raw, 0)
    decoder = json.JSONDecoder()
    obj, end = decoder.raw_decode(raw, idx=start)
    if not isinstance(obj, dict):
        raise json.JSONDecodeError("expected object", raw, start)
    rest = raw[end:].strip()
    if rest == "" or rest == "}":
        return obj
    raise json.JSONDecodeError("extra data", raw, end)


def _summarize_results(results: Iterable[dict[str, Any]], *, max_chars: int = 12_000) -> str:
    """
    Serialize results for the LLM prompt without letting it grow unbounded.
    """

    blob = _safe_json(list(results))
    if len(blob) <= max_chars:
        return blob
    return blob[:max_chars] + "...(truncated)"


def _build_system_instructions() -> str:
    """
    The EDGAR schema is stable and small; include it directly so the agent can
    draft SQL without needing external context.
    """

    return "\n".join(
        [
            "You are an expert market analyst and data analyst.",
            "You must answer the user question by querying our local SQLite SEC EDGAR database.",
            "",
            "Important:",
            "- Only produce SQLite SELECT queries (no INSERT/UPDATE/DELETE/DDL/PRAGMA).",
            "- Prefer small queries with LIMIT 50.",
            "- Do not invent column names, tables, or schema.",
            "- Do not assume knowledge, answers and results must come from queries.",
            "- You may do multiple rounds: request queries, observe results, then request follow-up queries or answer.",
            "",
            "Schema (SQLite):",
            "TABLE edgar_submissions (cik TEXT PRIMARY KEY, entity_name TEXT, fetched_at TEXT NOT NULL, payload TEXT NOT NULL);",
            "TABLE edgar_accession (cik TEXT NOT NULL, accession TEXT NOT NULL, base_url TEXT NOT NULL, file_count INTEGER NOT NULL, fetched_at TEXT NOT NULL, PRIMARY KEY (cik, accession));",
            "TABLE edgar_accession_file (accession TEXT NOT NULL, file_name TEXT NOT NULL, fetched_at TEXT, compressed_payload BLOB, source_url TEXT, PRIMARY KEY (accession, file_name), FOREIGN KEY(accession) REFERENCES edgar_accession(accession));",
            "TABLE edgar_fact_extract (cik TEXT NOT NULL, accession TEXT NOT NULL, processed_at TEXT NOT NULL, fact_count INTEGER, note TEXT, PRIMARY KEY (cik, accession));",
            "TABLE xbrl_context (context_id INTEGER PRIMARY KEY, accession TEXT NOT NULL, context_ref TEXT NOT NULL, entity_scheme TEXT, entity_id TEXT, period_type TEXT NOT NULL CHECK (period_type IN ('instant','duration')), start_date TEXT, end_date TEXT, instant_date TEXT, entity_scheme_id INTEGER, FOREIGN KEY(accession) REFERENCES edgar_accession(accession), UNIQUE (accession, context_ref));",
            "INDEX idx_context_accession ON xbrl_context(accession);",
            "INDEX idx_context_period ON xbrl_context(accession, period_type, start_date, end_date, instant_date);",
            "TABLE xbrl_concept (concept_id INTEGER PRIMARY KEY, qname TEXT NOT NULL UNIQUE, label TEXT, data_type TEXT);",
            "TABLE xbrl_unit (unit_id INTEGER PRIMARY KEY, accession TEXT NOT NULL, unit_ref TEXT NOT NULL, measure TEXT, FOREIGN KEY(accession) REFERENCES edgar_accession(accession), UNIQUE(accession, unit_ref));",
            "INDEX idx_unit_accession ON xbrl_unit(accession);",
            "TABLE xbrl_fact (fact_id INTEGER PRIMARY KEY, accession TEXT NOT NULL, concept_id INTEGER NOT NULL, context_id INTEGER NOT NULL, unit_id INTEGER, decimals INTEGER, precision INTEGER, sign INTEGER, value_numeric REAL, value_text TEXT, value_raw TEXT NOT NULL, is_nil INTEGER NOT NULL DEFAULT 0, footnote_html TEXT,",
            "  FOREIGN KEY(accession) REFERENCES edgar_accession(accession),",
            "  FOREIGN KEY(concept_id) REFERENCES xbrl_concept(concept_id),",
            "  FOREIGN KEY(context_id) REFERENCES xbrl_context(context_id),",
            "  FOREIGN KEY(unit_id) REFERENCES xbrl_unit(unit_id));",
            "INDEX idx_fact_lookup ON xbrl_fact(accession, concept_id, context_id);",
            "INDEX idx_fact_concept ON xbrl_fact(concept_id);",
            "INDEX idx_fact_context ON xbrl_fact(context_id);",
            "",
            "Response format (JSON only; no markdown):",
            "Always include:",
            '- "plan": {"description":"...","instructions":"..."}  (required on ROUND 0; may be repeated later)',
            '- "goal": "..."  (required on every ROUND, including the final answer round)',
            "To request database work:",
            '{ "action": "query", "plan": {"description":"...","instructions":"..."}, "goal": "...", "queries": [{"key":"cik_lookup","purpose":"Find the company CIK","sql":"SELECT ..."}] }',
            "To answer the user:",
            '{ "action": "answer", "plan": {"description":"...","instructions":"..."}, "goal": "...", "answer": "..." }',
        ]
    )


class InitialPromptStage(AgentTransformRunner):
    """
    LLM stage: decide the next database queries OR produce a final answer.

    The state machine drives a loop:
      initial_prompt -> db_query -> initial_prompt -> ... -> final
    """

    def __init__(
        self,
        *,
        backend,
        model: str | None = None,
        db_query_stage_name: str,
        final_stage_name: str,
    ) -> None:
        super().__init__(name=STAGE_INITIAL_PROMPT, backend=backend, model=model)
        self._db_query_stage_name = db_query_stage_name
        self._final_stage_name = final_stage_name

    def get_prompt(self, *, previous_history_entries: list[Any], user_context: dict[str, Any]) -> str:
        fa = user_context.get("financial_adviser")
        fa_dict = fa if isinstance(fa, dict) else {}
        question = fa_dict.get("question")
        if not isinstance(question, str) or not question.strip():
            raise ValueError("user_context.financial_adviser.question is required")

        round_num = int(fa_dict.get("round", 0) or 0)
        results = fa_dict.get("db_results") if isinstance(fa_dict.get("db_results"), list) else []
        results_json = _summarize_results(results)
        plan_value = fa_dict.get("plan")
        goal_by_round = fa_dict.get("goal_by_round") if isinstance(fa_dict.get("goal_by_round"), dict) else {}
        plan_json = _safe_json(plan_value)
        goals_json = _safe_json(goal_by_round)

        # Keep prompt explicit and machine-readable to make it easy to stub in tests.
        return "\n".join(
            [
                _build_system_instructions(),
                "",
                f"ROUND: {round_num}",
                f"USER_QUESTION: {question.strip()}",
                f"CURRENT_PLAN_JSON: {plan_json}",
                f"GOALS_BY_ROUND_JSON: {goals_json}",
                f"PREVIOUS_DB_RESULTS_JSON: {results_json}",
            ]
        )

    def process_prompt(
        self,
        *,
        result: str,
        previous_history_entries: list[Any],
        user_context: dict[str, Any],
    ) -> Plan:
        user_context.setdefault("financial_adviser", {})
        fa = user_context["financial_adviser"]
        if not isinstance(fa, dict):
            raise ValueError("user_context.financial_adviser must be a dict")

        current_round = int(fa.get("round", 0) or 0)

        try:
            payload = _parse_json_object(result)
        except json.JSONDecodeError as exc:
            # Treat malformed JSON as retryable so a worker can restart and the
            # next attempt will reuse the cached LLM output if desired.
            raise CachedRetryable(result=result, message=f"invalid_json: {exc}") from exc

        if not isinstance(payload, dict):
            raise CachedRetryable(result=result, message="response must be a JSON object")

        # Plan/goal bookkeeping.
        plan = _coerce_plan(payload.get("plan"))
        goal = _coerce_goal(payload.get("goal"))
        if current_round == 0 and plan is None and fa.get("plan") in (None, ""):
            raise CachedRetryable(result=result, message="ROUND 0 requires non-empty 'plan'")
        if goal is None:
            raise CachedRetryable(result=result, message="every ROUND requires non-empty 'goal'")
        if plan is not None:
            fa["plan"] = plan.to_dict()
        goals = fa.get("goal_by_round")
        if not isinstance(goals, dict):
            goals = {}
            fa["goal_by_round"] = goals
        goals[str(current_round)] = goal

        action = payload.get("action")
        if action is None:
            # Back-compat: infer based on fields.
            action = "answer" if isinstance(payload.get("answer"), str) else ("query" if payload.get("query") or payload.get("queries") else None)

        if action == "answer":
            answer = payload.get("answer")
            if not isinstance(answer, str) or not answer.strip():
                raise CachedRetryable(result=result, message="answer action requires non-empty 'answer'")
            fa["answer"] = answer.strip()
            fa["status"] = "completed"
            return Run(stage_name=self._final_stage_name)

        if action == "query":
            queries = _coerce_queries(payload)
            if not queries:
                raise CachedRetryable(result=result, message="query action requires at least one SQL query")
            # Keep the machine bounded; if the agent wants many queries, it can do multiple rounds.
            if len(queries) > 3:
                queries = queries[:3]
            fa["pending_queries"] = [{"key": q.key, "purpose": q.purpose, "sql": q.sql} for q in queries]
            fa["round"] = int(fa.get("round", 0) or 0) + 1
            return Run(stage_name=self._db_query_stage_name)

        raise CachedRetryable(result=result, message=f"unknown action: {action!r}")
