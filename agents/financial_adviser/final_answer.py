from __future__ import annotations

from typing import Any

from agentapi.plan import Fork
from agentapi.plan import Plan
from agentapi.runners import TransformRunner

STAGE_FINAL = "financial_adviser.final_answer"


class FinalAnswerStage(TransformRunner):
    """
    Terminal stage.

    We keep this as a separate stage so the LLM stage can decide to answer
    without being terminal itself (it might have pending work in other runs in
    future DAG expansions).
    """

    name = STAGE_FINAL

    def run(self, *, previous_history_entries: list[Any], user_context: dict[str, Any]) -> Plan:
        user_context.setdefault("financial_adviser", {})
        fa = user_context["financial_adviser"]
        if not isinstance(fa, dict):
            raise ValueError("user_context.financial_adviser must be a dict")

        # Invariants for downstream consumers (UI, callers).
        if "answer" not in fa:
            fa["answer"] = ""
        if "status" not in fa:
            fa["status"] = "completed"

        return Fork(children=[])

