from __future__ import annotations

from typing import Any

from agentapi.history_entry import HistoryEntry
from agentapi.plan import Fork


class FinalResponseStage:
    name = "final_response"
    """Terminal stage that persists the final answer."""

    def run(
        self,
        *,
        previous_history_entries: list[HistoryEntry],
        user_context: dict[str, Any],
    ):
        final_answer = str(user_context.get("final_answer", "")).strip()
        user_context["final_answer"] = final_answer
        return Fork(children=[])
