from __future__ import annotations

from agentapi.plan import Fork


PROMPT = """\
STAGE: final_response

This is the terminal stage. It finalizes the agent run and persists the final answer.
No further work may be emitted from this stage.
"""


class FinalResponseStage:
    name = "final_response"

    def run(self, *, previous_history_entries):
        parent = previous_history_entries[-1].metadata if previous_history_entries else {}
        self._final_answer = str(parent.get("final_answer", "")).strip()
        return Fork(children=[])

    def history_metadata(self, *, fragment, previous_history_entries):
        return {"final_answer": getattr(self, "_final_answer", ""), "user_context": {"final_answer": getattr(self, "_final_answer", "")}}

