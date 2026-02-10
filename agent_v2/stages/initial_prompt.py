from __future__ import annotations

from datetime import date
from typing import Any, Optional

from agentapi.history_entry import HistoryEntry

from agentapi.plan import Run
from agentapi.runners import AgentTransformRunner

from agent_v2.constants import STAGE_QUERY_PRIOR_INSIGHTS
from agent_v2.json_utils import parse_json_object


PROMPT = """\
STAGE: initial_prompt

You are an expert in finance and economics.

You can query these internal data sources:
- Market (stocks/crypto/commodities)
- SEC/Edgar (filings, XBRL-derived fundamentals)
- Real Estate (regional housing/market metrics)

Goal:
Given the USER_QUESTION, propose a small set of INSIGHT_TAGS and an optional date window
to search for prior insights that could help answer the question.

Output STRICT JSON (no markdown) with:
{
  "tags": ["tag1", "tag2", ...],
  "start_date": "YYYY-MM-DD" | null,
  "end_date": "YYYY-MM-DD" | null
}
"""


def _parse_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        return None
    return date.fromisoformat(value.strip())


class InitialPromptStage(AgentTransformRunner):
    def __init__(self, *, question: str, backend) -> None:
        super().__init__(name="initial_prompt", backend=backend)
        self._question = question.strip()

    def get_prompt(
        self,
        *,
        previous_history_entries: list[HistoryEntry],
        user_context: dict[str, Any],
    ) -> str:
        return f"{PROMPT}\nUSER_QUESTION:\n{self._question}\n"

    def process_prompt(
        self,
        *,
        result: str,
        previous_history_entries: list[HistoryEntry],
        user_context: dict[str, Any],
    ) -> Run:
        payload = parse_json_object(result, stage=self.name)
        tags_raw = payload.get("tags") or []
        tags = [str(t).strip() for t in tags_raw if isinstance(t, str) and t.strip()]
        start_date = _parse_date(payload.get("start_date"))
        end_date = _parse_date(payload.get("end_date"))
        user_context["question"] = self._question
        user_context["tags"] = tags
        user_context["start_date"] = start_date.isoformat() if start_date else None
        user_context["end_date"] = end_date.isoformat() if end_date else None
        user_context.setdefault("prior_insights", [])
        return Run(stage_name=STAGE_QUERY_PRIOR_INSIGHTS)
