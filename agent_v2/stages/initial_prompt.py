from __future__ import annotations

from datetime import date
from typing import Any, Mapping, Optional

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
        self._parsed: Mapping[str, Any] | None = None

    def get_prompt(self, *, previous_history_entries) -> str:
        return f"{PROMPT}\nUSER_QUESTION:\n{self._question}\n"

    def process_prompt(self, *, result: str, previous_history_entries) -> Run:
        payload = parse_json_object(result, stage=self.name)
        self._parsed = payload
        return Run(stage_name=STAGE_QUERY_PRIOR_INSIGHTS)

    def history_metadata(self, *, fragment, previous_history_entries):
        tags_raw = []
        start_date = None
        end_date = None
        if self._parsed:
            tags_raw = self._parsed.get("tags") or []
            start_date = _parse_date(self._parsed.get("start_date"))
            end_date = _parse_date(self._parsed.get("end_date"))
        tags = [str(t).strip() for t in tags_raw if isinstance(t, str) and t.strip()]
        return {
            "question": self._question,
            "tags": tags,
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
            "user_context": {
                "question": self._question,
                "tags": tags,
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
            },
        }

