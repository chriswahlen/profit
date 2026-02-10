from __future__ import annotations

from datetime import date
from typing import Any, Mapping, Optional

from agentapi.plan import Fork, Join, Run
from agentapi.runners import AgentTransformRunner

from agent_v2.constants import (
    STAGE_COMPILE_DATA,
    STAGE_DATA_LOOKUP_MARKET,
    STAGE_DATA_LOOKUP_REAL_ESTATE,
    STAGE_DATA_LOOKUP_SEC,
)
from agent_v2.insights_store import InsightsStore
from agent_v2.json_utils import parse_json_object
from agent_v2.models import DataRequest


PROMPT = """\
STAGE: query_prior_insights

You are an expert finance/economics research agent.

You are given:
- USER_QUESTION
- INSIGHT_TAGS and an optional date window
- PRIOR_INSIGHTS (0+), each with tags and optional date window

Task:
1) Consider which prior insights are relevant (or note if none match).
2) Propose DATA REQUESTS to query our internal sources (Market, Real Estate, SEC/Edgar).
   Requests can be high-level, but must be concrete enough to execute.

Output STRICT JSON (no markdown) with:
{
  "market_requests": [{"key":"...", "request":"...", "why":"..."}, ...],
  "real_estate_requests": [{"key":"...", "request":"...", "why":"..."}, ...],
  "sec_requests": [{"key":"...", "request":"...", "why":"..."}, ...],
  "additional_insight_tags": ["tag3", ...]
}
"""


def _parse_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        return None
    return date.fromisoformat(value.strip())


class QueryPriorInsightsStage(AgentTransformRunner):
    def __init__(self, *, backend, insights_store: InsightsStore) -> None:
        super().__init__(name="query_prior_insights", backend=backend)
        self._insights_store = insights_store
        self._parsed: Mapping[str, Any] | None = None
        self._prior_insights: list[dict[str, Any]] = []
        self._question: str = ""
        self._tags: list[str] = []
        self._start_date: Optional[date] = None
        self._end_date: Optional[date] = None

    def _load_context(self, previous_history_entries) -> None:
        parent = previous_history_entries[-1].metadata if previous_history_entries else {}
        self._question = str(parent.get("question", "")).strip()
        tags_raw = parent.get("tags") or []
        self._tags = [str(t).strip() for t in tags_raw if isinstance(t, str) and t.strip()]
        self._start_date = _parse_date(parent.get("start_date"))
        self._end_date = _parse_date(parent.get("end_date"))

        hits = self._insights_store.search(
            tags=self._tags, start_date=self._start_date, end_date=self._end_date, limit=25
        )
        self._prior_insights = [
            {"insight_id": row.insight_id, **row.insight.to_dict()} for row in hits
        ]

    def get_prompt(self, *, previous_history_entries) -> str:
        self._load_context(previous_history_entries)
        prior_text = "[]"
        if self._prior_insights:
            import json as _json

            prior_text = _json.dumps(self._prior_insights, ensure_ascii=False, sort_keys=True)
        return (
            f"{PROMPT}\n"
            f"USER_QUESTION:\n{self._question}\n\n"
            f"INSIGHT_TAGS: {self._tags}\n"
            f"DATE_WINDOW: start={self._start_date.isoformat() if self._start_date else None} "
            f"end={self._end_date.isoformat() if self._end_date else None}\n\n"
            f"PRIOR_INSIGHTS_JSON:\n{prior_text}\n"
        )

    def process_prompt(self, *, result: str, previous_history_entries) -> Any:
        payload = parse_json_object(result, stage=self.name)
        self._parsed = payload

        market = [DataRequest.from_dict(d).to_dict() for d in (payload.get("market_requests") or []) if isinstance(d, dict)]
        re = [DataRequest.from_dict(d).to_dict() for d in (payload.get("real_estate_requests") or []) if isinstance(d, dict)]
        sec = [DataRequest.from_dict(d).to_dict() for d in (payload.get("sec_requests") or []) if isinstance(d, dict)]

        children: list[Any] = []
        if market:
            children.append(Run(stage_name=STAGE_DATA_LOOKUP_MARKET))
        if re:
            children.append(Run(stage_name=STAGE_DATA_LOOKUP_REAL_ESTATE))
        if sec:
            children.append(Run(stage_name=STAGE_DATA_LOOKUP_SEC))

        if children:
            return Join(children=[Fork(children=children)], then=Run(stage_name=STAGE_COMPILE_DATA))
        return Run(stage_name=STAGE_COMPILE_DATA)

    def history_metadata(self, *, fragment, previous_history_entries):
        payload = self._parsed or {}
        def _reqs(key: str) -> list[dict[str, str]]:
            out: list[dict[str, str]] = []
            raw = payload.get(key) or []
            for item in raw:
                if not isinstance(item, dict):
                    continue
                dr = DataRequest.from_dict(item)
                if dr.key and dr.request:
                    out.append(dr.to_dict())
            return out

        market = _reqs("market_requests")
        re = _reqs("real_estate_requests")
        sec = _reqs("sec_requests")
        add_tags_raw = payload.get("additional_insight_tags") or []
        add_tags = [str(t).strip() for t in add_tags_raw if isinstance(t, str) and t.strip()]
        merged_tags = sorted({*self._tags, *add_tags})

        return {
            "question": self._question,
            "tags": merged_tags,
            "start_date": self._start_date.isoformat() if self._start_date else None,
            "end_date": self._end_date.isoformat() if self._end_date else None,
            "prior_insights": self._prior_insights,
            "market_requests": market,
            "real_estate_requests": re,
            "sec_requests": sec,
            "user_context": {
                "question": self._question,
                "tags": merged_tags,
                "start_date": self._start_date.isoformat() if self._start_date else None,
                "end_date": self._end_date.isoformat() if self._end_date else None,
                "prior_insights_count": len(self._prior_insights),
            },
        }

