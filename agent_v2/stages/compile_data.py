from __future__ import annotations

import json
from datetime import date
from typing import Any, Optional

from agentapi.history_entry import HistoryEntry
from agentapi.plan import Run
from agentapi.runners import AgentTransformRunner

from agent_v2.constants import STAGE_FINAL_RESPONSE, STAGE_QUERY_PRIOR_INSIGHTS
from agent_v2.insights_store import InsightsStore
from agent_v2.json_utils import parse_json_object
from agent_v2.models import Insight


PROMPT = """\
STAGE: compile_data

You are an expert in finance and economics.

You are given:
- USER_QUESTION
- PRIOR_INSIGHTS (0+)
- DATASETS from internal sources (Market, Real Estate, SEC/Edgar)

Task:
1) Use the datasets + insights to answer the USER_QUESTION.
2) Optionally propose new INSIGHTS to store for future reuse (tagged, with optional start/end dates).
3) Either:
   - Return a FINAL answer; or
   - Request MORE DATA by refining insight tags and/or date window to search again.

Output STRICT JSON (no markdown) with:
{
  "action": "final" | "more_data",
  "final_answer": string | null,
  "insights_to_store": [
    {
      "text": "...",                     # full insight text drawn from the available data
      "description": "...",              # short conclusion that helps answer the user question
      "tags": ["..."],
      "start_date": "YYYY-MM-DD"|null,
      "end_date": "YYYY-MM-DD"|null
    }, ...
  ],
  "drop_dataset_keys": ["req_key_to_drop", ...],
  "refined_tags": ["tag1", ...],
  "refined_start_date": "YYYY-MM-DD" | null,
  "refined_end_date": "YYYY-MM-DD" | null
}

Each stored insight should be a conclusion drawn from the data collected so far that feels useful for answering the user question; the `description` field should summarize that conclusion.
"""


def _parse_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        return None
    return date.fromisoformat(value.strip())


class CompileDataStage(AgentTransformRunner):
    def __init__(self, *, backend, insights_store: InsightsStore) -> None:
        super().__init__(name="compile_data", backend=backend)
        self._insights_store = insights_store

    def _load_prompt_context(self, user_context: dict[str, Any]) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
        question = str(user_context.get("question", "")).strip()
        prior_insights = user_context.get("prior_insights") or []
        datasets = {
            "market": user_context.get("market_datasets") or {},
            "real_estate": user_context.get("real_estate_datasets") or {},
            "sec": user_context.get("sec_datasets") or {},
        }
        return question, prior_insights, datasets

    def get_prompt(
        self,
        *,
        previous_history_entries: list[HistoryEntry],
        user_context: dict[str, Any],
    ) -> str:
        question, prior_insights, datasets = self._load_prompt_context(user_context)
        return (
            f"{PROMPT}\n"
            f"USER_QUESTION:\n{question}\n\n"
            f"PRIOR_INSIGHTS_JSON:\n{json.dumps(prior_insights, ensure_ascii=False, sort_keys=True)}\n\n"
            f"DATASETS_JSON:\n{json.dumps(datasets, ensure_ascii=False, sort_keys=True)}\n"
        )

    def process_prompt(
        self,
        *,
        result: str,
        previous_history_entries: list[HistoryEntry],
        user_context: dict[str, Any],
    ) -> Run:
        question = str(user_context.get("question", "")).strip()

        payload = parse_json_object(result, stage=self.name)
        action = str(payload.get("action", "")).strip().lower()

        insights_raw = payload.get("insights_to_store") or []
        insights: list[Insight] = []
        for item in insights_raw:
            if not isinstance(item, dict):
                continue
            ins = Insight.from_dict(item)
            if ins.text and ins.tags:
                insights.append(ins)
        if insights:
            self._insights_store.add(insights)

        if action == "more_data":
            refined_tags_raw = payload.get("refined_tags") or []
            refined_tags = [str(t).strip() for t in refined_tags_raw if isinstance(t, str) and t.strip()]
            refined_start = _parse_date(payload.get("refined_start_date"))
            refined_end = _parse_date(payload.get("refined_end_date"))
            user_context["tags"] = refined_tags
            user_context["start_date"] = refined_start.isoformat() if refined_start else None
            user_context["end_date"] = refined_end.isoformat() if refined_end else None
            user_context["question"] = question
            return Run(stage_name=STAGE_QUERY_PRIOR_INSIGHTS)

        final_answer = str(payload.get("final_answer") or "").strip()
        user_context["final_answer"] = final_answer
        user_context["question"] = question
        return Run(stage_name=STAGE_FINAL_RESPONSE)
