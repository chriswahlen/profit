from __future__ import annotations

import json
from datetime import date
from typing import Any, Mapping, Optional

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
  "insights_to_store": [{"text": "...", "tags": ["..."], "start_date": "YYYY-MM-DD"|null, "end_date": "YYYY-MM-DD"|null}, ...],
  "drop_dataset_keys": ["req_key_to_drop", ...],
  "refined_tags": ["tag1", ...],
  "refined_start_date": "YYYY-MM-DD" | null,
  "refined_end_date": "YYYY-MM-DD" | null
}
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
        self._parsed: Mapping[str, Any] | None = None
        self._final_answer: str | None = None
        self._refined: dict[str, Any] = {}
        self._question: str = ""

    def get_prompt(self, *, previous_history_entries) -> str:
        merged: dict[str, Any] = {}
        for entry in previous_history_entries:
            for k in (
                "question",
                "tags",
                "start_date",
                "end_date",
                "prior_insights",
                "market_requests",
                "real_estate_requests",
                "sec_requests",
                "market_datasets",
                "real_estate_datasets",
                "sec_datasets",
            ):
                if k in entry.metadata and k not in merged:
                    merged[k] = entry.metadata.get(k)

        question = str(merged.get("question", "")).strip()
        self._question = question
        prior_insights = merged.get("prior_insights") or []
        datasets = {
            "market": merged.get("market_datasets") or {},
            "real_estate": merged.get("real_estate_datasets") or {},
            "sec": merged.get("sec_datasets") or {},
        }
        return (
            f"{PROMPT}\n"
            f"USER_QUESTION:\n{question}\n\n"
            f"PRIOR_INSIGHTS_JSON:\n{json.dumps(prior_insights, ensure_ascii=False, sort_keys=True)}\n\n"
            f"DATASETS_JSON:\n{json.dumps(datasets, ensure_ascii=False, sort_keys=True)}\n"
        )

    def process_prompt(self, *, result: str, previous_history_entries) -> Run:
        payload = parse_json_object(result, stage=self.name)
        self._parsed = payload
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
            self._refined = {
                "tags": refined_tags,
                "start_date": refined_start.isoformat() if refined_start else None,
                "end_date": refined_end.isoformat() if refined_end else None,
            }
            return Run(stage_name=STAGE_QUERY_PRIOR_INSIGHTS)

        self._final_answer = str(payload.get("final_answer") or "").strip()
        return Run(stage_name=STAGE_FINAL_RESPONSE)

    def history_metadata(self, *, fragment, previous_history_entries):
        md: dict[str, Any] = {}
        if self._question:
            md["question"] = self._question
        if self._final_answer is not None:
            md["final_answer"] = self._final_answer
        if self._refined:
            md.update(self._refined)
            md["user_context"] = {
                "question": self._question,
                "tags": self._refined.get("tags") or [],
                "start_date": self._refined.get("start_date"),
                "end_date": self._refined.get("end_date"),
            }
        return md
