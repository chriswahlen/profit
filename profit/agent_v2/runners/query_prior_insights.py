from __future__ import annotations

import json
from datetime import datetime, timezone

from agentapi.plan import Run

from profit.agent.types import InsightSummary
from profit.agent_v2.exceptions import AgentV2RuntimeError
from profit.agent_v2.insights import InsightLookup, InsightsManager
from profit.agent_v2.runners.common import ContextualAgentRunner, NoopLLMBackend
from llm.llm_backend import LLMBackend

PROMPT = '''
You are a financial planning and market research expert.

As an expert, you leverage existing insights to reduce redundant work and focus new data requests.

USER QUERY:
{{QUESTION}}

APPROACH:
{{APPROACH}}

Potentially Relevant Insights (from the insight store):
{{INSIGHTS_JSON}}

INSTRUCTIONS:
Produce ONLY a JSON object matching this shape:
{
  "approach": "<revised instructions on how you plan to answer>",
  "insights": ["insight_key_001", "insight_key_002"],
  "data_queries": [
    {
      "type": "market",
      "description": "Why you need this market time-series",
      "period": "time period needed"
      "aggregation": ["7-day average", "median"]
      "filters": {"ticker": "XNAS:MSFT", "fields": ["close"]}
    },
    {
      "type": "real_estate",
      "description": "Which zipcode/region needs coverage",
      "period": "time period needed"
      "filters": {"geo_id": "US:NYC"}
    },
    {
      "type": "sec",
      "description": "Which filing or concept you want from EDGAR",
      "period": "time period needed"
      "filters": {"ticker": "XNAS:MSFT" }
    }
  ]
}

Focus data_queries on the data types you can fetch:
  - market OHLCV/indicator series
  - real estate region-level metrics
  - SEC/EDGAR filings and XBRL concepts. Look up by ticker or by CIK.

Only emit entries for the domains you still need to cover the user's question.
'''


class QueryPriorInsightsRunner(ContextualAgentRunner):
    """Stage: gather prior insights for the planner."""

    def __init__(self, *, backend: LLMBackend | None = None, insights_manager: InsightsManager | None = None):
        self.insights = insights_manager or InsightsManager()
        self._last_matches: list[InsightSummary] | None = None
        super().__init__(name="query_prior_insights", backend=backend or NoopLLMBackend())

    def get_prompt(self, *, previous_history_entries):
        meta = previous_history_entries[0].metadata if previous_history_entries else {}
        user_context = self._extract_user_context(meta)
        question = self._extract_question(meta)
        approach = user_context.get("approach", "").strip() or "No explicit approach supplied."
        matches = self._collect_insights(user_context)
        insights_json = json.dumps(
            [self._serialize_insight(match) for match in matches],
            ensure_ascii=False,
            indent=2,
        )
        prompt = PROMPT.replace("{{QUESTION}}", question)
        prompt = prompt.replace("{{APPROACH}}", approach)
        prompt = prompt.replace("{{INSIGHTS_JSON}}", insights_json)
        print("[QueryPriorInsights] PROMPT\n%s\n" % prompt)
        return prompt.strip()

    def process_prompt(self, *, result: str, previous_history_entries):
        print("[QueryPriorInsights] RESPONSE\n%s\n" % result)
        meta = previous_history_entries[0].metadata if previous_history_entries else {}
        user_context = self._extract_user_context(meta)
        matches = self._last_matches or self._collect_insights(user_context)
        parsed = self._parse_result(result)

        revised_approach = parsed.get("approach") or user_context.get("approach")
        data_queries = parsed.get("data_queries", [])
        if not isinstance(data_queries, list):
            data_queries = [data_queries]

        updated_context = dict(user_context)
        if revised_approach:
            updated_context["approach"] = revised_approach
        if data_queries:
            updated_context["data_queries"] = data_queries
        # TODO: Branch off into data_lookup_market, data_lookup_sec, data_lookup_realestate
        raise Exception("TODO")


    def _collect_insights(self, user_context: dict) -> list[InsightSummary]:
        lookups = self._build_lookups(user_context)
        matches = self.insights.lookup(lookups) if lookups else []
        self._last_matches = matches
        return matches

    def _build_lookups(self, user_context: dict) -> list[InsightLookup]:
        lookups: list[InsightLookup] = []
        entries = user_context.get("insights", [])
        if not isinstance(entries, list):
            raise AgentV2RuntimeError("user_context.insights must be a list")
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            tags = [tag.strip() for tag in entry.get("tags", []) or [] if isinstance(tag, str) and tag.strip()]
            if not tags:
                continue
            horizon = self._compute_freshness(entry)
            lookups.append(InsightLookup(tags=tags, freshness_horizon_days=horizon))
        return lookups

    @staticmethod
    def _compute_freshness(entry: dict) -> int:
        horizon = entry.get("freshness_horizon_days")
        if horizon is not None:
            try:
                value = int(horizon)
                return max(0, value)
            except (TypeError, ValueError):
                pass
        start_date = entry.get("start_date")
        if isinstance(start_date, str):
            try:
                dt = datetime.fromisoformat(start_date)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                delta = datetime.now(timezone.utc) - dt
                return max(0, delta.days)
            except ValueError:
                pass
        return 365

    @staticmethod
    def _serialize_insight(summary: InsightSummary) -> dict:
        return {
            "insight_id": summary.insight_id,
            "title": summary.title,
            "body": list(summary.body),
            "created_at": summary.created_at,
            "matched_tags": list(summary.matched_tags) if summary.matched_tags else [],
        }

    @staticmethod
    def _extract_user_context(meta: dict) -> dict:
        user_context = meta.get("user_context", {})
        if not isinstance(user_context, dict):
            raise AgentV2RuntimeError("user_context must be a dict")
        return user_context

    @staticmethod
    def _extract_question(meta: dict) -> str:
        question = meta.get("question")
        if question is None:
            return ""
        if not isinstance(question, str):
            raise AgentV2RuntimeError("question metadata must be a string")
        return question

    @staticmethod
    def _parse_result(raw: str) -> dict:
        body = raw.strip()
        if not body:
            return {}
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            return {}
        if not isinstance(payload, dict):
            return {}
        return payload


__all__ = ["QueryPriorInsightsRunner"]
