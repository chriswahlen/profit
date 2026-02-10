from __future__ import annotations

from agentapi.plan import Run

from profit.agent_v2.runners.common import ContextualAgentRunner, NoopLLMBackend
from profit.agent_v2.insights import InsightLookup, InsightsManager
from profit.agent_v2.exceptions import AgentV2RuntimeError
from datetime import datetime, timezone

class QueryPriorInsightsRunner(ContextualAgentRunner):
    """Stage: fetch prior insights requested in step1."""

    def __init__(self, *, insights_manager: InsightsManager | None = None):
        self.insights = insights_manager or InsightsManager()
        super().__init__(name="query_prior_insights", backend=NoopLLMBackend())

    def get_prompt(self, *, previous_history_entries):
        return ""

    def process_prompt(self, *, result: str, previous_history_entries):
        meta = previous_history_entries[0].metadata if previous_history_entries else {}
        step1 = meta.get("step1", {})

        # Insight lookups now come solely from the validated initial prompt (user_context).
        user_ctx = meta.get("user_context", {})
        insight_search = user_ctx.get("insights", [])
        if not isinstance(insight_search, list):
            raise AgentV2RuntimeError("user_context.insights must be a list")

        lookups = []
        for query in insight_search:
            if not isinstance(query, dict):
                continue
            tags = [t for t in query.get("tags", []) if t]
            if not tags:
                continue
            # If the request specified a start_date, convert it into a freshness window; otherwise default to 365 days.
            horizon_days = 365
            start_date = query.get("start_date")
            if start_date:
                try:
                    start_dt = datetime.fromisoformat(start_date)
                    now = datetime.now(timezone.utc)
                    delta = now - start_dt.replace(tzinfo=timezone.utc) if start_dt.tzinfo is None else now - start_dt
                    horizon_days = max(0, int(delta.days))
                except Exception:
                    # fall back to default horizon
                    horizon_days = 365
            lookups.append(
                InsightLookup(
                    tags=tags,
                    freshness_horizon_days=int(query.get("freshness_horizon_days", horizon_days)),
                )
            )

        prior = self.insights.lookup(lookups) if lookups else []
        # Expose hits to the LLM by passing them forward in metadata.
        self.set_meta(step1=step1, prior_insights=[i.__dict__ for i in prior])
        return Run(stage_name="compile_data")
