from __future__ import annotations

from agentapi.plan import Run

from profit.agent_v2.runners.common import ContextualAgentRunner, NoopLLMBackend
from profit.agent_v2.insights import InsightLookup, InsightsManager

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
        lookups = []
        for query in step1.get("insight_ops", {}).get("search", []):
            lookups.append(
                InsightLookup(
                    tags=list(query.get("tags", [])),
                    freshness_horizon_days=int(query.get("freshness_horizon_days", 365)),
                )
            )
        prior = self.insights.lookup(lookups) if lookups else []
        self.set_meta(step1=step1, prior_insights=[i.__dict__ for i in prior])
        return Run(stage_name="compile_data")
