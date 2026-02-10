from __future__ import annotations

from agentapi.plan import Run

from profit.agent_v2.runners.common import ContextualAgentRunner, NoopLLMBackend
from profit.agent_v2.insights import InsightLookup, InsightsManager
from profit.agent_v2.exceptions import AgentV2RuntimeError
from datetime import datetime, timezone

PROMPT = '''
You are a financial planning and market research expert.

As an expert, you will develop key insights in order to answer the user's question. These key
insights will be derived from market data, real estate data, and SEC/EDGAR filings.

USER QUERY:
{{QUESTION}}

APPROACH:
{{APPROACH}}

Potentially Relevant Insights:
{{INSIGHTS_JSON}}

INSTRUCTIONS:
Produce ONLY a JSON object matching this shape:
{
  "approach": "<revised instructions on how you plan to answer>",
  // A list of insights to retrieve
  "insights": [ "insight_key_001", "insight_key_002", ... ]
  // TODO: A list of data needed in JSON format.
}
'''
class QueryPriorInsightsRunner(ContextualAgentRunner):
    """Stage: fetch prior insights requested in step1."""

    def __init__(self, *, insights_manager: InsightsManager | None = None):
        self.insights = insights_manager or InsightsManager()
        super().__init__(name="query_prior_insights", backend=NoopLLMBackend())

    def get_prompt(self, *, previous_history_entries):
        prompt = PROMPT
        # TODO: replace {{QUESTION}} with the original user question
        # TODO: replace {{APPROACH}} with the origina approach field.
        # TODO: Query the insights from the tags given in `user_context`, and list the matches as a JSON Formatted field in {{INSIGHTS_JSON}}
        # TODO: Give instructions on what kind of market, real estate, and SEC data it can query.
        return ""

    def process_prompt(self, *, result: str, previous_history_entries):
        # TODO: Look at the result str for the appropriate entries
        # TODO: Store the revised approach in the user_context
        # TODO: Store the data queries in the data context
        # TODO: Spin off subgraphs of the right data fetcher to obtain the data, ending on the compile step.
        raise Exception("not implemented")