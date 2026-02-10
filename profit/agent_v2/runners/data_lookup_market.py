from __future__ import annotations

from agentapi.plan import Run

from profit.agent_v2.runners.common import ContextualAgentRunner
from profit.agent_v2.models import MarketOhlcvParams, MarketOhlcvRequest, Request
from profit.agent_v2.retrievers import MarketRetrieverV2
from profit.agent_v2.runners.common import NoopLLMBackend


class DataLookupMarketRunner(ContextualAgentRunner):
    """Stage 3a: fetch market data."""

    def __init__(self):
        super().__init__(name="data_lookup_market", backend=NoopLLMBackend())

    def get_prompt(self, *, previous_history_entries):
        return ""


    def process_prompt(self, *, result: str, previous_history_entries):
        # For now, bypass market lookups and continue.
        return Run(stage_name="data_lookup_real_estate")
