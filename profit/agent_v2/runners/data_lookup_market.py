from __future__ import annotations

from agentapi.plan import Run

from profit.agent_v2.runners.common import ContextualAgentRunner
from profit.agent_v2.models import MarketOhlcvParams, MarketOhlcvRequest, Step2Result, Request
from profit.agent_v2.retrievers import MarketRetrieverV2


class DataLookupMarketRunner(ContextualAgentRunner):
    """Stage 3a: fetch market data."""

    def __init__(self):
        # TODO: Implement
        pass

    def get_prompt(self, *, previous_history_entries):
        # TODO: Implement
        raise Exception("aislop")


    def process_prompt(self, *, result: str, previous_history_entries):
        # TODO: Implement
        raise Exception("aislop")
