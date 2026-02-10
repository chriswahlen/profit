from __future__ import annotations

from profit.agent_v2.runners.common import ContextualAgentRunner
from profit.agent_v2.models import RealEstateParams, RealEstateRequest
from profit.agent_v2.retrievers import RealEstateRetrieverV2


class DataLookupRealEstateRunner(ContextualAgentRunner):
    """Stage 3b: fetch real estate data."""

    def __init__(self):
        # TODO: Implement
        raise Exception("aislop")

    def get_prompt(self, *, previous_history_entries):
        # TODO: Implement
        raise Exception("aislop")

    def process_prompt(self, *, result: str, previous_history_entries):
        # TODO: Implement
        raise Exception("aislop")
