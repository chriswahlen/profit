from __future__ import annotations

from profit.agent_v2.runners.common import ContextualAgentRunner
from profit.agent_v2.models import RealEstateParams, RealEstateRequest
from profit.agent_v2.retrievers import RealEstateRetrieverV2
from agentapi.plan import Run
from profit.agent_v2.runners.common import NoopLLMBackend


class DataLookupRealEstateRunner(ContextualAgentRunner):
    """Stage 3b: fetch real estate data."""

    def __init__(self):
        super().__init__(name="data_lookup_real_estate", backend=NoopLLMBackend())

    def get_prompt(self, *, previous_history_entries):
        return ""

    def process_prompt(self, *, result: str, previous_history_entries):
        return Run(stage_name="data_lookup_sec")
