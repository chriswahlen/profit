from __future__ import annotations

from agentapi.plan import Run

from profit.agent_v2.runners.common import ContextualAgentRunner, NoopLLMBackend
from profit.agent_v2.models import RealEstateParams, RealEstateRequest
from profit.agent_v2.retrievers import RealEstateRetrieverV2
from profit.agent_v2.exceptions import AgentV2RuntimeError


class DataLookupRealEstateRunner(ContextualAgentRunner):
    """Stage 3b: fetch real estate data."""

    def __init__(self, retriever: RealEstateRetrieverV2 | None = None):
        self.retriever = retriever or RealEstateRetrieverV2()
        super().__init__(name="data_lookup_real_estate", backend=NoopLLMBackend())

    def get_prompt(self, *, previous_history_entries):
        return ""

    def process_prompt(self, *, result: str, previous_history_entries):
        meta = previous_history_entries[0].metadata if previous_history_entries else {}
        step2 = meta.get("step2")
        if step2 is None:
            raise AgentV2RuntimeError("real estate lookup requires step2")
        data_payloads = meta.get("data_payloads", [])
        data_needs = meta.get("data_needs", [])

        for batch in step2.get("batches", []):
            for req in batch.get("requests", []):
                if req.get("type") != "real_estate":
                    continue
                params = req["params"]
                typed = RealEstateRequest(
                    request_id=req["request_id"],
                    type=req["type"],
                    params=RealEstateParams(
                        geo_id=params["geo_id"],
                        start_utc=params["start_utc"],
                        end_utc=params["end_utc"],
                        measures=list(params.get("measures", [])),
                        aggregation=list(params.get("aggregation", [])),
                    ),
                    timeout_ms=req.get("timeout_ms"),
                )
                result_obj = self.retriever.fetch(typed)
                data_payloads.append(result_obj.payload)
                data_needs.extend(result_obj.data_needs)

        self.set_meta(step2=step2, data_payloads=data_payloads, data_needs=data_needs, step1=meta.get("step1"), prior_insights=meta.get("prior_insights"))
        return Run(stage_name="data_lookup_sec")
