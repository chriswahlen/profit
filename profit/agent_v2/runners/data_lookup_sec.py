from __future__ import annotations

from agentapi.plan import Run

from profit.agent_v2.runners.common import ContextualAgentRunner, NoopLLMBackend
from profit.agent_v2.models import EdgarParams, EdgarRequest
from profit.agent_v2.retrievers import EdgarRetrieverV2
from profit.agent_v2.exceptions import AgentV2RuntimeError


class DataLookupSecRunner(ContextualAgentRunner):
    """Stage 3c: fetch SEC/EDGAR data."""

    def __init__(self, retriever: EdgarRetrieverV2 | None = None):
        self.retriever = retriever or EdgarRetrieverV2()
        super().__init__(name="data_lookup_sec", backend=NoopLLMBackend())

    def get_prompt(self, *, previous_history_entries):
        return ""

    def process_prompt(self, *, result: str, previous_history_entries):
        meta = previous_history_entries[0].metadata if previous_history_entries else {}
        step2 = meta.get("step2")
        if step2 is None:
            raise AgentV2RuntimeError("sec lookup requires step2")
        data_payloads = meta.get("data_payloads", [])
        data_needs = meta.get("data_needs", [])

        for batch in step2.get("batches", []):
            for req in batch.get("requests", []):
                if req.get("type") != "edgar_xbrl":
                    continue
                params = req["params"]
                typed = EdgarRequest(
                    request_id=req["request_id"],
                    type=req["type"],
                    params=EdgarParams(
                        cik=params["cik"],
                        start_utc=params["start_utc"],
                        end_utc=params["end_utc"],
                        period_type=params["period_type"],
                        concept_aliases=list(params.get("concept_aliases", [])),
                        limit=params.get("limit", 100),
                    ),
                    timeout_ms=req.get("timeout_ms"),
                )
                result_obj = self.retriever.fetch(typed)
                data_payloads.append(result_obj.payload)
                data_needs.extend(result_obj.data_needs)

        self.set_meta(
            step2=step2,
            data_payloads=data_payloads,
            data_needs=data_needs,
            step1=meta.get("step1"),
            prior_insights=meta.get("prior_insights"),
        )
        return Run(stage_name="final_response")
