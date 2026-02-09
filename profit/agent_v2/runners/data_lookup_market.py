from __future__ import annotations

from agentapi.plan import Run

from profit.agent_v2.runners.common import ContextualAgentRunner, NoopLLMBackend
from profit.agent_v2.models import MarketOhlcvParams, MarketOhlcvRequest, Step2Result, Request
from profit.agent_v2.retrievers import MarketRetrieverV2
from profit.agent_v2.exceptions import AgentV2RuntimeError


class DataLookupMarketRunner(ContextualAgentRunner):
    """Stage 3a: fetch market data."""

    def __init__(self, retriever: MarketRetrieverV2 | None = None):
        self.retriever = retriever or MarketRetrieverV2()
        super().__init__(name="data_lookup_market", backend=NoopLLMBackend())

    def get_prompt(self, *, previous_history_entries):
        return ""

    def process_prompt(self, *, result: str, previous_history_entries):
        meta = previous_history_entries[0].metadata if previous_history_entries else {}
        step2 = meta.get("step2")
        if step2 is None:
            raise AgentV2RuntimeError("market lookup requires step2")
        data_payloads = meta.get("data_payloads", [])
        data_needs = meta.get("data_needs", [])

        step2_obj = Step2Result(raw=step2, batches=[])  # placeholder for typing
        for batch in step2.get("batches", []):
            for req in batch.get("requests", []):
                if req.get("type") != "market_ohlcv":
                    continue
                params = req["params"]
                typed = MarketOhlcvRequest(
                    request_id=req["request_id"],
                    type=req["type"],
                    params=MarketOhlcvParams(
                        ticker=params["ticker"],
                        exchange_mic=params["exchange_mic"],
                        start_utc=params["start_utc"],
                        end_utc=params["end_utc"],
                        bar_size=params.get("bar_size", "1d"),
                        fields=list(params.get("fields", [])),
                        adjust_splits=params.get("adjust_splits"),
                        adjust_dividends=params.get("adjust_dividends"),
                        post_aggregations=params.get("post_aggregations"),
                    ),
                    timeout_ms=req.get("timeout_ms"),
                )
                result_obj = self.retriever.fetch(typed)
                data_payloads.append(result_obj.payload)
                data_needs.extend(result_obj.data_needs)

        self.set_meta(step2=step2, data_payloads=data_payloads, data_needs=data_needs, step1=meta.get("step1"), prior_insights=meta.get("prior_insights"))
        return Run(stage_name="data_lookup_real_estate")
