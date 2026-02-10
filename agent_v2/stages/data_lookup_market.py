from __future__ import annotations

from typing import Any

from agentapi.plan import Fork
from agentapi.runners import TransformRunner

from agent_v2.data_generation import synthetic_daily_series


PROMPT = """\
STAGE: data_lookup_market

This stage executes Market data requests (stocks/crypto/commodities) against our internal stores.
It receives requests produced earlier and returns datasets keyed by request key.

Note: in non-live mode, results may be synthetic/deterministic.
"""


class DataLookupMarketStage:
    name = "data_lookup_market"

    def run(self, *, previous_history_entries) -> Fork:
        parent = previous_history_entries[-1].metadata if previous_history_entries else {}
        reqs = parent.get("market_requests") or []
        datasets: dict[str, Any] = {}
        for r in reqs:
            if not isinstance(r, dict):
                continue
            key = str(r.get("key", "")).strip()
            request = str(r.get("request", "")).strip()
            if not key or not request:
                continue
            datasets[key] = synthetic_daily_series(seed=f"market:{request}", days=10)
        self._datasets = datasets
        self._context_passthrough = {
            "question": parent.get("question"),
            "tags": parent.get("tags"),
            "start_date": parent.get("start_date"),
            "end_date": parent.get("end_date"),
            "prior_insights": parent.get("prior_insights"),
            "market_requests": parent.get("market_requests"),
            "real_estate_requests": parent.get("real_estate_requests"),
            "sec_requests": parent.get("sec_requests"),
        }
        return Fork(children=[])

    def history_metadata(self, *, fragment, previous_history_entries):
        return {**getattr(self, "_context_passthrough", {}), "market_datasets": getattr(self, "_datasets", {})}

