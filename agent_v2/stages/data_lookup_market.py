from __future__ import annotations

from typing import Any

from agentapi.history_entry import HistoryEntry
from agentapi.plan import Fork

from agent_v2.data_generation import synthetic_daily_series


PROMPT = """\
STAGE: data_lookup_market

This stage executes Market data requests (stocks/crypto/commodities) against our internal stores.
It receives requests produced earlier and returns datasets keyed by request key.

Note: in non-live mode, results may be synthetic/deterministic.
"""


class DataLookupMarketStage:
    name = "data_lookup_market"

    def run(
        self,
        *,
        previous_history_entries: list[HistoryEntry],
        user_context: dict[str, Any],
    ) -> Fork:
        reqs = user_context.get("market_requests") or []
        datasets: dict[str, Any] = {}
        for r in reqs:
            if not isinstance(r, dict):
                continue
            key = str(r.get("key", "")).strip()
            request = str(r.get("request", "")).strip()
            if not key or not request:
                continue
            datasets[key] = synthetic_daily_series(seed=f"market:{request}", days=10)
        user_context["market_datasets"] = datasets
        return Fork(children=[])
