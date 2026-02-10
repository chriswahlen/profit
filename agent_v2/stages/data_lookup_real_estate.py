from __future__ import annotations

from typing import Any

from agentapi.history_entry import HistoryEntry
from agentapi.plan import Fork


PROMPT = """\
STAGE: data_lookup_real_estate

This stage executes Real Estate data requests against our internal stores.
It receives requests produced earlier and returns datasets keyed by request key.
"""


class DataLookupRealEstateStage:
    name = "data_lookup_real_estate"

    def run(
        self,
        *,
        previous_history_entries: list[HistoryEntry],
        user_context: dict[str, Any],
    ) -> Fork:
        reqs = user_context.get("real_estate_requests") or []
        datasets: dict[str, Any] = {}
        for r in reqs:
            if not isinstance(r, dict):
                continue
            key = str(r.get("key", "")).strip()
            request = str(r.get("request", "")).strip()
            if not key or not request:
                continue
            datasets[key] = {
                "kind": "real_estate_placeholder",
                "request": request,
                "rows": [],
                "note": "Real Estate backend not wired in this agent_v2 scaffold.",
            }
        user_context["real_estate_datasets"] = datasets
        return Fork(children=[])
