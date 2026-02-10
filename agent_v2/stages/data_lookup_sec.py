from __future__ import annotations

from typing import Any

from agentapi.history_entry import HistoryEntry
from agentapi.plan import Fork


PROMPT = """\
STAGE: data_lookup_sec

This stage executes SEC/Edgar data requests against our internal stores.
It receives requests produced earlier and returns datasets keyed by request key.
"""


class DataLookupSECStage:
    name = "data_lookup_sec"

    def run(
        self,
        *,
        previous_history_entries: list[HistoryEntry],
        user_context: dict[str, Any],
    ) -> Fork:
        reqs = user_context.get("sec_requests") or []
        datasets: dict[str, Any] = {}
        for r in reqs:
            if not isinstance(r, dict):
                continue
            key = str(r.get("key", "")).strip()
            request = str(r.get("request", "")).strip()
            if not key or not request:
                continue
            datasets[key] = {
                "kind": "sec_placeholder",
                "request": request,
                "rows": [],
                "note": "SEC/Edgar backend not wired in this agent_v2 scaffold.",
            }
        user_context["sec_datasets"] = datasets
        return Fork(children=[])
