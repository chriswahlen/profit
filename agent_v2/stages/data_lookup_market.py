from __future__ import annotations

import json
from typing import Any

from agentapi.history_entry import HistoryEntry
from agentapi.plan import Fork
from agentapi.runners import AgentTransformRunner

from agent_v2.json_utils import parse_json_object
from agent_v2.stages.data_lookup_utils import extract_datasets


PROMPT = """\
STAGE: data_lookup_market

You are responsible for turning MARKET_REQUESTS into the structured datasets our
market data fetchers understand. Each request includes a `key`, a textual `request`,
and a short `why` explanation. You have access to the `market_metrics` table with
the following columns:

  - region_id
  - period_start_date
  - period_granularity
  - source_provider
  - data_revision
  - median_sale_price
  - median_list_price
  - homes_sold
  - new_listings
  - inventory
  - median_dom
  - sale_to_list_ratio
  - price_drops_pct
  - pending_sales
  - months_supply
  - avg_ppsf

Your job is to return STRICT JSON (no markdown) with:
{
  "datasets": [
    {
      "key": "...",                   # matches one of the MARKET_REQUESTS keys
      "rows": [{...}, ...],           # each row is a flat dict describing the data
      "query": {...},                 # optional structured description of what was fetched
      "summary": "text"               # optional human-friendly note
    }
  ]
}

Provide a dataset entry for every request you can satisfy. If you did not fetch
anything for a request, include it with an empty rows list. The rows list should
contain dictionaries with the columns above whenever possible.
"""


class DataLookupMarketStage(AgentTransformRunner):
    def __init__(self, *, backend) -> None:
        super().__init__(name="data_lookup_market", backend=backend)

    def get_prompt(
        self,
        *,
        previous_history_entries: list[HistoryEntry],
        user_context: dict[str, Any],
    ) -> str:
        question = str(user_context.get("question", "")).strip()
        requests = user_context.get("market_requests") or []
        try:
            requests_json = json.dumps(requests, ensure_ascii=False, sort_keys=True)
        except (TypeError, ValueError):
            requests_json = "[]"
        return (
            f"{PROMPT}\n"
            f"USER_QUESTION:\n{question}\n\n"
            f"MARKET_REQUESTS_JSON:\n{requests_json}\n"
        )

    def process_prompt(
        self,
        *,
        result: str,
        previous_history_entries: list[HistoryEntry],
        user_context: dict[str, Any],
    ) -> Fork:
        payload = parse_json_object(result, stage=self.name)
        datasets = extract_datasets(payload)
        user_context["market_datasets"] = datasets
        return Fork(children=[])
