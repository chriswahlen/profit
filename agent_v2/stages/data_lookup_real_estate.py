from __future__ import annotations

import json
from typing import Any

from agentapi.history_entry import HistoryEntry
from agentapi.plan import Fork
from agentapi.runners import AgentTransformRunner

from agent_v2.json_utils import parse_json_object
from agent_v2.stages.data_lookup_utils import extract_datasets


PROMPT = """\
STAGE: data_lookup_real_estate

You are delivering structured data for real estate requests. You can reference
the following tables:

 01. regions(region_id, region_type, name, canonical_code, country_iso2, parent_region_id,
              population, timezone, metadata, created_at)
 02. region_code_map(region_id, code_type, code_value, active_from, active_to)
 03. region_provider_map(provider, provider_region_id, region_id, provider_name,
                         active_from, active_to, data_revision)
 04. market_metrics(region_id, period_start_date, period_granularity, data_revision,
                    source_provider, median_sale_price, median_list_price, homes_sold,
                    new_listings, inventory, median_dom, sale_to_list_ratio,
                    price_drops_pct, pending_sales, months_supply, avg_ppsf, created_at)

Each dataset entry you return must reference the request key and document the query
you would execute. Output STRICT JSON with:
{
  "datasets": [
    {
      "key": "...",
      "table": "regions|market_metrics|region_provider_map|region_code_map",
      "filters": {...},
      "rows": [{...}, ...],
      "query": "...",
      "summary": "text"
    }
  ]
}

If you need to ask for more data before resolving a request, reply with loop=true
and describe what you still need in the summary. Otherwise set loop=false or omit it.
"""


class DataLookupRealEstateStage(AgentTransformRunner):
    def __init__(self, *, backend) -> None:
        super().__init__(name="data_lookup_real_estate", backend=backend)

    def get_prompt(
        self,
        *,
        previous_history_entries: list[HistoryEntry],
        user_context: dict[str, Any],
    ) -> str:
        question = str(user_context.get("question", "")).strip()
        requests = user_context.get("real_estate_requests") or []
        try:
            requests_json = json.dumps(requests, ensure_ascii=False, sort_keys=True)
        except (TypeError, ValueError):
            requests_json = "[]"
        return (
            f"{PROMPT}\n"
            f"USER_QUESTION:\n{question}\n\n"
            f"REAL_ESTATE_REQUESTS_JSON:\n{requests_json}\n"
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
        user_context["real_estate_datasets"] = datasets
        return Fork(children=[])
