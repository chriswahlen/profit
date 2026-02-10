from __future__ import annotations

import json
from typing import Any

from agentapi.history_entry import HistoryEntry
from agentapi.plan import Fork
from agentapi.runners import AgentTransformRunner

from agent_v2.json_utils import parse_json_object
from agent_v2.stages.data_lookup_utils import extract_datasets


PROMPT = """\
STAGE: data_lookup_sec

You have access to SEC/Edgar tables. Key tables include:

  - edgar_accession(cik, accession, base_url, file_count, fetched_at)
  - edgar_accession_file(accession, file_name, fetched_at, compressed_payload, source_url)
  - xbrl_context(context_id, accession, context_ref, entity_scheme_id, entity_id,
                 period_start_date, period_end_date, period_type, is_instant, is_forever)
  - xbrl_concept(concept_id, qname, label, data_type)
  - xbrl_unit(unit_id, accession, unit_ref, measure)
  - xbrl_fact(fact_id, accession, concept_id, context_id, unit_id, decimals, precision,
              sign, value_numeric, value_text, value_raw, is_nil, footnote_html)

Using the requests produced earlier, respond with STRICT JSON describing what you
would fetch. Your payload must look like:
{
  "datasets": [
    {
      "key": "...",
      "table": "edgar_accession|xbrl_context|xbrl_concept|xbrl_fact|xbrl_unit",
      "filters": {...},
      "rows": [{...}, ...],
      "query": "...",
      "summary": "text"
    }
  ],
  "loop": false
}

Rows should include column names from the tables above. Mention which accession/CIK
you solved. If you need to iterate further (e.g. to discover available concepts),
set loop=true and describe the follow-up in the summary.
"""


class DataLookupSECStage(AgentTransformRunner):
    def __init__(self, *, backend) -> None:
        super().__init__(name="data_lookup_sec", backend=backend)

    def get_prompt(
        self,
        *,
        previous_history_entries: list[HistoryEntry],
        user_context: dict[str, Any],
    ) -> str:
        question = str(user_context.get("question", "")).strip()
        requests = user_context.get("sec_requests") or []
        try:
            requests_json = json.dumps(requests, ensure_ascii=False, sort_keys=True)
        except (TypeError, ValueError):
            requests_json = "[]"
        return (
            f"{PROMPT}\n"
            f"USER_QUESTION:\n{question}\n\n"
            f"SEC_REQUESTS_JSON:\n{requests_json}\n"
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
        user_context["sec_datasets"] = datasets
        return Fork(children=[])
