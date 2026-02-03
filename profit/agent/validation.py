from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable

AGGREGATION_VALUES = {
    "7d_avg",
    "14d_avg",
    "30d_avg",
    "weekly_avg",
    "monthly_avg",
    "7d_median",
    "14d_median",
    "30d_median",
    "weekly_median",
    "monthly_median",
    "monthly_max",
    "monthly_min",
    "weekly_max",
    "weekly_min",
}

MARKET_FIELDS = {"open", "high", "low", "close", "volume", "adj_close", "vwap"}
DERIVED_FIELDS = {"pct_change", "volume_delta", "avg_spread", "market_depth", "trade_count"}
COMPANY_FIELDS = {
    "Revenues",
    "NetIncome",
    "EarningsPerShare",
    "Assets",
    "Liabilities",
    "Equity",
    "CashFlowsFromOperations",
    "CapitalExpenditures",
}


class AgentValidationError(ValueError):
    pass


def _expect_type(value: Any, expected_type: type, label: str) -> None:
    if not isinstance(value, expected_type):
        raise AgentValidationError(f"{label} must be a {expected_type.__name__}")


def _is_canonical_instrument(value: str) -> bool:
    return "|" in value and value.split("|")[0].isupper()


def _is_canonical_region(value: str) -> bool:
    return bool(re.match(r"^[a-z]+\\|", value))


def _is_cik(value: str) -> bool:
    return bool(re.match(r"^CIK:\\d+$", value))


def _validate_market_request(entry: dict) -> None:
    required = ["instruments", "fields", "aggregation"]
    for key in required:
        if key not in entry:
            raise AgentValidationError(f"market request missing {key}")
    instruments = entry["instruments"]
    _expect_type(instruments, list, "market.instruments")
    if not instruments:
        raise AgentValidationError("market.instruments cannot be empty")
    for instrument in instruments:
        if not isinstance(instrument, str) or not _is_canonical_instrument(instrument):
            raise AgentValidationError(f"invalid instrument id: {instrument}")

    fields = entry["fields"]
    _expect_type(fields, list, "market.fields")
    if not fields:
        raise AgentValidationError("market.fields cannot be empty")
    allowed_fields = MARKET_FIELDS | DERIVED_FIELDS
    for field in fields:
        if field not in allowed_fields:
            raise AgentValidationError(f"unsupported market field: {field}")

    aggregation = entry["aggregation"]
    _expect_type(aggregation, list, "market.aggregation")
    if not aggregation:
        raise AgentValidationError("market.aggregation cannot be empty")
    for agg in aggregation:
        if agg not in AGGREGATION_VALUES:
            raise AgentValidationError(f"invalid aggregation value: {agg}")

    for boundary in ("start", "end"):
        value = entry.get(boundary)
        if value is not None and value != "null" and not isinstance(value, str):
            raise AgentValidationError(f"{boundary} must be YYYY-MM-DD or null")


def _validate_real_estate_request(entry: dict) -> None:
    if "regions" not in entry:
        raise AgentValidationError("real_estate request missing regions")
    regions = entry["regions"]
    _expect_type(regions, list, "real_estate.regions")
    if not regions:
        raise AgentValidationError("real_estate.regions cannot be empty")
    for region in regions:
        if not isinstance(region, str) or not _is_canonical_region(region):
            raise AgentValidationError(f"invalid region id: {region}")

    if "aggregation" not in entry:
        raise AgentValidationError("real_estate request missing aggregation")
    aggregation = entry["aggregation"]
    _expect_type(aggregation, list, "real_estate.aggregation")
    if not aggregation:
        raise AgentValidationError("real_estate.aggregation cannot be empty")
    for agg in aggregation:
        if agg not in AGGREGATION_VALUES:
            raise AgentValidationError(f"invalid aggregation value: {agg}")

    for boundary in ("start", "end"):
        value = entry.get(boundary)
        if value is not None and value != "null" and not isinstance(value, str):
            raise AgentValidationError(f"{boundary} must be YYYY-MM-DD or null")


def _validate_company_facts_request(entry: dict) -> None:
    required = ["companies", "filings", "fields"]
    for key in required:
        if key not in entry:
            raise AgentValidationError(f"company_facts request missing {key}")
    companies = entry["companies"]
    _expect_type(companies, list, "company_facts.companies")
    if not companies:
        raise AgentValidationError("company_facts.companies cannot be empty")
    for company in companies:
        if not isinstance(company, str) or not (_is_canonical_instrument(company) or _is_cik(company)):
            raise AgentValidationError(f"invalid company id: {company}")

    filings = entry["filings"]
    _expect_type(filings, list, "company_facts.filings")
    if not filings:
        raise AgentValidationError("company_facts.filings cannot be empty")

    fields = entry["fields"]
    _expect_type(fields, list, "company_facts.fields")
    if not fields:
        raise AgentValidationError("company_facts.fields cannot be empty")
    for field in fields:
        _expect_type(field, dict, "company_facts.fields entry")
        key = field.get("key")
        if key not in COMPANY_FIELDS:
            raise AgentValidationError(f"unsupported company_facts field: {key}")

    for boundary in ("start", "end"):
        value = entry.get(boundary)
        if value is not None and value != "null" and not isinstance(value, str):
            raise AgentValidationError(f"{boundary} must be YYYY-MM-DD or null")


def _validate_snippet_request(entry: dict) -> None:
    action = entry.get("action")
    if action not in ("store", "lookup"):
        raise AgentValidationError("snippet action must be store or lookup")

    if action == "store":
        snippet = entry.get("snippet")
        if not snippet:
            raise AgentValidationError("snippet.store requires snippet payload")
        if not isinstance(snippet, dict):
            raise AgentValidationError("snippet must be an object")
        if "title" not in snippet or "body" not in snippet or "tags" not in snippet:
            raise AgentValidationError("snippet.store must include title, body, tags")
        if not isinstance(snippet["body"], list):
            raise AgentValidationError("snippet.body must be a list of strings")
        if not isinstance(snippet["tags"], list):
            raise AgentValidationError("snippet.tags must be a list of strings")
    else:
        filters = entry.get("filters")
        if not filters or not isinstance(filters, dict):
            raise AgentValidationError("snippet.lookup requires filters object")
        if "limit" in entry and not isinstance(entry["limit"], int):
            raise AgentValidationError("snippet.limit must be an integer")


def _validate_data_needs(needs: Iterable[Any]) -> None:
    for need in needs:
        if not isinstance(need, dict):
            raise AgentValidationError("data_needs entries must be objects")
        if "name" not in need:
            raise AgentValidationError("data_needs entry missing name")
        if need.get("criticality") not in (None, "high", "medium", "low"):
            raise AgentValidationError("data_needs criticality must be high|medium|low")


def validate_agent_response(payload: Any) -> None:
    if not isinstance(payload, dict):
        raise AgentValidationError("agent payload must be an object")
    data_request = payload.get("data_request")
    if not isinstance(data_request, list):
        raise AgentValidationError("data_request must be a list")
    agent_response = payload.get("agent_response")
    final_response = payload.get("final_response")
    if agent_response is None:
        if not final_response or not isinstance(final_response, str):
            raise AgentValidationError("agent_response must be a string")
    elif not isinstance(agent_response, str):
        raise AgentValidationError("agent_response must be a string")
    if final_response is not None and not isinstance(final_response, str):
        raise AgentValidationError("final_response must be a string")
    for entry in data_request:
        if not isinstance(entry, dict):
            raise AgentValidationError("data_request entries must be objects")
        typ = entry.get("type")
        if typ not in ("market", "real_estate", "company_facts", "snippet"):
            raise AgentValidationError(f"unknown request type: {typ}")
        request_body = entry.get("request")
        if not isinstance(request_body, dict):
            raise AgentValidationError("request must be an object")
        if typ == "market":
            _validate_market_request(request_body)
        elif typ == "real_estate":
            _validate_real_estate_request(request_body)
        elif typ == "company_facts":
            _validate_company_facts_request(request_body)
        else:
            _validate_snippet_request(request_body)

    data_needs = payload.get("data_needs", [])
    if data_needs:
        if not isinstance(data_needs, list):
            raise AgentValidationError("data_needs must be a list")
        _validate_data_needs(data_needs)
