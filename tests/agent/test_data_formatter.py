from __future__ import annotations

import json

from profit.agent.data_formatter import format_data_block


def _make_point(date: str, value: float) -> dict:
    return {"timestamp": f"{date}T00:00:00Z", "value": value}


def test_format_market_data_compact() -> None:
    payload = {
        "type": "market",
        "data": [
            {
                "instrument": "XNAS|AAPL",
            "field": "close",
            "points": [_make_point("2025-02-03", 257.49), _make_point("2025-02-04", 254.49)],
                "aggregations": {"monthly_avg": 256.0},
            }
        ],
    }
    formatted = format_data_block(payload)

    assert formatted.startswith("DATA\nXNAS|AAPL"), formatted
    assert "monthly_avg" in formatted


def test_format_market_data_truncates_rows() -> None:
    data = []
    for i in range(15):
        data.append({
            "instrument": "XNAS|AAPL",
            "field": "close",
            "points": [{"timestamp": f"2025-02-{i+1:02d}T00:00:00Z", "value": float(i)}],
            "aggregations": {},
        })
    payload = {"type": "market", "data": data}
    formatted = format_data_block(payload)

    assert "(+3 more dates)" in formatted


def test_format_generic_payload_flat() -> None:
    payload = {"type": "real_estate", "region": "metro|us|seattle"}
    formatted = format_data_block(payload)
    assert formatted.startswith("DATA")
    assert json.dumps(payload, ensure_ascii=False, separators=(',', ':'), sort_keys=True) in formatted
