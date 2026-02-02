import json
from datetime import date

from profit.agent.planner import interpret_planner_output, PlannerDecision


def test_interpret_planner_output_basic():
    raw = {
        "sources": [
            {
                "source": "prices",
                "instruments": ["AAPL"],
                "start": "2024-01-01",
                "end": "2024-01-31",
                "max_points": 40,
                "aggregations": ["7d_avg", "weekly"],
                "notes": "monthly trend",
            }
        ],
        "answer_prompt": "Use DATA to summarize.",
    }
    decision = interpret_planner_output(raw)
    assert isinstance(decision, PlannerDecision)
    src = decision.sources[0]
    assert src.source == "prices"
    assert src.instruments == ("AAPL",)
    assert src.start == date(2024, 1, 1)
    assert src.end == date(2024, 1, 31)
    assert src.max_points == 40
    assert src.aggregations == ("7d_avg", "weekly")
    assert decision.answer_prompt.startswith("Use DATA")


def test_interpret_planner_output_filters_invalid():
    raw = json.dumps(
        {
            "sources": [
                {
                    "source": "prices",
                    "instruments": ["AAPL"],
                    "aggregations": ["bad_rollup", "weekly"],
                },
                {"source": "made_up"},
            ],
            "answer_prompt": "test",
        }
    )
    decision = interpret_planner_output(raw)
    assert decision.sources[0].aggregations == ("weekly",)
    assert decision.sources[1].source == "unknown"
