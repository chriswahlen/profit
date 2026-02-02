from datetime import date

from profit.agent.prompt import build_messages
from profit.agent.types import Question, RetrievalPlan, RetrievedData


def test_build_messages_includes_today_and_data():
    question = Question(text="How did AAPL perform?")
    plan = RetrievalPlan(source="prices", instruments=("AAPL",), start=date(2024, 1, 1), end=date(2024, 1, 31))
    data = RetrievedData(
        source="prices",
        payload={"provider": "yfinance", "window": {"start": "2024-01-01", "end": "2024-01-31"}},
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
    )
    msgs = build_messages(question=question, plan=plan, data=data, today=date(2026, 2, 2))
    content = msgs[1]["content"]
    assert "2026-02-02" in msgs[0]["content"]
    assert "User question" in content
    assert "AAPL" in content
    assert "yfinance" in content
