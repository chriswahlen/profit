from profit.agent.router import Router
from profit.agent.types import Question


def test_router_classifies_price_queries_and_extracts_tickers():
    r = Router()
    q = Question(text="What's the price trend for AAPL and MSFT over the last month?")
    plan = r.route(q)
    assert plan.source == "prices"
    assert set(plan.instruments) == {"AAPL", "MSFT"}


def test_router_classifies_redfin_keywords():
    r = Router()
    q = Question(text="Show me Redfin inventory changes in Seattle")
    plan = r.route(q)
    assert plan.source == "redfin"


def test_router_classifies_edgar_keywords():
    r = Router()
    q = Question(text="Summarize risk factors from the latest 10-K filing.")
    plan = r.route(q)
    assert plan.source == "edgar"
