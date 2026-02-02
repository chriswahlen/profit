from profit.agent.planner import extract_planner_json


def test_extract_planner_json_from_text():
    text = """
    Sure, here's the plan:
    {
      "sources": [{"source": "prices", "instruments": ["AAPL"]}],
      "answer_prompt": "Use DATA to summarize."
    }
    """
    decision = extract_planner_json(text)
    assert decision.sources[0].source == "prices"
    assert decision.answer_prompt.startswith("Use DATA")


def test_extract_planner_json_raises_on_missing():
    try:
        extract_planner_json("no json here")
    except ValueError:
        return
    assert False, "expected ValueError"
