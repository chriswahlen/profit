import pytest

from profit.sources.equities.yfinance_ids import (
    exchange_to_mic,
    make_entity_id,
    normalize_symbol,
)


def test_normalize_symbol_basic():
    assert normalize_symbol("AAPL") == "aapl"
    assert normalize_symbol("BRK.B") == "brk-b"
    assert normalize_symbol("Shop.TO ") == "shop-to"
    assert normalize_symbol(" AC.MX ") == "ac-mx"

    with pytest.raises(ValueError):
        normalize_symbol("!!!")


def test_exchange_to_mic_mapping():
    assert exchange_to_mic("NASDAQ") == "xnas"
    assert exchange_to_mic("nysearca") == "arcx"
    assert exchange_to_mic("unknown") is None


def test_make_entity_id_with_mic():
    assert make_entity_id("NASDAQ", "AAPL") == "company:xnas:aapl"
    assert make_entity_id("NYSE", "BRK.B") == "company:xnys:brk-b"


def test_make_entity_id_fallback_exchange():
    entity_id = make_entity_id("BVC", "PFBCOLOM")
    assert entity_id == "company:xref:bvc:pfbcolom"
    # ensure slug validation passes
    assert entity_id.startswith("company:xref:")
