from __future__ import annotations

import os
from pathlib import Path

from profit import config
from profit.config import ProfitConfig


def test_load_profit_conf_sets_env(monkeypatch, tmp_path):
    cfg = tmp_path / ".profit.conf"
    cfg.write_text("PROFIT_CACHE_DIR=/tmp/custom_cache\nEXTRA=1\n")
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("PROFIT_CACHE_DIR", raising=False)
    config._reset_for_tests()

    parsed = config.ensure_profit_conf_loaded()
    assert os.environ["PROFIT_CACHE_DIR"] == "/tmp/custom_cache"
    assert "EXTRA" in os.environ
    # ensure idempotent
    config.ensure_profit_conf_loaded()
    assert parsed is None or parsed == {}  # ensure no exception


def test_default_cache_dir_honors_config(monkeypatch, tmp_path):
    cfg = tmp_path / ".profit.conf"
    cfg.write_text("PROFIT_CACHE_ROOT=/var/cache/profit\n")
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("PROFIT_CACHE_DIR", raising=False)
    monkeypatch.delenv("PROFIT_CACHE_ROOT", raising=False)
    config._reset_for_tests()
    base = ProfitConfig.resolve_cache_root()
    assert str(base) == "/var/cache/profit"


def test_get_data_root(monkeypatch, tmp_path):
    cfg = tmp_path / ".profit.conf"
    cfg.write_text("PROFIT_DATA_ROOT=/var/data/profit\n")
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("PROFIT_DATA_ROOT", raising=False)
    config._reset_for_tests()
    root = ProfitConfig.resolve_data_root()
    assert str(root) == "/var/data/profit"
