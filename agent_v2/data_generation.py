from __future__ import annotations

import hashlib
from datetime import date, timedelta
from typing import Any


def _stable_int(seed: str) -> int:
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return int(digest[:12], 16)


def synthetic_daily_series(*, seed: str, days: int = 10) -> dict[str, Any]:
    """
    Deterministic small synthetic time series for offline tests and non-live runs.
    """

    base = _stable_int(seed) % 500 + 50
    start = date(2024, 1, 1) + timedelta(days=_stable_int(seed + ":start") % 300)
    points = []
    for i in range(days):
        value = float(base + ((i * 7 + _stable_int(seed + f":{i}") % 11) - 5))
        points.append({"date": (start + timedelta(days=i)).isoformat(), "value": value})
    return {"kind": "synthetic_daily_series", "seed": seed, "points": points}

