from __future__ import annotations

"""
Lightweight helper around pandas_market_calendars with a safe fallback.

Use this to adjust query end dates to valid trading sessions.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

try:
    import pandas_market_calendars as pmc  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    pmc = None

logger = logging.getLogger(__name__)


def _weekday_only(end: datetime) -> datetime:
    while end.weekday() >= 5:  # 5=Sat,6=Sun
        end = end - timedelta(days=1)
    return end


def adjust_end_to_last_session(end: datetime, calendar_name: str = "NYSE") -> datetime:
    """
    Return the last trading day on or before `end` for the given calendar.
    Falls back to weekday-only if pandas_market_calendars is unavailable.
    """
    end = end.astimezone(timezone.utc)
    if pmc is None:
        return _weekday_only(end)

    try:
        cal = pmc.get_calendar(calendar_name)
        start_date = (end - timedelta(days=14)).date()
        valid = cal.valid_days(start_date=start_date, end_date=end.date())
        if valid.empty:
            return _weekday_only(end)
        last_day = valid[-1].to_pydatetime().astimezone(timezone.utc)
        return datetime.combine(last_day.date(), datetime.min.time(), tzinfo=timezone.utc)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("market calendar fallback (%s): %s", calendar_name, exc)
        return _weekday_only(end)

