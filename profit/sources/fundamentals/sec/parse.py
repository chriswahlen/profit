from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Iterable, Mapping, Any

from profit.sources.fundamentals.models import FactRow


def _to_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _dims_hash(dims_key: str) -> str:
    if not dims_key:
        return ""
    return hashlib.sha256(dims_key.encode("utf-8")).hexdigest()[:16]


def parse_xbrl_json(
    *,
    data: Mapping[str, Any],
    instrument_id: str,
    cik: str,
    accession: str,
    form: str,
    filed_at: datetime,
    accepted_at: datetime | None,
    known_at: datetime,
    asof: datetime,
) -> Iterable[FactRow]:
    """
    Parse SEC XBRL JSON (as served under /Archives/edgar/data/.../*-xbrl.json) into FactRow objects.
    This is a minimal parser focusing on facts, units, periods, and explicit dimensions.
    """
    # JSON shape reference: facts[namespace][tag] -> list of fact objects
    facts = data.get("facts", {})

    # Build period lookup
    periods = data.get("report") or {}

    def _period_bounds(pid: str):
        p = periods.get(pid) or {}
        start = p.get("start")
        end = p.get("end")
        if end:
            end_dt = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
        else:
            end_dt = None
        if start:
            start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        else:
            start_dt = None
        return start_dt, end_dt

    for ns, tags in facts.items():
        for tag, items in tags.items():
            tag_qname = f"{ns}:{tag}"
            for item in items:
                # Skip if required fields missing
                val = item.get("val")
                unit = item.get("unit")
                if unit is None:
                    unit = item.get("uom")
                if val is None or unit is None:
                    continue

                # Period
                pid = item.get("period")
                start_dt: datetime | None
                end_dt: datetime | None
                start_dt, end_dt = _period_bounds(pid) if pid else (None, None)
                if end_dt is None:
                    # Instant facts should still have end_dt; if missing, skip.
                    continue

                # Dimensions (explicit only)
                dims = item.get("dims") or {}
                if dims:
                    pairs = []
                    for axis, member in sorted(dims.items()):
                        pairs.append({"axis": axis, "member": member})
                    dims_json = json.dumps(pairs, separators=(",", ":"), sort_keys=True)
                    dims_key = "|".join(f"{d['axis']}={d['member']}" for d in pairs)
                else:
                    dims_json = "[]"
                    dims_key = ""
                dims_hash = _dims_hash(dims_key)

                # Typed dimensions: stash in attrs but exclude from dims_key/hash
                typed_dims = item.get("typedDims") or {}
                attrs = {}
                if typed_dims:
                    attrs["typed_dims"] = typed_dims

                value_kind = "number"
                value_num = None
                value_text = None
                try:
                    value_num = float(val)
                except Exception:
                    value_kind = "text"
                    value_text = str(val)

                yield FactRow(
                    instrument_id=instrument_id,
                    provider="sec",
                    provider_code=cik,
                    accession=accession,
                    form=form,
                    filed_at=_to_utc(filed_at),
                    accepted_at=_to_utc(accepted_at),
                    known_at=_to_utc(known_at) or _to_utc(filed_at),
                    asof=_to_utc(asof) or datetime.now(timezone.utc),
                    tag_qname=tag_qname,
                    period_start=_to_utc(start_dt),
                    period_end=_to_utc(end_dt),
                    unit=str(unit),
                    currency=None,
                    dims_json=dims_json,
                    dims_key=dims_key,
                    dims_hash=dims_hash,
                    value_kind=value_kind,
                    value_num=value_num,
                    value_text=value_text,
                    statement=None,
                    line_item_code=None,
                    decimals=item.get("decimals"),
                    attrs=attrs or None,
                )
