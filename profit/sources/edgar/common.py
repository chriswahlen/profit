from __future__ import annotations

import re

SEC_UA_ENV = "PROFIT_SEC_USER_AGENT"


def normalize_cik(raw: str | int) -> str:
    digits = re.sub(r"\D", "", str(raw))
    if not digits:
        raise ValueError("CIK must include at least one digit")
    if len(digits) > 10:
        digits = digits[-10:]
    return digits.zfill(10)


def normalize_accession(raw: str) -> str:
    digits = re.sub(r"[^0-9]", "", raw)
    if not digits:
        raise ValueError("accession must include digits")
    return digits


def strip_leading_zeros(val: str) -> str:
    stripped = val.lstrip("0")
    return stripped or "0"
