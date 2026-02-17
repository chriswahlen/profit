from __future__ import annotations

import re


class NameEquivalence:
    """Utilities for comparing different spellings/formatting of the same issuer name."""

    _FILLER_TOKENS = {
        "company",
        "co",
        "corp",
        "corporation",
        "inc",
        "ltd",
        "limited",
        "llc",
        "plc",
        "nv",
        "sa",
        "ag",
        "gmbh",
        "ab",
        "spac",
        "group",
        "adr",
        "hold",
        "holding",
        "holdings",
        "et",
        "expl",
        "p",
        "g",
        "cl",
        "a",
        "pg",
    }

    @classmethod
    def normalize(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = cls._split_camel_case(value)
        tokens = [tok for tok in re.split(r"[^\w]+", value.lower()) if tok]
        if not tokens:
            return ""
        tokens = cls._collapse_acronyms(tokens)
        tokens = [tok for tok in tokens if tok not in cls._FILLER_TOKENS]
        return "-".join(tokens)

    @classmethod
    def names_match(cls, left: str | None, right: str | None) -> bool:
        normalized_left = cls.normalize(left)
        normalized_right = cls.normalize(right)
        if normalized_left is None or normalized_right is None:
            return False
        if normalized_left == normalized_right:
            return True
        if normalized_left + "l" == normalized_right or normalized_right + "l" == normalized_left:
            return True
        return False

    @staticmethod
    def _collapse_acronyms(tokens: list[str]) -> list[str]:
        collapsed: list[str] = []
        buffer: list[str] = []
        for token in tokens:
            if len(token) == 1:
                buffer.append(token)
                continue
            if buffer:
                collapsed.append("".join(buffer))
                buffer.clear()
            collapsed.append(token)
        if buffer:
            collapsed.append("".join(buffer))
        return collapsed

    @staticmethod
    def _split_camel_case(value: str) -> str:
        return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", value)
