"""Heuristics for distinguishing product labels from company names."""

from __future__ import annotations

import re

from tools.name_equivalence import NameEquivalence


class ProductLabelDetector:
    """Utility to detect structured-product/ticker-style names."""

    _KEYWORDS = {
        "CALL",
        "PUT",
        "TB",
        "LG",
        "B",
        "OP",
        "BONUS",
        "CASH",
        "BARRIER",
        "WRAP",
        "CW",
        "BON",
        "CORR",
        "USD",
        "EUR",
        "TR",
        "ETC",
        "ETF",
        "STW",
        "ADX",
        "LEG",
        "BOOST",
        "CDI",
        "ORD",
        "CLASS",
        "SWAP",
        "RETURN",
        "CDS",
        "COUPON",
        "FLR",
        "FIX",
        "FLOAT",
        "PERP",
        "RATE",
        "YIELD",
        "ETN",
    }
    _TOKEN_PATTERN = re.compile(r"[A-Z0-9]*\d[A-Z0-9]*")

    @classmethod
    def is_product_label(cls, value: str | None) -> bool:
        if not value:
            return False
        normalized = value.strip().upper()
        if not normalized:
            return False

        tokens = [tok for tok in re.split(r"[\s/\\\-]+", normalized) if tok]
        if not tokens:
            return False

        if any(keyword in tokens for keyword in cls._KEYWORDS):
            return True

        if len(tokens) <= 4 and all(cls._is_code_like(tok) for tok in tokens):
            return True

        if len(tokens) > 1 and normalized == normalized.upper():
            digits = sum(char.isdigit() for char in normalized)
            letters = sum(char.isalpha() for char in normalized)
            if digits >= letters:
                return True
            if digits > 0:
                return True

        return False

    @staticmethod
    def _is_code_like(token: str) -> bool:
        return bool(ProductLabelDetector._TOKEN_PATTERN.fullmatch(token))


class CompanyNameDetector:
    """Heuristic to identify actual company names."""

    _LEGAL_SUFFIXES = {
        "INC",
        "CORP",
        "COMPANY",
        "LLC",
        "LTD",
        "PLC",
        "AG",
        "AB",
        "SA",
        "SAB",
        "SE",
        "GMBH",
        "SPAC",
        "HOLDINGS",
        "GROUP",
        "TRUST",
        "REIT",
        "PARTNERS",
        "BANCORP",
        "CO",
        "SCA",
        "SL",
    }

    _STRUCTURED_PRODUCT_KEYWORDS = {
        "share",
        "shares",
        "ordinary",
        "registered",
        "depositary",
        "depository",
        "preferred",
        "perpetual",
        "floating",
        "fixed",
        "warrant",
        "warrants",
        "unit",
        "units",
        "bond",
        "bonds",
        "bds",
        "cds",
        "cdi",
        "lof",
        "alloc",
        "flx",
        "hd",
        "class",
        "preferred",
        "preference",
        "convertible",
        "call",
        "put",
        "bonus",
        "vor",
        "sta",
        "nam",
        "inhaber",
        "common",
        "stock",
        "regist",
        "registe",
        "register",
        "regis",
        "regi",
        "reg",
        "pref",
        "pclr",
        "pcl",
        "rights",
        "fund",
        "fondo",
        "fondos",
        "fundo",
        "waran",
        "actions",
        "action",
        "nom",
        "adr",
        "spons",
        "sponsor",
        "svcs",
        "svc",
        "hld",
        "hldg",
        "navne",
        "agnam",
    }
    _STRUCTURED_NON_COMPANY_TOKENS = {
        "real",
        "estate",
        "investment",
        "management",
        "development",
        "international",
        "global",
        "company",
        "portfolio",
        "capital",
        "trust",
        "fund",
        "realestate",
        "invest",
    }

    @classmethod
    def is_company_name(cls, value: str | None) -> bool:
        if not value:
            return False
        stripped = value.strip()
        if not stripped:
            return False

        if FundNameDetector.is_fund_name(stripped):
            return False

        upper_value = stripped.upper()
        has_lower = any(ch.islower() for ch in stripped)
        raw_tokens = [tok for tok in re.split(r"[^\w]+", stripped) if tok]
        tokens = [tok.upper() for tok in raw_tokens]
        has_suffix = bool(tokens and any(token in cls._LEGAL_SUFFIXES for token in tokens))
        lower_tokens = [tok.lower() for tok in raw_tokens]
        if tokens and tokens[-1] in {"R", "RE"} and any(token in cls._LEGAL_SUFFIXES for token in tokens):
            return False
        if tokens and tokens[-1] == "ETN":
            return False
        if upper_value.endswith(" O.N") or upper_value.endswith(" O.N."):
            return False
        if "PRE-ROLL" in upper_value or "PREROLL" in upper_value:
            return False
        if tokens and tokens[-1] in {"NA", "NAM"} and any(token in {"ASA", "AG"} for token in tokens):
            return False
        if cls._is_structured_non_company(lower_tokens):
            return False
        is_product_label = ProductLabelDetector.is_product_label(stripped)
        if cls._has_structured_product_indicators(
            upper_value, lower_tokens, is_product_label
        ):
            return False

        if has_lower:
            if is_product_label:
                return False
            return True
        if has_suffix and not ProductLabelDetector.is_product_label(stripped):
            return True
        if is_product_label:
            return False

        return False

    @classmethod
    def _has_structured_product_indicators(
        cls, upper_value: str, lower_tokens: list[str], is_product_label: bool
    ) -> bool:
        if is_product_label:
            return True
        if any(token in cls._STRUCTURED_PRODUCT_KEYWORDS for token in lower_tokens):
            return True
        if "C/W" in upper_value:
            return True
        if "HD-" in upper_value:
            return True
        return False

    @classmethod
    def _is_structured_non_company(cls, lower_tokens: list[str]) -> bool:
        if not lower_tokens:
            return False
        last = lower_tokens[-1]
        if last in {
            "company",
            "corporation",
            "corp",
            "inc",
            "llc",
            "ltd",
            "plc",
            "ag",
            "nv",
        }:
            return False
        matches = sum(token in cls._STRUCTURED_NON_COMPANY_TOKENS for token in lower_tokens)
        if matches >= 3:
            return True
        if last.startswith("reg") and len(lower_tokens) > 1:
            return True
        return False


class FundNameDetector:
    """Heuristics for detecting fund-style entity names."""

    _FUNDY_SUFFIXES = {
        "FUND",
        "ETF",
        "TRUST",
        "INDEX",
        "MUTUAL",
        "BOND",
        "FOND",
    }

    _FUND_MARKERS = ("FUND", "FUNDO", "FONDOS", "FONDO", "FOND", "RENTE")
    _QPSC_MARKER = "QPSC"

    @classmethod
    def is_fund_name(cls, value: str | None) -> bool:
        if not value:
            return False
        stripped = value.strip()
        if not stripped:
            return False

        upper_value = stripped.upper()
        tokens = [tok.upper() for tok in re.split(r"[\W_]+", stripped) if tok]
        if not tokens:
            return False

        last_token = tokens[-1]
        # ETNs and other structured products should stay classified as securities.
        if last_token == "ETN":
            return False
        if last_token in cls._FUNDY_SUFFIXES:
            return True

        normalized = NameEquivalence.normalize(stripped)
        if normalized and (normalized.startswith("fund") or normalized.endswith("qpsc")):
            return True

        if any(token == cls._QPSC_MARKER for token in tokens):
            return True

        if "COLL" in tokens and any(char.isdigit() for char in stripped):
            return True

        if ProductLabelDetector.is_product_label(stripped):
            return False

        if any(marker in upper_value for marker in cls._FUND_MARKERS):
            return True

        return False


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
        value = NameEquivalence._split_camel_case(value)
        tokens = [tok for tok in re.split(r"[^\w]+", value.lower()) if tok]
        if not tokens:
            return ""
        tokens = cls._collapse_acronyms(tokens)
        tokens = [tok for tok in tokens if tok not in cls._FILLER_TOKENS]
        return "-".join(tokens)

    @classmethod
    def names_match(cls, left: str | None, right: str | None) -> bool:
        if left is None or right is None:
            return False
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
    def _split_camel_case(value: str) -> str:
        return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", value)

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
