from __future__ import annotations

from dataclasses import dataclass


def _slugify(text: str) -> str:
    cleaned = []
    last_sep = False
    for ch in text.lower():
        if ch.isalnum():
            cleaned.append(ch)
            last_sep = False
        else:
            if not last_sep:
                cleaned.append("-")
                last_sep = True
    slug = "".join(cleaned).strip("-")
    return slug or "unknown"


@dataclass(frozen=True)
class Currency:
    code: str  # ISO 4217

    @property
    def canonical_id(self) -> str:
        if not self.code or not self.code.strip():
            raise ValueError("Currency code is required")
        return f"ccy:{self.code.strip().lower()}"

    @classmethod
    def from_code(cls, code: str) -> "Currency":
        if not code or not code.strip():
            raise ValueError("Currency code is required")
        return cls(code=code)


@dataclass(frozen=True)
class Company:
    country_iso2: str
    name: str

    @property
    def canonical_id(self) -> str:
        if not self.country_iso2 or not self.country_iso2.strip():
            raise ValueError("Company country_iso2 is required")
        if not self.name or not self.name.strip():
            raise ValueError("Company name is required")
        slug = _slugify(self.name)
        return f"company:{self.country_iso2.strip().lower()}:{slug}"

    @classmethod
    def from_name(cls, name: str, country_iso2: str = "us") -> "Company":
        if not country_iso2 or not country_iso2.strip():
            raise ValueError("Company country_iso2 is required")
        if not name or not name.strip():
            raise ValueError("Company name is required")
        return cls(country_iso2=country_iso2, name=name)
