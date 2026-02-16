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


@dataclass(frozen=True)
class Exchange:
    mic: str
    name: str | None = None

    @property
    def canonical_id(self) -> str:
        if not self.mic or not self.mic.strip():
            raise ValueError("MIC is required for Exchange")
        return f"mic:{self.mic.strip().lower()}"

    @classmethod
    def from_mic(cls, mic: str, name: str | None = None) -> "Exchange":
        if not mic or not mic.strip():
            raise ValueError("MIC is required for Exchange")
        return cls(mic=mic, name=name or mic.upper())


@dataclass(frozen=True)
class Sector:
    name: str

    @property
    def canonical_id(self) -> str:
        if not self.name or not self.name.strip():
            raise ValueError("Sector name is required")
        slug = _slugify(self.name)
        return f"sector:financedatabase:{slug}"

    @classmethod
    def from_name(cls, name: str) -> "Sector":
        if not name or not name.strip():
            raise ValueError("Sector name is required")
        return cls(name=name)


@dataclass(frozen=True)
class Industry:
    name: str

    @property
    def canonical_id(self) -> str:
        if not self.name or not self.name.strip():
            raise ValueError("Industry name is required")
        slug = _slugify(self.name)
        return f"industry:financedatabase:{slug}"

    @classmethod
    def from_name(cls, name: str) -> "Industry":
        if not name or not name.strip():
            raise ValueError("Industry name is required")
        return cls(name=name)
