from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Callable, Dict


def _slugify(text: str) -> str:
    cleaned = []
    last_sep = False
    for ch in text.lower():
        if ch.isalnum():
            cleaned.append(ch)
            last_sep = False
        else:
            if not last_sep:
                cleaned.append("_")
                last_sep = True
    slug = "".join(cleaned).strip("_")
    return slug or "unknown"


def _strip_state_suffix(name: str, state_code: Optional[str]) -> str:
    if not state_code:
        return name.strip()
    state = state_code.strip()
    candidates = [f", {state}", f" {state}", f", {state.upper()}", f" {state.upper()}"]
    lowered = name.lower()
    for suf in candidates:
        if lowered.endswith(suf.lower()):
            return name[: -len(suf)].rstrip()
    return name.strip()


@dataclass(frozen=True)
class Region:
    region_type: str
    name: str
    country_iso2: str = "us"
    state_code: Optional[str] = None
    city: Optional[str] = None

    @property
    def canonical_id(self) -> str:
        key = self.region_type.lower()
        builders: Dict[str, Callable[["Region"], str]] = {
            "national": lambda r: f"region:national:{r.country_iso2.lower()}",
            "metro": _build_metro,
            "county": _build_county,
            "neighborhood": _build_neighborhood,
            "state": _build_state,
            "province": _build_state,
        }
        if key not in builders:
            raise ValueError(f"Unsupported region type: {self.region_type}")
        return builders[key](self)

    # Factory helpers
    @classmethod
    def national(cls, *, country_iso2: str = "us") -> "Region":
        return cls(region_type="national", name="National", country_iso2=country_iso2)

    @classmethod
    def metro(cls, *, name: str, state_code: str, country_iso2: str = "us") -> "Region":
        if not state_code or not state_code.strip():
            raise ValueError("Metro requires state_code")
        if not name or not name.strip():
            raise ValueError("Metro requires name")
        return cls(region_type="metro", name=name, country_iso2=country_iso2, state_code=state_code)

    @classmethod
    def county(cls, *, name: str, state_code: str, country_iso2: str = "us") -> "Region":
        if not state_code or not state_code.strip():
            raise ValueError("County requires state_code")
        if not name or not name.strip():
            raise ValueError("County requires name")
        return cls(region_type="county", name=name, country_iso2=country_iso2, state_code=state_code)

    @classmethod
    def neighborhood(cls, *, name: str, city: str, state_code: str, country_iso2: str = "us") -> "Region":
        if not state_code or not state_code.strip():
            raise ValueError("Neighborhood requires state_code")
        if not city or not city.strip():
            raise ValueError("Neighborhood requires city")
        if not name or not name.strip():
            raise ValueError("Neighborhood requires name")
        return cls(region_type="neighborhood", name=name, country_iso2=country_iso2, state_code=state_code, city=city)

    @classmethod
    def state(cls, *, code: str, country_iso2: str = "us") -> "Region":
        if not code or not code.strip():
            raise ValueError("State requires code")
        return cls(region_type="state", name=code.strip(), country_iso2=country_iso2, state_code=code.strip())

    @classmethod
    def province(cls, *, code: str, country_iso2: str = "ca") -> "Region":
        if not code or not code.strip():
            raise ValueError("Province requires code")
        return cls(region_type="province", name=code.strip(), country_iso2=country_iso2, state_code=code.strip())

    @classmethod
    def from_fields(
        cls,
        *,
        region_type: str,
        region_name: str,
        country_iso2: str = "us",
        state_code: Optional[str] = None,
        city: Optional[str] = None,
    ) -> "Region":
        key = region_type.lower()
        if key == "national":
            return cls.national(country_iso2=country_iso2)
        if key == "metro":
            if not state_code:
                raise ValueError("Metro requires state_code")
            return cls.metro(name=region_name, state_code=state_code, country_iso2=country_iso2)
        if key == "county":
            if not state_code:
                raise ValueError("County requires state_code")
            return cls.county(name=region_name, state_code=state_code, country_iso2=country_iso2)
        if key == "neighborhood":
            if not state_code or not city:
                raise ValueError("Neighborhood requires state_code and city")
            return cls.neighborhood(name=region_name, city=city, state_code=state_code, country_iso2=country_iso2)
        if key == "state":
            if not state_code:
                raise ValueError("State requires state_code")
            return cls.state(code=state_code, country_iso2=country_iso2)
        if key == "province":
            if not state_code:
                raise ValueError("Province requires state_code")
            return cls.province(code=state_code, country_iso2=country_iso2)
        raise ValueError(f"Unsupported region type: {region_type}")


# Internal builders ----------------------------------------------------------
def _build_metro(r: Region) -> str:
    slug = _slugify(_strip_state_suffix(r.name, r.state_code))
    return f"region:metro:{r.country_iso2.lower()}:{r.state_code.strip().lower()}:{slug}"


def _build_county(r: Region) -> str:
    slug = _slugify(_strip_state_suffix(r.name, r.state_code))
    return f"region:county:{r.country_iso2.lower()}:{r.state_code.strip().lower()}:{slug}"


def _build_neighborhood(r: Region) -> str:
    slug_city = _slugify(r.city or "")
    slug_name = _slugify(_strip_state_suffix(r.name, r.state_code))
    return f"region:neighborhood:{r.country_iso2.lower()}:{r.state_code.strip().lower()}:{slug_city}:{slug_name}"


def _build_state(r: Region) -> str:
    return f"region:{r.region_type.lower()}:{r.country_iso2.lower()}:{_slugify(r.state_code or '')}"
