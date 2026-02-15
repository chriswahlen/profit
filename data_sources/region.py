from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Callable, Dict, List


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
    region_type: str  # canonical type (e.g., admin1, county, metro, neighborhood, national)
    name: str
    country_iso2: str = "us"
    state_code: Optional[str] = None
    city: Optional[str] = None
    alias_type: Optional[str] = None  # e.g., state/province/territory for admin1

    @property
    def canonical_id(self) -> str:
        key = self.region_type.lower()
        builders: Dict[str, Callable[["Region"], str]] = {
            "national": lambda r: f"region:national:{r.country_iso2.lower()}",
            "metro": _build_metro,
            "county": _build_county,
            "neighborhood": _build_neighborhood,
            "admin1": _build_admin1,
        }
        if key not in builders:
            raise ValueError(f"Unsupported region type: {self.region_type}")
        return builders[key](self)

    def alias_ids(self) -> List[str]:
        """Return alternate IDs for this region (e.g., state/province aliases for admin1)."""
        aliases: List[str] = []
        if self.region_type.lower() == "admin1" and self.alias_type:
            base = _slugify(self.name if self.name else self.state_code or "")
            aliases.append(f"region:{self.alias_type.lower()}:{self.country_iso2.lower()}:{base}")
        if self.region_type.lower() == "admin1" and not self.alias_type and self.state_code:
            # Fallback alias using state code for U.S.-like usage
            aliases.append(f"region:state:{self.country_iso2.lower()}:{_slugify(self.state_code)}")
        return aliases

    def parent(self) -> Optional["Region"]:
        """Return the immediate parent region if derivable; otherwise None."""
        key = self.region_type.lower()
        if key in {"neighborhood", "metro", "county"}:
            if not self.state_code:
                return None
            return Region.from_fields(
                region_type="admin1",
                region_name=self.state_code,
                country_iso2=self.country_iso2,
                state_code=self.state_code,
            )
        if key in {"admin1"}:
            return Region.national(country_iso2=self.country_iso2)
        return None

    # Factory helpers
    @classmethod
    def national(cls, *, country_iso2: str = "us", name: str = "National") -> "Region":
        if not name or not name.strip():
            raise ValueError("National requires name")
        return cls(region_type="national", name=name, country_iso2=country_iso2)

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
    def admin1(cls, *, code: str, country_iso2: str = "us", name: str | None = None, alias_type: Optional[str] = None) -> "Region":
        if not code or not code.strip():
            raise ValueError("Admin1 requires code")
        final_name = name.strip() if name and name.strip() else code.strip()
        return cls(region_type="admin1", name=final_name, country_iso2=country_iso2, state_code=code.strip(), alias_type=alias_type)

    @classmethod
    def state(cls, *, code: str, country_iso2: str = "us", name: str | None = None) -> "Region":
        return cls.admin1(code=code, country_iso2=country_iso2, name=name, alias_type="state")

    @classmethod
    def province(cls, *, code: str, country_iso2: str = "ca", name: str | None = None) -> "Region":
        return cls.admin1(code=code, country_iso2=country_iso2, name=name, alias_type="province")

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
        if key.startswith("region:"):
            key = key.split(":", 1)[1]
        if key == "national":
            return cls.national(country_iso2=country_iso2, name=region_name or "National")
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
        if key in {"state", "province", "territory", "admin1"}:
            if not state_code:
                raise ValueError(f"{region_type} requires state_code")
            alias = "state" if key == "state" else "province" if key == "province" else "territory" if key == "territory" else None
            return cls.admin1(code=state_code, country_iso2=country_iso2, name=region_name or state_code, alias_type=alias)
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


def _build_admin1(r: Region) -> str:
    # Use name slug if present; fallback to code slug.
    base = r.name if r.name else (r.state_code or "")
    return f"region:admin1:{r.country_iso2.lower()}:{_slugify(base)}"
