from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
import math
from pathlib import Path
from typing import Iterable, Sequence


@dataclass(frozen=True)
class EdgarChunk:
    file: str
    text: str
    start_idx: int
    end_idx: int
    accession: str | None = None
    cik: str | None = None
    filing_type: str | None = None
    period_end: str | None = None
    score: float = 0.0


_ACCESSION_RE = re.compile(r"(?P<acc>\d{10}-\d{2}-\d{6})")
_CIK_RE = re.compile(r"(?P<cik>\d{10})")


def load_chunks(docs_path: Path, *, keywords: Sequence[str] | None = None, max_chars_per_chunk: int = 1200) -> list[EdgarChunk]:
    """
    Load markdown/HTML filings, split into coarse paragraphs, and attach basic metadata.
    Filters by keywords when provided.
    """
    keywords = [k.lower() for k in keywords or [] if k]
    raw: list[tuple[str, dict]] = []
    if not docs_path.exists():
        return []
    for path in sorted(docs_path.glob("*.md")) + sorted(docs_path.glob("*.htm*")):
        text = path.read_text(errors="ignore")
        meta = _metadata_from_path(path)
        for para in _split_paragraphs(text):
            cleaned = para.strip()
            if not cleaned:
                continue
            raw.append((cleaned, meta | {"file": path.name, "source_text": text}))

    # Compute BM25-lite scores
    df: dict[str, int] = {}
    if keywords:
        for text, _meta in raw:
            lower = text.lower()
            for kw in set(keywords):
                if kw in lower:
                    df[kw] = df.get(kw, 0) + 1
    chunks: list[EdgarChunk] = []
    N = len(raw) or 1
    for cleaned, meta in raw:
        lower = cleaned.lower()
        score = 0.0
        if keywords:
            for kw in keywords:
                tf = lower.count(kw)
                if tf == 0:
                    continue
                idf = math.log((N + 1) / (df.get(kw, 0) + 1)) + 1.0
                score += tf * idf
            if score <= 0:
                continue
        clipped = cleaned[:max_chars_per_chunk]
        start_idx = meta["source_text"].find(cleaned)
        end_idx = start_idx + len(clipped)
        chunks.append(
            EdgarChunk(
                file=meta["file"],
                text=clipped,
                start_idx=start_idx,
                end_idx=end_idx,
                accession=meta["accession"],
                cik=meta["cik"],
                filing_type=meta["filing_type"],
                period_end=meta["period_end"],
                score=score,
            )
        )
    chunks.sort(key=lambda c: c.score, reverse=True)
    return chunks


def _split_paragraphs(text: str) -> Iterable[str]:
    return re.split(r"\n\s*\n", text)


def _metadata_from_path(path: Path) -> dict[str, str | None]:
    name = path.name
    accession_match = _ACCESSION_RE.search(name)
    cik_match = _CIK_RE.search(name)
    return {
        "accession": accession_match.group("acc") if accession_match else None,
        "cik": cik_match.group("cik") if cik_match else None,
        "filing_type": _infer_filing_type(name),
        "period_end": None,
        "mtime": datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat(),
    }


def _infer_filing_type(name: str) -> str | None:
    upper = name.upper()
    for form in ("10-K", "10Q", "10-Q", "8-K", "20-F", "40-F"):
        if form.replace("-", "") in upper or form in upper:
            return form.replace("Q", "Q").replace("K", "K")
    return None


def _score(text: str, keywords: Sequence[str]) -> float:
    # Deprecated simple scorer retained for compatibility (not used).
    if not keywords:
        return 0.0
    lower = text.lower()
    return sum(lower.count(kw) for kw in keywords)
