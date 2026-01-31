from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta
from typing import Mapping, Sequence

from html.parser import HTMLParser

from profit.cache import FileCache
from profit.utils.url_fetcher import FetchFn, fetch_url, PermanentFetchError
from .common import SEC_UA_ENV, normalize_cik, normalize_accession, strip_leading_zeros

EDGAR_ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"
EDGAR_ARCHIVES_HTML_BASE = "https://www.sec.gov/Archives/edgar/data"
DEFAULT_TTL = timedelta(days=1)


@dataclass(frozen=True)
class AccessionIndex:
    base_url: str
    files: Sequence[Mapping[str, object]]
    raw: Mapping[str, object]


class EdgarAccessionReader:
    """
    Helper for fetching EDGAR accession directories and documents.
    """

    def __init__(
        self,
        *,
        cache: FileCache,
        user_agent: str,
        ttl: timedelta = DEFAULT_TTL,
        allow_network: bool = True,
        fetch_fn: FetchFn | None = None,
    ) -> None:
        if not user_agent:
            raise ValueError(f"{SEC_UA_ENV} (user_agent) is required")
        self.cache = cache
        self.user_agent = user_agent
        self.ttl = ttl
        self.allow_network = allow_network
        self.fetch_fn = fetch_fn

    def base_url(self, cik: str | int, accession: str) -> str:
        cik_norm = normalize_cik(str(cik))
        cik_path = strip_leading_zeros(cik_norm)
        acc_norm = normalize_accession(accession)
        return f"{EDGAR_ARCHIVES_BASE}/{cik_path}/{acc_norm}/"

    def fetch_index(self, cik: str | int, accession: str) -> AccessionIndex:
        """
        Fetch ``index.json`` for the accession directory and return parsed data.
        """
        base = self.base_url(cik, accession)
        acc_norm = normalize_accession(accession)
        url = base + "index.json"
        try:
            resp = fetch_url(
                url,
                cache=self.cache,
                ttl=self.ttl,
                allow_network=self.allow_network,
                headers={"User-Agent": self.user_agent, "Accept": "application/json"},
                fetch_fn=self.fetch_fn,
            )
            data = json.loads(resp)
            files = data.get("directory", {}).get("item", []) if isinstance(data, Mapping) else []
            if not isinstance(files, list):
                files = []
            return AccessionIndex(base_url=base, files=files, raw=data)
        except PermanentFetchError as exc:
            if exc.status != 404:
                raise
            # Fallback: directory listing (HTML index) when JSON not present.
            try:
                print("FETCH %s" % base)
                listing_html = fetch_url(
                    base,
                    cache=self.cache,
                    ttl=self.ttl,
                    allow_network=self.allow_network,
                    headers={"User-Agent": self.user_agent, "Accept": "text/html"},
                    fetch_fn=self.fetch_fn,
                )
                files = _parse_directory_listing(listing_html.decode("utf-8", errors="ignore"))
                if files:
                    return AccessionIndex(base_url=base, files=[{"name": f} for f in files], raw={"directory": {"item": files}})
            except PermanentFetchError as exc_dir:
                if exc_dir.status != 404:
                    raise
            # Final fallback: explicit HTML index file
            html_index_url = base + f"{acc_norm}-index.htm"
            html = fetch_url(
                html_index_url,
                cache=self.cache,
                ttl=self.ttl,
                allow_network=self.allow_network,
                headers={"User-Agent": self.user_agent, "Accept": "text/html"},
                fetch_fn=self.fetch_fn,
            )
            files = _parse_directory_listing(html.decode("utf-8", errors="ignore"))
            return AccessionIndex(base_url=base, files=[{"name": f} for f in files], raw={"directory": {"item": files}})

    def fetch_file(self, cik: str | int, accession: str, filename: str) -> bytes:
        """
        Fetch a specific file within the accession directory.
        """
        url = self.base_url(cik, accession) + filename
        return fetch_url(
            url,
            cache=self.cache,
            ttl=self.ttl,
            allow_network=self.allow_network,
            headers={"User-Agent": self.user_agent},
            fetch_fn=self.fetch_fn,
        )


class _DirListingParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.files: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        href = None
        for k, v in attrs:
            if k.lower() == "href":
                href = v
                break
        if not href:
            return
        if href in ("../", "./"):
            return
        if href.endswith("/"):
            return  # skip subdirectories
        self.files.append(href)


def _parse_directory_listing(html: str) -> list[str]:
    parser = _DirListingParser()
    parser.feed(html)
    # Deduplicate while preserving order.
    seen = set()
    ordered = []
    for f in parser.files:
        if f not in seen:
            seen.add(f)
            ordered.append(f)
    return ordered
