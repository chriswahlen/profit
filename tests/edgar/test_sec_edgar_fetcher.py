from __future__ import annotations

from datetime import datetime, timezone
import json
from profit.cache import FileCache
from profit.config import ProfitConfig
from profit.sources.edgar.sec_edgar import (
    EdgarSubmissionsFetcher,
    EdgarSubmissionsRequest,
    EdgarFiling,
)
from profit.utils.url_fetcher import FetchResponse
from profit.sources.types import LifecycleReader


class _AlwaysActiveLifecycle(LifecycleReader):
    def get_lifecycle(self, provider: str, provider_code: str):
        return datetime(1900, 1, 1, tzinfo=timezone.utc), None


class _NoopCatalogChecker:
    def ensure_fresh(self, provider: str):
        return

    def require_present(self, provider: str, provider_code: str):
        return


def _cfg(base):
    return ProfitConfig(
        data_root=base,
        cache_root=base,
        store_path=base / "col.sqlite3",
        log_level="INFO",
        refresh_catalog=False,
    )


def test_submissions_fetch_parses_recent_filings(tmp_path):
    requested_urls = []
    sent_headers = []

    def fake_fetch(url, *, timeout, headers):
        requested_urls.append(url)
        sent_headers.append(headers)
        payload = {
            "cik": "0000320193",
            "name": "Apple Inc.",
            "filings": {
                "recent": {
                    "accessionNumber": ["0000320193-24-000001"],
                    "form": ["10-K"],
                    "filingDate": ["2024-11-03"],
                    "reportDate": ["2024-09-28"],
                    "primaryDocument": ["a10-k2024.htm"],
                }
            },
        }
        return FetchResponse(status=200, body=json.dumps(payload).encode(), headers={})

    fetcher = EdgarSubmissionsFetcher(
        cfg=_cfg(tmp_path),
        cache=FileCache(base_dir=tmp_path),
        lifecycle=_AlwaysActiveLifecycle(),
        catalog_checker=_NoopCatalogChecker(),
        fetch_fn=fake_fetch,
        user_agent="test-agent/1.0",
    )

    req = EdgarSubmissionsRequest("320193")  # Apple
    result = fetcher.fetch(req)

    assert requested_urls == ["https://data.sec.gov/submissions/CIK0000320193.json"]
    assert sent_headers[0]["User-Agent"] == "test-agent/1.0"
    assert result.cik == "0000320193"
    assert result.entity_name == "Apple Inc."
    assert len(result.recent_filings) == 1
    filing: EdgarFiling = result.recent_filings[0]
    assert filing.accession_number == "0000320193-24-000001"
    assert filing.form == "10-K"
    assert filing.filing_date.isoformat() == "2024-11-03"
    assert filing.report_date.isoformat() == "2024-09-28"
    assert filing.primary_document == "a10-k2024.htm"


def test_submissions_fetch_follows_paged_files(tmp_path):
    requested_urls = []

    main_payload = {
        "cik": "0000000001",
        "name": "Test Co",
        "filings": {
            "recent": {
                "accessionNumber": ["0000000001-24-000003", "0000000001-24-000002"],
                "form": ["10-K", "10-Q"],
                "filingDate": ["2024-12-31", "2024-09-30"],
                "reportDate": ["2024-09-28", "2024-06-30"],
                "primaryDocument": ["k.htm", "q.htm"],
            },
            "files": [{"name": "CIK0000000001-001.json"}],
        },
    }

    page_payload = {
        "filings": {
            "recent": {
                "accessionNumber": ["0000000001-23-000001"],
                "form": ["10-K"],
                "filingDate": ["2023-12-31"],
                "reportDate": ["2023-09-28"],
                "primaryDocument": ["k2023.htm"],
            }
        }
    }

    def fake_fetch(url, *, timeout, headers):
        requested_urls.append(url)
        if url.endswith("CIK0000000001.json"):
            body = json.dumps(main_payload).encode()
        elif url.endswith("CIK0000000001-001.json"):
            body = json.dumps(page_payload).encode()
        else:
            raise AssertionError(f"unexpected url {url}")
        return FetchResponse(status=200, body=body, headers={})

    fetcher = EdgarSubmissionsFetcher(
        cfg=_cfg(tmp_path),
        cache=FileCache(base_dir=tmp_path),
        lifecycle=_AlwaysActiveLifecycle(),
        catalog_checker=_NoopCatalogChecker(),
        fetch_fn=fake_fetch,
        user_agent="test-agent/1.0",
    )

    req = EdgarSubmissionsRequest("1")
    result = fetcher.fetch(req)

    assert requested_urls == [
        "https://data.sec.gov/submissions/CIK0000000001.json",
        "https://data.sec.gov/submissions/CIK0000000001-001.json",
    ]
    assert len(result.recent_filings) == 3
    accessions = [f.accession_number for f in result.recent_filings]
    assert accessions == [
        "0000000001-24-000003",
        "0000000001-24-000002",
        "0000000001-23-000001",
    ]
