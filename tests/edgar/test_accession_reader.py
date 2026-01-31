from __future__ import annotations

import json
from datetime import timedelta

from profit.cache import FileCache
from profit.sources.edgar.accession_reader import EdgarAccessionReader
from profit.utils.url_fetcher import FetchResponse, PermanentFetchError


def test_accession_index_fetch(tmp_path, monkeypatch):
    calls = []

    def fake_fetch(url, *, timeout, headers):
        calls.append((url, headers))
        payload = {
            "directory": {
                "item": [
                    {"name": "0000320193-24-000001-index.html"},
                    {"name": "a10-k2024.htm"},
                ]
            }
        }
        return FetchResponse(status=200, body=json.dumps(payload).encode(), headers={})

    cache = FileCache(base_dir=tmp_path)
    reader = EdgarAccessionReader(
        cache=cache,
        user_agent="test-agent",
        ttl=timedelta(days=1),
        fetch_fn=fake_fetch,
    )

    idx = reader.fetch_index("0000320193", "0000320193-24-000001")

    assert idx.base_url.endswith("/Archives/edgar/data/320193/000032019324000001/")
    assert len(idx.files) == 2
    assert idx.files[0]["name"] == "0000320193-24-000001-index.html"
    # Header wiring
    assert calls[0][1]["User-Agent"] == "test-agent"


def test_fetch_file_builds_url(tmp_path):
    urls = []

    def fake_fetch(url, *, timeout, headers):
        urls.append(url)
        return FetchResponse(status=200, body=b"OK", headers={})

    reader = EdgarAccessionReader(
        cache=FileCache(base_dir=tmp_path),
        user_agent="ua",
        fetch_fn=fake_fetch,
    )

    reader.fetch_file("320193", "0000320193-24-000001", "a10-k2024.htm")

    assert urls[0].endswith("/Archives/edgar/data/320193/000032019324000001/a10-k2024.htm")


def test_fetch_index_falls_back_to_directory_listing(tmp_path):
    calls = []

    def fake_fetch(url, *, timeout, headers):
        calls.append(url)
        if url.endswith("index.json"):
            return FetchResponse(status=404, body=b"missing", headers={})
        body = b"""
        <html><body>
        <a href="../">Parent Directory</a>
        <a href="aapl-20251227.htm">doc</a>
        <a href="0000320193-26-000006-index.htm">index</a>
        </body></html>
        """
        return FetchResponse(status=200, body=body, headers={})

    reader = EdgarAccessionReader(
        cache=FileCache(base_dir=tmp_path),
        user_agent="ua",
        fetch_fn=fake_fetch,
    )

    idx = reader.fetch_index("320193", "0000320193-26-000006")
    names = [f["name"] for f in idx.files]
    assert names == ["aapl-20251227.htm", "0000320193-26-000006-index.htm"]


def test_fetch_index_falls_back_to_html_index_when_dir_404(tmp_path):
    calls = []

    def fake_fetch(url, *, timeout, headers):
        calls.append(url)
        if url.endswith("index.json"):
            return FetchResponse(status=404, body=b"", headers={})
        if url.endswith("/"):
            # directory listing 404
            return FetchResponse(status=404, body=b"", headers={})
        # html index
        body = b"""
        <html><body><a href="aapl-20251227.htm">doc</a></body></html>
        """
        return FetchResponse(status=200, body=body, headers={})

    reader = EdgarAccessionReader(
        cache=FileCache(base_dir=tmp_path),
        user_agent="ua",
        fetch_fn=fake_fetch,
    )

    idx = reader.fetch_index("320193", "0000320193-26-000006")
    names = [f["name"] for f in idx.files]
    assert names == ["aapl-20251227.htm"]
