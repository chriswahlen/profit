from __future__ import annotations

import io
import zipfile

from profit.edgar.zip_utils import expand_zip_archive


def _zip_payload(entries: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in entries.items():
            zf.writestr(name, data)
    buf.seek(0)
    return buf.read()


def test_expand_zip_archive_skips_ignored(tmp_path):
    payload = _zip_payload(
        {
            "doc.xml": b"text",
            "image.png": b"png",
            "styles.css": b"css",
            "nested/script.js": b"js",
        }
    )

    extracted = expand_zip_archive("0000320193-26-000006", payload)

    assert "doc.xml" in extracted
    assert "image.png" not in extracted
    assert "styles.css" not in extracted
    assert "nested/script.js" not in extracted
