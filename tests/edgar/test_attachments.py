from pathlib import Path

from profit.edgar.attachments import ATTACHMENT_SUFFIXES, is_attachment_filename, save_attachment


def test_is_attachment_filename_matches_expected_suffixes():
    for suffix in ATTACHMENT_SUFFIXES:
        assert is_attachment_filename(f"foo{suffix}")
    assert not is_attachment_filename("foo.txt")
    assert not is_attachment_filename("")


def test_save_attachment_writes_when_payload_present(tmp_path):
    target = tmp_path / "attachments"
    payload = b"hello"
    out = save_attachment("a.pdf", payload, target)
    assert out is not None
    assert out.exists()
    assert out.read_bytes() == payload

    # Skip when payload missing or extension not allowed
    assert save_attachment("a.pdf", None, target) is None
    assert save_attachment("note.txt", payload, target) is None


def test_save_attachment_sanitizes_name(tmp_path):
    payload = b"content"
    out = save_attachment("dir/bad.xlsx", payload, tmp_path)
    assert out.name == "dir_bad.xlsx"
    assert out.read_bytes() == payload

