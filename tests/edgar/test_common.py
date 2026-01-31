from profit.sources.edgar.common import is_main_submission_text, should_skip_accession_file


def test_is_main_submission_text_matches_hyphenated():
    assert is_main_submission_text("0000320193-26-000006", "0000320193-26-000006.txt")


def test_is_main_submission_text_matches_digits():
    assert is_main_submission_text("0000320193-26-000006", "000032019326000006.txt")


def test_is_main_submission_text_not_otherfile():
    assert not is_main_submission_text("0000320193-26-000006", "a10-k2024.htm")


def test_should_skip_accession_file_for_css():
    assert should_skip_accession_file("0000320193-26-000006", "style.css")


def test_should_skip_accession_file_for_js():
    assert should_skip_accession_file("0000320193-26-000006", "bundle.js")


def test_should_not_skip_other_files():
    assert not should_skip_accession_file("0000320193-26-000006", "a10-k2024.htm")
