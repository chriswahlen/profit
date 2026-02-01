from __future__ import annotations

from profit.utils.html_to_md import html_to_markdown


def test_html_to_markdown_basic_formatting():
    html = "See <b>details</b> in <i>Appendix</i>."
    assert html_to_markdown(html) == "See **details** in *Appendix*."


def test_html_to_markdown_lists_and_paragraphs():
    html = "<p>Line1</p><p><ul><li>Item</li></ul></p>"
    md = html_to_markdown(html)
    assert "Line1" in md and "- Item" in md


def test_html_to_markdown_complex_structure():
    html = """
    <div>
      <p><strong>Summary</strong><br/>over multiple lines.</p>
      <p>Next paragraph with <em>italic</em> and <span>span</span>.</p>
      <ul>
        <li>First</li>
        <li>Second with <b>bold</b></li>
      </ul>
      <ol>
        <li>Ordered</li>
      </ol>
    </div>
    """
    md = html_to_markdown(html)
    assert "**Summary**" in md
    assert "*italic*" in md
    assert "- First" in md
    assert "- Second with **bold**" in md
    assert "- Ordered" in md or "1." in md
    assert "\n\n" in md


def test_html_to_markdown_handles_inline_lists():
    html = "<p>Values:<br><span><b>A</b></span><br><span><i>B</i></span></p>"
    md = html_to_markdown(html)
    assert "Values" in md
    assert "**A**" in md
    assert "*B*" in md


def test_html_to_markdown_uppercase_tags_and_extra_spaces():
    html = "<P>    Leading<p>Double<b>Bold</b></P>"
    md = html_to_markdown(html)
    assert "Leading" in md
    assert "**Bold**" in md


def test_html_to_markdown_table():
    html = "<table><tr><th>Col</th><th>Val</th></tr><tr><td>A</td><td>1</td></tr></table>"
    md = html_to_markdown(html)
    assert "| Col | Val |" in md
    assert "| --- | --- |" in md
    assert "| A | 1 |" in md


def test_html_to_markdown_table_with_spans():
    html = """
    <table>
      <tr><th>H1</th><th>H2</th><th>H3</th></tr>
      <tr><td rowspan="2">A</td><td>B</td><td>C</td></tr>
      <tr><td colspan="2">D</td></tr>
    </table>
    """
    md = html_to_markdown(html)
    lines = [line.strip() for line in md.splitlines() if line.strip()]
    assert "| H1 | H2 | H3 |" in lines[0]
    assert "| --- | --- | --- |" in lines[1]
    assert "| A | B | C |" in lines[2]
    assert "| D |  |  |" in lines[3] or "|  | D |  |" in lines[3]


def test_html_to_markdown_trims_redundant_newlines():
    html = "<p>Line1</p><br><br><p>Line2</p>"
    md = html_to_markdown(html)
    assert "Line1" in md and "Line2" in md
    assert "\n\n\n" not in md


def test_html_to_markdown_removes_script_blocks():
    html = "<p>Safe</p><script>console.log('x');</script><p>After</p>"
    md = html_to_markdown(html)
    assert "console.log" not in md
    assert "Safe" in md and "After" in md
