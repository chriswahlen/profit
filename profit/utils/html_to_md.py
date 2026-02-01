from __future__ import annotations

from dataclasses import dataclass
import re
from html.parser import HTMLParser
from typing import Dict, List


@dataclass
class TableContext:
    rows: list[list[Dict[str, str]]] = None
    current_row: list[Dict[str, str]] | None = None
    current_cell: list[str] | None = None
    current_cell_attrs: Dict[str, str] | None = None

    def __post_init__(self):
        if self.rows is None:
            self.rows = []


@dataclass
class MarkerContext:
    marker: str
    saw_text: bool = False


class Markdownifier(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts: list[str] = []
        self.table_stack: list[TableContext] = []
        self.marker_stack: list[MarkerContext] = []
        self.script_mode = False

    def _push_marker(self, marker: str):
        self.parts.append(marker)
        self.marker_stack.append(MarkerContext(marker=marker))

    def _close_marker(self, marker: str):
        if not self.marker_stack or self.marker_stack[-1].marker != marker:
            self.parts.append(marker)
            return
        ctx = self.marker_stack.pop()
        if not ctx.saw_text:
            if self.parts and self.parts[-1] == marker:
                self.parts.pop()
            return
        self.parts.append(marker)

    def _mark_text_seen(self):
        if not self.marker_stack:
            return
        for ctx in self.marker_stack:
            ctx.saw_text = True

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        attrs_map = {k.lower(): v for k, v in attrs}
        if tag == "script":
            self.script_mode = True
            return
        if tag in {"b", "strong"}:
            self._push_marker("**")
        elif tag in {"i", "em", "u"}:
            self._push_marker("*")
        elif tag == "span":
            self.parts.append(" ")
        elif tag in {"br"}:
            self.parts.append("\n")
        elif tag in {"p", "div"}:
            self.parts.append("\n\n")
        elif tag in {"ul", "ol"}:
            self.parts.append("\n")
        elif tag == "li":
            self.parts.append("- ")
        elif tag == "table":
            self.table_stack.append(TableContext())
        elif tag == "tr" and self.table_stack:
            self.table_stack[-1].current_row = []
        elif tag in {"td", "th"} and self.table_stack:
            ctx = self.table_stack[-1]
            ctx.current_cell = []
            ctx.current_cell_attrs = attrs_map

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag == "script":
            self.script_mode = False
            return
        if tag in {"b", "strong"}:
            self._close_marker("**")
        elif tag in {"i", "em", "u"}:
            self._close_marker("*")
        elif tag in {"p", "div"}:
            self.parts.append("\n\n")
        elif tag == "li":
            self.parts.append("\n")
        elif tag == "span":
            self.parts.append(" ")
        elif tag in {"td", "th"} and self.table_stack:
            ctx = self.table_stack[-1]
            if ctx.current_row is not None and ctx.current_cell is not None:
                text = "".join(ctx.current_cell).strip()
                cell = {
                    "text": text,
                    "colspan": int(ctx.current_cell_attrs.get("colspan", "1")) if ctx.current_cell_attrs else 1,
                    "rowspan": int(ctx.current_cell_attrs.get("rowspan", "1")) if ctx.current_cell_attrs else 1,
                }
                ctx.current_row.append(cell)
            ctx.current_cell = None
            ctx.current_cell_attrs = None
        elif tag == "tr" and self.table_stack:
            ctx = self.table_stack[-1]
            if ctx.current_row is not None:
                ctx.rows.append(ctx.current_row)
            ctx.current_row = None
        elif tag == "table" and self.table_stack:
            ctx = self.table_stack.pop()
            if ctx.rows:
                table_lines = self._render_table(ctx.rows)
                if table_lines:
                    self.parts.append("\n" + "\n".join(table_lines) + "\n")

    def handle_data(self, data):
        if self.script_mode:
            return
        if data.strip():
            self._mark_text_seen()
        if self.table_stack and self.table_stack[-1].current_cell is not None:
            self.table_stack[-1].current_cell.append(data)
        else:
            self.parts.append(data)

    def markdown(self) -> str:
        return "".join(self.parts).strip()

    def _render_table(self, rows: list[list[Dict[str, str]]]) -> list[str]:
        grid: list[list[str]] = []
        for r, row in enumerate(rows):
            while len(grid) <= r:
                grid.append([])
            col = 0
            for cell in row:
                while col < len(grid[r]) and grid[r][col] != "":
                    col += 1
                colspan = cell.get("colspan", 1)
                rowspan = cell.get("rowspan", 1)
                text = cell.get("text", "")
                for rr in range(r, r + rowspan):
                    while len(grid) <= rr:
                        grid.append([])
                    for cc in range(col, col + colspan):
                        while len(grid[rr]) <= cc:
                            grid[rr].append("")
                        grid[rr][cc] = text if (rr == r and cc == col) else ""
                col += colspan
        if not grid:
            return []
        header = grid[0] if grid else []
        lines = []
        if header:
            header_line = "| " + " | ".join(header) + " |"
            divider = "| " + " | ".join("---" for _ in header) + " |"
            lines.append(header_line)
            lines.append(divider)
        for row in grid[1:]:
            # Pad rows to header width.
            padded = row + [""] * (len(header) - len(row))
            lines.append("| " + " | ".join(padded) + " |")
        return lines


def html_to_markdown(html: str) -> str:
    parser = Markdownifier()
    parser.feed(html)
    text = parser.markdown()
    cleaned = re.sub(r"[ \t]+\n", "\n", text)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    lines = []
    for line in cleaned.strip().splitlines():
        stripped = line.strip()
        if stripped and set(stripped) == {"*"}:
            continue
        lines.append(line.rstrip())
    return "\n".join(lines).strip()
