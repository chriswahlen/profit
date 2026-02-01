from __future__ import annotations

import re
from html.parser import HTMLParser
from typing import Dict, List


class Markdownifier(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts: list[str] = []
        self.table_mode = False
        self.table_rows_data: list[list[Dict[str, str]]] = []
        self.current_row: list[Dict[str, str]] | None = None
        self.current_cell: list[str] | None = None
        self.current_cell_attrs: Dict[str, str] | None = None
        self.script_mode = False

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        attrs_map = {k.lower(): v for k, v in attrs}
        if tag == "script":
            self.script_mode = True
            return
        if tag in {"b", "strong"}:
            self.parts.append("**")
        elif tag in {"i", "em", "u"}:
            self.parts.append("*")
        elif tag in {"br"}:
            self.parts.append("\n")
        elif tag in {"p", "div"}:
            self.parts.append("\n\n")
        elif tag in {"ul", "ol"}:
            self.parts.append("\n")
        elif tag == "li":
            self.parts.append("- ")
        elif tag == "table":
            self.table_mode = True
            self.table_rows_data = []
            self.current_row = None
        elif tag == "tr" and self.table_mode:
            self.current_row = []
        elif tag in {"td", "th"} and self.table_mode:
            self.current_cell = []
            self.current_cell_attrs = attrs_map

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag == "script":
            self.script_mode = False
            return
        if tag in {"b", "strong"}:
            self.parts.append("**")
        elif tag in {"i", "em", "u"}:
            self.parts.append("*")
        elif tag in {"p", "div"}:
            self.parts.append("\n\n")
        elif tag == "li":
            self.parts.append("\n")
        elif tag in {"td", "th"} and self.table_mode:
            if self.current_row is not None and self.current_cell is not None:
                text = "".join(self.current_cell).strip()
                cell = {
                    "text": text,
                    "colspan": int(self.current_cell_attrs.get("colspan", "1")) if self.current_cell_attrs else 1,
                    "rowspan": int(self.current_cell_attrs.get("rowspan", "1")) if self.current_cell_attrs else 1,
                }
                self.current_row.append(cell)
            self.current_cell = None
            self.current_cell_attrs = None
        elif tag == "tr" and self.table_mode:
            if self.current_row is not None:
                self.table_rows_data.append(self.current_row)
            self.current_row = None
        elif tag == "table" and self.table_mode:
            if self.table_rows_data:
                table_lines = self._render_table(self.table_rows_data)
                if table_lines:
                    self.parts.append("\n" + "\n".join(table_lines) + "\n")
            self.table_mode = False
            self.table_rows_data = []
            self.current_row = None
            self.current_cell = None
            self.current_cell_attrs = None

    def handle_data(self, data):
        if self.script_mode:
            return
        if self.table_mode and self.current_cell is not None:
            self.current_cell.append(data)
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
    return re.sub(r"\n{3,}", "\n\n", text)
