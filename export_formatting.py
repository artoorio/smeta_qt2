import html
import math
import textwrap
from typing import Iterable

import pandas as pd
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter


DEFAULT_ROW_HEIGHT = 18
NAME_MIN_WIDTH = 24
NAME_MAX_WIDTH = 80
NAME_TARGET_LINES = 4
HEADER_FILL = PatternFill("solid", fgColor="EAF1F8")
GRID_SIDE = Side(style="thin", color="C9D3E0")
GRID_BORDER = Border(left=GRID_SIDE, right=GRID_SIDE, top=GRID_SIDE, bottom=GRID_SIDE)


def _stringify(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value)


def _iter_lines(value: object) -> Iterable[str]:
    text = _stringify(value).replace("\r\n", "\n").replace("\r", "\n")
    parts = text.split("\n")
    return parts or [""]


def _max_word_length(text: str) -> int:
    words = [len(word) for word in text.split()]
    return max(words, default=0)


def _wrapped_line_count(value: object, width_chars: int) -> int:
    width_chars = max(1, int(width_chars))
    total = 0
    for line in _iter_lines(value):
        normalized = line.strip()
        if not normalized:
            total += 1
            continue
        wrapped = textwrap.wrap(
            normalized,
            width=width_chars,
            break_long_words=True,
            break_on_hyphens=False,
        )
        total += max(1, len(wrapped))
    return max(1, total)


def suggest_name_width(
    values: Iterable[object],
    target_lines: int = NAME_TARGET_LINES,
    min_width: int = NAME_MIN_WIDTH,
    max_width: int = NAME_MAX_WIDTH,
) -> int:
    max_len = 0
    max_word = 0
    for value in values:
        for line in _iter_lines(value):
            max_len = max(max_len, len(line))
            max_word = max(max_word, _max_word_length(line))

    base_width = math.ceil(max_len / max(1, target_lines)) + 2
    width = max(base_width, max_word + 1, min_width)
    return min(max_width, width)


def apply_readable_sheet_layout(
    ws,
    df: pd.DataFrame,
    name_column: str = "Наименование",
    target_lines: int = NAME_TARGET_LINES,
) -> None:
    headers = list(df.columns)
    if not headers:
        return

    name_index = headers.index(name_column) + 1 if name_column in headers else None
    name_width = (
        suggest_name_width(df[name_column].tolist(), target_lines=target_lines)
        if name_index
        else None
    )

    for col_index, header in enumerate(headers, start=1):
        column_letter = get_column_letter(col_index)
        if col_index == name_index and name_width is not None:
            ws.column_dimensions[column_letter].width = name_width
            continue

        values = [_stringify(header)] + [_stringify(value) for value in df[header].head(300)]
        max_len = max((len(line) for value in values for line in _iter_lines(value)), default=0)
        width = min(40, max(10, max_len + 2))
        ws.column_dimensions[column_letter].width = width

    for header_cell in ws[1]:
        header_cell.alignment = Alignment(wrapText=True, vertical="top")
        header_cell.font = Font(bold=True)
        header_cell.fill = HEADER_FILL
        header_cell.border = GRID_BORDER

    ws.freeze_panes = "A2"

    if not name_index or name_width is None:
        return

    for row_index in range(2, len(df) + 2):
        cell = ws.cell(row=row_index, column=name_index)
        cell.alignment = Alignment(wrapText=True, vertical="top")
        line_count = _wrapped_line_count(cell.value, name_width)
        ws.row_dimensions[row_index].height = max(DEFAULT_ROW_HEIGHT, line_count * 15)

    for row in ws.iter_rows(min_row=2, max_row=len(df) + 1, min_col=1, max_col=len(headers)):
        for cell in row:
            cell.border = GRID_BORDER


def apply_named_column_widths(ws, headers: list[str], width_map: dict[str, int | float]) -> None:
    for col_index, header in enumerate(headers, start=1):
        width = width_map.get(str(header))
        if width is not None:
            ws.column_dimensions[get_column_letter(col_index)].width = width


def apply_section_row_grouping(ws, df: pd.DataFrame, divider_column: str) -> None:
    if divider_column not in df.columns:
        return

    divider_values = df[divider_column].fillna("").astype(str).tolist()
    section_rows: list[int] = []
    for idx, value in enumerate(divider_values, start=2):
        text = value.strip()
        if text.startswith("-- ") and not text.startswith("---- "):
            section_rows.append(idx)

    if not section_rows:
        return

    ws.sheet_properties.outlinePr.summaryBelow = False

    for i, section_row in enumerate(section_rows):
        start_row = section_row + 1
        end_row = (section_rows[i + 1] - 1) if i + 1 < len(section_rows) else (len(df) + 1)
        if start_row <= end_row:
            ws.row_dimensions.group(start_row, end_row, outline_level=1, hidden=False)


def dataframe_to_readable_html(
    df: pd.DataFrame,
    title: str = "Обработанная смета",
    name_column: str = "Наименование",
    target_lines: int = NAME_TARGET_LINES,
) -> str:
    html_df = df.copy()
    for column in html_df.columns:
        html_df[column] = html_df[column].map(
            lambda value: html.escape(_stringify(value)).replace("\n", "<br>")
        )

    name_width = (
        suggest_name_width(df[name_column].tolist(), target_lines=target_lines)
        if name_column in df.columns
        else NAME_MIN_WIDTH
    )
    name_index = list(df.columns).index(name_column) + 1 if name_column in df.columns else None
    name_selector = (
        f"thead th:nth-child({name_index}), tbody td:nth-child({name_index})"
        if name_index
        else ""
    )
    name_css = (
        f"""
        {name_selector} {{
          width: {name_width}ch;
          min-width: {name_width}ch;
          max-width: {name_width}ch;
          white-space: normal;
          word-break: break-word;
        }}
        """
        if name_selector
        else ""
    )

    table_html = html_df.to_html(index=False, escape=False, border=0, classes="processed-table")
    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <style>
    body {{
      margin: 24px;
      font-family: "Segoe UI", Arial, sans-serif;
      color: #17202a;
      background: #ffffff;
    }}
    h1 {{
      margin: 0 0 16px;
      font-size: 24px;
    }}
    .processed-table {{
      width: 100%;
      border-collapse: collapse;
      table-layout: auto;
      font-size: 14px;
    }}
    .processed-table th,
    .processed-table td {{
      padding: 8px 10px;
      border: 1px solid #d5dbe3;
      text-align: left;
      vertical-align: top;
      white-space: nowrap;
    }}
    .processed-table th {{
      background: #eef3f8;
      position: sticky;
      top: 0;
    }}
    {name_css}
  </style>
</head>
<body>
  <h1>{html.escape(title)}</h1>
  {table_html}
</body>
</html>
"""
