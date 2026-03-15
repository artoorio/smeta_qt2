# Smeta Toolkit Web Architecture

## Goals
1. Expose the existing `process_smeta`, `SmetaComparator`, and export helpers over HTTP so the same parsing and comparison rules power the web UI.
2. Provide interactive controls for: processing arbitrary smeta files (with optional material prices) and comparing two smeta files (column selection, diffs, missing positions, and exports).
3. Keep the experience lightweight by reusing the proven pandas/OpenPyXL logic and keeping state in memory per session.

## Backend (FastAPI)
- Serve a single-page frontend (`templates/index.html`) plus static JS/CSS from `web_app/static`.
- `/api/process` accepts multiple Excel uploads plus an optional materials upload, runs `process_smeta`, and returns JSON with the processed rows, column list, and summary stats.
- `/api/process/export` reuses the same uploads to stream either a plain Excel file or a formula-driven export (via `fact_export.export_with_fact_formula`).
- `/api/compare` accepts two uploads plus column metadata (`compare_column`, `value_column`, optional `extra_columns` and `subsection_column`), builds an `SmetaComparator`, caches it behind a `report_id`, and returns the detail table, subsection summary, missing positions, and the generated `report_id`.
- `/api/compare/export/{format}` streams HTML/Excel/missing/diff reports by reusing the cached comparator from `report_id`. Cached entries are short-lived to avoid long-term memory growth.

## Frontend (Vanilla JS + CSS)
- Sectioned layout with “Process” and “Compare” cards styled with gradients and card shadows for clarity.
- Drag-and-drop or browse for files; keep references to the chosen `File` objects so we can re-send them for exports without forcing users to re-upload.
- After processing, render the returned rows into a responsive table and show summary numbers (total cost, number of rows).
- For comparison, allow users to type in column names (defaulting to the historical defaults) and submit; display both the detailed report and the subsection summary tables plus the missing positions list.
- Export buttons call `${prefix}/api/.../export` with the stored `report_id` so users can download HTML/Excel/TXT/diff files generated from the same comparator.

## Persistence & Cleanup
- Uploaded files are saved to `tempfile.TemporaryDirectory`; each API request writes the uploads to disk before invoking the existing parser/comparator logic.
- In-memory caches (like the comparator registry) are capped in size and timestamped; `report_id`s expire after a few minutes.

This scaffolding keeps the heavy lifting inside the existing modules, while the FastAPI glue and the lightweight UI reflect the original workflow in a browser.
