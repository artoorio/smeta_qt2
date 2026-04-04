import json
import os
import shutil
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

import pandas as pd
from fastapi import (FastAPI, File, Form, HTTPException, Request,
                     UploadFile)
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.background import BackgroundTask

from data_processing import CATEGORY_PREFIXES, process_smeta
from db import FileRecord, SessionLocal, SmetaRow
from fact_export import export_with_fact_formula
from export_formatting import apply_readable_sheet_layout, dataframe_to_readable_html, dataframes_to_readable_html
from pydantic import BaseModel, Field
from smeta_compare import SmetaComparator


def _save_upload(upload: UploadFile, target_dir: str) -> str:
    path = os.path.join(target_dir, upload.filename)
    with open(path, "wb") as buffer:
        shutil.copyfileobj(upload.file, buffer)
    upload.file.close()
    return path


def _df_preview(df: pd.DataFrame, limit: int = 200) -> Dict[str, Any]:
    preview = df.head(limit).fillna("")
    visible_columns = [col for col in preview.columns if not str(col).startswith("__meta_")]
    return {
        "columns": visible_columns,
        "rows": jsonable_encoder(preview.to_dict(orient="records")),
        "row_count": len(df),
    }


def _prepare_process_frames(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    processed = df.copy()
    raw_processed = df.copy()

    if "Раздел" in processed.columns:
        raw_sections = processed["Раздел"].fillna("").astype(str).str.strip()
        title_sections = processed.get("Название раздела", raw_sections).fillna("").astype(str).str.strip()
        section_map: Dict[str, str] = {}
        for raw_value, title_value in zip(raw_sections, title_sections):
            if not raw_value:
                continue
            if raw_value.lower().startswith("раздел"):
                section_map.setdefault(raw_value, raw_value)
            if title_value:
                section_map.setdefault(title_value, f"{raw_value}. {title_value}" if raw_value != title_value else raw_value)
        processed["Раздел"] = processed["Раздел"].map(
            lambda value: section_map.get(str(value).strip(), value) if str(value or "").strip() else value
        )

    processed = processed.rename(columns={
        "Номер позиции": "№",
        "Единица измерения": "Ед.изм.",
    })

    preferred_order = [
        "Название объекта",
        "Раздел",
        "Название раздела",
        "Подраздел",
        "№",
        "Код расценки",
        "Наименование",
        "Ед.изм.",
        "Количество",
        "Стоимость",
        "Материалы",
        "ФОТ",
        "ЭМ",
        "НР",
        "СП",
        "ОТм",
        "Вспомогательные ресурсы",
        "Оборудование",
    ]
    ordered = [col for col in preferred_order if col in processed.columns]
    processed = processed[ordered]

    if "Название объекта" in df.columns:
        files = (
            df.groupby("Название объекта", dropna=False)
            .agg(
                Строк=("Наименование", "size"),
                **{"Общая стоимость": ("Стоимость", "sum")},
            )
            .reset_index()
            .rename(columns={"Название объекта": "Файл"})
        )
    else:
        files = pd.DataFrame([{"Файл": "Текущий набор", "Строк": len(df), "Общая стоимость": float(df.get("Стоимость", pd.Series([0])).sum())}])

    summary_source = _annotate_process2_hierarchy(raw_processed.copy())
    if "Название объекта" not in summary_source.columns:
        summary_source["Название объекта"] = "Текущий набор"
    if "Раздел" not in summary_source.columns:
        summary_source["Раздел"] = ""
    if "Подраздел" not in summary_source.columns:
        summary_source["Подраздел"] = ""
    if "Стоимость" not in summary_source.columns:
        summary_source["Стоимость"] = 0

    summary = (
        summary_source.groupby(
            ["Название объекта", "__meta_section_uid", "__meta_subsection_uid"],
            dropna=False,
            sort=False,
        )
        .agg({
            "Раздел": "first",
            "Название раздела": "first",
            "Подраздел": "first",
            "Стоимость": "sum",
            "__meta_section_focus_key": "first",
            "__meta_subsection_focus_key": "first",
        })
        .reset_index()
        .rename(columns={
            "Название объекта": "Файл",
            "Стоимость": "Ст-ть",
        })
    )

    totals = summary.groupby("Файл", dropna=False)["Ст-ть"].sum().to_dict()
    summary["Вес\nв смете, %"] = summary.apply(
        lambda row: (float(row["Ст-ть"]) / totals.get(row["Файл"], 0) * 100) if totals.get(row["Файл"], 0) else 0.0,
        axis=1,
    )
    summary["__meta_focus_key"] = summary["__meta_subsection_focus_key"].where(
        summary["__meta_subsection_focus_key"].astype(str).str.strip() != "",
        summary["__meta_section_focus_key"],
    )

    if "Раздел" in summary.columns:
        raw_sections = summary["Раздел"].fillna("").astype(str).str.strip()
        title_sections = raw_sections
        section_map: Dict[str, str] = {}
        if "Название раздела" in raw_processed.columns:
            title_by_raw = (
                raw_processed[["Раздел", "Название раздела"]]
                .fillna("")
                .astype(str)
                .apply(lambda col: col.str.strip())
                .drop_duplicates()
            )
            for _, pair in title_by_raw.iterrows():
                raw_value = pair["Раздел"]
                title_value = pair["Название раздела"]
                if not raw_value:
                    continue
                if title_value:
                    full_label = f"{raw_value}. {title_value}" if raw_value != title_value else raw_value
                    section_map.setdefault(raw_value, full_label)
                    section_map.setdefault(title_value, full_label)
                elif raw_value.lower().startswith("раздел"):
                    section_map.setdefault(raw_value, raw_value)
        summary["Раздел"] = summary["Раздел"].map(
            lambda value: section_map.get(str(value).strip(), value) if str(value or "").strip() else value
        )

    if "Раздел" in summary.columns:
        section_runs: List[Dict[str, Any]] = []
        current_label = ""
        current_uid = ""
        current_indices: List[int] = []
        section_uids = summary.get("__meta_section_uid", pd.Series([""] * len(summary))).fillna("").astype(str)
        for idx, value in section_uids.items():
            label = str(summary.at[idx, "Раздел"] or "").strip()
            if value != current_uid:
                if current_indices:
                    section_runs.append({"label": current_label, "uid": current_uid, "indices": current_indices[:]})
                current_label = label
                current_uid = value
                current_indices = [idx]
            else:
                current_indices.append(idx)
        if current_indices:
            section_runs.append({"label": current_label, "uid": current_uid, "indices": current_indices[:]})
        summary["__meta_row_type"] = ""
        summary["__meta_row_span"] = 1
        summary["__meta_hide_section"] = False
        for run in section_runs:
            if not run["indices"]:
                continue
            first_idx = run["indices"][0]
            summary.at[first_idx, "__meta_row_span"] = len(run["indices"])
            for idx in run["indices"][1:]:
                summary.at[idx, "__meta_hide_section"] = True

    summary = summary[[
        "Файл",
        "Раздел",
        "Подраздел",
        "Ст-ть",
        "Вес\nв смете, %",
        "__meta_row_span",
        "__meta_hide_section",
        "__meta_section_uid",
        "__meta_subsection_uid",
        "__meta_section_focus_key",
        "__meta_subsection_focus_key",
        "__meta_focus_key",
    ]]

    return {
        "detail": processed,
        "summary": summary,
        "files": files,
    }


def _empty_preview() -> Dict[str, Any]:
    return {"columns": [], "rows": [], "row_count": 0}


def _strip_hidden_columns(df: pd.DataFrame) -> pd.DataFrame:
    hidden = [col for col in df.columns if str(col).startswith("__meta_")]
    return df.drop(columns=hidden, errors="ignore")


def _annotate_process2_hierarchy(df: pd.DataFrame) -> pd.DataFrame:
    annotated = df.copy()
    annotated["__meta_section_uid"] = ""
    annotated["__meta_subsection_uid"] = ""
    annotated["__meta_section_focus_key"] = ""
    annotated["__meta_subsection_focus_key"] = ""
    annotated["__meta_focus_key"] = ""

    section_idx = 0
    subsection_idx = 0
    current_section = ""
    current_subsection = ""
    current_section_uid = ""
    current_subsection_uid = ""

    for idx, row in annotated.iterrows():
        section = str(row.get("Раздел", "") or "").strip()
        subsection = str(row.get("Подраздел", "") or "").strip()

        if section and section != current_section:
            section_idx += 1
            subsection_idx = 0
            current_section = section
            current_subsection = ""
            current_section_uid = f"section:{section_idx}"
            current_subsection_uid = ""

        if subsection and subsection != current_subsection and subsection != section:
            subsection_idx += 1
            current_subsection = subsection
            current_subsection_uid = f"subsection:{section_idx}:{subsection_idx}"

        annotated.at[idx, "__meta_section_uid"] = current_section_uid
        annotated.at[idx, "__meta_subsection_uid"] = current_subsection_uid
        annotated.at[idx, "__meta_section_focus_key"] = current_section_uid
        annotated.at[idx, "__meta_subsection_focus_key"] = current_subsection_uid or current_section_uid
        annotated.at[idx, "__meta_focus_key"] = current_subsection_uid or current_section_uid

    return annotated


def _build_process2_customer_frame(detail_df: pd.DataFrame) -> pd.DataFrame:
    source = _annotate_process2_hierarchy(detail_df)
    visible_columns = [
        col for col in source.columns
        if col not in {"Раздел", "Название раздела", "Подраздел"} and not str(col).startswith("__meta_")
    ]
    rows: List[Dict[str, Any]] = []
    current_section_uid = ""
    current_subsection_uid = ""

    def format_section_label(section_value: str, title_value: str) -> str:
        section_text = section_value.strip()
        title_text = title_value.strip()
        if section_text.lower().startswith("раздел") and title_text:
            return f"{section_text}. {title_text}"
        return title_text or section_text

    for _, row in source.iterrows():
        section = str(row.get("Раздел", "") or "").strip()
        subsection = str(row.get("Подраздел", "") or "").strip()
        section_title = str(row.get("Название раздела", "") or "").strip()
        section_key = str(row.get("__meta_section_focus_key", "") or "").strip()
        subsection_key = str(row.get("__meta_subsection_focus_key", "") or "").strip()
        section_uid = str(row.get("__meta_section_uid", "") or "").strip()
        subsection_uid = str(row.get("__meta_subsection_uid", "") or "").strip()

        if section_uid and section_uid != current_section_uid:
            current_section_uid = section_uid
            current_subsection_uid = ""
            divider_row = {col: "" for col in visible_columns}
            if "Наименование" in divider_row:
                divider_row["Наименование"] = format_section_label(section, section_title)
            divider_row["__meta_row_type"] = "divider"
            divider_row["__meta_section_focus_key"] = section_key
            divider_row["__meta_subsection_focus_key"] = ""
            divider_row["__meta_focus_key"] = section_key
            divider_row["__meta_section_label"] = format_section_label(section, section_title)
            divider_row["__meta_subsection_label"] = ""
            rows.append(divider_row)

        if subsection_uid and subsection_uid != current_subsection_uid:
            current_subsection_uid = subsection_uid
            subdivider_row = {col: "" for col in visible_columns}
            if "Наименование" in subdivider_row:
                subdivider_row["Наименование"] = subsection
            subdivider_row["__meta_row_type"] = "subdivider"
            subdivider_row["__meta_section_focus_key"] = section_key
            subdivider_row["__meta_subsection_focus_key"] = subsection_key
            subdivider_row["__meta_focus_key"] = subsection_key or section_key
            subdivider_row["__meta_section_label"] = format_section_label(section, section_title)
            subdivider_row["__meta_subsection_label"] = subsection
            rows.append(subdivider_row)

        position_row = {col: row.get(col, "") for col in visible_columns}
        position_row["__meta_row_type"] = ""
        position_row["__meta_section_focus_key"] = section_key
        position_row["__meta_subsection_focus_key"] = subsection_key
        position_row["__meta_focus_key"] = ""
        position_row["__meta_section_label"] = format_section_label(section, section_title)
        position_row["__meta_subsection_label"] = subsection
        rows.append(position_row)

    customer = pd.DataFrame(
        rows,
        columns=[
            *visible_columns,
            "__meta_row_type",
            "__meta_section_focus_key",
            "__meta_subsection_focus_key",
            "__meta_focus_key",
            "__meta_section_label",
            "__meta_subsection_label",
        ],
    )
    return customer.fillna("")


def _build_process2_payload(df: pd.DataFrame) -> tuple[str, Dict[str, Any]]:
    frames = _prepare_process_frames(df)
    report_id = uuid4().hex
    detail = _build_process2_customer_frame(frames["detail"].drop(columns=["Название объекта"], errors="ignore"))
    summary = _annotate_process2_hierarchy(frames["summary"].drop(columns=["Файл"], errors="ignore"))
    empty_df = pd.DataFrame()
    process2_registry[report_id] = {
        "frames": {
            "detail": detail,
            "summary": summary,
            "info": empty_df.copy(),
            "files": frames["files"],
            "unit_diff": empty_df.copy(),
        },
        "ts": datetime.utcnow(),
    }
    detail_preview = _df_preview(detail)
    payload = {
        "report_id": report_id,
        "detail": detail_preview,
        "summary": _df_preview(summary),
        "info": _empty_preview(),
        "files": _df_preview(frames["files"]),
        "unit_diff": _empty_preview(),
        "missing": [],
        "total_cost": float(df.get("Стоимость", pd.Series([0])).sum()),
        "row_count": detail_preview["row_count"],
    }
    return report_id, payload


def _prepare_compare_frames(cmp_obj: SmetaComparator) -> Dict[str, pd.DataFrame]:
    detail = cmp_obj.generate_customer_report()
    summary = cmp_obj.generate_subsection_summary()
    info = cmp_obj.generate_top_difference_report()
    unit_diff = cmp_obj.generate_unit_difference_report()

    short_names = {
        "Количество": "Кол-во",
        "Стоимость": "Ст-ть",
    }

    def build_section_display_map() -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        for source_df in (cmp_obj.df1, cmp_obj.df2):
            if "Раздел" not in source_df.columns:
                continue
            raw_sections = source_df.get("Раздел", pd.Series(dtype="object")).fillna("").astype(str).str.strip()
            title_sections = source_df.get("Название раздела", raw_sections).fillna("").astype(str).str.strip()
            for raw_value, title_value in zip(raw_sections, title_sections):
                if not raw_value:
                    continue
                if raw_value.lower().startswith("раздел"):
                    mapping.setdefault(raw_value, raw_value)
                if title_value:
                    mapping.setdefault(title_value, f"{raw_value}. {title_value}" if raw_value != title_value else raw_value)
        return mapping

    def apply_section_display(frame: pd.DataFrame, section_map: Dict[str, str]) -> pd.DataFrame:
        if "Раздел" not in frame.columns:
            return frame
        updated = frame.copy()
        updated["Раздел"] = updated["Раздел"].map(
            lambda value: section_map.get(str(value).strip(), value) if str(value or "").strip() else value
        )
        return updated

    def apply_customer_divider_display(frame: pd.DataFrame, section_map: Dict[str, str]) -> pd.DataFrame:
        compare_col = cmp_obj.compare_column
        if compare_col not in frame.columns:
            return frame
        updated = frame.copy()
        updated["__meta_row_type"] = ""
        updated["__meta_focus_key"] = ""
        updated["__meta_category"] = ""

        def map_divider(value: object) -> tuple[object, str]:
            text = str(value or "").strip()
            if not text.startswith("--"):
                return value, ""
            inner = text.strip("- ").strip()
            if text.startswith("----"):
                return inner, "subdivider"
            mapped = section_map.get(inner, inner)
            return mapped, "divider"

        mapped_values = updated[compare_col].map(map_divider)
        updated[compare_col] = mapped_values.map(lambda item: item[0])
        updated["__meta_row_type"] = mapped_values.map(lambda item: item[1])
        return updated

    def infer_category_from_code(value: object) -> str:
        code = str(value or "").strip()
        if not code:
            return ""
        normalized = code.lower().replace(" ", "")
        for category, prefixes in CATEGORY_PREFIXES.items():
            for prefix in prefixes:
                if normalized.startswith(prefix.lower().replace(" ", "")):
                    return category
        return ""

    def attach_hidden_category(frame: pd.DataFrame) -> pd.DataFrame:
        if "Код расценки" not in frame.columns:
            return frame
        updated = frame.copy()
        updated["__meta_category"] = updated["Код расценки"].map(infer_category_from_code)
        return updated

    def attach_focus_keys(detail_frame: pd.DataFrame, summary_frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        detail = detail_frame.copy()
        summary = summary_frame.copy()

        section_idx = 0
        subsection_idx = 0
        current_section = ""
        detail["__meta_focus_key"] = detail.get("__meta_focus_key", "")

        section_keys: list[tuple[str, str]] = []
        subsection_keys: list[tuple[str, str, str]] = []

        compare_col = cmp_obj.compare_column
        compare_values = detail.get(compare_col, pd.Series([""] * len(detail))).fillna("").astype(str)
        row_types = detail.get("__meta_row_type", pd.Series([""] * len(detail))).fillna("").astype(str)

        for idx, row_type in enumerate(row_types):
            label = compare_values.iloc[idx].strip()
            if row_type == "divider":
                section_idx += 1
                subsection_idx = 0
                current_section = label
                focus_key = f"section:{section_idx}"
                detail.at[detail.index[idx], "__meta_focus_key"] = focus_key
                section_keys.append((current_section, focus_key))
            elif row_type == "subdivider":
                subsection_idx += 1
                focus_key = f"subsection:{section_idx}:{subsection_idx}"
                detail.at[detail.index[idx], "__meta_focus_key"] = focus_key
                subsection_keys.append((current_section, label, focus_key))

        summary["__meta_focus_key"] = ""
        section_seen: dict[str, int] = {}
        subsection_seen: dict[tuple[str, str], int] = {}
        section_lookup: dict[tuple[str, int], str] = {}
        subsection_lookup: dict[tuple[str, str, int], str] = {}

        for section_label, focus_key in section_keys:
            order = section_seen.get(section_label, 0) + 1
            section_seen[section_label] = order
            section_lookup[(section_label, order)] = focus_key

        for section_label, subsection_label, focus_key in subsection_keys:
            key = (section_label, subsection_label)
            order = subsection_seen.get(key, 0) + 1
            subsection_seen[key] = order
            subsection_lookup[(section_label, subsection_label, order)] = focus_key

        summary_section_seen: dict[str, int] = {}
        summary_subsection_seen: dict[tuple[str, str], int] = {}
        for idx, row in summary.iterrows():
            section_label = str(row.get("Раздел", "") or "").strip()
            subsection_label = str(row.get("Подраздел", "") or "").strip()
            section_order = summary_section_seen.get(section_label, 0) + 1
            if section_label and (idx == 0 or str(summary.iloc[idx - 1].get("Раздел", "") or "").strip() != section_label):
                summary_section_seen[section_label] = section_order
            else:
                section_order = summary_section_seen.get(section_label, section_order)
            if subsection_label:
                key = (section_label, subsection_label)
                order = summary_subsection_seen.get(key, 0) + 1
                summary_subsection_seen[key] = order
                focus_key = subsection_lookup.get((section_label, subsection_label, order), "")
                if not focus_key:
                    focus_key = section_lookup.get((section_label, section_order), "")
                summary.at[idx, "__meta_focus_key"] = focus_key
            else:
                summary.at[idx, "__meta_focus_key"] = section_lookup.get((section_label, section_order), "")

        return detail, summary

    def shorten_metric(name: str) -> str:
        return short_names.get(name, name)

    def with_line_suffix(label: str, suffix: str) -> str:
        return f"{label}\n({suffix})"

    detail_renames: Dict[str, str] = {}
    summary_renames: Dict[str, str] = {}
    for value_name in cmp_obj.value_column:
        short_value = shorten_metric(value_name)
        detail_renames[f"{value_name} ({cmp_obj.file1_name})"] = with_line_suffix(short_value, "Проект")
        detail_renames[f"{value_name} ({cmp_obj.file2_name})"] = with_line_suffix(short_value, "Факт")
        detail_renames[f"Разница ({value_name})"] = f"Разница\n({short_value})"
    summary_renames[f"Стоимость ({cmp_obj.file1_name})"] = with_line_suffix("Ст-ть", "Проект")
    summary_renames[f"Стоимость ({cmp_obj.file2_name})"] = with_line_suffix("Ст-ть", "Факт")
    summary_renames["Разница (Стоимость)"] = "Разница\n(Ст-ть)"

    detail = detail.rename(columns=detail_renames)
    summary = summary.rename(columns=summary_renames)
    section_map = build_section_display_map()
    detail = apply_section_display(detail, section_map)
    detail = apply_customer_divider_display(detail, section_map)
    detail = attach_hidden_category(detail)
    summary = apply_section_display(summary, section_map)
    detail, summary = attach_focus_keys(detail, summary)
    detail = detail.rename(columns={"Единица измерения": "Ед.изм."})
    info = info.rename(columns={
        "Номер позиции": "№",
        "Единица измерения": "Ед.изм.",
        "Кол-во (Проект)": with_line_suffix("Кол-во", "Проект"),
        "Кол-во (Факт)": with_line_suffix("Кол-во", "Факт"),
        "Ст-ть (Проект)": with_line_suffix("Ст-ть", "Проект"),
        "Ст-ть (Факт)": with_line_suffix("Ст-ть", "Факт"),
        "Разница (Ст-ть)": "Разница\n(Ст-ть)",
        "Ед. изм. (Проект)": with_line_suffix("Ед.изм.", "Проект"),
        "Ед. изм. (Факт)": with_line_suffix("Ед.изм.", "Факт"),
    })
    info = apply_section_display(info, section_map)
    unit_diff = unit_diff.rename(columns={
        "Номер позиции": "№",
        "Единица измерения": "Ед.изм.",
        "Кол-во (Проект)": with_line_suffix("Кол-во", "Проект"),
        "Кол-во (Факт)": with_line_suffix("Кол-во", "Факт"),
        "Ст-ть (Проект)": with_line_suffix("Ст-ть", "Проект"),
        "Ст-ть (Факт)": with_line_suffix("Ст-ть", "Факт"),
        "Разница (Ст-ть)": "Разница\n(Ст-ть)",
        "Ед. изм. (Проект)": with_line_suffix("Ед.изм.", "Проект"),
        "Ед. изм. (Факт)": with_line_suffix("Ед.изм.", "Факт"),
    })
    unit_diff = apply_section_display(unit_diff, section_map)

    preferred_order = [
        "№",
        "Код расценки",
        "Наименование",
        "Ед.изм.",
        "Раздел",
        "Подраздел",
        "Кол-во\n(Проект)",
        "Кол-во\n(Факт)",
        "Разница\n(Кол-во)",
        "Ст-ть\n(Проект)",
        "Ст-ть\n(Факт)",
        "Разница\n(Ст-ть)",
        "Категория",
    ]
    ordered = [col for col in preferred_order if col in detail.columns]
    ordered.extend(col for col in detail.columns if col not in ordered)
    detail = detail[ordered]

    files = pd.DataFrame(
        [
            {
                "Роль": "Проект",
                "Файл": cmp_obj.file1_name,
                "Общая стоимость": float(pd.to_numeric(cmp_obj.df1.get("Стоимость"), errors="coerce").fillna(0).sum()),
            },
            {
                "Роль": "Факт",
                "Файл": cmp_obj.file2_name,
                "Общая стоимость": float(pd.to_numeric(cmp_obj.df2.get("Стоимость"), errors="coerce").fillna(0).sum()),
            },
        ]
    )

    return {
        "detail": detail,
        "summary": summary,
        "info": info,
        "files": files,
        "unit_diff": unit_diff,
    }


class ReportRegistry:
    """Keeps a short-lived cache of SmetaComparator instances."""

    def __init__(self, max_items: int = 20, ttl_seconds: int = 600):
        self._entries: Dict[str, Dict[str, Any]] = {}
        self._order: List[str] = []
        self._max_items = max_items
        self._ttl = timedelta(seconds=ttl_seconds)

    def add(self, cmp: SmetaComparator) -> str:
        key = uuid4().hex
        self._entries[key] = {"cmp": cmp, "ts": datetime.utcnow()}
        self._order.append(key)
        self._trim()
        return key

    def get(self, key: str) -> Optional[SmetaComparator]:
        entry = self._entries.get(key)
        if not entry:
            return None
        if datetime.utcnow() - entry["ts"] > self._ttl:
            self._entries.pop(key, None)
            self._order.remove(key)
            return None
        return entry["cmp"]

    def _trim(self) -> None:
        while len(self._order) > self._max_items:
            oldest = self._order.pop(0)
            self._entries.pop(oldest, None)


def _parse_list_field(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


app = FastAPI(title="Smeta Toolkit Web")
templates = Jinja2Templates(directory=Path(__file__).resolve().parent / "templates")
app.mount("/static", StaticFiles(directory=Path(__file__).resolve().parent / "static"), name="static")
registry = ReportRegistry()
process2_registry: Dict[str, Dict[str, Any]] = {}
process3_registry: Dict[str, Dict[str, Any]] = {}
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DEBUG_PROJECT = PROJECT_ROOT / "проект.xlsx"
DEFAULT_DEBUG_FACT = PROJECT_ROOT / "факт.xlsx"


def _get_default_compare_paths() -> tuple[str, str]:
    if not DEFAULT_DEBUG_PROJECT.exists() or not DEFAULT_DEBUG_FACT.exists():
        missing = []
        if not DEFAULT_DEBUG_PROJECT.exists():
            missing.append(str(DEFAULT_DEBUG_PROJECT.name))
        if not DEFAULT_DEBUG_FACT.exists():
            missing.append(str(DEFAULT_DEBUG_FACT.name))
        raise HTTPException(
            status_code=400,
            detail=f"Не найдены отладочные файлы: {', '.join(missing)}",
        )
    return str(DEFAULT_DEBUG_PROJECT), str(DEFAULT_DEBUG_FACT)


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "index.html", {"request": request})


@app.get("/process", response_class=HTMLResponse)
def process_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "process.html", {"request": request})


@app.get("/process2", response_class=HTMLResponse)
def process2_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "process2.html", {"request": request})


@app.get("/process3", response_class=HTMLResponse)
def process3_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "process3.html", {"request": request})


@app.get("/compare", response_class=HTMLResponse)
def compare_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "compare.html", {"request": request})


@app.get("/materials", response_class=HTMLResponse)
def materials_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "materials.html", {"request": request})


@app.get("/api/materials")
def materials_api() -> Dict[str, Any]:
    session = SessionLocal()
    try:
        rows = (
            session.query(SmetaRow.row_data, FileRecord.orig_name)
            .join(FileRecord, FileRecord.id == SmetaRow.file_id)
            .order_by(FileRecord.orig_name)
            .all()
        )
        result = []
        for raw, file_name in rows:
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            material_cost = payload.get("Материалы")
            if not material_cost and material_cost != 0:
                continue
            result.append({
                "file": file_name,
                "code": payload.get("Код расценки", ""),
                "name": payload.get("Наименование", ""),
                "unit": payload.get("Единица измерения", ""),
                "quantity": payload.get("Количество"),
                "cost": payload.get("Стоимость"),
                "materials": material_cost,
                "category": payload.get("Категория", ""),
            })
        columns = ["file", "code", "name", "unit", "quantity", "cost", "materials", "category"]
        return {"columns": columns, "rows": jsonable_encoder(result)}
    finally:
        session.close()


class MaterialPayload(BaseModel):
    file_name: str = Field(..., min_length=1)
    code: str = Field(..., min_length=1)
    name: str = Field(..., min_length=1)
    unit: str = Field(..., min_length=1)
    quantity: Optional[float] = None
    cost: Optional[float] = None
    materials: Optional[float] = None
    category: str = Field(default="Материалы")


@app.post("/api/materials/add")
def add_material(payload: MaterialPayload) -> Dict[str, Any]:
    session = SessionLocal()
    try:
        file = session.query(FileRecord).filter_by(orig_name=payload.file_name).first()
        if not file:
            file = FileRecord.from_path(payload.file_name, status="manual")
            session.add(file)
            session.flush()

        material_cost = payload.materials if payload.materials is not None else payload.cost
        row = {
            "Код расценки": payload.code,
            "Наименование": payload.name,
            "Единица измерения": payload.unit,
            "Количество": payload.quantity,
            "Стоимость": payload.cost,
            "Материалы": material_cost,
            "Категория": payload.category,
        }
        entry = SmetaRow(file_id=file.id, row_data=json.dumps(row, ensure_ascii=False))
        session.add(entry)
        session.commit()
        return {"id": entry.id, "materials": material_cost}
    finally:
        session.close()


@app.post("/api/process")
async def process_endpoint(
    files: List[UploadFile] = File(...),
    materials: UploadFile | None = File(None),
) -> Dict[str, Any]:
    if not files:
        raise HTTPException(status_code=400, detail="Необходимо отправить хотя бы один файл сметы.")

    with tempfile.TemporaryDirectory() as tmpdir:
        saved_paths = [_save_upload(upload, tmpdir) for upload in files]
        if materials:
            materials_path = _save_upload(materials, tmpdir)
        else:
            materials_path = None

        try:
            dfs = [process_smeta(path, materials_path) for path in saved_paths]
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))

    processed = pd.concat(dfs, ignore_index=True)
    frames = _prepare_process_frames(processed)
    detail_preview = _df_preview(frames["detail"])
    response = {
        "detail": detail_preview,
        "files": _df_preview(frames["files"]),
        "total_cost": float(processed.get("Стоимость", pd.Series([0])).sum()),
        "materials_applied": bool(materials_path),
        "row_count": detail_preview["row_count"],
    }
    return response


@app.post("/api/process2")
async def process2_endpoint(
    file: UploadFile | None = File(None),
    materials: UploadFile | None = File(None),
) -> Dict[str, Any]:
    with tempfile.TemporaryDirectory() as tmpdir:
        if file is None:
            path = str(DEFAULT_DEBUG_PROJECT)
        else:
            path = _save_upload(file, tmpdir)
        materials_path = _save_upload(materials, tmpdir) if materials else None
        try:
            df = process_smeta(path, materials_path)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))

    _, payload = _build_process2_payload(df)
    return payload


@app.post("/api/process2/export/{format}")
async def process2_export(
    format: str,
    report_id: str = Form(...),
) -> FileResponse:
    entry = process2_registry.get(report_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Отчёт Обработка2 не найден или устарел.")

    frames = entry["frames"]
    tmpdir = tempfile.mkdtemp()
    output_path = os.path.join(tmpdir, f"process2_{format}")
    visible_frames = {name: _strip_hidden_columns(df) for name, df in frames.items()}

    if format == "html":
        output_path += ".html"
        with open(output_path, "w", encoding="utf-8") as handle:
            handle.write(
                dataframes_to_readable_html(
                    [
                        ("Customer", visible_frames["detail"]),
                        ("Summary", visible_frames["summary"]),
                        ("Инфо", visible_frames["info"]),
                        ("Файлы", visible_frames["files"]),
                        ("Отличается единица измерения", visible_frames["unit_diff"]),
                    ],
                    title="Обработка2",
                    no_wrap_columns={"Файлы": {"Файл"}},
                )
            )
    elif format in {"excel", "diff"}:
        output_path += ".xlsx"
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            for sheet_name, df in [
                ("Customer", visible_frames["detail"]),
                ("Summary", visible_frames["summary"]),
                ("Инфо", visible_frames["info"]),
                ("Файлы", visible_frames["files"]),
                ("Отличается единица измерения", visible_frames["unit_diff"]),
            ]:
                df.to_excel(writer, index=False, sheet_name=sheet_name)
                ws = writer.sheets[sheet_name]
                if not df.empty:
                    apply_readable_sheet_layout(ws, df)
    elif format == "missing":
        output_path += ".txt"
        with open(output_path, "w", encoding="utf-8") as handle:
            handle.write("Отсутствующих позиций не обнаружено.")
    else:
        shutil.rmtree(tmpdir)
        raise HTTPException(status_code=400, detail="Неизвестный формат экспорта.")

    filename = os.path.basename(output_path)
    return FileResponse(
        output_path,
        filename=filename,
        background=BackgroundTask(lambda: shutil.rmtree(tmpdir, ignore_errors=True)),
    )


@app.post("/api/process3")
async def process3_endpoint(
    mode: str = Form("single"),
    file1: UploadFile | None = File(None),
    file2: UploadFile | None = File(None),
    materials: UploadFile | None = File(None),
) -> Dict[str, Any]:
    normalized_mode = "compare" if str(mode).strip().lower() == "compare" else "single"

    if normalized_mode == "single":
        with tempfile.TemporaryDirectory() as tmpdir:
            path = str(DEFAULT_DEBUG_PROJECT) if file1 is None else _save_upload(file1, tmpdir)
            materials_path = _save_upload(materials, tmpdir) if materials else None
            try:
                df = process_smeta(path, materials_path)
            except Exception as exc:
                raise HTTPException(status_code=500, detail=str(exc))

        report_id, payload = _build_process2_payload(df)
        process3_registry[report_id] = {
            "kind": "single",
            "frames": process2_registry[report_id]["frames"],
            "created": datetime.utcnow(),
        }
        payload["mode"] = "single"
        return payload

    if (file1 is None) != (file2 is None):
        raise HTTPException(status_code=400, detail="Либо загрузите оба файла, либо оставьте оба пустыми.")

    with tempfile.TemporaryDirectory() as tmpdir:
        proj = str(DEFAULT_DEBUG_PROJECT) if file1 is None else _save_upload(file1, tmpdir)
        fact = str(DEFAULT_DEBUG_FACT) if file2 is None else _save_upload(file2, tmpdir)
        try:
            cmp = SmetaComparator(
                process_smeta(proj),
                process_smeta(fact),
                file1_name=Path(proj).name,
                file2_name=Path(fact).name,
                compare_column="Наименование",
                value_column=["Количество", "Стоимость"],
                extra_column=["Единица измерения", "Код расценки"],
                subsection_column="Подраздел",
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))

    report_id = registry.add(cmp)
    frames = _prepare_compare_frames(cmp)
    process3_registry[report_id] = {
        "kind": "compare",
        "cmp": cmp,
        "frames": frames,
        "created": datetime.utcnow(),
    }
    return {
        "report_id": report_id,
        "mode": "compare",
        "detail": _df_preview(frames["detail"]),
        "summary": _df_preview(frames["summary"]),
        "info": _df_preview(frames["info"]),
        "files": _df_preview(frames["files"]),
        "unit_diff": _df_preview(frames["unit_diff"]),
        "missing": cmp.get_missing_positions(),
        "used_defaults": file1 is None and file2 is None,
        "total_cost": float(pd.to_numeric(cmp.df1.get("Стоимость"), errors="coerce").fillna(0).sum() + pd.to_numeric(cmp.df2.get("Стоимость"), errors="coerce").fillna(0).sum()),
        "row_count": _df_preview(frames["detail"])["row_count"],
    }


@app.post("/api/process3/export/{format}")
async def process3_export(
    format: str,
    report_id: str = Form(...),
) -> FileResponse:
    entry = process3_registry.get(report_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Отчёт Обработка3 не найден или устарел.")

    kind = entry["kind"]
    tmpdir = tempfile.mkdtemp()
    output_path = os.path.join(tmpdir, f"process3_{format}")

    if kind == "single":
        frames = entry.get("frames")
        if not frames:
            shutil.rmtree(tmpdir, ignore_errors=True)
            raise HTTPException(status_code=404, detail="Исходный отчёт Обработка3 не найден или устарел.")
        visible_frames = {name: _strip_hidden_columns(df) for name, df in frames.items()}
        if format == "html":
            output_path += ".html"
            with open(output_path, "w", encoding="utf-8") as handle:
                handle.write(
                    dataframes_to_readable_html(
                        [
                            ("Данные", visible_frames["detail"]),
                            ("Итоги", visible_frames["summary"]),
                            ("Файлы", visible_frames["files"]),
                        ],
                        title="Обработка3",
                        no_wrap_columns={"Файлы": {"Файл"}},
                    )
                )
        elif format == "excel":
            output_path += ".xlsx"
            with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                for sheet_name, df in [
                    ("Данные", visible_frames["detail"]),
                    ("Итоги", visible_frames["summary"]),
                    ("Файлы", visible_frames["files"]),
                ]:
                    df.to_excel(writer, index=False, sheet_name=sheet_name)
                    ws = writer.sheets[sheet_name]
                    if not df.empty:
                        apply_readable_sheet_layout(ws, df)
        elif format == "missing":
            output_path += ".txt"
            with open(output_path, "w", encoding="utf-8") as handle:
                handle.write("Отсутствующих позиций не обнаружено.")
        elif format == "diff":
            shutil.rmtree(tmpdir, ignore_errors=True)
            raise HTTPException(status_code=400, detail="Формат diff недоступен для режима одной сметы.")
        else:
            shutil.rmtree(tmpdir, ignore_errors=True)
            raise HTTPException(status_code=400, detail="Неизвестный формат экспорта.")
    else:
        cmp = entry["cmp"]
        frames = entry["frames"]
        if format == "html":
            output_path += ".html"
            with open(output_path, "w", encoding="utf-8") as handle:
                handle.write(
                    dataframes_to_readable_html(
                        [
                            ("Customer", frames["detail"]),
                            ("Summary", frames["summary"]),
                            ("Инфо", frames["info"]),
                            ("Файлы", frames["files"]),
                            ("Отличается единица измерения", frames["unit_diff"]),
                        ],
                        title="Сравнение смет",
                        no_wrap_columns={"Файлы": {"Файл"}},
                    )
                )
        elif format == "excel":
            output_path += ".xlsx"
            cmp.export_customer_excel(output_path)
        elif format == "missing":
            output_path += ".txt"
            cmp.export_positions_absent_in_d2(output_path)
        elif format == "diff":
            output_path += ".xlsx"
            cmp.export_added_removed_positions(output_path)
        else:
            shutil.rmtree(tmpdir, ignore_errors=True)
            raise HTTPException(status_code=400, detail="Неизвестный формат экспорта.")

    filename = os.path.basename(output_path)
    return FileResponse(
        output_path,
        filename=filename,
        background=BackgroundTask(lambda: shutil.rmtree(tmpdir, ignore_errors=True)),
    )


@app.post("/api/process/export")
async def process_export(
    files: List[UploadFile] = File(...),
    mode: str = Form("plain"),
    materials: UploadFile | None = File(None),
) -> FileResponse:
    if not files:
        raise HTTPException(status_code=400, detail="Нужно отправить файлы смет для экспорта.")

    tmpdir = tempfile.mkdtemp()
    paths = [_save_upload(upload, tmpdir) for upload in files]
    materials_path = _save_upload(materials, tmpdir) if materials else None
    df = pd.concat([process_smeta(path, materials_path) for path in paths], ignore_index=True)
    output_path = os.path.join(tmpdir, "processed.xlsx")
    if mode == "fact":
        export_with_fact_formula(df, output_path)
    elif mode == "html":
        output_path = os.path.join(tmpdir, "processed.html")
        with open(output_path, "w", encoding="utf-8") as handle:
            handle.write(dataframe_to_readable_html(df, title="Обработанная смета"))
    else:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Данные")
            apply_readable_sheet_layout(writer.sheets["Данные"], df)
    return FileResponse(
        output_path,
        filename=os.path.basename(output_path),
        background=BackgroundTask(shutil.rmtree, tmpdir),
    )


@app.post("/api/compare/columns")
async def compare_columns(files: List[UploadFile] = File(...)) -> Dict[str, Any]:
    if len(files) != 2:
        raise HTTPException(status_code=400, detail="Нужно ровно два файла для сравнения.")

    with tempfile.TemporaryDirectory() as tmpdir:
        p1 = _save_upload(files[0], tmpdir)
        p2 = _save_upload(files[1], tmpdir)
        try:
            df1 = process_smeta(p1)
            df2 = process_smeta(p2)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))

    columns = sorted(set(df1.columns) | set(df2.columns))
    return {"columns": columns}


@app.post("/api/compare")
async def compare_endpoint(
    files: List[UploadFile] = File(default=[]),
    compare_column: str = Form("Наименование"),
    value_column: str = Form("Стоимость"),
    extra_columns: str = Form(""),
    subsection_column: str = Form("Подраздел"),
    use_defaults: bool = Form(False),
) -> Dict[str, Any]:
    if use_defaults:
        proj, fact = _get_default_compare_paths()
        try:
            cmp = SmetaComparator(
                process_smeta(proj),
                process_smeta(fact),
                file1_name=Path(proj).name,
                file2_name=Path(fact).name,
                compare_column=compare_column,
                value_column=[v for v in _parse_list_field(value_column) or [value_column]],
                extra_column=_parse_list_field(extra_columns),
                subsection_column=subsection_column or None,
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))
    else:
        if len(files) != 2:
            raise HTTPException(status_code=400, detail="Нужно 2 файла: проект и факт.")

        with tempfile.TemporaryDirectory() as tmpdir:
            proj = _save_upload(files[0], tmpdir)
            fact = _save_upload(files[1], tmpdir)
            try:
                cmp = SmetaComparator(
                    process_smeta(proj),
                    process_smeta(fact),
                    file1_name=Path(proj).name,
                    file2_name=Path(fact).name,
                    compare_column=compare_column,
                    value_column=[v for v in _parse_list_field(value_column) or [value_column]],
                    extra_column=_parse_list_field(extra_columns),
                    subsection_column=subsection_column or None,
                )
            except Exception as exc:
                raise HTTPException(status_code=500, detail=str(exc))

    report_id = registry.add(cmp)
    frames = _prepare_compare_frames(cmp)
    response = {
        "report_id": report_id,
        "detail": _df_preview(frames["detail"]),
        "summary": _df_preview(frames["summary"]),
        "info": _df_preview(frames["info"]),
        "files": _df_preview(frames["files"]),
        "unit_diff": _df_preview(frames["unit_diff"]),
        "missing": cmp.get_missing_positions(),
        "used_defaults": use_defaults,
    }
    return response


@app.post("/api/compare/export/{format}")
async def compare_export(
    format: str,
    report_id: str = Form(...),
    value_column: Optional[str] = Form(None),
) -> FileResponse:
    cmp = registry.get(report_id)
    if not cmp:
        raise HTTPException(status_code=404, detail="Сравнение не найдено или устарело.")

    tmpdir = tempfile.mkdtemp()
    output_path = os.path.join(tmpdir, f"compare_{format}")
    if format == "html":
        output_path += ".html"
        frames = _prepare_compare_frames(cmp)
        with open(output_path, "w", encoding="utf-8") as handle:
            handle.write(
                dataframes_to_readable_html(
                    [
                        ("Customer", frames["detail"]),
                        ("Summary", frames["summary"]),
                        ("Инфо", frames["info"]),
                        ("Файлы", frames["files"]),
                        ("Отличается единица измерения", frames["unit_diff"]),
                    ],
                    title="Сравнение смет",
                    no_wrap_columns={"Файлы": {"Файл"}},
                )
            )
    elif format == "excel":
        output_path += ".xlsx"
        cmp.export_customer_excel(output_path)
    elif format == "missing":
        output_path += ".txt"
        cmp.export_positions_absent_in_d2(output_path)
    elif format == "diff":
        output_path += ".xlsx"
        cmp.export_added_removed_positions(output_path)
    else:
        shutil.rmtree(tmpdir)
        raise HTTPException(status_code=400, detail="Неизвестный формат экспорта.")
    filename = os.path.basename(output_path)
    return FileResponse(
        output_path,
        filename=filename,
        background=BackgroundTask(shutil.rmtree, tmpdir),
    )
