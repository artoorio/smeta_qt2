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

from data_processing import process_smeta
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
    return {
        "columns": list(df.columns),
        "rows": jsonable_encoder(preview.to_dict(orient="records")),
        "row_count": len(df),
    }


def _prepare_compare_frames(cmp_obj: SmetaComparator) -> Dict[str, pd.DataFrame]:
    detail = cmp_obj.generate_customer_report()
    summary = cmp_obj.generate_subsection_summary()
    info = cmp_obj.generate_top_difference_report()
    unit_diff = cmp_obj.generate_unit_difference_report()

    short_names = {
        "Количество": "Кол-во",
        "Стоимость": "Ст-ть",
    }

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


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "index.html", {"request": request})


@app.get("/process", response_class=HTMLResponse)
def process_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "process.html", {"request": request})


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
    response = _df_preview(processed)
    response["total_cost"] = float(processed.get("Стоимость", pd.Series([0])).sum())
    response["materials_applied"] = bool(materials_path)
    return response


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
    files: List[UploadFile] = File(...),
    compare_column: str = Form("Наименование"),
    value_column: str = Form("Стоимость"),
    extra_columns: str = Form(""),
    subsection_column: str = Form("Подраздел"),
) -> Dict[str, Any]:
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
