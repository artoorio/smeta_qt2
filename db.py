from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, Float, Boolean, ForeignKey, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import pandas as pd, json, pathlib

engine = create_engine("sqlite:///smeta.db")
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class FileRecord(Base):
    __tablename__ = "files"
    id = Column(Integer, primary_key=True)
    orig_name = Column(String)
    saved_path = Column(String)
    processed_path = Column(String)
    status = Column(String, default="uploaded")
    comment = Column(Text)
    timestamp = Column(DateTime, default=datetime.utcnow)

    @classmethod
    def from_path(cls, path, status="uploaded", comment=None):
        return cls(orig_name=pathlib.Path(path).name,
                   saved_path=path, status=status, comment=comment)

class SmetaRow(Base):
    __tablename__ = "items"
    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, index=True, nullable=False)
    row_order = Column(Integer, default=0, index=True)
    section = Column(String, default="", index=True)
    subsection = Column(String, default="", index=True)
    position_number = Column(String, default="", index=True)
    code = Column(String, default="", index=True)
    name = Column(String, default="", index=True)
    category = Column(String, default="", index=True)
    unit = Column(String, default="", index=True)
    quantity = Column(Float, default=0.0)
    cost = Column(Float, default=0.0)
    parent_work_id = Column(Integer, default=None, index=True)
    row_data = Column(Text)


class MaterialCatalog(Base):
    __tablename__ = "material_catalog"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, index=True)
    unit = Column(String, nullable=False, index=True)
    cost = Column(Float, nullable=False, default=0.0)
    supplier = Column(String, default="", index=True)
    region = Column(String, default="", index=True)
    source_name = Column(String, default="web", index=True)
    notes = Column(Text, default="")
    date_added = Column(DateTime, default=datetime.utcnow, nullable=False)


class MaterialCodeLink(Base):
    __tablename__ = "material_code_links"
    id = Column(Integer, primary_key=True)
    material_id = Column(Integer, ForeignKey("material_catalog.id"), index=True, nullable=False)
    code = Column(String, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class MaterialAlias(Base):
    __tablename__ = "material_aliases"
    id = Column(Integer, primary_key=True)
    material_id = Column(Integer, ForeignKey("material_catalog.id"), index=True, nullable=False)
    alias = Column(String, nullable=False, index=True)
    alias_type = Column(String, default="name", index=True)
    confidence = Column(Float, default=0.0)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class MaterialUnitRule(Base):
    __tablename__ = "material_unit_rules"
    id = Column(Integer, primary_key=True)
    material_id = Column(Integer, ForeignKey("material_catalog.id"), index=True, nullable=False)
    source_unit = Column(String, nullable=False, index=True)
    target_unit = Column(String, nullable=False, index=True)
    coefficient = Column(Float, nullable=False, default=1.0)
    active = Column(Boolean, default=True, nullable=False)
    note = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class MaterialBinding(Base):
    __tablename__ = "material_bindings"
    id = Column(Integer, primary_key=True)
    material_id = Column(Integer, ForeignKey("material_catalog.id"), index=True, nullable=False)
    smeta_file_name = Column(String, default="", index=True)
    smeta_name = Column(String, nullable=False, index=True)
    smeta_unit = Column(String, default="", index=True)
    smeta_code = Column(String, default="", index=True)
    smeta_signature = Column(String, default="", index=True)
    coefficient = Column(Float, nullable=False, default=1.0)
    match_score = Column(Float, nullable=False, default=0.0)
    source_name = Column(String, default="", index=True)
    status = Column(String, default="confirmed", index=True)
    note = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class SmetaMaterialLink(Base):
    __tablename__ = "smeta_material_links"
    id = Column(Integer, primary_key=True)
    smeta_file_name = Column(String, nullable=False, index=True)
    smeta_position_number = Column(String, default="", index=True)
    smeta_name = Column(String, nullable=False, index=True)
    smeta_code = Column(String, default="", index=True)
    smeta_cost = Column(Float, nullable=False, default=0.0)
    material_id = Column(Integer, ForeignKey("material_catalog.id"), index=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

def _to_float(value, default=0.0):
    try:
        if value is None:
            return default
        if isinstance(value, str) and not value.strip():
            return default
        return float(value)
    except Exception:
        return default


def _json_safe(value):
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        try:
            return _json_safe(value.item())
        except Exception:
            pass
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    if pd.isna(value):
        return None
    return value


def save_dataframe(session, file_id, df: pd.DataFrame):
    current_work_id = None
    inserted = 0

    for row_order, row in enumerate(df.to_dict(orient="records"), start=1):
        category = str(row.get("Категория", "") or "").strip()
        parent_work_id = None if category == "Работа" else current_work_id
        obj = SmetaRow(
            file_id=file_id,
            row_order=row_order,
            section=str(row.get("Раздел", "") or "").strip(),
            subsection=str(row.get("Подраздел", "") or "").strip(),
            position_number=str(row.get("Номер позиции", "") or "").strip(),
            code=str(row.get("Код расценки", "") or "").strip(),
            name=str(row.get("Наименование", "") or "").strip(),
            category=category,
            unit=str(row.get("Единица измерения", "") or "").strip(),
            quantity=_to_float(row.get("Количество", 0.0), 0.0),
            cost=_to_float(row.get("Стоимость", 0.0), 0.0),
            parent_work_id=parent_work_id,
            row_data=json.dumps(_json_safe({**row, "__meta_row_order": row_order, "__meta_parent_work_id": parent_work_id}), ensure_ascii=False),
        )
        session.add(obj)
        session.flush()
        inserted += 1

        if category == "Работа":
            current_work_id = obj.id

    return inserted


def load_dataframe(session, file_id) -> pd.DataFrame:
    file_record = session.query(FileRecord).filter(FileRecord.id == file_id).first()
    file_name = file_record.orig_name if file_record and file_record.orig_name else "Текущий набор"
    rows = (
        session.query(SmetaRow)
        .filter(SmetaRow.file_id == file_id)
        .order_by(SmetaRow.row_order, SmetaRow.id)
        .all()
    )
    records = []
    for row in rows:
        payload = {}
        if row.row_data:
            try:
                payload = json.loads(row.row_data)
            except Exception:
                payload = {}
        payload.update({
            "Название объекта": file_name,
            "__meta_db_row_id": row.id,
            "__meta_row_order": row.row_order or 0,
            "__meta_parent_work_id": row.parent_work_id,
            "Раздел": row.section or payload.get("Раздел", ""),
            "Подраздел": row.subsection or payload.get("Подраздел", ""),
            "Номер позиции": row.position_number or payload.get("Номер позиции", ""),
            "Код расценки": row.code or payload.get("Код расценки", ""),
            "Наименование": row.name or payload.get("Наименование", ""),
            "Категория": row.category or payload.get("Категория", ""),
            "Единица измерения": row.unit or payload.get("Единица измерения", ""),
            "Количество": row.quantity if row.quantity is not None else payload.get("Количество", 0),
            "Стоимость": row.cost if row.cost is not None else payload.get("Стоимость", 0),
        })
        records.append(payload)
    return pd.DataFrame(records)

Base.metadata.create_all(engine)


def _ensure_material_binding_file_column() -> None:
    inspector = inspect(engine)
    columns = {column["name"] for column in inspector.get_columns("material_bindings")}
    if "smeta_file_name" in columns:
        return
    with engine.begin() as connection:
        connection.exec_driver_sql("ALTER TABLE material_bindings ADD COLUMN smeta_file_name TEXT DEFAULT ''")


def _ensure_smeta_row_columns() -> None:
    inspector = inspect(engine)
    try:
        existing = {column["name"] for column in inspector.get_columns("items")}
    except Exception:
        return

    required = {
        "row_order": "INTEGER DEFAULT 0",
        "section": "TEXT DEFAULT ''",
        "subsection": "TEXT DEFAULT ''",
        "position_number": "TEXT DEFAULT ''",
        "code": "TEXT DEFAULT ''",
        "name": "TEXT DEFAULT ''",
        "category": "TEXT DEFAULT ''",
        "unit": "TEXT DEFAULT ''",
        "quantity": "FLOAT DEFAULT 0",
        "cost": "FLOAT DEFAULT 0",
        "parent_work_id": "INTEGER",
    }
    with engine.begin() as connection:
        for column_name, ddl in required.items():
            if column_name in existing:
                continue
            connection.exec_driver_sql(f"ALTER TABLE items ADD COLUMN {column_name} {ddl}")


_ensure_material_binding_file_column()
_ensure_smeta_row_columns()
