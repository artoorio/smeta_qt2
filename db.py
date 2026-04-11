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
    file_id = Column(Integer)
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

def save_dataframe(session, file_id, df: pd.DataFrame):
    objs = [SmetaRow(file_id=file_id, row_data=json.dumps(r._asdict(), ensure_ascii=False))
            for r in df.itertuples(index=False)]
    session.bulk_save_objects(objs)
    session.commit()

Base.metadata.create_all(engine)


def _ensure_material_binding_file_column() -> None:
    inspector = inspect(engine)
    columns = {column["name"] for column in inspector.get_columns("material_bindings")}
    if "smeta_file_name" in columns:
        return
    with engine.begin() as connection:
        connection.exec_driver_sql("ALTER TABLE material_bindings ADD COLUMN smeta_file_name TEXT DEFAULT ''")


_ensure_material_binding_file_column()
