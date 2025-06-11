from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text
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

def save_dataframe(session, file_id, df: pd.DataFrame):
    objs = [SmetaRow(file_id=file_id, row_data=json.dumps(r._asdict(), ensure_ascii=False))
            for r in df.itertuples(index=False)]
    session.bulk_save_objects(objs)
    session.commit()

Base.metadata.create_all(engine)
