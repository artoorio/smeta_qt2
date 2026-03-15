"""Visual shell for the material catalog with DB editing capabilities."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd
from PyQt5.QtCore import QAbstractTableModel, QModelIndex, Qt
from PyQt5.QtWidgets import (
    QApplication,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from db import FileRecord, SessionLocal, SmetaRow
from materials_editor import MaterialCatalog, MaterialCatalogManager, DEFAULT_MATERIAL_COLUMNS, KEY_COLUMN


class EditableDataModel(QAbstractTableModel):
    def __init__(self, dataframe: pd.DataFrame):
        super().__init__()
        self._df = dataframe.copy()

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self._df)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return self._df.shape[1]

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid():
            return None
        value = self._df.iat[index.row(), index.column()]
        if role in (Qt.DisplayRole, Qt.EditRole):
            return "" if pd.isna(value) else str(value)
        return None

    def setData(self, index: QModelIndex, value, role: int = Qt.EditRole):
        if role != Qt.EditRole or not index.isValid():
            return False
        self._df.iat[index.row(), index.column()] = value
        self.dataChanged.emit(index, index, [Qt.DisplayRole, Qt.EditRole])
        return True

    def flags(self, index: QModelIndex):
        if not index.isValid():
            return Qt.NoItemFlags
        return Qt.ItemIsSelectable | Qt.ItemIsEnabled | Qt.ItemIsEditable

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            return str(self._df.columns[section])
        return str(section + 1)

    def dataframe(self) -> pd.DataFrame:
        return self._df


class ColumnMappingDialog(QDialog):
    def __init__(self, actual_columns: Iterable[str], targets: Iterable[str], parent=None):
        super().__init__(parent)
        self.setWindowTitle("Сопоставление колонок")
        layout = QVBoxLayout(self)
        self.mapping: Dict[str, QComboBox] = {}
        actual = sorted({str(col) for col in actual_columns if col is not None})
        for target in targets:
            row = QHBoxLayout()
            label = QLabel(target)
            combo = QComboBox()
            combo.addItem("")
            combo.addItems(actual)
            if target in actual:
                combo.setCurrentText(target)
            row.addWidget(label)
            row.addWidget(combo)
            layout.addLayout(row)
            self.mapping[target] = combo
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def result(self) -> Dict[str, str]:
        return {combo.currentText(): target for target, combo in self.mapping.items() if combo.currentText()}


def load_db_materials() -> pd.DataFrame:
    session = SessionLocal()
    try:
        rows = (
            session.query(SmetaRow.row_data, FileRecord.orig_name)
            .join(FileRecord, FileRecord.id == SmetaRow.file_id)
            .order_by(FileRecord.orig_name)
            .all()
        )
        records = []
        for raw, filename in rows:
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            payload["file_name"] = filename
            records.append(payload)
        return pd.DataFrame(records) if records else pd.DataFrame()
    finally:
        session.close()


def persist_db_materials(frame: pd.DataFrame, file_name: str) -> None:
    if frame.empty:
        return
    session = SessionLocal()
    try:
        file = session.query(FileRecord).filter_by(orig_name=file_name).first()
        if not file:
            file = FileRecord.from_path(file_name, status="manual")
            session.add(file)
            session.commit()
        session.query(SmetaRow).filter_by(file_id=file.id).delete()
        session.commit()
        for _, row in frame.iterrows():
            data = row.drop(labels=["file_name"], errors="ignore").to_dict()
            data.setdefault("Материалы", data.get("Материалы") or data.get("Стоимость") or 0)
            session.add(SmetaRow(file_id=file.id, row_data=json.dumps(data, ensure_ascii=False)))
        session.commit()
    finally:
        session.close()


class MaterialCatalogView(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Редактор справочника материалов")
        self.manager = MaterialCatalogManager()
        self.catalog: Optional[MaterialCatalog] = None
        self.table = QTableView()
        self.model: Optional[EditableDataModel] = None

        self.file_name_input = QLineEdit("materials-ui")
        self.file_name_input.setPlaceholderText("Имя файла в базе данных")

        btn_load = QPushButton("Загрузить прайс")
        btn_load.clicked.connect(self.load_catalog)
        btn_assign = QPushButton("Сопоставить коды")
        btn_assign.clicked.connect(self.assign_codes)
        btn_refresh = QPushButton("Обновить из БД")
        btn_refresh.clicked.connect(self.load_from_db)
        btn_save = QPushButton("Сохранить в БД")
        btn_save.clicked.connect(self.save_to_db)

        controls = QHBoxLayout()
        controls.addWidget(btn_load)
        controls.addWidget(btn_assign)
        controls.addWidget(btn_refresh)
        controls.addWidget(btn_save)
        controls.addWidget(self.file_name_input)

        layout = QVBoxLayout(self)
        layout.addLayout(controls)
        layout.addWidget(self.table)

    def set_dataframe(self, frame: pd.DataFrame):
        if frame.empty:
            self.model = EditableDataModel(pd.DataFrame(columns=DEFAULT_MATERIAL_COLUMNS + [KEY_COLUMN, "file_name"]))
        else:
            self.model = EditableDataModel(frame)
        self.table.setModel(self.model)

    def load_catalog(self):
        path, _ = QFileDialog.getOpenFileName(self, "Excel", ".", "Excel (*.xlsx *.xls)")
        if not path:
            return
        df = pd.read_excel(path)
        dialog = ColumnMappingDialog(df.columns, list(DEFAULT_MATERIAL_COLUMNS) + [KEY_COLUMN], self)
        if dialog.exec_() != QDialog.Accepted:
            return
        column_map = dialog.result()
        catalog = MaterialCatalog.from_excel(Path(path), column_map=column_map)
        self.catalog = catalog
        self.manager.catalogs[path] = catalog
        frame = catalog.df.copy()
        frame["file_name"] = Path(path).name
        self.set_dataframe(frame)

    def assign_codes(self):
        if not self.catalog:
            return
        path, _ = QFileDialog.getOpenFileName(self, "Коды позиций", ".", "Excel (*.xlsx *.xls)")
        if not path:
            return
        lookup = pd.read_excel(path)
        self.catalog.assign_codes(lookup)
        frame = self.catalog.df.copy()
        frame["file_name"] = self.file_name_input.text().strip() or "materials-ui"
        self.set_dataframe(frame)

    def load_from_db(self):
        frame = load_db_materials()
        self.set_dataframe(frame)

    def save_to_db(self):
        if not self.model:
            return
        frame = self.model.dataframe().copy()
        if "file_name" not in frame.columns or frame["file_name"].isna().all():
            frame["file_name"] = self.file_name_input.text().strip() or "materials-ui"
        file_series = frame["file_name"].dropna().astype(str)
        file_name = file_series.iloc[0] if not file_series.empty else self.file_name_input.text().strip() or "materials-ui"
        persist_db_materials(frame, file_name)


def main() -> None:
    app = QApplication([])
    view = MaterialCatalogView()
    view.resize(1100, 700)
    view.show()
    app.exec_()


if __name__ == "__main__":
    main()
