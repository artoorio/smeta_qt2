"""
materials_editor.py

Небольшое отдельное окно для просмотра и редактирования
Excel‑файла с материалами (или любым другим прайсом).

Пока это независимый модуль для тестирования.
Запуск:
    python materials_editor.py
"""

import sys
from pathlib import Path
from typing import Optional

import pandas as pd
from PyQt5.QtCore import QAbstractTableModel, QModelIndex, Qt
from PyQt5.QtWidgets import (
    QApplication,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QTableView,
    QVBoxLayout,
    QWidget,
)


class EditablePandasModel(QAbstractTableModel):
    """Простой редактируемый DataFrame → Qt Model."""

    def __init__(self, df: pd.DataFrame):
        super().__init__()
        self._df = df

    # --- размер ---
    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:  # type: ignore[override]
        return len(self._df)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:  # type: ignore[override]
        return self._df.shape[1]

    # --- данные ---
    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):  # type: ignore[override]
        if not index.isValid():
            return None

        row, col = index.row(), index.column()
        value = self._df.iat[row, col]

        if role in (Qt.DisplayRole, Qt.EditRole):
            return "" if pd.isna(value) else str(value)

        return None

    def setData(self, index: QModelIndex, value, role: int = Qt.EditRole):  # type: ignore[override]
        if role != Qt.EditRole or not index.isValid():
            return False

        row, col = index.row(), index.column()

        # Пытаемся конвертировать в число, если колонка числовая
        col_name = self._df.columns[col]
        old_val = self._df.at[row, col_name]

        if pd.api.types.is_numeric_dtype(self._df[col_name]):
            try:
                if value == "":
                    new_val = pd.NA
                else:
                    new_val = float(str(value).replace(",", "."))
            except ValueError:
                # оставляем старое значение, но не падаем
                return False
        else:
            new_val = value

        self._df.at[row, col_name] = new_val
        self.dataChanged.emit(index, index, [Qt.DisplayRole, Qt.EditRole])
        return True

    def flags(self, index: QModelIndex) -> Qt.ItemFlags:  # type: ignore[override]
        if not index.isValid():
            return Qt.NoItemFlags
        return Qt.ItemIsSelectable | Qt.ItemIsEnabled | Qt.ItemIsEditable

    # --- заголовки ---
    def headerData(
        self,
        section: int,
        orientation: Qt.Orientation,
        role: int = Qt.DisplayRole,
    ):
        if role != Qt.DisplayRole:
            return None

        if orientation == Qt.Horizontal:
            return str(self._df.columns[section])
        return str(section + 1)

    # --- доступ к DataFrame ---
    def dataframe(self) -> pd.DataFrame:
        return self._df


class MaterialsEditorWindow(QWidget):
    """
    Окно для:
      * выбора Excel‑файла с материалами,
      * просмотра/редактирования таблицы,
      * сохранения изменений.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Редактор материалов")
        self.resize(900, 600)

        self._current_path: Optional[Path] = None
        self._model: Optional[EditablePandasModel] = None

        # --- верхняя панель с кнопками ---
        btn_open = QPushButton("Открыть файл…")
        btn_save = QPushButton("Сохранить")
        btn_save_as = QPushButton("Сохранить как…")

        btn_open.clicked.connect(self._on_open)
        btn_save.clicked.connect(self._on_save)
        btn_save_as.clicked.connect(self._on_save_as)

        self.lbl_path = QLabel("Файл не выбран")
        self.lbl_path.setTextInteractionFlags(Qt.TextSelectableByMouse)

        top = QHBoxLayout()
        top.addWidget(btn_open)
        top.addWidget(btn_save)
        top.addWidget(btn_save_as)
        top.addStretch(1)
        top.addWidget(self.lbl_path)

        # --- таблица ---
        self.table = QTableView()
        self.table.setAlternatingRowColors(True)

        layout = QVBoxLayout(self)
        layout.addLayout(top)
        layout.addWidget(self.table)

    # ------------------------------------------------------------
    #  Загрузка / сохранение
    # ------------------------------------------------------------
    def _on_open(self):
        path_str, _ = QFileDialog.getOpenFileName(
            self,
            "Открыть Excel с материалами",
            "",
            "Excel файлы (*.xlsx *.xls)",
        )
        if not path_str:
            return

        path = Path(path_str)
        try:
            # читаем первый лист целиком — дальше можно фильтровать по колонкам
            df = pd.read_excel(path_str)
        except Exception as e:
            QMessageBox.critical(self, "Ошибка чтения", str(e))
            return

        self._current_path = path
        self.lbl_path.setText(str(path))

        self._model = EditablePandasModel(df)
        self.table.setModel(self._model)
        self.table.resizeColumnsToContents()
        self.table.resizeRowsToContents()

    def _ensure_model(self) -> Optional[EditablePandasModel]:
        if self._model is None:
            QMessageBox.information(self, "Нет данных", "Сначала откройте файл с материалами.")
            return None
        return self._model

    def _on_save(self):
        model = self._ensure_model()
        if model is None:
            return

        if not self._current_path:
            self._on_save_as()
            return

        self._save_to_path(self._current_path, model.dataframe())

    def _on_save_as(self):
        model = self._ensure_model()
        if model is None:
            return

        path_str, _ = QFileDialog.getSaveFileName(
            self,
            "Сохранить как",
            str(self._current_path or "materials.xlsx"),
            "Excel файлы (*.xlsx)",
        )
        if not path_str:
            return

        path = Path(path_str)
        self._save_to_path(path, model.dataframe())
        self._current_path = path
        self.lbl_path.setText(str(path))

    def _save_to_path(self, path: Path, df: pd.DataFrame):
        try:
            df.to_excel(str(path), index=False, engine="openpyxl")
            QMessageBox.information(self, "Готово", f"Файл сохранён:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка сохранения", str(e))


def main():
    app = QApplication(sys.argv)
    win = MaterialsEditorWindow()
    win.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()

