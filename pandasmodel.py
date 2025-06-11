# pandasmodel.py
import pandas as pd
import numpy as np
from PyQt5.QtCore import QAbstractTableModel, Qt
from PyQt5.QtGui import QBrush, QColor


class PandasModel(QAbstractTableModel):
    """DataFrame → Qt Model c цветовым форматированием."""

    def __init__(self, df: pd.DataFrame):
        super().__init__()
        self._df = df

    def rowCount(self, parent=None):
        return len(self._df)

    def columnCount(self, parent=None):
        return self._df.shape[1]

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None

        row, col = index.row(), index.column()
        col_name = self._df.columns[col]
        val = self._df.iat[row, col]

        # ─── DisplayRole: округляем float до 2 знаков ───
        if role == Qt.DisplayRole:
            if isinstance(val, (float, np.floating)) and not np.isnan(val):
                return f"{val:.2f}"
            return "" if pd.isna(val) else str(val)

        # ─── BackgroundRole: цвет строк по категории ───
        if role == Qt.BackgroundRole:
            cat = self._df.at[row, "Категория"] if "Категория" in self._df.columns else None
            qty = self._df.at[row, "Количество"] if "Количество" in self._df.columns else None

            # Работа → зелёный
            if cat == "Работа":
                return QBrush(QColor("#D6EFD6"))

            # Материалы → голубой или красный, только если qty<0
            if cat == "Материалы":
                q_val = None
                try:
                    q_val = float(qty)
                except Exception:
                    q_val = None
                color = "#F8D6D6" if (q_val is not None and q_val < 0) else "#D6EAF8"
                return QBrush(QColor(color))

            # Механизмы → другой цвет
            if cat == "Механизмы":
                return QBrush(QColor("#FFF6D6"))

        # ─── ForegroundRole: цвет текста для «Разница …» ───
        if role == Qt.ForegroundRole and col_name.startswith("Разница"):
            try:
                num = float(str(val).replace(" ", "").replace(",", "."))
                if num > 0:
                    return QBrush(QColor("green"))
                if num < 0:
                    return QBrush(QColor("red"))
            except Exception:
                pass

        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                return str(self._df.columns[section])
            else:
                return str(section)