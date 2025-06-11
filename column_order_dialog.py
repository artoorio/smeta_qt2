# column_order_dialog.py
from typing import List
from PyQt5.QtWidgets import (
    QDialog, QVBoxLayout, QListWidget, QPushButton,
    QHBoxLayout, QDialogButtonBox, QListWidgetItem
)


class ColumnOrderDialog(QDialog):
    """Перетаскивание колонок стрелками."""

    def __init__(self, columns: List[str], parent=None):
        super().__init__(parent)
        self.setWindowTitle("Порядок колонок в отчёте")

        self.listw = QListWidget()
        for c in columns:
            self.listw.addItem(QListWidgetItem(c))
        self.listw.setCurrentRow(0)

        # кнопки ▲▼
        btn_up   = QPushButton("▲")
        btn_down = QPushButton("▼")
        btn_up.clicked.connect(lambda: self._move(-1))
        btn_down.clicked.connect(lambda: self._move(+1))

        arrows = QVBoxLayout(); arrows.addWidget(btn_up); arrows.addWidget(btn_down); arrows.addStretch()

        hl = QHBoxLayout(); hl.addWidget(self.listw); hl.addLayout(arrows)

        btn_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btn_box.accepted.connect(self.accept); btn_box.rejected.connect(self.reject)

        layout = QVBoxLayout(self); layout.addLayout(hl); layout.addWidget(btn_box)

    def _move(self, delta: int):
        row = self.listw.currentRow()
        if row < 0: return
        new_row = row + delta
        if 0 <= new_row < self.listw.count():
            item = self.listw.takeItem(row)
            self.listw.insertItem(new_row, item)
            self.listw.setCurrentRow(new_row)

    def result_order(self) -> List[str]:
        return [self.listw.item(i).text() for i in range(self.listw.count())]