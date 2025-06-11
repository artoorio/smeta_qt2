# missing_dialog.py
from typing import List
from PyQt5.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout,
    QPushButton, QPlainTextEdit, QFileDialog, QMessageBox
)
from PyQt5.QtGui import QClipboard
from PyQt5.QtCore import Qt


class MissingDialog(QDialog):
    """
    Диалог: показывает отсутствующие позиции,
    с кнопками «Копировать всё» и «Сохранить…».
    """
    def __init__(self, missing_list: List[str], parent=None):
        super().__init__(parent)
        self.setWindowTitle("Отсутствующие позиции")
        self.resize(500, 400)

        self._missing = missing_list

        # Основной лэйаут
        layout = QVBoxLayout(self)

        # Текстовое поле для копирования
        self.text_edit = QPlainTextEdit(self)
        self.text_edit.setReadOnly(True)
        # Заполняем текст
        self.text_edit.setPlainText("\n".join(missing_list) or "Нет отсутствующих позиций.")
        layout.addWidget(self.text_edit)

        # Кнопки: Копировать, Сохранить, Закрыть
        btn_copy = QPushButton("Копировать всё")
        btn_save = QPushButton("Сохранить в файл…")
        btn_close = QPushButton("Закрыть")

        btn_copy.clicked.connect(self._on_copy_all)
        btn_save.clicked.connect(self._on_save)
        btn_close.clicked.connect(self.accept)

        btn_layout = QHBoxLayout()
        btn_layout.addWidget(btn_copy)
        btn_layout.addWidget(btn_save)
        btn_layout.addStretch()
        btn_layout.addWidget(btn_close)
        layout.addLayout(btn_layout)

    def _on_copy_all(self):
        """Копирует весь текст в системный буфер обмена."""
        clipboard: QClipboard = QApplication.clipboard()
        clipboard.setText(self.text_edit.toPlainText(), mode=QClipboard.Clipboard)
        # Поддержка macOS универсального буфера
        clipboard.setText(self.text_edit.toPlainText(), mode=QClipboard.Selection)
        QMessageBox.information(self, "Скопировано", "Текст скопирован в буфер обмена.")

    def _on_save(self):
        """Сохраняет текст в выбранный файл."""
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Сохранить TXT",
            "missing_in_d2.txt",
            "Text (*.txt)"
        )
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(self.text_edit.toPlainText())
            QMessageBox.information(self, "Сохранено", f"Файл сохранён:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка при сохранении", str(e))