import sys
from pathlib import Path
from typing import List

import pandas as pd
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QPushButton, QListWidget,
    QListWidgetItem, QFileDialog, QHBoxLayout, QMessageBox, QLabel,
    QStackedWidget, QMainWindow
)
from column_order_dialog import ColumnOrderDialog
from PyQt5.QtCore import QThread, pyqtSignal
from pathlib import Path
import pandas as pd
from data_processing import process_smeta
from smeta_compare import SmetaComparator
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QMessageBox, QPushButton,
    QFileDialog, QDialog
)
from PyQt5.QtWidgets import QTabWidget
import pandas as pd
from missing_dialog import MissingDialog

# эти классы уже существуют в вашем main.py

from pandasmodel import PandasModel
from data_processing import process_smeta

# ────────────────────────────────────────────────
#   ПЛАГИНЫ БИЗНЕС-ЛОГИКИ  (ваши функции)
# ────────────────────────────────────────────────
from data_processing import process_smeta
from smeta_compare import compare_smetas
from pandasmodel import PandasModel

# column_dialog.py
from typing import List, Tuple, Optional
from PyQt5.QtWidgets import (
    QDialog, QLabel, QVBoxLayout, QHBoxLayout, QComboBox,
    QListWidget, QListWidgetItem, QDialogButtonBox
)


import json, os
from typing import List, Tuple, Optional
from PyQt5.QtWidgets import (
    QDialog, QLabel, QVBoxLayout, QHBoxLayout, QComboBox,
    QListWidget, QListWidgetItem, QDialogButtonBox
)

_PREFS_FILE = os.path.join(os.path.dirname(__file__), "column_prefs.json")


def _load_prefs() -> dict:
    if os.path.exists(_PREFS_FILE):
        try:
            with open(_PREFS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {}


def _save_prefs(prefs: dict):
    try:
        with open(_PREFS_FILE, "w", encoding="utf-8") as f:
            json.dump(prefs, f, ensure_ascii=False, indent=2)
    except IOError:
        pass


class ColumnSelectDialog(QDialog):
    """Диалог выбора колонок с запоминанием последнего набора."""

    def __init__(self, columns: List[str], parent=None):
        super().__init__(parent)
        self.setWindowTitle("Выбор колонок для сравнения")
        self.resize(420, 380)

        layout = QVBoxLayout(self)

        # --- compare_column ---
        self.cb_compare = QComboBox(); self.cb_compare.addItems(columns)
        layout.addLayout(self._row("Колонка-ключ (compare_column):", self.cb_compare))

        # --- value_column ---
        self.cb_value = QComboBox(); self.cb_value.addItems(columns)
        layout.addLayout(self._row("Колонка стоимости (value_column):", self.cb_value))

        # --- subsection_column (необ.) ---
        self.cb_sub = QComboBox(); self.cb_sub.addItems([""] + columns)
        layout.addLayout(self._row("Подраздел (subsection_column):", self.cb_sub))

        # --- extra_column (множество) ---
        layout.addWidget(QLabel("Доп. колонки (extra_column):"))
        self.extra_list = QListWidget(); self.extra_list.setSelectionMode(QListWidget.MultiSelection)
        for c in columns:
            self.extra_list.addItem(QListWidgetItem(c))
        layout.addWidget(self.extra_list)

        # --- OK / Cancel ---
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self._on_accept); btns.rejected.connect(self.reject)
        layout.addWidget(btns)

        # применяем сохранённые значения (если колонки существуют)
        self._apply_saved(columns)

    # --------------------------------------------------------------
    def _apply_saved(self, columns: List[str]):
        prefs = _load_prefs()
        if not prefs: return

        if prefs.get("compare_column") in columns:
            self.cb_compare.setCurrentText(prefs["compare_column"])

        if prefs.get("value_column") in columns:
            self.cb_value.setCurrentText(prefs["value_column"])

        if prefs.get("subsection_column") in columns:
            self.cb_sub.setCurrentText(prefs["subsection_column"] or "")

        saved_extra = prefs.get("extra_column", [])
        for i in range(self.extra_list.count()):
            item = self.extra_list.item(i)
            if item.text() in saved_extra:
                item.setSelected(True)

    # --------------------------------------------------------------
    def _on_accept(self):
        # сохраняем выбор
        prefs = {
            "compare_column": self.cb_compare.currentText(),
            "value_column":   self.cb_value.currentText(),
            "subsection_column": self.cb_sub.currentText() or None,
            "extra_column": [i.text() for i in self.extra_list.selectedItems()],
        }
        _save_prefs(prefs)
        self.accept()

    # --------------------------------------------------------------
    @staticmethod
    def _row(label: str, widget):
        box = QHBoxLayout(); box.addWidget(QLabel(label)); box.addWidget(widget)
        return box

    # результат для вызывающего окна
    def get_selection(self) -> Tuple[str, str, List[str], Optional[str]]:
        extras = [i.text() for i in self.extra_list.selectedItems()]
        subcol = self.cb_sub.currentText() or None
        return (self.cb_compare.currentText(),
                self.cb_value.currentText(),
                extras,
                subcol)
# ╭──────────────────╮
# │  Поток обработки │
# ╰──────────────────╯
class ProcessWorker(QThread):
    progress = pyqtSignal(int, str)
    finished = pyqtSignal(pd.DataFrame)

    def __init__(self, files: List[str]):
        super().__init__()
        self.files = files

    def run(self):
        merged = []
        total = len(self.files)
        for idx, path in enumerate(self.files, 1):
            try:
                df = process_smeta(path)
                merged.append(df)
            except Exception as e:
                print(f"⚠ {path}: {e}")
            self.progress.emit(int(idx / total * 100), Path(path).name)
        if merged:
            self.finished.emit(pd.concat(merged, ignore_index=True))


# ╭──────────────────╮
# │  Поток сравнения │
# ╰──────────────────╯
class CompareWorker(QThread):
    finished = pyqtSignal(pd.DataFrame, object)      # DataFrame, SmetaComparator

    def __init__(self, proj: str, fact: str,
                 cmp_col: str, val_col: str,
                 extra_cols, sub_col):
        super().__init__()
        self.proj, self.fact = proj, fact
        self.cmp_col, self.val_col = cmp_col, val_col
        self.extra_cols, self.sub_col = extra_cols, sub_col

    def run(self):
        df_proj = process_smeta(self.proj)
        df_fact = process_smeta(self.fact)

        cmp = SmetaComparator(
            df_proj, df_fact,
            file1_name=Path(self.proj).name,
            file2_name=Path(self.fact).name,
            compare_column=self.cmp_col,
            value_column=self.val_col,
            extra_column=self.extra_cols,
            subsection_column=self.sub_col,
        )
        df_report = cmp.generate_customer_report()
        self.finished.emit(df_report, cmp)


# ╭────────────────────────────────────────╮
# │  Виджет со списком файлов + кнопками  │
# ╰────────────────────────────────────────╯
class FileListWidget(QWidget):
    """Базовый виджет: drag-and-drop + список + кнопки."""

    run_requested = pyqtSignal(list)      # -> список файлов

    def __init__(self, title: str, max_files: int | None = None):
        super().__init__()
        self.max_files = max_files
        self.setAcceptDrops(True)

        self.label = QLabel(f"<b>{title}</b>")
        self.listw = QListWidget()
        self.listw.setSelectionMode(QListWidget.SingleSelection)

        btn_add    = QPushButton("Добавить файл(ы)…")
        btn_remove = QPushButton("Удалить выбранный")
        btn_clear  = QPushButton("Очистить список")
        self.btn_run = QPushButton("Запустить")

        btn_add.clicked.connect(self.add_files)
        btn_remove.clicked.connect(self.remove_selected)
        btn_clear.clicked.connect(self.listw.clear)
        self.btn_run.clicked.connect(self._emit_run)

        lay = QVBoxLayout(self)
        lay.addWidget(self.label)
        lay.addWidget(self.listw)

        h = QHBoxLayout()
        h.addWidget(btn_add)
        h.addWidget(btn_remove)
        h.addWidget(btn_clear)
        lay.addLayout(h)
        lay.addWidget(self.btn_run)

    # — drag-and-drop —
    def dragEnterEvent(self, e): e.acceptProposedAction()
    def dropEvent(self, e):
        for url in e.mimeData().urls():
            self._try_add(url.toLocalFile())
        e.acceptProposedAction()

    # — helpers —
    def _try_add(self, path: str):
        if self.max_files and self.listw.count() >= self.max_files:
            QMessageBox.warning(self, "Лимит",
                                f"Можно загрузить не более {self.max_files} файлов.")
            return
        if path and Path(path).suffix.lower() in (".xlsx", ".xls"):
            self.listw.addItem(path)

    def add_files(self):
        files, _ = QFileDialog.getOpenFileNames(self, "Excel файлы", "",
                                                "Excel (*.xlsx *.xls)")
        for f in files:
            self._try_add(f)

    def remove_selected(self):
        for item in self.listw.selectedItems():
            self.listw.takeItem(self.listw.row(item))

    def _emit_run(self):
        files = [self.listw.item(i).text() for i in range(self.listw.count())]
        self.run_requested.emit(files)


# ╭──────────────────╮
# │  Окно обработки  │
# ╰──────────────────╯
# main.py  – замените ваш класс ProcessWindow
from fact_export import export_with_fact_formula
# … остальной импорт ProcessWindow без изменений …

class ProcessWindow(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.widget = FileListWidget("Сметы для обработки (любое количество)")
        self.widget.run_requested.connect(self._start_process)

        # две кнопки экспорта
        self.btn_plain  = QPushButton("↗  Сохранить в Excel (простой)")
        self.btn_fact   = QPushButton("↗  Excel с формулами (факт)")
        for b in (self.btn_plain, self.btn_fact): b.setEnabled(False)

        self.btn_plain.clicked.connect(self._save_plain)
        self.btn_fact.clicked.connect(self._save_fact)

        lay = QVBoxLayout(self)
        lay.addWidget(self.widget)
        lay.addWidget(self.btn_plain)
        lay.addWidget(self.btn_fact)

        self._df: pd.DataFrame | None = None

    # ------------ запуск фоновой обработки ------------
    def _start_process(self, files):
        if not files:
            QMessageBox.information(self, "Нет файлов", "Добавьте хотя бы один файл.")
            return
        self.widget.btn_run.setEnabled(False)
        for b in (self.btn_plain, self.btn_fact): b.setEnabled(False)
        self.worker = ProcessWorker(files)
        self.worker.finished.connect(self._show_result)
        self.worker.start()

    # ------------ результат ------------
    def _show_result(self, df: pd.DataFrame):
        self._df = df
        TableDialog(df, "Результат обработки").exec_()
        for b in (self.btn_plain, self.btn_fact): b.setEnabled(True)
        self.widget.btn_run.setEnabled(True)

    # ------------ обычный Excel ------------
    def _save_plain(self):
        if self._df is None: return
        path, _ = QFileDialog.getSaveFileName(self, "Excel-файл",
                                              "processed.xlsx", "Excel (*.xlsx)")
        if path:
            try:
                self._df.to_excel(path, index=False, engine="openpyxl")
                QMessageBox.information(self, "Готово", path)
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", str(e))

    # ------------ Excel с формулами ------------
    def _save_fact(self):
        if self._df is None: return
        path, _ = QFileDialog.getSaveFileName(self, "Excel-файл",
                                              "processed_fact.xlsx", "Excel (*.xlsx)")
        if path:
            try:
                export_with_fact_formula(self._df, path)
                QMessageBox.information(self, "Готово", path)
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", str(e))
# ╭──────────────────╮
# │  Окно сравнения  │
# ╰──────────────────╯
class CompareWindow(QWidget):
    """Окно «Сравнение смет» + выбор колонок + экспорт с выбранным порядком."""

    def __init__(self, parent=None):
        super().__init__(parent)

        self.widget = FileListWidget("Выберите ДВЕ сметы", max_files=2)
        self.widget.run_requested.connect(self._start_compare)

        # три кнопки экспорта
        self.btn_html    = QPushButton("↗  HTML-отчёт")
        self.btn_excel   = QPushButton("↗  Excel-отчёт")
        self.btn_missing = QPushButton("↗  TXT «Отсутствующие»")
        for b in (self.btn_html, self.btn_excel, self.btn_missing):
            b.setEnabled(False)

        self.btn_html.clicked.connect(self._export_html)
        self.btn_excel.clicked.connect(self._export_excel)
        self.btn_missing.clicked.connect(self._export_missing)

        lay = QVBoxLayout(self)
        lay.addWidget(self.widget)
        lay.addWidget(self.btn_html)
        lay.addWidget(self.btn_excel)
        lay.addWidget(self.btn_missing)

        self._cmp = None          # SmetaComparator
        self._col_order = None    # порядок колонок

    # ────────────────────────────────────────────────
    #  запуск сравнения
    # ────────────────────────────────────────────────
    def _start_compare(self, files):
        if len(files) != 2:
            QMessageBox.warning(self, "Нужно 2 файла",
                                 "Загрузите ПЕРВУЮ (проектную) и ВТОРУЮ (фактическую) смету.")
            return

        # читаем файлы, чтобы узнать список колонок
        try:
            cols = sorted(set(process_smeta(files[0]).columns) |
                          set(process_smeta(files[1]).columns))
        except Exception as e:
            QMessageBox.critical(self, "Ошибка парсинга", str(e))
            return

        # диалог выбора колонок
        dlg = ColumnSelectDialog(cols, self)
        if dlg.exec_() != QDialog.Accepted:
            return
        cmp_col, val_col, extra_cols, sub_col = dlg.get_selection()

        # запускаем CompareWorker
        self.widget.btn_run.setEnabled(False)
        self.worker = CompareWorker(files[0], files[1],
                                    cmp_col, val_col,
                                    extra_cols, sub_col)
        self.worker.finished.connect(self._show_report)
        self.worker.start()

    # ────────────────────────────────────────────────
    #  показать отчёт  +  выбрать порядок колонок
    # ────────────────────────────────────────────────
    def _show_report(self, df: pd.DataFrame, cmp_obj):
        from column_order_dialog import ColumnOrderDialog

        # сохранённый компаратор
        self._cmp = cmp_obj

        # диалог порядка колонок
        cols = list(df.columns)
        dlg_order = ColumnOrderDialog(cols, self)
        if dlg_order.exec_() == QDialog.Accepted:
            self._col_order = dlg_order.result_order()
            df = df[[c for c in self._col_order if c in df.columns]]
        else:
            self._col_order = cols

        # показать таблицу
        # получаем детальный и сводный отчёты
        df_detail = df
        df_summary = cmp_obj.generate_subsection_summary()

        # собираем вкладки
        dlg = QDialog(self)
        dlg.setWindowTitle("Отчёт сравнения")
        dlg.resize(900, 600)
        tabs = QTabWidget(dlg)

        # вкладка «Детали»
        w1 = QWidget();
        lay1 = QVBoxLayout(w1)
        tv1 = QTableView();
        tv1.setModel(PandasModel(df_detail))
        tv1.resizeRowsToContents();
        tv1.setWordWrap(True)
        lay1.addWidget(tv1)
        tabs.addTab(w1, "Детали")

        # вкладка «Сводка по подразделам»
        w2 = QWidget();
        lay2 = QVBoxLayout(w2)
        tv2 = QTableView();
        tv2.setModel(PandasModel(df_summary))
        tv2.resizeRowsToContents();
        tv2.setWordWrap(True)
        lay2.addWidget(tv2)
        tabs.addTab(w2, "Сводка по подразделам")

        # собрать диалог
        dlg_layout = QVBoxLayout(dlg)
        dlg_layout.addWidget(tabs)
        dlg.exec_()

        for b in (self.btn_html, self.btn_excel, self.btn_missing):
            b.setEnabled(True)
        self.widget.btn_run.setEnabled(True)

    # ────────────────────────────────────────────────
    #  экспортные кнопки
    # ────────────────────────────────────────────────
    def _export_html(self):
        if not self._cmp:
            return
        path, _ = QFileDialog.getSaveFileName(
            self, "HTML-файл", "customer_report.html", "HTML (*.html)"
        )
        if not path:
            return
        try:
            self._cmp.export_customer_html(path)
            QMessageBox.information(self, "Готово", f"HTML-отчёт сохранён:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", str(e))

    def _export_excel(self):
        if not self._cmp:
            return
        path, _ = QFileDialog.getSaveFileName(
            self, "Excel-файл", "customer_report.xlsx", "Excel (*.xlsx)"
        )
        if not path:
            return
        try:
            self._cmp.export_customer_excel(path)
            QMessageBox.information(self, "Готово", f"Excel-отчёт сохранён:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", str(e))

    def _export_missing(self):
        if not self._cmp:
            return
        missing = self._cmp.get_missing_positions()
        dlg = MissingDialog(missing, self)
        dlg.exec_()
# ╭──────────────────╮
# │  Диалог-таблица  │
# ╰──────────────────╯
from PyQt5.QtWidgets import QDialog, QTableView

class TableDialog(QDialog):
    def __init__(self, df: pd.DataFrame, title: str):
        super().__init__()
        self.setWindowTitle(title); self.resize(1000, 700)

        view = QTableView()
        model = PandasModel(df); view.setModel(model)

        # ―–– колонка «Наименование» = ~400 px (40 «символов»)
        if "Наименование" in df.columns:
            col_idx = list(df.columns).index("Наименование")
            view.setColumnWidth(col_idx, 400)

        # перенос и авто-высота
        view.resizeRowsToContents()
        view.setWordWrap(True)

        lay = QVBoxLayout(self); lay.addWidget(view)


# ╭──────────────────╮
# │  Главное меню    │
# ╰──────────────────╯
class MainMenu(QWidget):
    switch_to_process = pyqtSignal()
    switch_to_compare = pyqtSignal()

    def __init__(self):
        super().__init__()
        btn_proc = QPushButton("Обработать сметы")
        btn_comp = QPushButton("Сравнить сметы")
        btn_proc.clicked.connect(self.switch_to_process)
        btn_comp.clicked.connect(self.switch_to_compare)

        lay = QVBoxLayout(self)
        lay.addStretch(1)
        lay.addWidget(btn_proc)
        lay.addWidget(btn_comp)
        lay.addStretch(1)


# ╭──────────────────╮
# │  Главный QMainWindow │
# ╰──────────────────╯
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Smeta Toolkit")
        self.resize(800, 500)

        self.stack = QStackedWidget()
        self.menu = MainMenu()
        self.proc_win = ProcessWindow()
        self.comp_win = CompareWindow()

        self.stack.addWidget(self.menu)      # index 0
        self.stack.addWidget(self.proc_win)  # index 1
        self.stack.addWidget(self.comp_win)  # index 2
        self.setCentralWidget(self.stack)

        # сигналы
        self.menu.switch_to_process.connect(lambda: self.stack.setCurrentIndex(1))
        self.menu.switch_to_compare.connect(lambda: self.stack.setCurrentIndex(2))

        # «домой» по Esc
        self.stack.currentChanged.connect(self._update_title)

    def _update_title(self, idx):
        titles = ["Меню", "Обработка смет", "Сравнение смет"]
        self.setWindowTitle(f"Smeta Toolkit — {titles[idx]}")

    def keyPressEvent(self, e):
        if e.key() == Qt.Key_Escape and self.stack.currentIndex() != 0:
            self.stack.setCurrentIndex(0)


# ╭──────────────────╮
# │   run            │
# ╰──────────────────╯
if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = MainWindow(); win.show()
    sys.exit(app.exec_())