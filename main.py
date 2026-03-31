import sys
from pathlib import Path
from typing import List
import logging
from datetime import datetime
import math

import pandas as pd
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QPushButton, QListWidget,
    QListWidgetItem, QFileDialog, QHBoxLayout, QMessageBox, QLabel,
    QStackedWidget, QMainWindow, QHeaderView, QTableView
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
from export_formatting import apply_readable_sheet_layout, dataframe_to_readable_html

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
_MATERIALS_PREFS = os.path.join(os.path.dirname(__file__), "materials_prefs.json")


def with_timestamp(filename: str) -> str:
    stem = Path(filename).stem
    suffix = Path(filename).suffix
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return f"{stem}_{timestamp}{suffix}"


def offer_open_file(parent, path: str, title: str = "Готово", label: str = "Файл сохранён") -> None:
    reply = QMessageBox.question(
        parent,
        title,
        f"{label}:\n{path}\n\nОткрыть файл сейчас?",
        QMessageBox.Yes | QMessageBox.No,
        QMessageBox.Yes,
    )
    if reply == QMessageBox.Yes:
        try:
            os.startfile(path)
        except Exception as e:
            QMessageBox.warning(parent, "Не удалось открыть файл", str(e))


def configure_table_view(view: QTableView, df: pd.DataFrame, mode: str = "default") -> None:
    header = view.horizontalHeader()
    header.setStretchLastSection(False)
    header.setSectionResizeMode(QHeaderView.Interactive)
    view.setWordWrap(True)

    fm = view.fontMetrics()
    sample_limit = min(len(df), 200)

    def max_text_length(series: pd.Series, default: str = "") -> int:
        values = [default]
        if sample_limit:
            values.extend("" if pd.isna(v) else str(v) for v in series.head(sample_limit))
        max_len = 0
        for value in values:
            for part in str(value).split("\n"):
                max_len = max(max_len, len(part))
        return max_len

    def compute_width(col: str) -> int:
        header_len = max(len(part) for part in col.split("\n"))
        content_len = max_text_length(df[col], col)
        effective_len = max(header_len, min(content_len, 80))
        pixel_width = fm.horizontalAdvance("W" * max(1, effective_len)) + 28

        if mode == "unit_diff" and col in {"Подраздел", "Наименование"}:
            wrapped_len = max(header_len, math.ceil(min(content_len, 150) / 3))
            wrapped_width = fm.horizontalAdvance("W" * max(1, wrapped_len)) + 28
            return max(250, min(560, wrapped_width))

        if col == "№":
            return 60
        if col in {"Код расценки", "№ позиции"}:
            return max(105, min(160, pixel_width))
        if col in {"Ед.изм.", "Ед.изм.\n(Проект)", "Ед.изм.\n(Факт)", "Единица измерения"}:
            return max(90, min(120, pixel_width))
        if "Кол-во" in col or "Количество" in col:
            return max(78, min(95, pixel_width))
        if "Ст-ть" in col or "Стоимость" in col or "Разница" in col:
            return max(105, min(145, pixel_width))
        if col == "Наименование":
            return max(320, min(560, pixel_width))
        if col in {"Раздел", "Название раздела"}:
            return max(150, min(260, pixel_width))
        if col == "Подраздел":
            return max(180, min(320, pixel_width))
        if col == "Файл":
            return max(260, min(520, pixel_width))
        return max(100, min(240, pixel_width))

    for idx, col_name in enumerate(df.columns):
        view.setColumnWidth(idx, compute_width(str(col_name)))

    view.resizeRowsToContents()


def prepare_compare_display_frames(cmp_obj, df_detail: pd.DataFrame, df_summary: pd.DataFrame, df_info: pd.DataFrame):
    short_names = {
        "Количество": "Кол-во",
        "Стоимость": "Ст-ть",
    }

    def shorten_metric(name: str) -> str:
        return short_names.get(name, name)

    def with_line_suffix(label: str, suffix: str) -> str:
        return f"{label}\n({suffix})"

    detail_renames = {}
    summary_renames = {}
    for value_name in cmp_obj.value_column:
        short_value = shorten_metric(value_name)
        detail_renames[f"{value_name} ({cmp_obj.file1_name})"] = with_line_suffix(short_value, "Проект")
        detail_renames[f"{value_name} ({cmp_obj.file2_name})"] = with_line_suffix(short_value, "Факт")
        detail_renames[f"Разница ({value_name})"] = f"Разница\n({short_value})"
    summary_renames[f"Стоимость ({cmp_obj.file1_name})"] = with_line_suffix("Ст-ть", "Проект")
    summary_renames[f"Стоимость ({cmp_obj.file2_name})"] = with_line_suffix("Ст-ть", "Факт")
    summary_renames["Разница (Стоимость)"] = "Разница\n(Ст-ть)"

    detail_view = df_detail.rename(columns=detail_renames)
    summary_view = df_summary.rename(columns=summary_renames)
    info_view = df_info.rename(columns={
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

    detail_view = detail_view.rename(columns={"Единица измерения": "Ед.изм."})

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
    detail_columns = [col for col in preferred_order if col in detail_view.columns]
    detail_columns.extend(col for col in detail_view.columns if col not in detail_columns)
    detail_view = detail_view[detail_columns]

    return detail_view, summary_view, info_view


def build_compare_files_frame(cmp_obj) -> pd.DataFrame:
    return pd.DataFrame(
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


def get_default_compare_column_order(columns: list[str]) -> list[str]:
    preferred = ["№", "Код расценки", "Наименование", "Единица измерения"]
    ordered = [col for col in preferred if col in columns]
    ordered.extend(col for col in columns if col not in ordered)
    return ordered

def load_materials_path() -> str | None:
    if os.path.exists(_MATERIALS_PREFS):
        try:
            with open(_MATERIALS_PREFS, "r", encoding="utf-8") as f:
                return json.load(f).get("materials_path")
        except Exception:
            pass
    return None


def save_materials_path(path: str):
    with open(_MATERIALS_PREFS, "w", encoding="utf-8") as f:
        json.dump({"materials_path": path}, f, ensure_ascii=False, indent=2)


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


from typing import List, Tuple, Optional
from PyQt5.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QComboBox,
    QListWidget, QListWidgetItem, QDialogButtonBox
)


class ColumnSelectDialog(QDialog):
    """Диалог выбора доп. колонок для сравнения."""

    def __init__(self, columns: List[str], parent=None):
        super().__init__(parent)
        self.setWindowTitle("Выбор колонок для сравнения")
        self.resize(450, 420)

        layout = QVBoxLayout(self)
        self.fixed_compare_column = "Наименование"
        self.fixed_value_columns = ["Количество", "Стоимость"]
        self.fixed_subsection_column = "Подраздел"
        self.default_extra_columns = {"Единица измерения", "Код расценки"}

        fixed_info = QLabel(
            "Колонка-ключ: Наименование\n"
            "Колонки сравнения: Количество, Стоимость\n"
            "Подраздел: Подраздел"
        )
        fixed_info.setWordWrap(True)
        layout.addWidget(fixed_info)

        # --- extra_column (множественный выбор) ---
        layout.addWidget(QLabel("Доп. колонки (можно отключить):"))
        self.extra_list = QListWidget()
        self.extra_list.setSelectionMode(QListWidget.MultiSelection)
        for c in columns:
            self.extra_list.addItem(QListWidgetItem(c))
        layout.addWidget(self.extra_list)

        # --- OK / Cancel ---
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self._on_accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

        # применяем сохранённые значения (если колонки существуют)
        self._apply_saved(columns)

    # --------------------------------------------------------------
    def _apply_saved(self, columns: List[str]):
        prefs = _load_prefs()
        saved_extra = prefs.get("extra_column", []) if prefs else []
        selected_extra = set(saved_extra) | self.default_extra_columns
        for i in range(self.extra_list.count()):
            item = self.extra_list.item(i)
            if item.text() in selected_extra:
                item.setSelected(True)

    # --------------------------------------------------------------
    def _on_accept(self):
        # сохраняем выбор
        prefs = {
            "compare_column": self.fixed_compare_column,
            "value_column": self.fixed_value_columns,
            "subsection_column": self.fixed_subsection_column,
            "extra_column": [i.text() for i in self.extra_list.selectedItems()],
        }
        _save_prefs(prefs)
        self.accept()

    # --------------------------------------------------------------
    @staticmethod
    def _row(label: str, widget):
        box = QHBoxLayout()
        box.addWidget(QLabel(label))
        box.addWidget(widget)
        return box

    # результат для вызывающего окна
    def get_selection(self) -> Tuple[str, List[str], List[str], Optional[str]]:
        extras = [i.text() for i in self.extra_list.selectedItems()]
        return (
            self.fixed_compare_column,
            self.fixed_value_columns,
            extras,
            self.fixed_subsection_column,
        )
# ╭──────────────────╮
# │  Поток обработки │
# ╰──────────────────╯
class ProcessWorker(QThread):
    progress = pyqtSignal(int, str)
    finished = pyqtSignal(pd.DataFrame)



    def __init__(self, files: List[str], materials_path: str | None):
        super().__init__()
        self.files = files
        self.materials_path = materials_path

    def run(self):
        merged = []
        for path in self.files:
            df = process_smeta(path, self.materials_path)
            merged.append(df)
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
        self.btn_html   = QPushButton("↗  Сохранить в HTML")
        for b in (self.btn_plain, self.btn_fact, self.btn_html): b.setEnabled(False)

        self.btn_plain.clicked.connect(self._save_plain)
        self.btn_fact.clicked.connect(self._save_fact)
        self.btn_html.clicked.connect(self._save_html)

        # выбор файла с матералами
        self.btn_materials = QPushButton("Файл цен на материалы (опционально)")
        self.btn_materials.clicked.connect(self._select_materials_file)


        self.materials_path = load_materials_path()

        lay = QVBoxLayout(self)
        lay.addWidget(self.widget)
        lay.addWidget(self.btn_materials)

        lay.addWidget(self.btn_plain)
        lay.addWidget(self.btn_fact)
        lay.addWidget(self.btn_html)

        self._df: pd.DataFrame | None = None

    # ------------ запуск фоновой обработки ------------
    def _start_process(self, files):
        if not files:
            QMessageBox.information(self, "Нет файлов", "Добавьте хотя бы один файл.")
            return
        self.widget.btn_run.setEnabled(False)
        for b in (self.btn_plain, self.btn_fact, self.btn_html): b.setEnabled(False)
        self.worker = ProcessWorker(files, self.materials_path)
        self.worker.finished.connect(self._show_result)
        self.worker.start()

    # ------------ выбор файла с ценами на материалы ------------
    def _select_materials_file(self):
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Файл цен на материалы",
            "",
            "Excel (*.xlsx *.xls)"
        )
        if path:
            self.materials_path = path
            save_materials_path(path)

    # ------------ результат ------------
    def _show_result(self, df: pd.DataFrame):
        self._df = df
        TableDialog(df, "Результат обработки").exec_()

        for b in (self.btn_plain, self.btn_fact, self.btn_html):
            b.setEnabled(True)

        self.widget.btn_run.setEnabled(True)

    # ------------ обычный Excel ------------
    def _save_plain2(self):
        if self._df is None: return
        path, _ = QFileDialog.getSaveFileName(self, "Excel-файл",
                                              with_timestamp("processed.xlsx"), "Excel (*.xlsx)")
        if path:
            try:
                self._df.to_excel(path, index=False, engine="openpyxl")
                offer_open_file(self, path)
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", str(e))

    def _save_plain(self):
        if self._df is None:
            return

        path, _ = QFileDialog.getSaveFileName(
            self, "Excel-файл", with_timestamp("processed.xlsx"), "Excel (*.xlsx)"
        )

        if not path:
            return

        try:
            export_df = self._df.copy()

            # Проверяем наличие необходимых столбцов
            if 'Подраздел' not in export_df.columns or 'Стоимость' not in export_df.columns:
                QMessageBox.warning(self, "Предупреждение",
                                    "Отсутствуют необходимые столбцы: 'Подраздел' или 'Стоимость'")
                return

            # Создаем категориальный тип с порядком первого вхождения
            export_df['Подраздел_порядок'] = pd.Categorical(
                export_df['Подраздел'],
                categories=export_df['Подраздел'].dropna().unique(),
                ordered=True
            )

            # Группируем с сохранением порядка
            sums_by_subsection = export_df.groupby('Подраздел_порядок', observed=True, sort=False)[
                'Стоимость'].sum().reset_index()
            sums_by_subsection.columns = ['Подраздел', 'Сумма стоимости']

            # Создаем Excel writer
            with pd.ExcelWriter(path, engine='openpyxl') as writer:
                # Сохраняем основной лист (убираем временный столбец)
                clean_df = export_df.drop('Подраздел_порядок', axis=1, errors='ignore')
                clean_df.to_excel(writer, sheet_name='Данные', index=False)
                ws = writer.sheets['Данные']

                apply_readable_sheet_layout(
                    ws,
                    clean_df,
                )

                # Добавляем строку ИТОГО
                total_sum = sums_by_subsection['Сумма стоимости'].sum()
                total_row = pd.DataFrame({
                    'Подраздел': ['ИТОГО(без НДС)'],
                    'Сумма стоимости': [total_sum]
                })
                sums_by_subsection = pd.concat([sums_by_subsection, total_row], ignore_index=True)

                # Сохраняем лист с суммами
                sums_by_subsection.to_excel(writer, sheet_name='Суммы по подразделу', index=False)
                #rows = materials_summary_by_object(self._df)
                #rows.to_excel(writer, sheet_name='Сводка по материалам', index=False)
                if self.materials_path and Path(self.materials_path).exists():
                    rows = materials_summary_by_object(self._df)
                    rows.to_excel(writer, sheet_name='Сводка по материалам', index=False)
                #logging.info(rows[1])

            offer_open_file(self, path)

        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось сохранить файл:\n{str(e)}")

    # ------------ Excel с формулами ------------
    def _save_fact(self):
        if self._df is None: return
        path, _ = QFileDialog.getSaveFileName(self, "Excel-файл",
                                              with_timestamp("processed_fact.xlsx"), "Excel (*.xlsx)")
        if path:
            try:
                export_with_fact_formula(self._df, path)
                offer_open_file(self, path)
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", str(e))

    def _save_html(self):
        if self._df is None:
            return
        path, _ = QFileDialog.getSaveFileName(
            self, "HTML-файл", with_timestamp("processed.html"), "HTML (*.html)"
        )
        if not path:
            return
        try:
            clean_df = self._df.drop('Подраздел_порядок', axis=1, errors='ignore')
            html_content = dataframe_to_readable_html(clean_df, title="Обработанная смета")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(html_content)
            QMessageBox.information(self, "Готово", f"HTML-файл сохранен:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", str(e))


def materials_summary_by_object(df: pd.DataFrame) -> pd.DataFrame:
    required = {"Стоимость материала, всего", "Материалы"}
    if not required.issubset(df.columns):
        return pd.DataFrame()
    rows = []

    for obj, g in df.groupby("Название объекта"):

        cost = g["Стоимость"].sum()
        smet_mat = g["Материалы"].sum()
        fakt_mat = g["Стоимость материала, всего"].sum()

        anal_mat = g.loc[
            g["Стоимость материала, всего"] > 0,
            "Материалы"
        ].sum()

        rows.append({
            "Название объекта": obj,
            "Стоимость": cost,
            "Материалы": smet_mat,
            "Фактические материалы": fakt_mat,
            "Проанализировано материалов": anal_mat/smet_mat
        })

        if smet_mat > 0:
            logging.info(
                "[%s] Материалы: %.2f | Факт: %.2f | Анализ: %.2f%%",
                obj,
                smet_mat,
                fakt_mat,
                anal_mat * 100 / smet_mat
            )

    return pd.DataFrame(rows)
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
        self.btn_diff = QPushButton("Excel исключаемые/добавляемые")
        self.btn_diff.setEnabled(False)
        self.btn_diff.clicked.connect(self._save_diff)


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
        lay.addWidget(self.btn_diff)

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
        cols = get_default_compare_column_order(list(df.columns))
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
        df_info = cmp_obj.generate_top_difference_report()
        df_unit_diff = cmp_obj.generate_unit_difference_report()
        df_detail, df_summary, df_info = prepare_compare_display_frames(
            cmp_obj,
            df_detail,
            df_summary,
            df_info,
        )
        df_unit_diff = df_unit_diff.rename(columns={
            "№": "№",
            "Ед. изм. (Проект)": "Ед.изм.\n(Проект)",
            "Ед. изм. (Факт)": "Ед.изм.\n(Факт)",
            "Кол-во (Проект)": "Кол-во\n(Проект)",
            "Кол-во (Факт)": "Кол-во\n(Факт)",
            "Ст-ть (Проект)": "Ст-ть\n(Проект)",
            "Ст-ть (Факт)": "Ст-ть\n(Факт)",
            "Разница (Ст-ть)": "Разница\n(Ст-ть)",
        })
        df_files = build_compare_files_frame(cmp_obj)

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
        configure_table_view(tv1, df_detail)
        lay1.addWidget(tv1)
        tabs.addTab(w1, "Детали")

        # вкладка «Сводка по подразделам»
        w2 = QWidget();
        lay2 = QVBoxLayout(w2)
        tv2 = QTableView();
        tv2.setModel(PandasModel(df_summary))
        configure_table_view(tv2, df_summary)
        lay2.addWidget(tv2)
        tabs.addTab(w2, "Сводка по подразделам")

        w3 = QWidget();
        lay3 = QVBoxLayout(w3)
        tv3 = QTableView();
        tv3.setModel(PandasModel(df_info))
        configure_table_view(tv3, df_info)
        lay3.addWidget(tv3)
        tabs.addTab(w3, "Инфо")

        w4 = QWidget();
        lay4 = QVBoxLayout(w4)
        tv4 = QTableView();
        tv4.setModel(PandasModel(df_files))
        configure_table_view(tv4, df_files)
        lay4.addWidget(tv4)
        tabs.addTab(w4, "Файлы")

        w5 = QWidget();
        lay5 = QVBoxLayout(w5)
        tv5 = QTableView();
        tv5.setModel(PandasModel(df_unit_diff))
        configure_table_view(tv5, df_unit_diff, mode="unit_diff")
        lay5.addWidget(tv5)
        tabs.addTab(w5, "Отличается ед. изм.")

        # собрать диалог
        dlg_layout = QVBoxLayout(dlg)
        dlg_layout.addWidget(tabs)
        dlg.exec_()

        for b in (self.btn_html, self.btn_excel, self.btn_missing, self.btn_diff):
            b.setEnabled(True)
        self.widget.btn_run.setEnabled(True)

    def _save_diff(self):
        if not hasattr(self, "_cmp") or self._cmp is None:
            QMessageBox.warning(self, "Ошибка", "Сначала нужно сравнить сметы.")
            return

        path, _ = QFileDialog.getSaveFileName(
            self,
            "Сохранить Excel",
            with_timestamp("added_removed.xlsx"),
            "Excel (*.xlsx)"
        )
        if path:
            try:
                self._cmp.export_added_removed_positions(path, value_col="Количество")
                offer_open_file(self, path)
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", str(e))
    # ────────────────────────────────────────────────
    #  экспортные кнопки
    # ────────────────────────────────────────────────
    def _export_html(self):
        if not self._cmp:
            return
        path, _ = QFileDialog.getSaveFileName(
            self, "HTML-файл", with_timestamp("customer_report.html"), "HTML (*.html)"
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
            self, "Excel-файл", with_timestamp("customer_report.xlsx"), "Excel (*.xlsx)"
        )
        if not path:
            return
        try:
            self._cmp.export_customer_excel(path)
            offer_open_file(self, path, label="Excel-отчёт сохранён")
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
from PyQt5.QtWidgets import QDialog

class TableDialog(QDialog):
    def __init__(self, df: pd.DataFrame, title: str):
        super().__init__()
        self.setWindowTitle(title); self.resize(1000, 700)

        view = QTableView()
        model = PandasModel(df); view.setModel(model)
        configure_table_view(view, df)

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
