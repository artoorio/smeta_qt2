"""
materials_editor.py

ТЕСТОВЫЙ модуль (отдельный файл) для управления справочниками:

1) Позиции сметы:
   - Код расценки
   - Наименование
   - Единица измерения

2) Прайс материалов (с регионом):
   - Регион
   - Наименование товара
   - Единица измерения
   - Цена за единицу измерения(без НДС)
   - Цена за единицу измерения с НДС

3) Связи many-to-many между позициями и материалами:
   - Код расценки
   - Регион
   - Наименование товара
   - Единица измерения   (ед. изм. товара, чтобы однозначно выбрать строку прайса)
   - Коэффициент перевода

Данные хранятся в Excel, но при загрузке формируется SQLite-кэш (для дальнейшего развития).

Запуск:
    python materials_editor.py
"""

from __future__ import annotations

import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

import pandas as pd
from PyQt5.QtCore import (
    QAbstractTableModel,
    QModelIndex,
    QSortFilterProxyModel,
    Qt,
)
from PyQt5.QtWidgets import (
    QApplication,
    QFileDialog,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTabWidget,
    QTableView,
    QVBoxLayout,
    QWidget,
)

VAT_RATE_DEFAULT = 0.22

POSITIONS_COLUMNS = ["Код расценки", "Наименование", "Единица измерения"]
MATERIALS_COLUMNS = [
    "Регион",
    "Наименование товара",
    "Единица измерения",
    "Цена за единицу измерения(без НДС)",
    "Цена за единицу измерения с НДС",
]
LINKS_COLUMNS = [
    "Код расценки",
    "Регион",
    "Наименование товара",
    "Единица измерения",
    "Коэффициент перевода",
]


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _ensure_columns(df: pd.DataFrame, required: Iterable[str], title: str) -> pd.DataFrame:
    df = _normalize_columns(df)
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{title}: не найдены колонки: {', '.join(missing)}")
    return df


def _coerce_price_columns(df: pd.DataFrame, vat_rate: float) -> pd.DataFrame:
    """Нормализует цены: пытается привести к float, заполняет отсутствующую колонку через НДС."""
    df = df.copy()
    c_no = "Цена за единицу измерения(без НДС)"
    c_v = "Цена за единицу измерения с НДС"

    def to_num(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series.astype(str).str.replace(" ", "").str.replace(",", "."), errors="coerce")

    if c_no in df.columns:
        df[c_no] = to_num(df[c_no])
    if c_v in df.columns:
        df[c_v] = to_num(df[c_v])

    if c_no in df.columns and c_v in df.columns:
        # если одно из значений пустое — попробуем вычислить
        mask_no = df[c_no].isna() & df[c_v].notna()
        df.loc[mask_no, c_no] = df.loc[mask_no, c_v] / (1.0 + vat_rate)
        mask_v = df[c_v].isna() & df[c_no].notna()
        df.loc[mask_v, c_v] = df.loc[mask_v, c_no] * (1.0 + vat_rate)
        return df

    raise ValueError("Таблица материалов должна содержать обе колонки цен (с НДС и без НДС).")


class EditablePandasModel(QAbstractTableModel):
    """Редактируемый DataFrame → Qt Model (без сложной типизации)."""

    def __init__(self, df: pd.DataFrame):
        super().__init__()
        self._df = df

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:  # type: ignore[override]
        return len(self._df)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:  # type: ignore[override]
        return self._df.shape[1]

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
        col_name = self._df.columns[col]

        # Попытка конвертировать в число для числовых колонок
        if pd.api.types.is_numeric_dtype(self._df[col_name]):
            try:
                if str(value).strip() == "":
                    new_val = pd.NA
                else:
                    new_val = float(str(value).replace(" ", "").replace(",", "."))
            except ValueError:
                return False
        else:
            new_val = str(value)

        self._df.at[row, col_name] = new_val
        self.dataChanged.emit(index, index, [Qt.DisplayRole, Qt.EditRole])
        return True

    def flags(self, index: QModelIndex) -> Qt.ItemFlags:  # type: ignore[override]
        if not index.isValid():
            return Qt.NoItemFlags
        return Qt.ItemIsSelectable | Qt.ItemIsEnabled | Qt.ItemIsEditable

    def headerData(  # type: ignore[override]
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

    def dataframe(self) -> pd.DataFrame:
        return self._df

    def set_dataframe(self, df: pd.DataFrame):
        self.beginResetModel()
        self._df = df
        self.endResetModel()


class LinksFilterProxyModel(QSortFilterProxyModel):
    """Фильтрует связи по коду позиции и региону (плюс текстовый фильтр от базового класса)."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._position_code: Optional[str] = None
        self._region: Optional[str] = None
        self.setFilterCaseSensitivity(Qt.CaseInsensitive)
        self.setFilterKeyColumn(-1)  # фильтровать по всем колонкам

    def set_position_code(self, code: Optional[str]):
        self._position_code = code or None
        self.invalidateFilter()

    def set_region(self, region: Optional[str]):
        self._region = region or None
        self.invalidateFilter()

    def filterAcceptsRow(self, source_row: int, source_parent: QModelIndex) -> bool:  # type: ignore[override]
        model = self.sourceModel()
        if model is None:
            return False

        # 1) фильтр по позиции
        if self._position_code:
            idx = model.index(source_row, LINKS_COLUMNS.index("Код расценки"), source_parent)
            if str(model.data(idx, Qt.DisplayRole) or "") != self._position_code:
                return False

        # 2) фильтр по региону
        if self._region and self._region != "Все регионы":
            idx = model.index(source_row, LINKS_COLUMNS.index("Регион"), source_parent)
            if str(model.data(idx, Qt.DisplayRole) or "") != self._region:
                return False

        # 3) текстовый фильтр
        return super().filterAcceptsRow(source_row, source_parent)


@dataclass
class DatasetPaths:
    positions_xlsx: Optional[Path] = None
    materials_xlsx: Optional[Path] = None
    links_xlsx: Optional[Path] = None
    sqlite_cache: Optional[Path] = None


class DatasetStore:
    def __init__(self, vat_rate: float = VAT_RATE_DEFAULT):
        self.vat_rate = vat_rate
        self.paths = DatasetPaths()

        self.positions_df = pd.DataFrame(columns=POSITIONS_COLUMNS)
        self.materials_df = pd.DataFrame(columns=MATERIALS_COLUMNS)
        self.links_df = pd.DataFrame(columns=LINKS_COLUMNS)

    # -----------------------------
    # Load / Save Excel
    # -----------------------------
    def load_positions_excel(self, path: Path):
        df = pd.read_excel(str(path))
        df = _ensure_columns(df, POSITIONS_COLUMNS, "Позиции")
        df = df[POSITIONS_COLUMNS].copy()
        df = df.dropna(how="all")
        self.positions_df = df
        self.paths.positions_xlsx = path

    def load_materials_excel(self, path: Path):
        df = pd.read_excel(str(path))
        df = _ensure_columns(df, MATERIALS_COLUMNS, "Материалы")
        df = df[MATERIALS_COLUMNS].copy()
        df = df.dropna(how="all")
        df = _coerce_price_columns(df, self.vat_rate)
        self.materials_df = df
        self.paths.materials_xlsx = path

    def load_links_excel_optional(self, path: Optional[Path]):
        if path is None:
            self.links_df = pd.DataFrame(columns=LINKS_COLUMNS)
            self.paths.links_xlsx = None
            return

        df = pd.read_excel(str(path))
        df = _ensure_columns(df, LINKS_COLUMNS, "Связи")
        df = df[LINKS_COLUMNS].copy()
        df = df.dropna(how="all")
        # коэффициент → число
        df["Коэффициент перевода"] = pd.to_numeric(
            df["Коэффициент перевода"].astype(str).str.replace(" ", "").str.replace(",", "."),
            errors="coerce",
        ).fillna(1.0)
        self.links_df = df
        self.paths.links_xlsx = path

    def save_all_to_excel(self):
        if not self.paths.positions_xlsx or not self.paths.materials_xlsx:
            raise ValueError("Не заданы пути к Excel для позиций и материалов.")

        self.positions_df.to_excel(str(self.paths.positions_xlsx), index=False, engine="openpyxl")
        self.materials_df.to_excel(str(self.paths.materials_xlsx), index=False, engine="openpyxl")

        # связи: если путь не был задан — создадим рядом с materials.xlsx
        if not self.paths.links_xlsx:
            base_dir = self.paths.materials_xlsx.parent
            self.paths.links_xlsx = base_dir / "links.xlsx"
        self.links_df.to_excel(str(self.paths.links_xlsx), index=False, engine="openpyxl")

    # -----------------------------
    # Import (upsert) from Excel
    # -----------------------------
    def import_positions(self, path: Path) -> Tuple[int, int]:
        """Возвращает (added, updated)."""
        df_new = pd.read_excel(str(path))
        df_new = _ensure_columns(df_new, POSITIONS_COLUMNS, "Импорт позиций")
        df_new = df_new[POSITIONS_COLUMNS].dropna(how="all").copy()

        df = self.positions_df.copy()
        df["__key__"] = df["Код расценки"].astype(str).str.strip()
        df_new["__key__"] = df_new["Код расценки"].astype(str).str.strip()

        existing = set(df["__key__"].tolist())
        added_rows = df_new[~df_new["__key__"].isin(existing)].copy()
        updated = 0

        # обновляем по ключу
        df_idx = {k: i for i, k in enumerate(df["__key__"].tolist())}
        for _, r in df_new[df_new["__key__"].isin(existing)].iterrows():
            i = df_idx[r["__key__"]]
            before = df.loc[i, POSITIONS_COLUMNS].astype(str).tolist()
            df.loc[i, POSITIONS_COLUMNS] = r[POSITIONS_COLUMNS].tolist()
            after = df.loc[i, POSITIONS_COLUMNS].astype(str).tolist()
            if before != after:
                updated += 1

        df = pd.concat([df, added_rows], ignore_index=True)
        df = df.drop(columns=["__key__"], errors="ignore")
        self.positions_df = df
        return (len(added_rows), updated)

    def import_materials(self, path: Path) -> Tuple[int, int]:
        """Возвращает (added, updated)."""
        df_new = pd.read_excel(str(path))
        df_new = _ensure_columns(df_new, MATERIALS_COLUMNS, "Импорт материалов")
        df_new = df_new[MATERIALS_COLUMNS].dropna(how="all").copy()
        df_new = _coerce_price_columns(df_new, self.vat_rate)

        df = self.materials_df.copy()
        df["__key__"] = (
            df["Регион"].astype(str).str.strip()
            + "||" + df["Наименование товара"].astype(str).str.strip()
            + "||" + df["Единица измерения"].astype(str).str.strip()
        )
        df_new["__key__"] = (
            df_new["Регион"].astype(str).str.strip()
            + "||" + df_new["Наименование товара"].astype(str).str.strip()
            + "||" + df_new["Единица измерения"].astype(str).str.strip()
        )

        existing = set(df["__key__"].tolist())
        added_rows = df_new[~df_new["__key__"].isin(existing)].copy()
        updated = 0

        df_idx = {k: i for i, k in enumerate(df["__key__"].tolist())}
        for _, r in df_new[df_new["__key__"].isin(existing)].iterrows():
            i = df_idx[r["__key__"]]
            before = df.loc[i, MATERIALS_COLUMNS].astype(str).tolist()
            df.loc[i, MATERIALS_COLUMNS] = r[MATERIALS_COLUMNS].tolist()
            after = df.loc[i, MATERIALS_COLUMNS].astype(str).tolist()
            if before != after:
                updated += 1

        df = pd.concat([df, added_rows], ignore_index=True)
        df = df.drop(columns=["__key__"], errors="ignore")
        self.materials_df = df
        return (len(added_rows), updated)

    # -----------------------------
    # SQLite cache
    # -----------------------------
    def rebuild_sqlite_cache(self, sqlite_path: Path) -> Tuple[int, int, int, int]:
        """
        Пересобирает sqlite-файл заново.
        Возвращает (positions_ok, materials_ok, links_ok, links_skipped).
        """
        if sqlite_path.exists():
            sqlite_path.unlink()

        con = sqlite3.connect(str(sqlite_path))
        try:
            con.execute("PRAGMA foreign_keys = ON;")
            con.executescript(
                """
                CREATE TABLE positions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL UNIQUE,
                    name TEXT,
                    unit TEXT
                );

                CREATE TABLE materials (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    region TEXT NOT NULL,
                    name TEXT NOT NULL,
                    unit TEXT NOT NULL,
                    price_no_vat REAL,
                    price_with_vat REAL,
                    UNIQUE(region, name, unit)
                );

                CREATE TABLE links (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    position_id INTEGER NOT NULL,
                    material_id INTEGER NOT NULL,
                    coef REAL NOT NULL DEFAULT 1.0,
                    UNIQUE(position_id, material_id),
                    FOREIGN KEY(position_id) REFERENCES positions(id) ON DELETE CASCADE,
                    FOREIGN KEY(material_id) REFERENCES materials(id) ON DELETE CASCADE
                );
                """
            )

            # positions
            pos_rows = []
            for _, r in self.positions_df.iterrows():
                code = str(r["Код расценки"]).strip()
                if not code or code.lower() == "nan":
                    continue
                pos_rows.append((code, str(r["Наименование"]), str(r["Единица измерения"])))
            con.executemany(
                "INSERT INTO positions(code, name, unit) VALUES(?, ?, ?)",
                pos_rows,
            )

            # materials
            mat_rows = []
            for _, r in self.materials_df.iterrows():
                region = str(r["Регион"]).strip()
                name = str(r["Наименование товара"]).strip()
                unit = str(r["Единица измерения"]).strip()
                if not region or not name or not unit:
                    continue
                mat_rows.append(
                    (
                        region,
                        name,
                        unit,
                        float(r["Цена за единицу измерения(без НДС)"]) if pd.notna(r["Цена за единицу измерения(без НДС)"]) else None,
                        float(r["Цена за единицу измерения с НДС"]) if pd.notna(r["Цена за единицу измерения с НДС"]) else None,
                    )
                )
            con.executemany(
                """
                INSERT INTO materials(region, name, unit, price_no_vat, price_with_vat)
                VALUES(?, ?, ?, ?, ?)
                """,
                mat_rows,
            )

            # map ids
            pos_map: Dict[str, int] = {}
            for row in con.execute("SELECT id, code FROM positions"):
                pos_map[row[1]] = int(row[0])

            mat_map: Dict[str, int] = {}
            for row in con.execute("SELECT id, region, name, unit FROM materials"):
                key = f"{row[1]}||{row[2]}||{row[3]}"
                mat_map[key] = int(row[0])

            # links
            links_ok = 0
            links_skipped = 0
            for _, r in self.links_df.iterrows():
                code = str(r["Код расценки"]).strip()
                region = str(r["Регион"]).strip()
                name = str(r["Наименование товара"]).strip()
                unit = str(r["Единица измерения"]).strip()
                coef = r.get("Коэффициент перевода", 1.0)
                try:
                    coef_f = float(coef)
                except Exception:
                    coef_f = 1.0

                pid = pos_map.get(code)
                mid = mat_map.get(f"{region}||{name}||{unit}")
                if not pid or not mid:
                    links_skipped += 1
                    continue

                con.execute(
                    "INSERT OR IGNORE INTO links(position_id, material_id, coef) VALUES(?, ?, ?)",
                    (pid, mid, coef_f),
                )
                links_ok += 1

            con.commit()
            self.paths.sqlite_cache = sqlite_path
            return (len(pos_rows), len(mat_rows), links_ok, links_skipped)
        finally:
            con.close()


class CatalogEditorWindow(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Редактор позиций / материалов / связей (Excel + SQLite)")
        self.resize(1200, 750)

        self.store = DatasetStore()

        # ---------------------------
        # Top controls
        # ---------------------------
        btn_open_positions = QPushButton("Открыть позиции…")
        btn_open_materials = QPushButton("Открыть прайс…")
        btn_open_links = QPushButton("Открыть связи…")
        btn_save_all = QPushButton("Сохранить всё в Excel")
        btn_build_sqlite = QPushButton("Пересобрать SQLite-кэш")

        btn_open_positions.clicked.connect(self._open_positions)
        btn_open_materials.clicked.connect(self._open_materials)
        btn_open_links.clicked.connect(self._open_links)
        btn_save_all.clicked.connect(self._save_all)
        btn_build_sqlite.clicked.connect(self._build_sqlite)

        self.lbl_status = QLabel("Файлы не загружены")
        self.lbl_status.setTextInteractionFlags(Qt.TextSelectableByMouse)

        top = QHBoxLayout()
        top.addWidget(btn_open_positions)
        top.addWidget(btn_open_materials)
        top.addWidget(btn_open_links)
        top.addWidget(btn_save_all)
        top.addWidget(btn_build_sqlite)
        top.addStretch(1)
        top.addWidget(self.lbl_status)

        # ---------------------------
        # Models + views
        # ---------------------------
        self.positions_model = EditablePandasModel(self.store.positions_df)
        self.materials_model = EditablePandasModel(self.store.materials_df)
        self.links_model = EditablePandasModel(self.store.links_df)

        # Proxies (search)
        self.positions_proxy = QSortFilterProxyModel(self)
        self.positions_proxy.setSourceModel(self.positions_model)
        self.positions_proxy.setFilterCaseSensitivity(Qt.CaseInsensitive)
        self.positions_proxy.setFilterKeyColumn(-1)

        self.materials_proxy = QSortFilterProxyModel(self)
        self.materials_proxy.setSourceModel(self.materials_model)
        self.materials_proxy.setFilterCaseSensitivity(Qt.CaseInsensitive)
        self.materials_proxy.setFilterKeyColumn(-1)

        self.links_proxy = LinksFilterProxyModel(self)
        self.links_proxy.setSourceModel(self.links_model)

        # UI widgets
        self.tabs = QTabWidget()
        self.tabs.addTab(self._build_positions_tab(), "Позиции")
        self.tabs.addTab(self._build_materials_tab(), "Материалы")
        self.tabs.addTab(self._build_links_tab(), "Связи")

        layout = QVBoxLayout(self)
        layout.addLayout(top)
        layout.addWidget(self.tabs)

        self._refresh_region_choices()
        self._update_status()

    # ------------------------------------------------------------
    # Tabs
    # ------------------------------------------------------------
    def _build_positions_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)

        search = QLineEdit()
        search.setPlaceholderText("Поиск по позициям (код / наименование / единица)…")
        search.textChanged.connect(lambda t: self.positions_proxy.setFilterFixedString(t))

        btn_import = QPushButton("Импорт новых позиций из Excel…")
        btn_import.clicked.connect(self._import_positions)

        header = QHBoxLayout()
        header.addWidget(search)
        header.addWidget(btn_import)

        self.positions_view = QTableView()
        self.positions_view.setModel(self.positions_proxy)
        self.positions_view.setSelectionBehavior(QTableView.SelectRows)
        self.positions_view.setAlternatingRowColors(True)
        self.positions_view.clicked.connect(self._on_position_selected_for_links)

        layout.addLayout(header)
        layout.addWidget(self.positions_view)
        return w

    def _build_materials_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)

        header = QHBoxLayout()

        self.materials_region = QLineEdit()
        self.materials_region.setPlaceholderText("Фильтр по региону (точное совпадение)…")
        self.materials_region.textChanged.connect(self._apply_materials_region_filter)

        search = QLineEdit()
        search.setPlaceholderText("Поиск по материалам…")
        search.textChanged.connect(lambda t: self.materials_proxy.setFilterFixedString(t))

        btn_import = QPushButton("Импорт новых материалов из Excel…")
        btn_import.clicked.connect(self._import_materials)

        header.addWidget(self.materials_region)
        header.addWidget(search)
        header.addWidget(btn_import)

        self.materials_view = QTableView()
        self.materials_view.setModel(self.materials_proxy)
        self.materials_view.setSelectionBehavior(QTableView.SelectRows)
        self.materials_view.setAlternatingRowColors(True)

        layout.addLayout(header)
        layout.addWidget(self.materials_view)
        return w

    def _build_links_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)

        # Верхняя панель: регион + поиск
        header = QHBoxLayout()

        self.links_region = QLineEdit()
        self.links_region.setPlaceholderText("Регион (точное совпадение; пусто = все)…")
        self.links_region.textChanged.connect(self._apply_links_region_filter)

        links_search = QLineEdit()
        links_search.setPlaceholderText("Поиск по связям…")
        links_search.textChanged.connect(lambda t: self.links_proxy.setFilterFixedString(t))

        btn_link = QPushButton("Связать выбранные (позиция + материал)")
        btn_unlink = QPushButton("Удалить выбранную связь")
        btn_link.clicked.connect(self._create_link_from_selection)
        btn_unlink.clicked.connect(self._delete_selected_link)

        header.addWidget(self.links_region)
        header.addWidget(links_search)
        header.addWidget(btn_link)
        header.addWidget(btn_unlink)

        # Три панели: позиции | материалы | связи (по выбранной позиции)
        splitter = QSplitter(Qt.Horizontal)

        # позиции
        left = QWidget()
        left_l = QVBoxLayout(left)
        left_l.setContentsMargins(0, 0, 0, 0)
        left_l.addWidget(QLabel("Позиции (выберите строку):"))
        self.positions_view_links = QTableView()
        self.positions_view_links.setModel(self.positions_proxy)
        self.positions_view_links.setSelectionBehavior(QTableView.SelectRows)
        self.positions_view_links.setAlternatingRowColors(True)
        self.positions_view_links.clicked.connect(self._on_position_selected_for_links)
        left_l.addWidget(self.positions_view_links)

        # материалы
        mid = QWidget()
        mid_l = QVBoxLayout(mid)
        mid_l.setContentsMargins(0, 0, 0, 0)
        mid_l.addWidget(QLabel("Материалы (выберите строку):"))
        self.materials_view_links = QTableView()
        self.materials_view_links.setModel(self.materials_proxy)
        self.materials_view_links.setSelectionBehavior(QTableView.SelectRows)
        self.materials_view_links.setAlternatingRowColors(True)
        mid_l.addWidget(self.materials_view_links)

        # связи
        right = QWidget()
        right_l = QVBoxLayout(right)
        right_l.setContentsMargins(0, 0, 0, 0)
        right_l.addWidget(QLabel("Связи (фильтр по выбранной позиции):"))
        self.links_view = QTableView()
        self.links_view.setModel(self.links_proxy)
        self.links_view.setSelectionBehavior(QTableView.SelectRows)
        self.links_view.setAlternatingRowColors(True)
        right_l.addWidget(self.links_view)

        splitter.addWidget(left)
        splitter.addWidget(mid)
        splitter.addWidget(right)
        splitter.setStretchFactor(0, 2)
        splitter.setStretchFactor(1, 2)
        splitter.setStretchFactor(2, 3)

        layout.addLayout(header)
        layout.addWidget(splitter)
        return w

    # ------------------------------------------------------------
    # Helpers: selection / filters
    # ------------------------------------------------------------
    def _apply_materials_region_filter(self):
        region = self.materials_region.text().strip()
        if not region:
            self.materials_proxy.setFilterFixedString(self.materials_proxy.filterRegExp().pattern())
            return
        # простой способ: текстовый фильтр по всем колонкам; регион — точное совпадение в отдельной логике
        # чтобы не усложнять, используем regex "^{region}$" по колонке региона
        self.materials_proxy.setFilterKeyColumn(MATERIALS_COLUMNS.index("Регион"))
        self.materials_proxy.setFilterFixedString(region)
        self.materials_proxy.setFilterKeyColumn(-1)

    def _apply_links_region_filter(self):
        region = self.links_region.text().strip()
        self.links_proxy.set_region(region if region else None)

    def _on_position_selected_for_links(self):
        code = self._get_selected_position_code()
        self.links_proxy.set_position_code(code)

    def _get_selected_position_code(self) -> Optional[str]:
        view = getattr(self, "positions_view_links", None) or getattr(self, "positions_view", None)
        if view is None:
            return None
        idxs = view.selectionModel().selectedRows()
        if not idxs:
            return None
        # idxs in proxy → map to source
        proxy_index = idxs[0]
        src = self.positions_proxy.mapToSource(proxy_index)
        df = self.positions_model.dataframe()
        try:
            code = str(df.iloc[src.row()]["Код расценки"]).strip()
            return code
        except Exception:
            return None

    def _get_selected_position_row(self) -> Optional[pd.Series]:
        idxs = self.positions_view_links.selectionModel().selectedRows()
        if not idxs:
            return None
        src = self.positions_proxy.mapToSource(idxs[0])
        return self.positions_model.dataframe().iloc[src.row()]

    def _get_selected_material_row(self) -> Optional[pd.Series]:
        idxs = self.materials_view_links.selectionModel().selectedRows()
        if not idxs:
            return None
        src = self.materials_proxy.mapToSource(idxs[0])
        return self.materials_model.dataframe().iloc[src.row()]

    # ------------------------------------------------------------
    # Actions: open/save/import/build sqlite
    # ------------------------------------------------------------
    def _open_positions(self):
        path_str, _ = QFileDialog.getOpenFileName(self, "Открыть позиции", "", "Excel (*.xlsx *.xls)")
        if not path_str:
            return
        try:
            self.store.load_positions_excel(Path(path_str))
            self.positions_model.set_dataframe(self.store.positions_df)
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", str(e))
            return
        self._update_status()

    def _open_materials(self):
        path_str, _ = QFileDialog.getOpenFileName(self, "Открыть прайс материалов", "", "Excel (*.xlsx *.xls)")
        if not path_str:
            return
        try:
            self.store.load_materials_excel(Path(path_str))
            self.materials_model.set_dataframe(self.store.materials_df)
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", str(e))
            return
        self._refresh_region_choices()
        self._update_status()

    def _open_links(self):
        path_str, _ = QFileDialog.getOpenFileName(
            self, "Открыть связи (можно отменить, будет пусто)", "", "Excel (*.xlsx *.xls)"
        )
        path = Path(path_str) if path_str else None
        try:
            self.store.load_links_excel_optional(path)
            self.links_model.set_dataframe(self.store.links_df)
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", str(e))
            return
        self._update_status()

    def _save_all(self):
        try:
            # забираем актуальные df из моделей
            self.store.positions_df = self.positions_model.dataframe()
            self.store.materials_df = self.materials_model.dataframe()
            self.store.links_df = self.links_model.dataframe()

            self.store.save_all_to_excel()
            QMessageBox.information(self, "Готово", "Файлы Excel сохранены.")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка сохранения", str(e))
            return

        # после сохранения — обновим sqlite
        self._build_sqlite(silent=True)
        self._update_status()

    def _build_sqlite(self, silent: bool = False):
        # sqlite рядом с файлом прайса (если он открыт), иначе рядом со скриптом
        if self.store.paths.materials_xlsx:
            sqlite_path = self.store.paths.materials_xlsx.parent / "catalog_cache.sqlite"
        else:
            sqlite_path = Path(__file__).with_name("catalog_cache.sqlite")

        try:
            self.store.positions_df = self.positions_model.dataframe()
            self.store.materials_df = self.materials_model.dataframe()
            self.store.links_df = self.links_model.dataframe()
            pos_ok, mat_ok, links_ok, skipped = self.store.rebuild_sqlite_cache(sqlite_path)
        except Exception as e:
            if not silent:
                QMessageBox.critical(self, "Ошибка SQLite", str(e))
            return

        if not silent:
            QMessageBox.information(
                self,
                "SQLite-кэш обновлён",
                f"positions: {pos_ok}\nmaterials: {mat_ok}\nlinks: {links_ok}\nskipped links: {skipped}\n\n{sqlite_path}",
            )
        self._update_status()

    def _import_positions(self):
        path_str, _ = QFileDialog.getOpenFileName(self, "Импорт новых позиций", "", "Excel (*.xlsx *.xls)")
        if not path_str:
            return
        try:
            self.store.positions_df = self.positions_model.dataframe()
            added, updated = self.store.import_positions(Path(path_str))
            self.positions_model.set_dataframe(self.store.positions_df)
            QMessageBox.information(self, "Импорт позиций", f"Добавлено: {added}\nОбновлено: {updated}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка импорта", str(e))
        self._update_status()

    def _import_materials(self):
        path_str, _ = QFileDialog.getOpenFileName(self, "Импорт новых материалов", "", "Excel (*.xlsx *.xls)")
        if not path_str:
            return
        try:
            self.store.materials_df = self.materials_model.dataframe()
            added, updated = self.store.import_materials(Path(path_str))
            self.materials_model.set_dataframe(self.store.materials_df)
            self._refresh_region_choices()
            QMessageBox.information(self, "Импорт материалов", f"Добавлено: {added}\nОбновлено: {updated}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка импорта", str(e))
        self._update_status()

    # ------------------------------------------------------------
    # Links: create/delete
    # ------------------------------------------------------------
    def _create_link_from_selection(self):
        pos = self._get_selected_position_row()
        mat = self._get_selected_material_row()
        if pos is None or mat is None:
            QMessageBox.information(self, "Выбор", "Выберите одну позицию и один материал.")
            return

        code = str(pos["Код расценки"]).strip()
        pos_unit = str(pos["Единица измерения"]).strip()

        region = str(mat["Регион"]).strip()
        mat_name = str(mat["Наименование товара"]).strip()
        mat_unit = str(mat["Единица измерения"]).strip()

        coef = 1.0
        if pos_unit and mat_unit and pos_unit != mat_unit:
            coef, ok = QInputDialog.getDouble(
                self,
                "Коэффициент перевода",
                f"Ед. позиции: {pos_unit}\nЕд. товара: {mat_unit}\n\nВведите коэффициент перевода:",
                1.0,
                0.0,
                1e12,
                6,
            )
            if not ok:
                return

        # upsert в links_df по ключу (code, region, name, unit)
        df = self.links_model.dataframe().copy()
        if df.empty:
            df = pd.DataFrame(columns=LINKS_COLUMNS)

        key = (code, region, mat_name, mat_unit)
        mask = (
            (df["Код расценки"].astype(str).str.strip() == key[0])
            & (df["Регион"].astype(str).str.strip() == key[1])
            & (df["Наименование товара"].astype(str).str.strip() == key[2])
            & (df["Единица измерения"].astype(str).str.strip() == key[3])
        )

        if mask.any():
            df.loc[mask, "Коэффициент перевода"] = float(coef)
        else:
            df = pd.concat(
                [
                    df,
                    pd.DataFrame(
                        [
                            {
                                "Код расценки": code,
                                "Регион": region,
                                "Наименование товара": mat_name,
                                "Единица измерения": mat_unit,
                                "Коэффициент перевода": float(coef),
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )

        self.links_model.set_dataframe(df)
        self.store.links_df = df
        self._on_position_selected_for_links()

    def _delete_selected_link(self):
        idxs = self.links_view.selectionModel().selectedRows()
        if not idxs:
            QMessageBox.information(self, "Удаление", "Выберите связь в таблице справа.")
            return

        # Удаляем из source df по строке source
        proxy_idx = idxs[0]
        src = self.links_proxy.mapToSource(proxy_idx)
        df = self.links_model.dataframe().copy()
        df = df.drop(df.index[src.row()]).reset_index(drop=True)
        self.links_model.set_dataframe(df)
        self.store.links_df = df

    # ------------------------------------------------------------
    # Misc
    # ------------------------------------------------------------
    def _refresh_region_choices(self):
        # здесь пока только «подсветка» — фильтр по региону делаем текстовым полем
        pass

    def _update_status(self):
        p = self.store.paths.positions_xlsx
        m = self.store.paths.materials_xlsx
        l = self.store.paths.links_xlsx
        s = self.store.paths.sqlite_cache
        self.lbl_status.setText(
            " | ".join(
                [
                    f"positions: {p.name if p else '—'}",
                    f"materials: {m.name if m else '—'}",
                    f"links: {l.name if l else '—'}",
                    f"sqlite: {s.name if s else '—'}",
                ]
            )
        )


def main():
    app = QApplication(sys.argv)
    win = CatalogEditorWindow()
    win.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()

