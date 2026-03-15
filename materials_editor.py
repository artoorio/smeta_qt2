"""Advanced material catalog loader for both GUI and web contexts."""

from __future__ import annotations

import argparse
import logging
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

VAT_RATE_DEFAULT = 0.22

DEFAULT_MATERIAL_COLUMNS = [
    "Регион",
    "Наименование товара",
    "Единица измерения",
    "Цена за единицу измерения(без НДС)",
    "Цена за единицу измерения с НДС",
]

OPTIONAL_COLUMNS = {
    "Регион",
    "Цена за единицу измерения(без НДС)",
    "Цена за единицу измерения с НДС",
}

KEY_COLUMN = "Код расценки"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class ColumnMapper:
    """Helps normalize and rename incoming headers."""

    @staticmethod
    def normalize(dataframe: pd.DataFrame) -> pd.DataFrame:
        df = dataframe.copy()
        df.columns = [str(col).strip() for col in df.columns]
        return df

    @staticmethod
    def remap(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
        candidates = {str(k).strip(): v for k, v in mapping.items()}
        rename = {orig: dest for orig, dest in candidates.items() if orig in df.columns}
        return df.rename(columns=rename)


@dataclass
class MaterialCatalog:
    df: pd.DataFrame = field(default_factory=lambda: pd.DataFrame(columns=DEFAULT_MATERIAL_COLUMNS))
    vat_rate: float = VAT_RATE_DEFAULT

    @classmethod
    def from_excel(
        cls,
        path: Path,
        column_map: Optional[Dict[str, str]] = None,
        expect_code: bool = False,
    ) -> MaterialCatalog:
        raw = pd.read_excel(path)
        df = ColumnMapper.normalize(raw)
        if column_map:
            df = ColumnMapper.remap(df, column_map)

        missing_required = [c for c in DEFAULT_MATERIAL_COLUMNS if c not in df.columns and c not in OPTIONAL_COLUMNS]
        missing_optional = [c for c in DEFAULT_MATERIAL_COLUMNS if c not in df.columns and c in OPTIONAL_COLUMNS]
        if missing_required:
            raise ValueError(f"В файле {path.name} не найдены обязательные колонки: {', '.join(missing_required)}")
        for opt in missing_optional:
            df[opt] = pd.NA

        if expect_code and KEY_COLUMN not in df.columns:
            raise ValueError(f"Код расценки ({KEY_COLUMN}) обязателен, но не найден.")

        df = df[DEFAULT_MATERIAL_COLUMNS + ([KEY_COLUMN] if KEY_COLUMN in df.columns else [])]
        catalog = cls(df=df, vat_rate=VAT_RATE_DEFAULT)
        catalog._normalize_prices()
        return catalog

    def _normalize_prices(self):
        c_no = "Цена за единицу измерения(без НДС)"
        c_v = "Цена за единицу измерения с НДС"
        if c_no not in self.df.columns and c_v not in self.df.columns:
            return

        self.df[c_no] = pd.to_numeric(
            self.df.get(c_no, pd.Series(dtype=float)), errors="coerce"
        )
        self.df[c_v] = pd.to_numeric(
            self.df.get(c_v, pd.Series(dtype=float)), errors="coerce"
        )

        if c_no in self.df.columns and c_v in self.df.columns:
            mask_no = self.df[c_no].isna() & self.df[c_v].notna()
            self.df.loc[mask_no, c_no] = self.df.loc[mask_no, c_v] / (1.0 + self.vat_rate)
            mask_v = self.df[c_v].isna() & self.df[c_no].notna()
            self.df.loc[mask_v, c_v] = self.df.loc[mask_v, c_no] * (1.0 + self.vat_rate)
        elif c_no in self.df.columns:
            self.df[c_v] = self.df[c_no] * (1.0 + self.vat_rate)
        elif c_v in self.df.columns:
            self.df[c_no] = self.df[c_v] / (1.0 + self.vat_rate)

    def assign_codes(self, lookup: pd.DataFrame, code_column: str = "Код расценки") -> MaterialCatalog:
        if KEY_COLUMN in self.df.columns:
            logging.info("Готовые коды уже присутствуют в прайсе.")
            return self
        mapping = (
            lookup.copy()
            .assign(_key=lambda d: d[code_column].astype(str).str.strip())
            .set_index("_key")
        )
        self.df[KEY_COLUMN] = (
            self.df["Наименование товара"].astype(str).str.lower().map(mapping[code_column])
        )
        return self

    def to_sqlite(self, sqlite_path: Path, table_name: str = "materials") -> None:
        conn = sqlite3.connect(str(sqlite_path))
        try:
            self.df.to_sql(table_name, conn, if_exists="replace", index=False)
        finally:
            conn.close()

    def summary(self) -> Dict[str, Any]:
        price_col = "Цена за единицу измерения с НДС"
        total_price = float(self.df.get(price_col, pd.Series(dtype=float)).sum(skipna=True))
        return {
            "rows": len(self.df),
            "regions": self.df["Регион"].nunique(dropna=True),
            "total_price": total_price,
        }


class MaterialCatalogManager:
    def __init__(self, vat_rate: float = VAT_RATE_DEFAULT):
        self.vat_rate = vat_rate
        self.catalogs: Dict[str, MaterialCatalog] = {}

    def load(self, name: str, path: Path, column_map: Optional[Dict[str, str]] = None) -> MaterialCatalog:
        catalog = MaterialCatalog.from_excel(path, column_map=column_map)
        self.catalogs[name] = catalog
        logging.info("Загружен прайс %s (%d строк)", name, len(catalog.df))
        return catalog

    def export_combined(self, target: Path) -> None:
        frames = [catalog.df for catalog in self.catalogs.values()]
        if not frames:
            raise ValueError("Нет загруженных прайсов для экспорта.")
        pd.concat(frames, ignore_index=True).to_excel(target, index=False, engine="openpyxl")


def main() -> None:
    parser = argparse.ArgumentParser(description="Material catalog loader")
    parser.add_argument("--input", type=Path, help="Excel file with price data")
    parser.add_argument("--output-sqlite", type=Path, help="Store catalog in SQLite")
    parser.add_argument("--name", default="default", help="Catalog name")
    args = parser.parse_args()
    if not args.input:
        parser.print_help()
        return
    manager = MaterialCatalogManager()
    catalog = manager.load(args.name, args.input)
    logging.info("Сводка: rows=%d", len(catalog.df))
    if args.output_sqlite:
        catalog.to_sqlite(args.output_sqlite)


if __name__ == "__main__":
    main()
