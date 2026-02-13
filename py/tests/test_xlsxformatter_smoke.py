from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

# Ensure src-layout imports work when running tests from repo checkout.
SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

from axiomkit.io.xlsx import XlsxWriter  # noqa: E402


def test_write_sheet_smoke_creates_xlsx_and_records_report(tmp_path: Path) -> None:
    out_file = tmp_path / "smoke.xlsx"

    df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})

    with XlsxWriter(out_file) as xf:
        xf.write_sheet(df, "Sheet1")
        reports = xf.report()

        assert len(reports) == 1
        assert len(reports[0].sheets) == 1
        assert reports[0].warnings == []

    assert out_file.exists()
    assert out_file.stat().st_size > 0
