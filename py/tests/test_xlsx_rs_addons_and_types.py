from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pytest

# Ensure src-layout imports work when running tests from repo checkout.
SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

from axiomkit.io.xlsx import XlsxWriter  # noqa: E402
from axiomkit.io.xlsx._rs_bridge import is_rs_backend_available  # noqa: E402
from axiomkit.io.xlsx.spec import (  # noqa: E402
    SpecSheetSlice,
    SpecXlsxReport,
)


def test_xlsx_rs_report_types_align_spec(tmp_path: Path) -> None:
    if not is_rs_backend_available():
        pytest.skip("Rust xlsx backend is unavailable")

    path_file_out = tmp_path / "types.xlsx"

    with XlsxWriter(path_file_out) as inst_xlsx_writer:
        inst_xlsx_writer.write_sheet(pl.DataFrame({"a": [1, 2], "b": ["x", "y"]}), "S")
        reports = inst_xlsx_writer.report()

    assert isinstance(reports, tuple)
    assert len(reports) == 1
    assert isinstance(reports[0], SpecXlsxReport)
    assert len(reports[0].sheets) == 1
    assert isinstance(reports[0].sheets[0], SpecSheetSlice)


def test_xlsx_rs_writer_no_longer_accepts_addons(tmp_path: Path) -> None:
    if not is_rs_backend_available():
        pytest.skip("Rust xlsx backend is unavailable")

    with XlsxWriter(tmp_path / "no_addons.xlsx") as inst_xlsx_writer:
        with pytest.raises(TypeError):
            inst_xlsx_writer.write_sheet(  # type: ignore[call-arg]
                pl.DataFrame({"a": [1]}),
                "S",
                addons=(),
            )
