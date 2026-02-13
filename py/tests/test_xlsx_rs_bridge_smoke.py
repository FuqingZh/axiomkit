from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pytest

# Ensure src-layout imports work when running tests from repo checkout.
SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

from axiomkit.io.xlsx._rs_bridge import (  # noqa: E402
    create_xlsx_writer_via_rs,
    is_rs_backend_available,
)


def test_xlsx_rs_bridge_smoke(tmp_path: Path) -> None:
    if not is_rs_backend_available():
        pytest.skip("Rust xlsx backend is unavailable")

    path_file_out = tmp_path / "smoke_rs.xlsx"

    with create_xlsx_writer_via_rs(str(path_file_out)) as inst_xlsx_writer:
        inst_xlsx_writer.write_sheet(
            pl.DataFrame({"a": [1, 2], "b": ["x", "y"]}),
            "Sheet1",
        )
        reports = inst_xlsx_writer.report()

    assert path_file_out.exists()
    assert path_file_out.stat().st_size > 0
    assert len(reports) == 1
    assert len(reports[0].sheets) == 1
    assert reports[0].warnings == []
