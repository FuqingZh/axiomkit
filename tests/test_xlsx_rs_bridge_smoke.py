from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from axiomkit.io.xlsx import _axiomkit_io_xlsx_rs  # noqa: E402
from axiomkit.io.xlsx._rs_bridge import (  # noqa: E402
    EXPECTED_BRIDGE_ABI,
    EXPECTED_BRIDGE_CONTRACT,
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


def test_xlsx_rs_bridge_contract_constants_match() -> None:
    if not is_rs_backend_available():
        pytest.skip("Rust xlsx backend is unavailable")

    assert _axiomkit_io_xlsx_rs.__bridge_abi__ == EXPECTED_BRIDGE_ABI
    assert _axiomkit_io_xlsx_rs.__bridge_contract__ == EXPECTED_BRIDGE_CONTRACT
