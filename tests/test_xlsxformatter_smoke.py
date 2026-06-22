from __future__ import annotations

import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import polars as pl
import pytest

from axiomkit.io.xlsx import XlsxWriter  # noqa: E402
from axiomkit.io.xlsx._rs_bridge import is_rs_backend_available  # noqa: E402

NS_MAIN = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


def _read_workbook_strings(path_xlsx: Path) -> list[str]:
    with zipfile.ZipFile(path_xlsx) as zf:
        values: list[str] = []
        try:
            value_xml = zf.read("xl/sharedStrings.xml")
        except KeyError:
            pass
        else:
            root = ET.fromstring(value_xml)
            for node_si in root.findall(".//m:si", NS_MAIN):
                nodes_text = node_si.findall(".//m:t", NS_MAIN)
                values.append("".join((node.text or "") for node in nodes_text))

        for name in zf.namelist():
            if not name.startswith("xl/worksheets/sheet") or not name.endswith(".xml"):
                continue
            root_sheet = ET.fromstring(zf.read(name))
            for node_cell in root_sheet.findall(".//m:c[@t='inlineStr']", NS_MAIN):
                nodes_text = node_cell.findall(".//m:is/m:t", NS_MAIN)
                values.append("".join((node.text or "") for node in nodes_text))
        return values


def test_write_sheet_smoke_creates_xlsx_and_records_report(tmp_path: Path) -> None:
    if not is_rs_backend_available():
        pytest.skip("Rust xlsx backend is unavailable")

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


def test_write_sheet_smoke_multiline_header_has_unquoted_strings(tmp_path: Path) -> None:
    if not is_rs_backend_available():
        pytest.skip("Rust xlsx backend is unavailable")

    out_file = tmp_path / "smoke_header.xlsx"

    df = pl.DataFrame({"_ProteinId": ["P1"], "_GO": ["GO:0001"]})
    header = pl.DataFrame(
        {
            "_ProteinId": ["蛋白 ID", "Protein ID"],
            "_GO": ["GO 注释", "GO"],
        }
    )

    with XlsxWriter(out_file) as xf:
        xf.write_sheet(df, "Sheet1", header=header)

    workbook_strings = _read_workbook_strings(out_file)
    assert "蛋白 ID" in workbook_strings
    assert "Protein ID" in workbook_strings
    assert '"蛋白 ID"' not in workbook_strings
    assert '"Protein ID"' not in workbook_strings
