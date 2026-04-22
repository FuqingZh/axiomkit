from __future__ import annotations

import pytest

from axiomkit.io.fasta import read_fasta


def test_read_fasta_parses_header_with_built_in_rules(tmp_path) -> None:
    path_fasta = tmp_path / "proteins.fasta"
    path_fasta.write_text(
        ">sp|P12345|FOO_HUMAN Foo protein OS=Homo sapiens GN=FOO\n"
        "MPEPTIDE\n",
        encoding="utf-8",
    )

    df_fasta = read_fasta(path_fasta)

    assert df_fasta.columns == [
        "File",
        "ProteinId",
        "ProteinSymbol",
        "ProteinName",
        "GeneSymbol",
        "MWKDa",
        "Length",
    ]
    row = df_fasta.row(0, named=True)
    assert row["ProteinId"] == "P12345"
    assert row["ProteinSymbol"] == "FOO_HUMAN"
    assert row["ProteinName"] == "Foo protein"
    assert row["GeneSymbol"] == "FOO"
    assert row["Length"] == 8


def test_read_fasta_no_longer_accepts_rules_fallback(tmp_path) -> None:
    path_fasta = tmp_path / "proteins.fasta"
    path_fasta.write_text(">sp|P12345|FOO_HUMAN Foo protein\nMPEPTIDE\n", encoding="utf-8")

    with pytest.raises(TypeError, match="rules_fallback"):
        read_fasta(path_fasta, rules_fallback=False)
