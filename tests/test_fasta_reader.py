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


def test_read_fasta_accepts_varargs_and_iterable_inputs(tmp_path) -> None:
    path_a = tmp_path / "a.fasta"
    path_b = tmp_path / "b.fasta"
    path_a.write_text(">sp|P00001|A_HUMAN A protein\nMPEPTIDE\n", encoding="utf-8")
    path_b.write_text(">sp|P00002|B_HUMAN B protein\nMPEPTIDER\n", encoding="utf-8")

    df_varargs = read_fasta(path_a, path_b)
    df_iterable = read_fasta([path_a, path_b])

    assert df_varargs.get_column("ProteinId").to_list() == ["P00001", "P00002"]
    assert df_iterable.get_column("ProteinId").to_list() == ["P00001", "P00002"]


def test_read_fasta_rejects_invalid_iterable_items(tmp_path) -> None:
    path_fasta = tmp_path / "proteins.fasta"
    path_fasta.write_text(">sp|P12345|FOO_HUMAN Foo protein\nMPEPTIDE\n", encoding="utf-8")

    with pytest.raises(TypeError, match="Unsupported file input type in iterable"):
        read_fasta([path_fasta, object()])


def test_read_fasta_deduplicates_by_first_input_order(tmp_path) -> None:
    path_a = tmp_path / "a.fasta"
    path_b = tmp_path / "b.fasta"
    path_a.write_text(
        ">sp|P00001|FIRST_HUMAN First protein\nMPEPTIDE\n",
        encoding="utf-8",
    )
    path_b.write_text(
        ">sp|P00001|SECOND_HUMAN Second protein\nMPEPTIDER\n",
        encoding="utf-8",
    )

    df_fasta = read_fasta(path_a, path_b)

    assert df_fasta.get_column("ProteinId").to_list() == ["P00001"]
    assert df_fasta.row(0, named=True)["ProteinSymbol"] == "FIRST_HUMAN"
