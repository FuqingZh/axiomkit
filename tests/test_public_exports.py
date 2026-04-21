from __future__ import annotations

import pytest

import axiomkit
import axiomkit.cli as ak_cli
import axiomkit.io as ak_io


def test_root_exports_stable_subsystem_modules() -> None:
    assert axiomkit.io.__name__ == "axiomkit.io"
    assert axiomkit.cli.__name__ == "axiomkit.cli"
    assert axiomkit.stats.__name__ == "axiomkit.stats"
    assert axiomkit.runner.__name__ == "axiomkit.runner"
    assert axiomkit.workspace.__name__ == "axiomkit.workspace"


def test_root_deprecated_module_aliases_still_work_with_warning() -> None:
    with pytest.warns(DeprecationWarning, match="axiomkit.io_fasta"):
        io_fasta = axiomkit.io_fasta
    assert io_fasta.__name__ == "axiomkit.io.fasta"

    with pytest.warns(DeprecationWarning, match="axiomkit.cli_parser"):
        cli_parser = axiomkit.cli_parser
    assert cli_parser.__name__ == "axiomkit.cli.parser"


def test_io_is_namespace_only() -> None:
    assert ak_io.fasta.__name__ == "axiomkit.io.fasta"
    assert ak_io.fs.__name__ == "axiomkit.io.fs"

    with pytest.raises(AttributeError):
        _ = ak_io.read_fasta


def test_cli_is_namespace_first_with_deprecated_symbol_shims() -> None:
    assert ak_cli.console.__name__ == "axiomkit.cli.console"
    assert ak_cli.parser.__name__ == "axiomkit.cli.parser"

    with pytest.warns(DeprecationWarning, match="axiomkit.cli.ParserBuilder"):
        parser_builder = ak_cli.ParserBuilder
    assert parser_builder.__name__ == "ParserBuilder"

    with pytest.warns(DeprecationWarning, match="axiomkit.cli.CliHeadings"):
        cli_headings = ak_cli.CliHeadings
    assert cli_headings.__name__ == "CliHeadings"

    with pytest.raises(AttributeError):
        _ = ak_cli.ArgumentParser
