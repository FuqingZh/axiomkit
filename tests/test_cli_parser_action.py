from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from axiomkit.cli.parser import ActionNumericRange, ActionPath  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(prog="demo")


def test_non_negative_accepts_zero_and_rejects_negative() -> None:
    parser = _build_parser()
    parser.add_argument(
        "--thr",
        action=ActionNumericRange.non_negative(value_kind="float"),
    )

    ns = parser.parse_args(["--thr", "0"])
    assert ns.thr == 0.0

    with pytest.raises(SystemExit):
        parser.parse_args(["--thr", "-0.1"])


def test_positive_rejects_zero() -> None:
    parser = _build_parser()
    parser.add_argument(
        "--lr",
        action=ActionNumericRange.positive(value_kind="float"),
    )

    ns = parser.parse_args(["--lr", "0.01"])
    assert ns.lr == 0.01

    with pytest.raises(SystemExit):
        parser.parse_args(["--lr", "0"])


def test_unit_interval_defaults_to_closed_interval() -> None:
    parser = _build_parser()
    parser.add_argument(
        "--p",
        action=ActionNumericRange.unit_interval(),
    )

    ns_zero = parser.parse_args(["--p", "0"])
    ns_one = parser.parse_args(["--p", "1"])

    assert ns_zero.p == 0.0
    assert ns_one.p == 1.0


def test_unit_interval_supports_open_left_closed_right() -> None:
    parser = _build_parser()
    parser.add_argument(
        "--x",
        action=ActionNumericRange.unit_interval(
            should_include_min=False, should_include_max=True
        ),
    )

    with pytest.raises(SystemExit):
        parser.parse_args(["--x", "0"])

    ns = parser.parse_args(["--x", "1"])
    assert ns.x == 1.0


def test_action_path_dir_accepts_missing_writable_output_dir() -> None:
    parser = _build_parser()
    parser.add_argument(
        "--dir_out",
        action=ActionPath.dir(should_exist=False, is_writable=True),
    )

    ns = parser.parse_args(["--dir_out", "out"])
    assert ns.dir_out.name == "out"


def test_action_path_file_accepts_single_string_extension(tmp_path: Path) -> None:
    file_in = tmp_path / "go.obo"
    file_in.write_text("[Term]\n", encoding="utf-8")

    parser = _build_parser()
    parser.add_argument("--file_in", action=ActionPath.file("obo"))

    ns = parser.parse_args(["--file_in", str(file_in)])
    assert ns.file_in == file_in.resolve()


def test_action_path_file_accepts_variadic_extensions(tmp_path: Path) -> None:
    file_in = tmp_path / "table.tsv.gz"
    file_in.write_text("x\n", encoding="utf-8")

    parser = _build_parser()
    parser.add_argument("--file_in", action=ActionPath.file("tsv", "tsv.gz"))

    ns = parser.parse_args(["--file_in", str(file_in)])
    assert ns.file_in == file_in.resolve()
