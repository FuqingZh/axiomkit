from __future__ import annotations

from pathlib import Path

import pytest

from axiomkit.cli.parser import (  # noqa: E402
    ActionCommandPrefix,
    ActionHexColor,
    ActionNumericRange,
    ActionPath,
)
from axiomkit.cli.parser.runtime import ArgumentParser  # noqa: E402


def _build_parser() -> ArgumentParser:
    return ArgumentParser(prog="demo")


def _assert_help_renders(parser: ArgumentParser) -> None:
    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(["--help"])
    assert exc_info.value.code == 0


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


def test_action_path_lazy_default_keeps_help_renderable() -> None:
    parser = _build_parser()
    parser.add_argument(
        "--file_in",
        action=ActionPath.file("obo"),
        default="missing.obo",
    )

    _assert_help_renders(parser)


def test_action_path_lazy_default_fails_without_explicit_override() -> None:
    parser = _build_parser()
    parser.add_argument(
        "--file_in",
        action=ActionPath.file("obo"),
        default="missing.obo",
    )

    with pytest.raises(SystemExit):
        parser.parse_args([])


def test_action_path_lazy_default_explicit_override_wins(tmp_path: Path) -> None:
    file_in = tmp_path / "go.obo"
    file_in.write_text("[Term]\n", encoding="utf-8")

    parser = _build_parser()
    parser.add_argument(
        "--file_in",
        action=ActionPath.file("obo"),
        default="missing.obo",
    )

    ns = parser.parse_args(["--file_in", str(file_in)])
    assert ns.file_in == file_in.resolve()


def test_action_path_lazy_default_normalizes_valid_default(tmp_path: Path) -> None:
    file_in = tmp_path / "go.obo"
    file_in.write_text("[Term]\n", encoding="utf-8")

    parser = _build_parser()
    parser.add_argument(
        "--file_in",
        action=ActionPath.file("obo"),
        default=str(file_in),
    )

    ns = parser.parse_args([])
    assert ns.file_in == file_in.resolve()


def test_action_command_prefix_lazy_default_help_and_failure() -> None:
    parser = _build_parser()
    parser.add_argument(
        "--rule_exec_prefix",
        action=ActionCommandPrefix,
        default="definitely_missing_command_xyz",
    )

    _assert_help_renders(parser)

    with pytest.raises(SystemExit):
        parser.parse_args([])


def test_action_command_prefix_lazy_default_normalizes_valid_default() -> None:
    parser = _build_parser()
    parser.add_argument(
        "--rule_exec_prefix",
        action=ActionCommandPrefix,
        default="bash -lc",
    )

    ns = parser.parse_args([])
    assert ns.rule_exec_prefix == ("bash", "-lc")


def test_action_hex_color_lazy_default_help_and_failure() -> None:
    parser = _build_parser()
    parser.add_argument(
        "--panel_border_color",
        action=ActionHexColor,
        default="#12",
    )

    _assert_help_renders(parser)

    with pytest.raises(SystemExit):
        parser.parse_args([])


def test_action_hex_color_lazy_default_normalizes_valid_default() -> None:
    parser = _build_parser()
    parser.add_argument(
        "--panel_border_color",
        action=ActionHexColor,
        default="#33aaFF",
    )

    ns = parser.parse_args([])
    assert ns.panel_border_color == "#33AAFF"


def test_action_numeric_range_lazy_default_help_and_failure() -> None:
    parser = _build_parser()
    parser.add_argument(
        "--thr",
        action=ActionNumericRange.non_negative(value_kind="float"),
        default="-1",
    )

    _assert_help_renders(parser)

    with pytest.raises(SystemExit):
        parser.parse_args([])


def test_action_numeric_range_lazy_default_normalizes_valid_default() -> None:
    parser = _build_parser()
    parser.add_argument(
        "--thr",
        action=ActionNumericRange.non_negative(value_kind="float"),
        default="1.5",
    )

    ns = parser.parse_args([])
    assert ns.thr == 1.5


def test_argument_parser_parse_known_args_finalizes_lazy_defaults(
    tmp_path: Path,
) -> None:
    file_in = tmp_path / "go.obo"
    file_in.write_text("[Term]\n", encoding="utf-8")

    parser = _build_parser()
    parser.add_argument(
        "--file_in",
        action=ActionPath.file("obo"),
        default=str(file_in),
    )

    ns, extras = parser.parse_known_args([])
    assert extras == []
    assert ns.file_in == file_in.resolve()
