from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

from axiomkit.cli.parser import ActionNumericRange  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(prog="demo")


def test_non_negative_accepts_zero_and_rejects_negative() -> None:
    parser = _build_parser()
    parser.add_argument(
        "--thr",
        action=ActionNumericRange.non_negative(kind_value="float"),
    )

    ns = parser.parse_args(["--thr", "0"])
    assert ns.thr == 0.0

    with pytest.raises(SystemExit):
        parser.parse_args(["--thr", "-0.1"])


def test_positive_rejects_zero() -> None:
    parser = _build_parser()
    parser.add_argument(
        "--lr",
        action=ActionNumericRange.positive(kind_value="float"),
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
            if_inclusive_min=False, if_inclusive_max=True
        ),
    )

    with pytest.raises(SystemExit):
        parser.parse_args(["--x", "0"])

    ns = parser.parse_args(["--x", "1"])
    assert ns.x == 1.0
