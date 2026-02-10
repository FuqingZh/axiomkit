from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

from axiomkit.cli import EnumGroupKey, ParserBuilder, SpecParam  # noqa: E402


def _build_demo_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("--demo-flag", action="store_true")
    return parser


def _register_demo_params(app: ParserBuilder) -> None:
    app.register_param(
        SpecParam(
            id="executables.rscript",
            group=EnumGroupKey.EXECUTABLES,
            help="Path to Rscript executable",
            arg_builder=lambda g, s: s.add_argument(g, type=str),
        )
    )
    app.register_param(
        SpecParam(
            id="data_table.threads_dt",
            group=EnumGroupKey.PERFORMANCE,
            help="Thread count for data-table compute.",
            arg_builder=lambda g, s: s.add_argument(g, type=int),
        )
    )


def test_parser_builder_applies_param_keys_and_resolves_alias() -> None:
    app = ParserBuilder(prog="demo")
    _register_demo_params(app)
    app = app.add_command(
        id="demo",
        help="Demo command",
        arg_builder=_build_demo_args,
        aliases=("dm",),
        param_keys=(
            "executables.rscript",
            "data_table.threads_dt",
        ),
    )

    _ = app.build_parser()
    ns = app.parse_args(["dm", "--rscript", "Rscript", "--threads_dt", "4"])

    assert ns.command == "demo"
    assert ns._cmd_id == "demo"
    assert ns.rscript == "Rscript"
    assert ns.threads_dt == 4


def test_parser_builder_requires_param_registry_entries_for_param_keys() -> None:
    app = ParserBuilder(prog="demo").add_command(
        id="demo",
        help="Demo command",
        arg_builder=_build_demo_args,
        param_keys=("executables.rscript",),
    )

    with pytest.raises(ValueError, match="Unknown key/alias"):
        app.build_parser()


def test_param_dest_must_not_shadow_command_metadata_fields() -> None:
    app = ParserBuilder(prog="demo")
    app.register_param(
        SpecParam(
            id="general.cmd_meta",
            dest="_cmd_id",
            group=EnumGroupKey.GENERAL,
            arg_builder=lambda g, s: s.add_argument(g, type=str),
        )
    )
    app.add_command(
        id="demo",
        help="Demo command",
        arg_builder=_build_demo_args,
        param_keys=("general.cmd_meta",),
    )

    with pytest.raises(ValueError, match="reserved"):
        app.build_parser()


def test_param_flag_collision_is_rejected() -> None:
    app = ParserBuilder(prog="demo")
    app.select_group(EnumGroupKey.GENERAL).add_argument("--threads_dt", type=int)
    app.register_param(
        SpecParam(
            id="data_table.worker_threads",
            dest="worker_threads",
            flags=("--threads_dt",),
            group=EnumGroupKey.GENERAL,
            arg_builder=lambda g, s: s.add_argument(g, type=int),
        )
    )

    with pytest.raises(ValueError, match="flag already exists on parser"):
        app.apply_param_specs("data_table.worker_threads")


def test_extract_params_rejects_cross_group_param_keys() -> None:
    app = ParserBuilder(prog="demo")
    _register_demo_params(app)

    with pytest.raises(ValueError, match="belongs to group"):
        app.select_group(EnumGroupKey.INPUTS).extract_params("executables.rscript")


def test_deprecated_param_emits_warning() -> None:
    app = ParserBuilder(prog="demo")
    app.register_param(
        SpecParam(
            id="general.legacy_threads",
            group=EnumGroupKey.GENERAL,
            if_deprecated=True,
            replace_by="data_table.threads_dt",
            arg_builder=lambda g, s: s.add_argument(g, type=int),
        )
    )

    with pytest.warns(UserWarning, match="Deprecated param"):
        app.apply_param_specs("general.legacy_threads")
