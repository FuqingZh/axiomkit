from __future__ import annotations

import argparse
from enum import StrEnum

import pytest

from axiomkit.cli.parser import EnumGroupKey, ParserBuilder, SpecParam  # noqa: E402


class EnumParamKey(StrEnum):
    EXE_RSCRIPT = "executables.rscript"
    THR_THREADS = "data_table.threads_dt"


def _build_demo_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("--demo-flag", action="store_true")
    return parser


def _register_demo_params(app: ParserBuilder) -> None:
    app.register_params(
        SpecParam(
            id="executables.rscript",
            group=EnumGroupKey.EXECUTABLES,
            help="Path to Rscript executable",
            arg_builder=lambda g, s: s.add_argument(g, type=str),
        ),
        SpecParam(
            id="data_table.threads_dt",
            group=EnumGroupKey.PERFORMANCE,
            help="Thread count for data-table compute.",
            arg_builder=lambda g, s: s.add_argument(g, type=int),
        ),
    )


def test_parser_builder_applies_param_keys() -> None:
    app = ParserBuilder(prog="demo")
    _register_demo_params(app)
    app = app.command(
        "demo",
        help="Demo command",
        arg_builder=_build_demo_args,
        param_keys=(
            "executables.rscript",
            "data_table.threads_dt",
        ),
    ).done()

    parser = app.build()
    ns = parser.parse_args(["demo", "--rscript", "Rscript", "--threads_dt", "4"])

    assert ns.command == "demo"
    assert ns.rscript == "Rscript"
    assert ns.threads_dt == 4


def test_parser_builder_requires_param_registry_entries_for_param_keys() -> None:
    app = ParserBuilder(prog="demo").command(
        "demo",
        help="Demo command",
        arg_builder=_build_demo_args,
        param_keys=("executables.rscript",),
    ).done()

    with pytest.raises(ValueError, match="Register it first"):
        app.build()


def test_param_dest_must_not_shadow_command_metadata_fields() -> None:
    app = ParserBuilder(prog="demo")
    app.register_params(
        SpecParam(
            id="general.cmd_meta",
            dest="_cmd_group",
            group=EnumGroupKey.GENERAL,
            arg_builder=lambda g, s: s.add_argument(g, type=str),
        )
    )
    app.command(
        "demo",
        help="Demo command",
        arg_builder=_build_demo_args,
        param_keys=("general.cmd_meta",),
    ).done()

    with pytest.raises(ValueError, match="reserved"):
        app.build()


def test_param_flag_collision_is_rejected() -> None:
    app = ParserBuilder(prog="demo")
    app.select_group(EnumGroupKey.GENERAL).add_argument("--threads_dt", type=int)
    app.register_params(
        SpecParam(
            id="data_table.worker_threads",
            dest="worker_threads",
            flags=("--threads_dt",),
            group=EnumGroupKey.GENERAL,
            arg_builder=lambda g, s: s.add_argument(g, type=int),
        )
    )

    with pytest.raises(ValueError, match="flag already exists on parser"):
        app.select_group(EnumGroupKey.GENERAL).extract_params("data_table.worker_threads")


def test_extract_params_rejects_cross_group_param_keys() -> None:
    app = ParserBuilder(prog="demo")
    _register_demo_params(app)

    with pytest.raises(ValueError, match="belongs to group"):
        app.select_group(EnumGroupKey.INPUTS).extract_params("executables.rscript")


def test_deprecated_param_emits_warning() -> None:
    app = ParserBuilder(prog="demo")
    app.register_params(
        SpecParam(
            id="general.legacy_threads",
            group=EnumGroupKey.GENERAL,
            is_deprecated=True,
            replace_by="data_table.threads_dt",
            arg_builder=lambda g, s: s.add_argument(g, type=int),
        )
    )

    with pytest.warns(UserWarning, match="Deprecated param"):
        app.select_group(EnumGroupKey.GENERAL).extract_params("general.legacy_threads")


def test_fluent_dsl_supports_multi_command_grouped_build() -> None:
    app = ParserBuilder(prog="demo")
    _register_demo_params(app)

    (
        app.command("t_test", help="T-test command")
        .group(EnumGroupKey.EXECUTABLES)
        .extract_params("executables.rscript")
        .end()
        .group(EnumGroupKey.INPUTS)
        .add_argument("--file-in", type=str, required=True)
        .end()
        .done()
    )
    (
        app.command("anova", help="ANOVA command")
        .group(EnumGroupKey.PERFORMANCE)
        .extract_params("data_table.threads_dt")
        .end()
        .done()
    )

    parser = app.build()

    ns_t = parser.parse_args(
        ["t_test", "--rscript", "Rscript", "--file-in", "in.parquet"]
    )
    assert ns_t.command == "t_test"
    assert ns_t.rscript == "Rscript"
    assert ns_t.file_in == "in.parquet"

    ns_a = parser.parse_args(["anova", "--threads_dt", "8"])
    assert ns_a.command == "anova"
    assert ns_a.threads_dt == 8


def test_fluent_dsl_extract_params_rejects_cross_group_param_keys() -> None:
    app = ParserBuilder(prog="demo")
    _register_demo_params(app)

    (
        app.command("demo", help="Demo command")
        .group(EnumGroupKey.INPUTS)
        .extract_params("executables.rscript")
        .end()
        .done()
    )

    with pytest.raises(ValueError, match="belongs to group"):
        app.build()


def test_extract_params_supports_str_enum_keys() -> None:
    app = ParserBuilder(prog="demo")
    _register_demo_params(app)

    (
        app.command("demo", help="Demo command")
        .group(EnumGroupKey.EXECUTABLES)
        .extract_params(EnumParamKey.EXE_RSCRIPT)
        .end()
        .group(EnumGroupKey.PERFORMANCE)
        .extract_params(EnumParamKey.THR_THREADS)
        .end()
        .done()
    )

    parser = app.build()
    ns = parser.parse_args(["demo", "--rscript", "Rscript", "--threads_dt", "2"])

    assert ns.command == "demo"
    assert ns.rscript == "Rscript"
    assert ns.threads_dt == 2


def test_build_accepts_should_require_command_false() -> None:
    app = ParserBuilder(prog="demo").command(
        "demo",
        help="Demo command",
        arg_builder=_build_demo_args,
    ).done()

    parser = app.build(should_require_command=False)
    ns = parser.parse_args([])

    assert ns.command is None


def test_registries_accept_should_sort_false() -> None:
    app = ParserBuilder(prog="demo")
    _register_demo_params(app)
    app.command("b_cmd", help="B command", arg_builder=_build_demo_args, order=2).done()
    app.command("a_cmd", help="A command", arg_builder=_build_demo_args, order=1).done()

    command_ids = [spec.id for spec in app.commands.list_commands(should_sort=False)]
    param_ids = [spec.id for spec in app.params.list_params(should_sort=False)]

    assert command_ids == ["b_cmd", "a_cmd"]
    assert param_ids == ["executables.rscript", "data_table.threads_dt"]


def test_fluent_dsl_supports_nested_subcommands() -> None:
    app = ParserBuilder(prog="demo")

    (
        app.command("go", help="Gene Ontology")
        .command("ontology", help="Ontology assets")
        .command("tidy", help="Build tidy outputs")
        .group(EnumGroupKey.INPUTS)
        .add_argument("--file-in", type=str, required=True)
        .end()
        .done()
        .done()
        .done()
    )

    parser = app.build()
    ns = parser.parse_args(["go", "ontology", "tidy", "--file-in", "go.obo"])

    assert ns.command == "go.ontology.tidy"
    assert ns.file_in == "go.obo"


def test_nested_subcommands_require_leaf_selection() -> None:
    app = ParserBuilder(prog="demo")
    (
        app.command("go", help="Gene Ontology")
        .command("ontology", help="Ontology assets")
        .command("tidy", help="Build tidy outputs")
        .done()
        .done()
        .done()
    )

    parser = app.build()

    with pytest.raises(SystemExit):
        parser.parse_args(["go", "ontology"])


def test_nested_subcommands_support_extract_params() -> None:
    app = ParserBuilder(prog="demo")
    _register_demo_params(app)

    (
        app.command("go", help="Gene Ontology")
        .command("ontology", help="Ontology assets")
        .command("tidy", help="Build tidy outputs")
        .group(EnumGroupKey.EXECUTABLES)
        .extract_params("executables.rscript")
        .end()
        .done()
        .done()
        .done()
    )

    parser = app.build()
    ns = parser.parse_args(["go", "ontology", "tidy", "--rscript", "Rscript"])

    assert ns.command == "go.ontology.tidy"
    assert ns.rscript == "Rscript"


def test_list_commands_includes_nested_paths() -> None:
    app = ParserBuilder(prog="demo")
    (
        app.command("go", help="Gene Ontology")
        .command("ontology", help="Ontology assets")
        .command("tidy", help="Build tidy outputs")
        .done()
        .done()
        .done()
    )

    command_ids = [spec.id for spec in app.commands.list_commands(should_sort=False)]

    assert command_ids == ["go", "go.ontology", "go.ontology.tidy"]


def test_build_rejects_unclosed_nested_command_builders() -> None:
    app = ParserBuilder(prog="demo")
    app.command("go", help="Gene Ontology").command(
        "ontology",
        help="Ontology assets",
    )

    with pytest.raises(ValueError, match="missing done\\(\\)"):
        app.build()


def test_done_all_returns_root_parser_builder_for_nested_chain() -> None:
    app = ParserBuilder(prog="demo")

    app = (
        app.command("go", help="Gene Ontology")
        .command("ontology", help="Ontology assets")
        .command("tidy", help="Build tidy outputs")
        .done_all()
    )

    assert isinstance(app, ParserBuilder)
    parser = app.build()
    ns = parser.parse_args(["go", "ontology", "tidy"])
    assert ns.command == "go.ontology.tidy"


def test_done_all_matches_done_for_single_command() -> None:
    app = ParserBuilder(prog="demo")

    app = app.command("demo", help="Demo command").done_all()

    assert isinstance(app, ParserBuilder)
    parser = app.build()
    ns = parser.parse_args(["demo"])
    assert ns.command == "demo"
