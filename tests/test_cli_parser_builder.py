from __future__ import annotations

import argparse
from enum import StrEnum

import pytest

from axiomkit.cli.parser import (  # noqa: E402
    ActionHexColor,
    ActionNumericRange,
    GroupKey,
    ParserBuilder,
)
from axiomkit.cli.parser.runtime import ArgumentParser  # noqa: E402
from axiomkit.cli.parser.spec import ParamSpec  # noqa: E402


class ParamKey(StrEnum):
    EXE_RSCRIPT = "executables.rscript"
    THR_THREADS = "data_table.threads_dt"


def _build_demo_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("--demo-flag", action="store_true")
    return parser


def _register_demo_params(app: ParserBuilder) -> None:
    app.register_params(
        ParamSpec(
            id="executables.rscript",
            group=GroupKey.EXECUTABLES,
            help="Path to Rscript executable",
            arg_builder=lambda g, s: s.add_argument(g, type=str),
        ),
        ParamSpec(
            id="data_table.threads_dt",
            group=GroupKey.PERFORMANCE,
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
        ParamSpec(
            id="general.cmd_meta",
            dest="_cmd_group",
            group=GroupKey.GENERAL,
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
    app.select_group(GroupKey.GENERAL).add_argument("--threads_dt", type=int)
    app.register_params(
        ParamSpec(
            id="data_table.worker_threads",
            dest="worker_threads",
            flags=("--threads_dt",),
            group=GroupKey.GENERAL,
            arg_builder=lambda g, s: s.add_argument(g, type=int),
        )
    )

    with pytest.raises(ValueError, match="flag already exists on parser"):
        app.select_group(GroupKey.GENERAL).extract_params("data_table.worker_threads")


def test_extract_params_rejects_cross_group_param_keys() -> None:
    app = ParserBuilder(prog="demo")
    _register_demo_params(app)

    with pytest.raises(ValueError, match="belongs to group"):
        app.select_group(GroupKey.INPUTS).extract_params("executables.rscript")


def test_deprecated_param_emits_warning() -> None:
    app = ParserBuilder(prog="demo")
    app.register_params(
        ParamSpec(
            id="general.legacy_threads",
            group=GroupKey.GENERAL,
            is_deprecated=True,
            replace_by="data_table.threads_dt",
            arg_builder=lambda g, s: s.add_argument(g, type=int),
        )
    )

    with pytest.warns(UserWarning, match="Deprecated param"):
        app.select_group(GroupKey.GENERAL).extract_params("general.legacy_threads")


def test_fluent_dsl_supports_multi_command_grouped_build() -> None:
    app = ParserBuilder(prog="demo")
    _register_demo_params(app)

    (
        app.command("t_test", help="T-test command")
        .group(GroupKey.EXECUTABLES)
        .extract_params("executables.rscript")
        .end()
        .group(GroupKey.INPUTS)
        .add_argument("--file-in", type=str, required=True)
        .end()
        .done()
    )
    (
        app.command("anova", help="ANOVA command")
        .group(GroupKey.PERFORMANCE)
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
        .group(GroupKey.INPUTS)
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
        .group(GroupKey.EXECUTABLES)
        .extract_params(ParamKey.EXE_RSCRIPT)
        .end()
        .group(GroupKey.PERFORMANCE)
        .extract_params(ParamKey.THR_THREADS)
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


def test_parser_builder_uses_axiomkit_argument_parser_by_default() -> None:
    app = ParserBuilder(prog="demo").command(
        "demo",
        help="Demo command",
        arg_builder=_build_demo_args,
    ).done()

    parser = app.build()
    assert isinstance(parser, ArgumentParser)


def test_parser_builder_keeps_external_parser_instance() -> None:
    external_parser = argparse.ArgumentParser(prog="demo")
    app = ParserBuilder(parser=external_parser).command(
        "demo",
        help="Demo command",
        arg_builder=_build_demo_args,
    ).done()

    parser = app.build()
    assert parser is external_parser


def test_unselected_subcommand_defaults_are_not_finalized() -> None:
    app = ParserBuilder(prog="demo")
    (
        app.command("ok", help="Selected command")
        .group(GroupKey.GENERAL)
        .add_argument("--demo-flag", action="store_true")
        .end()
        .done()
    )
    (
        app.command("bad", help="Unselected command")
        .group(GroupKey.PLOTS)
        .add_argument(
            "--panel-border-color",
            action=ActionHexColor,
            default="#12",
        )
        .end()
        .done()
    )

    parser = app.build()
    ns = parser.parse_args(["ok"])

    assert ns.command == "ok"


def test_selected_subcommand_explicit_value_beats_lazy_default() -> None:
    app = ParserBuilder(prog="demo")
    (
        app.command("sub", help="Selected command")
        .group(GroupKey.THRESHOLDS)
        .add_argument(
            "--value",
            action=ActionNumericRange.non_negative(value_kind="int"),
            default=0,
        )
        .end()
        .done()
    )

    parser = app.build()
    ns = parser.parse_args(["sub", "--value", "7"])

    assert ns.command == "sub"
    assert ns.value == 7


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
        .group(GroupKey.INPUTS)
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
        .group(GroupKey.EXECUTABLES)
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


def test_open_command_builders_exposes_read_only_snapshot() -> None:
    app = ParserBuilder(prog="demo")
    root = app.command("go", help="Gene Ontology")
    leaf = root.command("ontology", help="Ontology assets")

    open_builders = app.open_command_builders

    assert isinstance(open_builders, tuple)
    assert open_builders == (root, leaf)
    assert [builder.id for builder in open_builders] == ["go", "go.ontology"]

    leaf.done()

    assert app.open_command_builders == (root,)
    assert open_builders == (root, leaf)


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
