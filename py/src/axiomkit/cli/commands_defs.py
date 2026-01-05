import argparse
from collections.abc import Callable, Sequence

from .params_defs import COMMON_PARAM_KEYS, T_TEST_PARAM_KEYS, build_param_registry
from .registry import CommandRegistry, CommandSpec, ParserRegistry

CommandRegistrar = Callable[[CommandRegistry], None]


def register_builtin_commands(reg: CommandRegistry) -> None:
    """Register the built-in commands shipped with this package."""


def build_command_registry(
    *, extra_registrars: Sequence[CommandRegistrar] = ()
) -> CommandRegistry:
    """Build a command registry.

    Projects can customize the command set by passing extra registrars.
    """
    cls_registry = CommandRegistry()

    def _build_t_test_args(p: argparse.ArgumentParser) -> None:
        """Example command: demonstrate how to reuse shared ParamSpec definitions."""
        reg_params = build_param_registry()
        reg_parser = ParserRegistry(p)
        reg_params.apply(
            parser_reg=reg_parser, keys=(*COMMON_PARAM_KEYS, *T_TEST_PARAM_KEYS)
        )

    cls_registry.register(
        CommandSpec(
            id="t_test",
            help="T-test stage (example).",
            args_builder=_build_t_test_args,
            group="stats",
            order=10,
            aliases=("ttest",),
        )
    )

    for fn in extra_registrars:
        fn(cls_registry)

    return cls_registry
