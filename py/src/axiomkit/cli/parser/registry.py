import argparse
import warnings
from collections.abc import Callable, Sequence
from typing import Protocol, Self, cast

from .base import ArgAdder, CanonicalRegistry, SmartFormatter
from .spec import EnumGroupKey, EnumScope, SpecCommand, SpecParam

_RESERVED_PARAM_DESTS: frozenset[str] = frozenset(
    {
        "command",
        "_handler",
        "_cmd_id",
        "_cmd_entry",
        "_cmd_group",
    }
)


def default_reserved_param_dests(*, command_dest: str = "command") -> set[str]:
    """Return reserved destination names for parameter materialization."""
    set_dests = set(_RESERVED_PARAM_DESTS)
    set_dests.add(command_dest)
    return set_dests


def _iter_parser_actions(parser: argparse.ArgumentParser) -> Sequence[argparse.Action]:
    """Return parser actions with private-API access isolated in one place."""
    actions = getattr(parser, "_actions", ())
    if not isinstance(actions, Sequence):
        return ()
    return cast(Sequence[argparse.Action], actions)


def _collect_existing_dests(parser: argparse.ArgumentParser) -> set[str]:
    """Collect existing destination names from parser actions."""
    set_dests: set[str] = set()
    for action in _iter_parser_actions(parser):
        if isinstance(action.dest, str):
            set_dests.add(action.dest)
    return set_dests


def _collect_existing_flags(parser: argparse.ArgumentParser) -> set[str]:
    """Collect existing option flags from parser actions."""
    set_flags: set[str] = set()
    for action in _iter_parser_actions(parser):
        set_flags |= set(action.option_strings)
    return set_flags


class ParserRegistry(Protocol):
    """Protocol for parser group selectors used by ``ParamRegistry``.

    Attributes:
        parser: Backing ``argparse.ArgumentParser`` instance.

    Methods:
        select_group: Resolve logical group key into an ``ArgAdder`` target.
    """

    parser: argparse.ArgumentParser

    def select_group(self, key: EnumGroupKey | str) -> ArgAdder: ...


class CommandRegistry:
    """Registry for command specifications and aliases.

    This class stores ``SpecCommand`` objects by canonical id and resolves
    aliases through the shared ``CanonicalRegistry`` infrastructure.

    Examples:
        >>> reg = CommandRegistry()
        >>> _ = reg.register_command(
        ...     SpecCommand(id="run", help="Run", arg_builder=lambda p: p)
        ... )
        >>> reg.select_command("run").id
        'run'
    """

    def __init__(self) -> None:
        """Initialize an empty command registry."""
        self._core: CanonicalRegistry[SpecCommand] = CanonicalRegistry.new()

    def register_command(self, spec: SpecCommand) -> Self:
        """Register one command specification.

        Args:
            spec: Command specification to register.

        Returns:
            Self: ``self`` for fluent chaining.

        Raises:
            ValueError: If id or alias conflicts with existing registrations.
        """
        self._core.register(spec, aliases=spec.aliases)
        return self

    def select_command(self, key_or_alias: str) -> SpecCommand:
        """Select a command by canonical id or alias.

        Args:
            key_or_alias: Command id or alias.

        Returns:
            SpecCommand: Resolved command specification.

        Raises:
            ValueError: If the key/alias is unknown.
        """
        return self._core.get(key_or_alias)

    def list_commands(self, if_sort: bool = True) -> list[SpecCommand]:
        """List registered command specs.

        Args:
            if_sort:
                Whether to sort by ``(group, order, id)``.
                If ``False``, insertion order is preserved.

        Returns:
            list[SpecCommand]: Registered command specifications.
        """
        if not if_sort:
            return self._core.list_specs(kind_sort="insertion")
        return self._core.list_specs(rule_sort=lambda s: (s.group, s.order, s.id))

    def resolve_command_namespace(
        self,
        ns: argparse.Namespace,
        *,
        attr: str = "command",
    ) -> str:
        """Resolve a namespace command attribute to canonical id in-place.

        Args:
            ns: Parsed namespace object.
            attr: Namespace attribute storing command id or alias.

        Returns:
            str: Canonical command id.

        Raises:
            ValueError: If ``attr`` is missing or is ``None``.
        """
        if not hasattr(ns, attr):
            raise ValueError(f"Namespace has no attribute {attr!r}")
        if (v := getattr(ns, attr, None)) is None:
            raise ValueError(f"No command selected (ns.{attr} is None).")

        c_id = self._core.resolve_alias(v)
        setattr(ns, attr, c_id)
        return c_id

    def build_subparsers(
        self,
        parser: argparse.ArgumentParser,
        *,
        title: str = "Commands",
        dest: str = "command",
        kind_formatter: type[argparse.HelpFormatter] | None = SmartFormatter,
        if_required: bool = True,
        if_include_group_in_help: bool = True,
        if_sort_specs: bool = True,
        param_registry: "ParamRegistry | None" = None,
        group_registry_factory: Callable[[argparse.ArgumentParser], ParserRegistry]
        | None = None,
        if_apply_param_keys: bool = True,
    ):
        """Build argparse subparsers from command specs.

        Args:
            parser: Root parser receiving subparsers.
            title: Subparser section title in help output.
            dest: Namespace field that stores selected command.
            kind_formatter: Formatter class for each command subparser.
            if_required: Whether command selection is required.
            if_include_group_in_help:
                Whether to prefix command help with group tag.
            if_sort_specs: Whether to sort command specs before build.
            param_registry:
                Registry used to apply per-command ``param_keys``.
            group_registry_factory:
                Factory that builds a ``ParserRegistry`` wrapper for each
                command subparser.
            if_apply_param_keys:
                Whether to materialize ``SpecCommand.param_keys``.

        Returns:
            argparse._SubParsersAction: Subparsers action from argparse.

        Raises:
            ValueError:
                If ``if_apply_param_keys=True`` and required dependencies are
                missing for commands that contain ``param_keys``.

        Examples:
            >>> parser = argparse.ArgumentParser(prog="demo")
            >>> reg = CommandRegistry()
            >>> _ = reg.register_command(
            ...     SpecCommand(id="run", help="Run", arg_builder=lambda p: p)
            ... )
            >>> _ = reg.build_subparsers(parser)
            >>> isinstance(parser, argparse.ArgumentParser)
            True
        """
        cls_sub = parser.add_subparsers(title=title, dest=dest, required=if_required)

        dict_aliases_by_id: dict[str, list[str]] = {k: [] for k in self._core.list_ids()}
        for _ali, _id in self._core.iter_alias_pairs():
            dict_aliases_by_id.setdefault(_id, []).append(_ali)

        cls_fmt = kind_formatter or parser.formatter_class
        for spec in self.list_commands(if_sort=if_sort_specs):
            c_help = spec.help
            if if_include_group_in_help and spec.group:
                c_help = f"\\[{spec.group}] {c_help}"

            l_aliases = sorted(dict_aliases_by_id.get(spec.id, []))
            sub = cls_sub.add_parser(
                spec.id,
                help=c_help,
                formatter_class=cls_fmt,
                aliases=l_aliases if l_aliases else [],
            )
            sub.set_defaults(_cmd_id=spec.id, _cmd_entry=spec.entry, _cmd_group=spec.group)
            spec.arg_builder(sub)

            if if_apply_param_keys and spec.param_keys:
                if param_registry is None:
                    raise ValueError(
                        "`param_registry` is required when `if_apply_param_keys=True` "
                        "and command has `param_keys`."
                    )
                if group_registry_factory is None:
                    raise ValueError(
                        "`group_registry_factory` is required when applying `param_keys`."
                    )

                param_registry.apply_param_specs(
                    parser_reg=group_registry_factory(sub),
                    keys=spec.param_keys,
                    reserved_dests=default_reserved_param_dests(command_dest=dest),
                )

        return cls_sub


class ParamRegistry:
    """Registry for parameter specifications.

    The registry stores ``SpecParam`` instances and can materialize selected
    parameters into parser groups with collision checks.

    Examples:
        >>> reg = ParamRegistry()
        >>> _ = reg.register_param(
        ...     SpecParam(id="general.flag", arg_builder=lambda g, s: s.add_argument(g))
        ... )
        >>> reg.contains_param("general.flag")
        True
    """

    def __init__(self) -> None:
        """Initialize an empty parameter registry."""
        self._core: CanonicalRegistry[SpecParam] = CanonicalRegistry.new()

    def register_param(self, spec: SpecParam) -> SpecParam:
        """Register one parameter specification.

        Args:
            spec: Parameter specification.

        Returns:
            SpecParam: The registered specification.

        Raises:
            ValueError: If id or alias conflicts are detected.
        """
        return self._core.register(spec, aliases=spec.aliases)

    def select_param(self, key_or_alias: str) -> SpecParam:
        """Select a parameter specification by id or alias.

        Args:
            key_or_alias: Parameter id or alias.

        Returns:
            SpecParam: Resolved parameter specification.

        Raises:
            ValueError: If the key/alias is unknown.
        """
        return self._core.get(key_or_alias)

    def contains_param(self, key_or_alias: str) -> bool:
        """Check whether a parameter id/alias exists.

        Args:
            key_or_alias: Parameter id or alias.

        Returns:
            bool: ``True`` if resolvable; otherwise ``False``.
        """
        try:
            self.select_param(key_or_alias)
            return True
        except ValueError:
            return False

    def list_params(
        self,
        *,
        scope: EnumScope | None = None,
        group: str | None = None,
        if_sort: bool = True,
    ) -> list[SpecParam]:
        """List registered parameter specs with optional filtering.

        Args:
            scope: Optional scope filter.
            group: Optional group filter.
            if_sort:
                Whether to sort by ``(group, order, id)``.
                If ``False``, insertion order is preserved.

        Returns:
            list[SpecParam]: Filtered parameter specs.
        """
        if not if_sort:
            cls_specs = self._core.list_specs(kind_sort="insertion")
        else:
            cls_specs = self._core.list_specs(rule_sort=lambda s: (s.group, s.order, s.id))

        if scope is not None:
            cls_specs = [s for s in cls_specs if s.scope == scope]
        if group is not None:
            cls_specs = [s for s in cls_specs if s.group == group]

        return cls_specs

    def apply_param_specs(
        self,
        *,
        parser_reg: ParserRegistry,
        keys: Sequence[str],
        reserved_dests: set[str] | None,
    ) -> None:
        """Apply selected parameter specs onto a parser registry.

        Args:
            parser_reg: Parser/group wrapper used to resolve logical groups.
            keys: Parameter ids or aliases to apply in order.
            reserved_dests:
                Dest names that are forbidden for parameter materialization.
                Defaults to ``{"command", "_handler"}``.

        Raises:
            ValueError:
                If a key is unknown, ``arg_builder`` is missing, or any
                destination/flag collision is detected.
        """
        if reserved_dests is None:
            reserved_dests = default_reserved_param_dests()

        parser = parser_reg.parser

        set_existing_dests = _collect_existing_dests(parser)
        set_existing_flags = _collect_existing_flags(parser)

        dict_seen_dests: dict[str, str] = {}
        dict_seen_flags: dict[str, str] = {}

        for k in keys:
            cls_spec_ = self.select_param(k)
            if cls_spec_.if_deprecated:
                warnings.warn(
                    (
                        f"Deprecated param: {cls_spec_.id!r}; "
                        f"use {cls_spec_.replace_by!r} instead."
                    ),
                    category=UserWarning,
                    stacklevel=2,
                )
            if cls_spec_.arg_builder is None:
                raise ValueError(f"`SpecParam` missing `arg_builder`: {cls_spec_.id!r}")

            c_dest_ = cls_spec_.resolved_dest
            tup_flags_ = cls_spec_.resolved_flags

            if c_dest_ in reserved_dests:
                raise ValueError(
                    f"Param dest is reserved: {c_dest_!r} (spec id: {cls_spec_.id!r})"
                )
            if c_dest_ in set_existing_dests:
                raise ValueError(
                    f"Param dest already exists on parser: {c_dest_!r} (spec id: {cls_spec_.id!r})"
                )
            if c_dest_ in dict_seen_dests:
                raise ValueError(
                    f"Param dest collision: {c_dest_!r} "
                    f"(spec ids: {dict_seen_dests[c_dest_]!r}, {cls_spec_.id!r})"
                )
            dict_seen_dests[c_dest_] = cls_spec_.id

            for _f in tup_flags_:
                if _f in set_existing_flags:
                    raise ValueError(
                        f"Param flag already exists on parser: {_f!r} (spec id: {cls_spec_.id!r})"
                    )
                if _f in dict_seen_flags:
                    raise ValueError(
                        f"Param flag collision: {_f!r} "
                        f"(spec ids: {dict_seen_flags[_f]!r}, {cls_spec_.id!r})"
                    )
                dict_seen_flags[_f] = cls_spec_.id

            cls_group = parser_reg.select_group(cls_spec_.group)
            cls_spec_.arg_builder(cls_group, cls_spec_)

            set_existing_dests.add(c_dest_)
            set_existing_flags |= set(tup_flags_)
