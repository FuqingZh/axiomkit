import argparse
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Self

from .core import RegistryCore, SmartFormatter

BuilderArg = Callable[[argparse.ArgumentParser], argparse.ArgumentParser | None]


@dataclass(frozen=True, slots=True)
class SpecCommand:
    """
    Immutable specification for a CLI command registered in ``RegistryCommand``.

    Each instance describes a single command, including:

    - ``id``: canonical command identifier used for registration and lookup.
    - ``help``: short help string shown in command listings.
    - ``arg_builder``: callback that configures an ``argparse.ArgumentParser``
      with this command's arguments and options.
    - ``entry``: optional entry point (such as a module path or script path)
      associated with the command.
    - ``group``: logical group name used to organize commands in the registry.
    - ``order``: numeric sort key controlling display order within a group.
    - ``aliases``: additional names that may be resolved to the canonical ``id``.
    """

    id: str
    help: str
    arg_builder: BuilderArg
    entry: str | Path | None = None
    group: str = "default"
    order: int = 0
    aliases: tuple[str, ...] = ()
    param_keys: tuple[str, ...] = ()


class RegistryCommand:
    """
    Maintain a registry of CLI command specifications and their aliases.

    The registry stores immutable :class:`SpecCommand` instances keyed by a
    canonical command identifier and optionally exposes additional alias names
    for each command. It provides utilities to:

    - register new command specifications, along with any aliases
    - resolve an arbitrary command key (canonical id or alias) to its
      canonical identifier
    - build and attach :mod:`argparse` subparsers from the registered
      command specifications, using each command's ``arg_builder``
      callback to configure the individual subparser.
    """

    def __init__(self) -> None:
        self._core: RegistryCore[SpecCommand] = RegistryCore.new()

    def register(self, spec: SpecCommand) -> Self:
        self._core.register(spec, aliases=spec.aliases)
        return self

    def get(self, key_or_alias: str) -> SpecCommand:
        return self._core.get(key_or_alias)

    def list_registered_commands(self, if_sort: bool = True) -> list[SpecCommand]:
        """
        Return all registered command specifications.

        If ``if_sort`` is True, the specifications are returned sorted by
        ``(group, order, id)``; otherwise they are returned in insertion order.

        Args:
            if_sort (bool, optional): Whether to sort the returned specifications
                by group, order, and id. Defaults to True.

        Returns:
            list[SpecCommand]: The list of registered command specifications.
        """
        if not if_sort:
            return self._core.list_specs(kind_sort="insertion")
        return self._core.list_specs(rule_sort=lambda s: (s.group, s.order, s.id))

    def canonicalize_namespace(
        self, ns: argparse.Namespace, *, attr: str = "command"
    ) -> str:
        """
        Normalize a parsed subcommand string (which may be an alias) to its
        canonical command id and write it back to the given namespace.

        The value of ``getattr(ns, attr)`` is resolved using the underlying
        registry core (``self._core.resolve_alias``) and the resulting canonical
        id is written back to ``ns.<attr>``.

        Args:
            ns (argparse.Namespace): The namespace whose attribute should be
                canonicalized in-place.
            attr (str, optional): The name of the attribute on ``ns`` that
                contains the command id or alias to normalize. Defaults to
                ``"command"``.

        Raises:
            ValueError: If the namespace does not have an attribute named
                ``attr``.

        Returns:
            str: The canonical command id that was written back to ``ns.<attr>``.

        Examples:
            >>> def build_list(p: argparse.ArgumentParser) -> None:
            ...     p.add_argument("--in", default="")
            >>> registry = RegistryCommand()
            >>> registry.register(CommandSpec(id="list", help="", args_builder=build_list, aliases=("ls",)))
            Suppose ``ns.stages`` currently contains the alias ``"ls"`` and that
            ``"ls"`` has been registered as an alias for the canonical id
            ``"list"``. Then:

            >>> registry.canonicalize_namespace(ns)
            'list'
            >>> ns.stages
            'list'
        """
        if not hasattr(ns, attr):
            raise ValueError(f"Namespace has no attribute {attr!r}")
        if (v := getattr(ns, attr, None)) is None:
            # If subparser is required=True, this usually won't happen.
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
    ):
        """
        Build argparse subparsers for all registered commands, creating a
        subcommand interface on the given parser.

        Each registered :class:`CommandSpec` becomes a subcommand. Any aliases
        registered for a command id are also exposed as subcommands that execute
        the same underlying command.

        Args:
            parser (argparse.ArgumentParser): The base parser on which to add
                the subparsers.
            title (str, optional): Title for the subcommands section in the
                help output. Defaults to ``"Commands"``.
            dest (str, optional): Name of the attribute on the parsed namespace
                that will store the selected subcommand id. Defaults to
                ``"command"``.
            kind_formatter (type[argparse.HelpFormatter] | None, optional):
                Custom help formatter class for subcommand parsers. If ``None``,
                argparse's default formatter is used. Defaults to ``SmartFormatter``.
            if_required (bool, optional): Whether selecting a subcommand is
                required. If ``True``, argparse will error if no subcommand is
                provided. Defaults to ``True``.
            if_include_group_in_help (bool, optional): If ``True``, include the
                command's group name in its help text when displaying the list
                of subcommands. Defaults to ``True``.
            if_sort_specs (bool, optional): If ``True``, subcommands are ordered
                according to their ``order`` attribute; otherwise the registry
                insertion order is used. Defaults to ``True``.

        Returns:
            argparse._SubParsersAction: The subparsers action object created by
            :meth:`argparse.ArgumentParser.add_subparsers`.

        Examples:
            >>> parser = argparse.ArgumentParser(prog="tool")
            >>> registry = RegistryCommand()
            >>> _ = registry.build_subparsers(parser)
        """
        cls_sub = parser.add_subparsers(title=title, dest=dest, required=if_required)

        # canonical -> [aliases...]
        dict_aliases_by_id: dict[str, list[str]] = {
            k: [] for k in self._core.list_ids()
        }
        for _ali, _id in self._core.iter_alias_pairs():
            dict_aliases_by_id.setdefault(_id, []).append(_ali)

        cls_fmt = kind_formatter or parser.formatter_class
        for spec in self.list_registered_commands(if_sort=if_sort_specs):
            c_help = spec.help
            print(spec.id, spec.group, repr(c_help))
            if if_include_group_in_help and spec.group:
                c_help = f"\\[{spec.group}] {c_help}"

            l_aliases = sorted(dict_aliases_by_id.get(spec.id, []))
            sub = cls_sub.add_parser(
                spec.id,
                help=c_help,
                formatter_class=cls_fmt,
                aliases=l_aliases if l_aliases else [],
            )
            sub.set_defaults(
                _cmd_id=spec.id, _cmd_entry=spec.entry, _cmd_group=spec.group
            )
            spec.arg_builder(sub)

        return cls_sub
