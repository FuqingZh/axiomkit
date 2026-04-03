"""Runtime parser helpers independent from optional rich formatter deps."""

from __future__ import annotations

import argparse

_ATTR_EXPLICIT_DESTS = "__axiomkit_cli_parser_explicit_dests__"


def mark_namespace_dest_explicit(
    namespace: argparse.Namespace,
    dest: str,
) -> None:
    """Record that a namespace field was populated by explicit CLI input."""
    explicit_dests = getattr(namespace, _ATTR_EXPLICIT_DESTS, None)
    if explicit_dests is None:
        explicit_dests = set()
        setattr(namespace, _ATTR_EXPLICIT_DESTS, explicit_dests)
    explicit_dests.add(dest)


def namespace_dest_is_explicit(
    namespace: argparse.Namespace,
    dest: str,
) -> bool:
    """Return whether ``dest`` was set from explicit CLI input."""
    explicit_dests = getattr(namespace, _ATTR_EXPLICIT_DESTS, ())
    return dest in explicit_dests


class ArgumentParser(argparse.ArgumentParser):
    """Argument parser that finalizes lazy action defaults after parsing.

    Compatible custom actions may expose a
    ``_finalize_default_into_namespace(namespace)`` hook. This parser runs
    those hooks for the active parser path after parsing completes, but only
    for destinations that were not provided explicitly on the CLI.

    Notes:
        - ``--help`` remains available even when a default would fail action
          validation, because lazy defaults are finalized only after parsing.
        - Active subparser traversal follows the selected subcommand tokens
          stored in each subparser action ``dest``.
    """

    def parse_known_args(
        self,
        args: list[str] | None = None,
        namespace: argparse.Namespace | None = None,
    ) -> tuple[argparse.Namespace, list[str]]:
        """Parse arguments and finalize lazy defaults on the active parser path."""
        initial_dests = set(vars(namespace).keys()) if namespace is not None else set()
        explicit_state_previous = (
            getattr(namespace, _ATTR_EXPLICIT_DESTS)
            if namespace is not None and hasattr(namespace, _ATTR_EXPLICIT_DESTS)
            else None
        )

        namespace, extras = super().parse_known_args(args=args, namespace=namespace)

        try:
            self._finalize_lazy_defaults(
                namespace=namespace,
                initial_dests=initial_dests,
            )
            return namespace, extras
        finally:
            if explicit_state_previous is None:
                if hasattr(namespace, _ATTR_EXPLICIT_DESTS):
                    delattr(namespace, _ATTR_EXPLICIT_DESTS)
            else:
                setattr(namespace, _ATTR_EXPLICIT_DESTS, explicit_state_previous)

    def parse_args(
        self,
        args: list[str] | None = None,
        namespace: argparse.Namespace | None = None,
    ) -> argparse.Namespace:
        """Parse known args, then reject any leftovers."""
        namespace, extras = self.parse_known_args(args=args, namespace=namespace)
        if extras:
            self.error(f"unrecognized arguments: {' '.join(extras)}")
        return namespace

    def _finalize_lazy_defaults(
        self,
        *,
        namespace: argparse.Namespace,
        initial_dests: set[str],
    ) -> None:
        """Normalize/validate lazy defaults for the active parser path."""
        for parser in self._iter_active_parsers(namespace):
            for action in getattr(parser, "_actions", ()):
                dest = getattr(action, "dest", argparse.SUPPRESS)
                if dest is argparse.SUPPRESS or dest in initial_dests:
                    continue
                if namespace_dest_is_explicit(namespace, dest):
                    continue

                finalize_default = getattr(
                    action,
                    "_finalize_default_into_namespace",
                    None,
                )
                if not callable(finalize_default):
                    continue

                try:
                    finalize_default(namespace)
                except argparse.ArgumentError as err:
                    self.error(str(err))

    def _iter_active_parsers(
        self,
        namespace: argparse.Namespace,
    ) -> tuple[argparse.ArgumentParser, ...]:
        """Return the parser chain selected by parsed subcommands."""
        active_parsers: list[argparse.ArgumentParser] = [self]
        current_parser: argparse.ArgumentParser = self

        while True:
            next_parser: argparse.ArgumentParser | None = None
            for action in getattr(current_parser, "_actions", ()):
                if not isinstance(action, argparse._SubParsersAction):
                    continue
                dest = action.dest
                if dest is argparse.SUPPRESS or not hasattr(namespace, dest):
                    continue
                selected_token = getattr(namespace, dest)
                if not isinstance(selected_token, str):
                    continue

                choice = action.choices.get(selected_token)
                if isinstance(choice, argparse.ArgumentParser):
                    next_parser = choice
                    break

            if next_parser is None:
                return tuple(active_parsers)

            active_parsers.append(next_parser)
            current_parser = next_parser
