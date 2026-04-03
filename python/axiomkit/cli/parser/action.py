import argparse
import math
import os
import re
import shlex
import shutil
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, Literal, cast

from .runtime import mark_namespace_dest_explicit, namespace_dest_is_explicit


def _normalize_allowed_file_exts(
    exts: str | Iterable[str] = (),
    *more_exts: str,
) -> tuple[str, ...]:
    """Normalize mixed extension inputs into a canonical tuple.

    Accepted forms:

    - ``"obo"``
    - ``("tsv", "tsv.gz")``
    - ``"tsv", "tsv.gz"``
    """
    values: list[str] = []

    def _append_one(value: str) -> None:
        text = str(value).lower().lstrip(".").strip()
        if text:
            values.append(text)

    if isinstance(exts, str):
        _append_one(exts)
    else:
        for ext in exts:
            _append_one(ext)

    for ext in more_exts:
        _append_one(ext)

    return tuple(values)


@dataclass(frozen=True, slots=True)
class PathSpec:
    """Specification for validating path-like CLI inputs.

    Use this as the contract for :class:`ActionPath`. It encodes what kind of
    filesystem entry to accept and which validation rules to enforce.

    Attributes:
        entry_kind: Expected entry type, one of {"dir", "file", "exe"}.
        allowed_file_exts: Allowed file extensions (lowercase, no leading dots).
        should_exist: Whether the path must already exist.
        is_readable: Whether the path must be readable (if it exists).
        is_writable: Whether the path must be writable (mainly for outputs).

    Examples:
        Explicit file rule with suffix constraints:

        >>> spec = PathSpec(
        ...     entry_kind="file",
        ...     allowed_file_exts=("tsv", "tsv.gz"),
        ...     should_exist=True,
        ... )

        Output directory that may not exist yet:

        >>> out_spec = PathSpec(
        ...     entry_kind="dir",
        ...     should_exist=False,
        ...     is_writable=True,
        ... )
    """

    entry_kind: Literal["dir", "file", "exe"] = "file"
    # file-only: allowed extensions (case-insensitive, without leading dots)
    allowed_file_exts: tuple[str, ...] = ()
    # whether the target must already exist (useful for output paths if False)
    should_exist: bool = True
    # file/dir readability checks (only when should_exist=True)
    is_readable: bool = True
    # file/dir writability checks (mostly for outputs; only enforced when True)
    is_writable: bool = False


class _LazyDefaultAction(argparse.Action):
    """Base action with helpers for lazy default finalization."""

    def _mark_dest_explicit(self, namespace: argparse.Namespace) -> None:
        if self.dest is not argparse.SUPPRESS:
            mark_namespace_dest_explicit(namespace, self.dest)

    def _dest_was_explicit(self, namespace: argparse.Namespace) -> bool:
        if self.dest is argparse.SUPPRESS:
            return False
        return namespace_dest_is_explicit(namespace, self.dest)

    def _has_lazy_default(self) -> bool:
        return getattr(self, "default", None) not in {None, argparse.SUPPRESS}


class ActionPath(_LazyDefaultAction):
    """Validate and normalize path arguments for argparse.

    For ``file``/``dir`` kinds, resolves to absolute :class:`pathlib.Path` and
    enforces existence, readability, writability, and allowed extensions. For
    ``exe`` it resolves either an explicit path or a command via ``PATH``.

    Examples:
        Use built-in factories:

        >>> parser.add_argument("--file_in", action=ActionPath.file(exts=("tsv",)))
        >>> parser.add_argument(
        ...     "--dir_out",
        ...     action=ActionPath.dir(should_exist=False, is_writable=True),
        ... )
        >>> parser.add_argument("--path_rscript", action=ActionPath.exe(), default="Rscript")

        Use an explicit spec:

        >>> parser.add_argument(
        ...     "--report",
        ...     action=ActionPath.from_spec(
        ...         spec=PathSpec(entry_kind="file", allowed_file_exts=("xlsx",))
        ...     ),
        ... )

    Notes:
        - Parsed values stored in ``argparse.Namespace`` are always
          ``pathlib.Path`` objects (resolved absolute paths) when used with
          :class:`axiomkit.cli.parser.ArgumentParser`.
        - Defaults are normalized lazily after parsing rather than during
          parser construction.
        - Invalid non-``None`` defaults fail only if parsing completes without
          an explicit override; this keeps ``--help`` renderable.
        - For executables, "activate" scripts are not checked; only existence
          and executability are validated.
    """

    def __init__(
        self,
        option_strings: list[str],
        dest: str,
        *,
        spec: PathSpec | None = None,
        entry_kind: Literal["dir", "file", "exe"] = "file",
        allowed_file_exts: Iterable[str] | None = None,
        should_exist: bool = True,
        is_readable: bool = True,
        is_writable: bool = False,
        **kwargs: Any,
    ) -> None:
        """Construct a ActionPath.

        Args:
            option_strings: Option strings received from argparse.
            dest: Namespace attribute name.
            spec: Prebuilt ``PathSpec``; overrides other spec params when set.
            entry_kind: Expected entry type when ``spec`` is not provided.
            allowed_file_exts: Allowed file extensions for file inputs.
            should_exist: Whether the path must already exist.
            is_readable: Whether readability is enforced when the path exists.
            is_writable: Whether writability is enforced (mainly outputs).
            **kwargs: Forwarded to ``argparse.Action``.

        Raises:
            ValueError: If an invalid ``entry_kind`` is provided.
        """
        super().__init__(option_strings, dest, **kwargs)

        if spec is None:
            spec = PathSpec(
                entry_kind=entry_kind,
                allowed_file_exts=_normalize_allowed_file_exts(
                    allowed_file_exts or ()
                ),
                should_exist=should_exist,
                is_readable=is_readable,
                is_writable=is_writable,
            )

        if spec.entry_kind not in ("dir", "file", "exe"):
            raise ValueError(
                f"[{dest}]: `entry_kind` must be 'dir'|'file'|'exe', got {spec.entry_kind!r}"
            )

        self.spec = spec
        self.entry_kind = spec.entry_kind

        if spec.entry_kind == "file" and spec.allowed_file_exts:
            self.allowed_file_exts = _normalize_allowed_file_exts(
                spec.allowed_file_exts
            )
        else:
            self.allowed_file_exts = None

    def _normalize_one(
        self, *, value: str | os.PathLike[str] | os.PathLike[bytes], name: str
    ) -> Path:
        """Normalize a single path-like value.

        Args:
            value: Input path-like value.
            name: Display name for error messages.

        Returns:
            An absolute ``Path`` instance.

        Raises:
            argparse.ArgumentError: If the value is empty, invalid, missing,
                not readable/writable, or violates extension rules.
        """
        raw_value = os.fsdecode(value)

        if not raw_value.strip():
            raise argparse.ArgumentError(self, f"[{name}]: Value cannot be empty.")

        if self.entry_kind in {"file", "dir"}:
            try:
                path = Path(raw_value).expanduser().resolve()
            except Exception:
                raise argparse.ArgumentError(
                    self, f"[{name}]: Invalid path: {raw_value!r}"
                )

            if self.entry_kind == "file":
                if self.spec.should_exist and not path.is_file():
                    raise argparse.ArgumentError(self, f"[{name}]: Not a file: {path}")

                if self.allowed_file_exts:
                    allowed_exts = set(self.allowed_file_exts)
                    suffix_compound = "".join(path.suffixes).lower().lstrip(".")
                    suffix_single = path.suffix.lower().lstrip(".")
                    if (suffix_compound not in allowed_exts) and (
                        suffix_single not in allowed_exts
                    ):
                        expected_exts = ", ".join(self.allowed_file_exts)
                        actual_ext = (
                            "." + suffix_compound if suffix_compound else "(none)"
                        )
                        raise argparse.ArgumentError(
                            self,
                            f"[{name}]: Expected extension(s) in ({expected_exts}), got {actual_ext}: {path}",
                        )

                if (
                    self.spec.should_exist
                    and self.spec.is_readable
                    and not os.access(path, os.R_OK)
                ):
                    raise argparse.ArgumentError(
                        self, f"[{name}]: File not readable: {path}"
                    )

            else:
                if self.spec.should_exist and not path.is_dir():
                    raise argparse.ArgumentError(
                        self,
                        f"[{name}]: Not a directory: {path}",
                    )
                if (
                    self.spec.should_exist
                    and self.spec.is_readable
                    and not os.access(path, os.R_OK | os.X_OK)
                ):
                    raise argparse.ArgumentError(
                        self,
                        f"[{name}]: Directory not accessible: {path}",
                    )

            return path

        # exe: path-ish vs command-ish
        raw_text = str(raw_value)
        has_separator = any(sep in raw_text for sep in (os.sep, os.altsep) if sep)
        is_path_like = raw_text.startswith(("~", ".")) or os.path.isabs(raw_text)

        if has_separator or is_path_like:
            executable_path = Path(raw_text).expanduser()
            if not executable_path.exists():
                raise argparse.ArgumentError(
                    self, f"[{name}]: Executable path not found: {executable_path}"
                )
            if executable_path.is_dir():
                raise argparse.ArgumentError(
                    self, f"[{name}]: Executable path is a directory: {executable_path}"
                )
            if os.name != "nt" and not os.access(executable_path, os.X_OK):
                raise argparse.ArgumentError(
                    self, f"[{name}]: Not executable: {executable_path}"
                )
            return executable_path.resolve()

        resolved_hit = shutil.which(raw_text)
        if not resolved_hit:
            raise argparse.ArgumentError(
                self,
                f"[{name}]: Executable not found in PATH: {raw_text!r} (consider passing an absolute path).",
            )
        return Path(resolved_hit).resolve()

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: object | None,
        option_string: str | None = None,
    ) -> None:
        """Parse, normalize, and set a path argument on the namespace.

        Args:
            parser: The owning ``ArgumentParser``.
            namespace: The argparse namespace being populated.
            values: The raw user-provided value.
            option_string: The option flag used (for error reporting).

        Raises:
            argparse.ArgumentError: On invalid value types or normalization
                failures.

        Notes:
            - ``namespace.<dest>`` is set to a normalized ``pathlib.Path``
              instance (resolved absolute path).
        """
        argument_name = option_string or self.dest

        if values is None:
            raise argparse.ArgumentError(
                self, f"[{argument_name}]: Value cannot be None."
            )

        if isinstance(values, (list, tuple)):
            seq = cast(list[object] | tuple[object, ...], values)
            if len(seq) != 1:
                raise argparse.ArgumentError(
                    self, f"[{argument_name}]: Expected a single value, got {values!r}."
                )
            values = seq[0]

        if not isinstance(values, (str, os.PathLike, Path)):
            raise argparse.ArgumentError(
                self,
                f"[{argument_name}]: Expected a path-like value (str/Path), got {type(values).__name__}.",
            )

        values = cast(str | os.PathLike[str] | os.PathLike[bytes], values)
        path = self._normalize_one(value=values, name=argument_name)
        self._mark_dest_explicit(namespace)
        setattr(namespace, self.dest, path)

    def _finalize_default_into_namespace(self, namespace: argparse.Namespace) -> None:
        """Normalize a default value into the namespace after parsing."""
        if not self._has_lazy_default():
            return
        setattr(
            namespace,
            self.dest,
            self._normalize_one(
                value=self.default,
                name=f"{self.dest} (default)",
            ),
        )

    # -------- convenience factories (avoid importing PathSpec explicitly) --------
    @classmethod
    def from_spec(cls, spec: PathSpec) -> type[argparse.Action]:
        """Factory returning a ``partial`` with a preset ``PathSpec``.

        Notes:
            - Validation semantics are the same as :class:`ActionPath`,
              including lazy default finalization when used with
              :class:`axiomkit.cli.parser.ArgumentParser`.

        Args:
            spec: Path specification to enforce.

        Returns:
            A callable suitable for argparse ``action``.

        Examples:
            >>> parser.add_argument(
            ...     "--file_in",
            ...     action=ActionPath.from_spec(
            ...         spec=PathSpec(kind_entry="file", allowed_file_exts=("parquet",))
            ...     ),
            ... )
        """
        return cast(type[argparse.Action], partial(cls, spec=spec))

    @classmethod
    def make(
        cls,
        entry_kind: Literal["dir", "file", "exe"] = "file",
        *,
        allowed_file_exts: Iterable[str] = (),
        should_exist: bool = True,
        is_readable: bool = True,
        is_writable: bool = False,
    ) -> type[argparse.Action]:
        """Convenience factory that builds ``PathSpec`` from keyword inputs.

        Notes:
            - Validation semantics are the same as :class:`ActionPath`,
              including lazy default finalization when used with
              :class:`axiomkit.cli.parser.ArgumentParser`.

        Args:
            entry_kind: Expected entry type.
            allowed_file_exts: Allowed file extensions for ``file`` kind.
            should_exist: Whether the target must already exist.
            is_readable: Whether readability is required.
            is_writable: Whether writability is required.

        Returns:
            A callable suitable for argparse ``action``.

        Examples:
            >>> parser.add_argument(
            ...     "--file_in",
            ...     action=ActionPath.make(
            ...         entry_kind="file",
            ...         allowed_file_exts=("csv",),
            ...     ),
            ... )
        """
        return cls.from_spec(
            spec=PathSpec(
                entry_kind=entry_kind,
                allowed_file_exts=tuple(
                    str(ext).lower().lstrip(".")
                    for ext in allowed_file_exts
                    if ext and str(ext).strip()
                ),
                should_exist=should_exist,
                is_readable=is_readable,
                is_writable=is_writable,
            )
        )

    @classmethod
    def file(
        cls,
        exts: str | Iterable[str] = (),
        *more_exts: str,
        should_exist: bool = True,
        is_readable: bool = True,
        is_writable: bool = False,
    ) -> type[argparse.Action]:
        """Factory for file path validation.

        Notes:
            - Non-``None`` defaults are finalized lazily after parsing when
              used with :class:`axiomkit.cli.parser.ArgumentParser`.
            - Invalid defaults therefore do not block ``--help`` output.

        Args:
            exts:
                Allowed file extensions. Accepts a single string or an iterable
                of strings.
            *more_exts:
                Additional allowed file extensions for the variadic call style,
                for example ``ActionPath.file("tsv", "tsv.gz")``.
            should_exist: Whether the file must already exist.
            is_readable: Whether readability is required.
            is_writable: Whether writability is required.

        Returns:
            A callable suitable for argparse ``action``.

        Examples:
            >>> parser.add_argument(
            ...     "--file_in",
            ...     action=ActionPath.file(exts=("tsv", "tsv.gz")),
            ... )
        """
        return cls.make(
            entry_kind="file",
            allowed_file_exts=_normalize_allowed_file_exts(exts, *more_exts),
            should_exist=should_exist,
            is_readable=is_readable,
            is_writable=is_writable,
        )

    @classmethod
    def dir(
        cls,
        *,
        should_exist: bool = True,
        is_readable: bool = True,
        is_writable: bool = False,
    ) -> type[argparse.Action]:
        """Factory for directory path validation.

        Notes:
            - Non-``None`` defaults are finalized lazily after parsing when
              used with :class:`axiomkit.cli.parser.ArgumentParser`.
            - Invalid defaults therefore do not block ``--help`` output.

        Args:
            should_exist: Whether the directory must already exist.
            is_readable: Whether readability/execute permission is required.
            is_writable: Whether writability is required.

        Returns:
            A callable suitable for argparse ``action``.

        Examples:
            >>> parser.add_argument(
            ...     "--dir_out",
            ...     action=ActionPath.dir(should_exist=False, is_writable=True),
            ... )
        """
        return cls.make(
            entry_kind="dir",
            should_exist=should_exist,
            is_readable=is_readable,
            is_writable=is_writable,
        )

    @classmethod
    def exe(cls) -> type[argparse.Action]:
        """Factory for executable path/command validation.

        Notes:
            - Non-``None`` defaults are finalized lazily after parsing when
              used with :class:`axiomkit.cli.parser.ArgumentParser`.
            - When used as ``action=ActionPath.exe(), default=\"some_tool\"``,
              ``--help`` remains available even if ``some_tool`` is not
              resolvable in current PATH.

        Returns:
            A callable suitable for argparse ``action``.

        Examples:
            >>> parser.add_argument("--exe_rscript", action=ActionPath.exe(), default="Rscript")
        """
        return cls.make(entry_kind="exe")


_RE_ENV_ASSIGN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=.*$")


class ActionCommandPrefix(_LazyDefaultAction):
    """Validate and tokenize shell-style command prefixes.

    Intended for prefixes like ``"micromamba run -n ENV"`` or
    ``"VAR=1 conda run -n ENV"``. Disallows activate-style commands and
    enforces the presence of an environment flag when using
    ``conda|mamba|micromamba run``.

    Examples:
        >>> parser.add_argument("--rule_exec_prefix", action=ActionCommandPrefix)
        # --rule_exec_prefix "VAR=1 micromamba run -n env"

    Caveats:
        - Rejects empty strings and malformed shell fragments.
        - Rejects ``... activate ...`` to avoid altering outer shells.
        - If ``run`` is used, requires ``-n/--name`` or ``-p/--prefix``.
    """

    def __init__(
        self,
        option_strings: list[str],
        dest: str,
        **kwargs: Any,
    ) -> None:
        """Construct a ActionCommandPrefix.

        Args:
            option_strings: Option strings received from argparse.
            dest: Namespace attribute name.
            **kwargs: Forwarded to ``argparse.Action``.
        """
        super().__init__(option_strings, dest, **kwargs)

    def _tokenize_and_validate(
        self,
        *,
        value: object | None,
        argument_name: str,
    ) -> list[str]:
        """Parse and validate a command prefix string.

        Args:
            value: Raw value from argparse.
            argument_name: Display name for error messages.

        Returns:
            Tokenized command prefix.

        Raises:
            argparse.ArgumentError: If the string is empty, invalid, lacks a
                command, points to a missing executable, or misuses env flags.
        """
        if value is None:
            raise argparse.ArgumentError(
                self,
                f"[{argument_name}]: Value cannot be None.",
            )

        if isinstance(value, str):
            input_text = value
        elif isinstance(value, (list, tuple)):
            seq = cast(list[object] | tuple[object, ...], value)
            input_text = " ".join(str(v) for v in seq)
        else:
            input_text = str(value)

        input_text = input_text.strip()
        if not input_text:
            raise argparse.ArgumentError(
                self,
                f"[{argument_name}]: Cannot be empty/whitespace.",
            )

        try:
            tokens = [token for token in shlex.split(input_text) if token.strip()]
        except ValueError as e:
            raise argparse.ArgumentError(
                self,
                f"[{argument_name}]: Invalid shell string: {e}. Yours: {input_text!r}",
            )

        if not tokens:
            raise argparse.ArgumentError(
                self,
                f"[{argument_name}]: Must contain at least one token. Yours: {input_text!r}",
            )

        # skip leading VAR=... assignments
        env_assignment_count = 0
        while env_assignment_count < len(tokens) and _RE_ENV_ASSIGN.match(
            tokens[env_assignment_count]
        ):
            env_assignment_count += 1
        if env_assignment_count >= len(tokens):
            raise argparse.ArgumentError(
                self,
                f"[{argument_name}]: Must contain a command after env assignments. Yours: {input_text!r}",
            )

        command_head = tokens[env_assignment_count]
        if command_head in {"conda", "mamba", "micromamba"}:
            # forbid activate
            if (
                env_assignment_count + 1 < len(tokens)
                and tokens[env_assignment_count + 1] == "activate"
            ):
                raise argparse.ArgumentError(
                    self,
                    f"[{argument_name}]: Do not use '... activate ...'. Use '... run -n <env>' style. Yours: {input_text!r}",
                )

        if shutil.which(command_head) is None:
            raise argparse.ArgumentError(
                self,
                f"[{argument_name}]: Executable not in PATH: {command_head!r}",
            )

        # conda/mamba/micromamba run: require env
        if (
            command_head in {"conda", "mamba", "micromamba"}
            and env_assignment_count + 1 < len(tokens)
            and tokens[env_assignment_count + 1] == "run"
        ):
            env_flags = {"-n", "--name", "-p", "--prefix"}
            tail_tokens = tokens[env_assignment_count + 2 :]
            if not any(flag in tail_tokens for flag in env_flags):
                raise argparse.ArgumentError(
                    self,
                    f"[{argument_name}]: '{command_head} run' must include -n/--name <env> or -p/--prefix <path>. Yours: {input_text!r}",
                )

            for flag in env_flags:
                if flag in tail_tokens:
                    index = tokens.index(flag, env_assignment_count + 2)
                    if index == len(tokens) - 1 or tokens[index + 1].startswith("-"):
                        raise argparse.ArgumentError(
                            self,
                            f"[{argument_name}]: Missing env name/path after {flag}. Yours: {input_text!r}",
                        )
                    break

        return list(tokens)

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: object | None,
        option_string: str | None = None,
    ) -> None:
        """Normalize and accumulate command prefix tokens.

        Args:
            parser: The owning ``ArgumentParser``.
            namespace: The argparse namespace being populated.
            values: Raw value provided by the user.
            option_string: Option flag used (for error reporting).

        Raises:
            argparse.ArgumentError: If validation fails or the namespace type
                is unexpected.
        """
        argument_name = option_string or self.dest
        tokens = self._tokenize_and_validate(
            value=values,
            argument_name=argument_name,
        )

        cur = getattr(namespace, self.dest, None)
        if not self._dest_was_explicit(namespace):
            self._mark_dest_explicit(namespace)
            setattr(namespace, self.dest, list(tokens))
            return

        if not isinstance(cur, list):
            raise argparse.ArgumentError(
                self, f"[{argument_name}]: Internal type error, expected list."
            )
        current_tokens = cast(list[str], cur)
        current_tokens.extend(tokens)

    def _finalize_default_into_namespace(self, namespace: argparse.Namespace) -> None:
        """Tokenize and freeze the default command prefix after parsing."""
        if not self._has_lazy_default():
            return
        setattr(
            namespace,
            self.dest,
            tuple(
                self._tokenize_and_validate(
                    value=self.default,
                    argument_name=f"{self.dest} (default)",
                )
            ),
        )


_RE_HEX6 = re.compile(r"#[0-9A-Fa-f]{6}$")


class ActionHexColor(_LazyDefaultAction):
    """Parse and validate hex colors in ``#RRGGBB`` format.

    Examples:
        >>> parser.add_argument("--panel_border_color", action=ActionHexColor)
        # --panel_border_color "#33AAFF"

    Notes:
        - Upcases the returned hex string.
        - Rejects shorthand ``#RGB`` or alpha variants.
    """

    def __init__(
        self,
        option_strings: list[str],
        dest: str,
        **kwargs: Any,
    ) -> None:
        """Construct a ActionHexColor.

        Args:
            option_strings: Option strings received from argparse.
            dest: Namespace attribute name.
            **kwargs: Forwarded to ``argparse.Action``.
        """
        super().__init__(option_strings, dest, **kwargs)

    def _normalize_hex(
        self,
        *,
        value: object | None,
        argument_name: str,
    ) -> str:
        """Normalize a hex color string.

        Args:
            value: Raw value from argparse.
            argument_name: Display name for error messages.

        Returns:
            Normalized uppercase hex string.

        Raises:
            argparse.ArgumentError: If the value is missing or not ``#RRGGBB``.
        """
        if value is None:
            raise argparse.ArgumentError(
                self,
                f"[{argument_name}]: Color hex string cannot be None.",
            )

        if isinstance(value, (list, tuple)):
            seq = cast(list[object] | tuple[object, ...], value)
            if len(seq) != 1:
                raise argparse.ArgumentError(
                    self,
                    f"[{argument_name}]: Expected a single value, got {value!r}.",
                )
            value = seq[0]

        if not isinstance(value, str):
            raise argparse.ArgumentError(
                self,
                f"[{argument_name}]: Expected a string in format '#RRGGBB', got {type(value).__name__}.",
            )

        color_value = value.strip()
        if not color_value:
            raise argparse.ArgumentError(
                self,
                f"[{argument_name}]: Color hex string cannot be empty.",
            )
        if not _RE_HEX6.fullmatch(color_value):
            raise argparse.ArgumentError(
                self,
                f"[{argument_name}]: Color hex string must be in the format '#RRGGBB', got {color_value!r}.",
            )

        return f"#{color_value[1:].upper()}"

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: object | None,
        option_string: str | None = None,
    ) -> None:
        """Parse and set a validated hex color on the namespace."""
        argument_name = option_string or self.dest
        self._mark_dest_explicit(namespace)
        setattr(
            namespace,
            self.dest,
            self._normalize_hex(value=values, argument_name=argument_name),
        )

    def _finalize_default_into_namespace(self, namespace: argparse.Namespace) -> None:
        """Normalize a default color into the namespace after parsing."""
        if not self._has_lazy_default():
            return
        setattr(
            namespace,
            self.dest,
            self._normalize_hex(
                value=self.default,
                argument_name=f"{self.dest} (default)",
            ),
        )


@dataclass(frozen=True, slots=True)
class NumericRangeSpec:
    """Specification describing allowed numeric inputs.

    Attributes:
        value_kind: Numeric kind, either "int" or "float".
        value_min: Minimum allowed value or ``None``.
        value_max: Maximum allowed value or ``None``.
        allowed_values: Explicitly whitelisted values.
        should_include_min: Whether the lower bound is inclusive.
        should_include_max: Whether the upper bound is inclusive.
        is_finite: Whether floats must be finite.

    Examples:
        Strict positive integer:

        >>> NumericRangeSpec(value_kind="int", value_min=0, should_include_min=False)

        Open-left, closed-right unit interval:

        >>> NumericRangeSpec(
        ...     value_kind="float",
        ...     value_min=0.0,
        ...     value_max=1.0,
        ...     should_include_min=False,
        ...     should_include_max=True,
        ... )
    """

    value_kind: Literal["int", "float"] = "float"
    value_min: int | float | None = None
    value_max: int | float | None = None

    # if provided, values in this set are accepted immediately (even if outside min/max)
    allowed_values: tuple[int | float, ...] = ()

    should_include_min: bool = True
    should_include_max: bool = True

    # float-only: reject NaN/Inf by default
    is_finite: bool = True


class ActionNumericRange(_LazyDefaultAction):
    """Argparse action enforcing numeric value constraints.

    Parses an option as int/float, validates it against ``NumericRangeSpec``,
    and finalizes defaults lazily after parsing.

    Examples:
        Directly provide a spec:

        >>> parser.add_argument(
        ...     "--learning_rate",
        ...     action=ActionNumericRange,
        ...     spec=NumericRangeSpec(value_kind="float", value_min=0, should_include_min=False),
        ... )

        Use convenience factories:

        >>> parser.add_argument("--epochs", action=ActionNumericRange.non_negative(value_kind="int"))
        >>> parser.add_argument("--lr", action=ActionNumericRange.positive(value_kind="float"))
        >>> parser.add_argument("--p", action=ActionNumericRange.unit_interval())

    Notes:
        - ``allowed_values`` are accepted even if outside min/max.
        - When ``value_kind="float"`` and ``is_finite=True``, rejects NaN/Inf.
        - Defaults are validated lazily after parsing when used with
          :class:`axiomkit.cli.parser.ArgumentParser`.
    """

    def __init__(
        self,
        option_strings: list[str],
        dest: str,
        *,
        spec: NumericRangeSpec,
        **kwargs: Any,
    ) -> None:
        """Construct a ActionNumericRange.

        Args:
            option_strings: Option strings received from argparse.
            dest: Namespace attribute name.
            spec: Numeric range specification to enforce.
            **kwargs: Forwarded to ``argparse.Action``.

        Raises:
            ValueError: If spec bounds are inconsistent or kind is invalid.
        """
        super().__init__(option_strings, dest, **kwargs)

        if spec.value_kind not in ("int", "float"):
            raise ValueError(
                f"[{dest}]: spec.value_kind must be 'int'|'float', got {spec.value_kind!r}"
            )

        if spec.value_min is not None and spec.value_max is not None:
            if float(spec.value_min) > float(spec.value_max):
                raise ValueError(
                    f"[{dest}]: spec.value_min ({spec.value_min}) must be <= spec.value_max ({spec.value_max})"
                )

        self.spec = spec
        self._allowed_float: set[float] = {float(v) for v in spec.allowed_values}

    # -------- convenience factories (avoid importing NumericRangeSpec explicitly) --------
    @classmethod
    def from_spec(
        cls,
        spec: NumericRangeSpec,
    ) -> type[argparse.Action]:
        """Core factory that uses an explicit ``NumericRangeSpec``.

        Args:
            spec: Numeric range specification.

        Returns:
            A callable suitable for argparse ``action``.

        Examples:
            >>> parser.add_argument(
            ...     "--threads",
            ...     action=ActionNumericRange.from_spec(
            ...         spec=NumericRangeSpec(value_kind="int", value_min=1)
            ...     ),
            ... )
        """
        return cast(type[argparse.Action], partial(cls, spec=spec))

    @classmethod
    def make(
        cls,
        value_kind: Literal["int", "float"] = "float",
        *,
        value_min: int | float | None = None,
        value_max: int | float | None = None,
        allowed_values: Sequence[int | float] = (),
        should_include_min: bool = True,
        should_include_max: bool = True,
        is_finite: bool = True,
    ) -> type[argparse.Action]:
        """Convenience factory for ``ActionNumericRange``.

        Args:
            value_kind: Numeric kind to parse.
            value_min: Minimum allowed value.
            value_max: Maximum allowed value.
            allowed_values: Explicitly whitelisted values.
            should_include_min: Whether ``value_min`` is inclusive.
            should_include_max: Whether ``value_max`` is inclusive.
            is_finite: Whether floats must be finite.

        Returns:
            A callable suitable for argparse ``action``.

        Examples:
            >>> parser.add_argument(
            ...     "--fold_change",
            ...     action=ActionNumericRange.make(
            ...         value_kind="float",
            ...         value_min=0.0,
            ...     ),
            ... )
        """
        return cls.from_spec(
            spec=NumericRangeSpec(
                value_kind=value_kind,
                value_min=value_min,
                value_max=value_max,
                allowed_values=tuple(allowed_values),
                should_include_min=should_include_min,
                should_include_max=should_include_max,
                is_finite=is_finite,
            )
        )

    @classmethod
    def non_negative(
        cls,
        value_kind: Literal["int", "float"] = "float",
        *,
        is_finite: bool = True,
    ) -> type[argparse.Action]:
        """Factory for non-negative values ``[0, +inf)``.

        Args:
            value_kind: Numeric kind to parse.
            is_finite: Whether floats must be finite.

        Returns:
            A callable suitable for argparse ``action``.

        Examples:
            >>> parser.add_argument("--thr_count", action=ActionNumericRange.non_negative(value_kind="int"))
        """
        return cls.make(
            value_kind=value_kind,
            value_min=0,
            should_include_min=True,
            is_finite=is_finite,
        )

    @classmethod
    def positive(
        cls,
        value_kind: Literal["int", "float"] = "float",
        *,
        is_finite: bool = True,
    ) -> type[argparse.Action]:
        """Factory for positive values ``(0, +inf)``.

        Args:
            value_kind: Numeric kind to parse.
            is_finite: Whether floats must be finite.

        Returns:
            A callable suitable for argparse ``action``.

        Examples:
            >>> parser.add_argument("--learning_rate", action=ActionNumericRange.positive())
        """
        return cls.make(
            value_kind=value_kind,
            value_min=0,
            should_include_min=False,
            is_finite=is_finite,
        )

    @classmethod
    def unit_interval(
        cls,
        value_kind: Literal["int", "float"] = "float",
        *,
        should_include_min: bool = True,
        should_include_max: bool = True,
        is_finite: bool = True,
    ) -> type[argparse.Action]:
        """Factory for interval constraints around ``[0, 1]``.

        Args:
            should_include_min: Whether the left bound (0) is inclusive.
            should_include_max: Whether the right bound (1) is inclusive.
            is_finite: Whether floats must be finite.

        Returns:
            A callable suitable for argparse ``action``.

        Examples:
            Closed interval (default):

            >>> parser.add_argument("--p", action=ActionNumericRange.unit_interval())

            Open-left, closed-right interval:

            >>> parser.add_argument(
            ...     "--x",
            ...     action=ActionNumericRange.unit_interval(
            ...         should_include_min=False,
            ...         should_include_max=True,
            ...     ),
            ... )
        """
        return cls.make(
            value_kind=value_kind,
            value_min=0.0,
            value_max=1.0,
            should_include_min=should_include_min,
            should_include_max=should_include_max,
            is_finite=is_finite,
        )

    def _parse_and_validate(
        self,
        *,
        value: Any,
        argument_name: str,
    ) -> int | float:
        """Parse and validate a numeric argument against ``spec``.

        Args:
            value: Raw value from argparse.
            argument_name: Display name for error messages.

        Returns:
            Parsed integer or float.

        Raises:
            argparse.ArgumentError: If the value is missing, cannot be parsed,
                is non-finite when disallowed, or violates bounds.
        """
        if value is None:
            raise argparse.ArgumentError(
                self,
                f"[{argument_name}]: Value cannot be None.",
            )

        if isinstance(value, (list, tuple)):
            seq = cast(list[object] | tuple[object, ...], value)
            if len(seq) != 1:
                raise argparse.ArgumentError(
                    self,
                    f"[{argument_name}]: Expected a single value, got {value!r}.",
                )
            value = seq[0]

        if not isinstance(value, (str, int, float)):
            raise argparse.ArgumentError(
                self,
                f"[{argument_name}]: Expected a number (int/float) or numeric string, got {type(value).__name__}.",
            )

        raw_value = str(value).strip()
        if not raw_value:
            raise argparse.ArgumentError(
                self,
                f"[{argument_name}]: Value cannot be empty.",
            )

        # fast path: allowed_values check by float membership
        try:
            float_candidate = float(raw_value)
        except Exception:
            float_candidate = None

        if float_candidate is not None and float_candidate in self._allowed_float:
            return int(raw_value) if self.spec.value_kind == "int" else float(raw_value)

        # parse
        try:
            parsed_value: int | float = (
                int(raw_value) if self.spec.value_kind == "int" else float(raw_value)
            )
        except Exception:
            raise argparse.ArgumentError(
                self,
                f"[{argument_name}]: Cannot parse {raw_value!r} as {self.spec.value_kind}.",
            )

        if self.spec.value_kind == "float" and self.spec.is_finite:
            if not math.isfinite(float(parsed_value)):
                raise argparse.ArgumentError(
                    self,
                    f"[{argument_name}]: Must be finite, got {parsed_value!r}.",
                )

        numeric_value = float(parsed_value)

        if self.spec.value_min is not None:
            value_min = float(self.spec.value_min)
            is_valid = (
                numeric_value >= value_min
                if self.spec.should_include_min
                else numeric_value > value_min
            )
            if not is_valid:
                op = ">=" if self.spec.should_include_min else ">"
                raise argparse.ArgumentError(
                    self,
                    f"[{argument_name}]: Must be {op} {self.spec.value_min}, got {parsed_value!r}.",
                )

        if self.spec.value_max is not None:
            value_max = float(self.spec.value_max)
            is_valid = (
                numeric_value <= value_max
                if self.spec.should_include_max
                else numeric_value < value_max
            )
            if not is_valid:
                op = "<=" if self.spec.should_include_max else "<"
                raise argparse.ArgumentError(
                    self,
                    f"[{argument_name}]: Must be {op} {self.spec.value_max}, got {parsed_value!r}.",
                )

        return parsed_value

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: str | Sequence[str] | None,
        option_string: str | None = None,
    ) -> None:
        """Parse, validate, and set a numeric value on the namespace."""
        argument_name = option_string or self.dest
        self._mark_dest_explicit(namespace)
        setattr(
            namespace,
            self.dest,
            self._parse_and_validate(value=values, argument_name=argument_name),
        )

    def _finalize_default_into_namespace(self, namespace: argparse.Namespace) -> None:
        """Parse and validate a default numeric value after parsing."""
        if not self._has_lazy_default():
            return
        setattr(
            namespace,
            self.dest,
            self._parse_and_validate(
                value=self.default,
                argument_name=f"{self.dest} (default)",
            ),
        )
