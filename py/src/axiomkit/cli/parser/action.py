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


@dataclass(frozen=True, slots=True)
class SpecPath:
    """Specification for validating path-like CLI inputs.

    Use this as the contract for :class:`ActionPath`. It encodes what kind of
    filesystem entry to accept and which validation rules to enforce.

    Attributes:
        kind_entry: Expected entry type, one of {"dir", "file", "exe"}.
        allowed_file_exts: Allowed file extensions (lowercase, no leading dots).
        should_exist: Whether the path must already exist.
        is_readable: Whether the path must be readable (if it exists).
        is_writable: Whether the path must be writable (mainly for outputs).

    Examples:
        Explicit file rule with suffix constraints:

        >>> spec = SpecPath(
        ...     kind_entry="file",
        ...     allowed_file_exts=("tsv", "tsv.gz"),
        ...     should_exist=True,
        ... )

        Output directory that may not exist yet:

        >>> out_spec = SpecPath(
        ...     kind_entry="dir",
        ...     should_exist=False,
        ...     is_writable=True,
        ... )
    """

    kind_entry: Literal["dir", "file", "exe"] = "file"
    # file-only: allowed extensions (case-insensitive, without leading dots)
    allowed_file_exts: tuple[str, ...] = ()
    # whether the target must already exist (useful for output paths if False)
    should_exist: bool = True
    # file/dir readability checks (only when should_exist=True)
    is_readable: bool = True
    # file/dir writability checks (mostly for outputs; only enforced when True)
    is_writable: bool = False


class ActionPath(argparse.Action):
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
        ...         spec=SpecPath(kind_entry="file", allowed_file_exts=("xlsx",))
        ...     ),
        ... )

    Notes:
        - Parsed values stored in ``argparse.Namespace`` are always
          ``pathlib.Path`` objects (resolved absolute paths).
        - Defaults are normalized during parser construction.
        - If a non-``None`` default fails validation, parser construction
          fails immediately; this can prevent even ``--help`` from rendering
          for that invocation.
        - For executables, "activate" scripts are not checked; only existence
          and executability are validated.
    """

    def __init__(
        self,
        option_strings: list[str],
        dest: str,
        *,
        spec: SpecPath | None = None,
        kind_entry: Literal["dir", "file", "exe"] = "file",
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
            spec: Prebuilt ``SpecPath``; overrides other spec params when set.
            kind_entry: Expected entry type when ``spec`` is not provided.
            allowed_file_exts: Allowed file extensions for file inputs.
            should_exist: Whether the path must already exist.
            is_readable: Whether readability is enforced when the path exists.
            is_writable: Whether writability is enforced (mainly outputs).
            **kwargs: Forwarded to ``argparse.Action``.

        Raises:
            ValueError: If an invalid ``kind_entry`` is provided.
            argparse.ArgumentError:
                If a non-``None`` default value fails validation. This happens
                at parser construction time (before parsing argv).
        """
        super().__init__(option_strings, dest, **kwargs)

        if spec is None:
            spec = SpecPath(
                kind_entry=kind_entry,
                allowed_file_exts=tuple(
                    str(ext).lower().lstrip(".")
                    for ext in (allowed_file_exts or ())
                    if ext and str(ext).strip()
                ),
                should_exist=should_exist,
                is_readable=is_readable,
                is_writable=is_writable,
            )

        if spec.kind_entry not in ("dir", "file", "exe"):
            raise ValueError(
                f"[{dest}]: `kind_entry` must be 'dir'|'file'|'exe', got {spec.kind_entry!r}"
            )

        self.spec = spec
        self.kind_entry = spec.kind_entry

        if spec.kind_entry == "file" and spec.allowed_file_exts:
            self.allowed_file_exts = tuple(
                str(ext).lower().lstrip(".")
                for ext in spec.allowed_file_exts
                if ext and str(ext).strip()
            )
        else:
            self.allowed_file_exts = None

        # Normalize/validate default as well (argparse does not call Action for defaults).
        if getattr(self, "default", None) not in {None, argparse.SUPPRESS}:
            self.default = self._normalize_one(
                value=self.default,
                name=f"{dest} (default)",
            )

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

        if self.kind_entry in {"file", "dir"}:
            try:
                path = Path(raw_value).expanduser().resolve()
            except Exception:
                raise argparse.ArgumentError(self, f"[{name}]: Invalid path: {raw_value!r}")

            if self.kind_entry == "file":
                if self.spec.should_exist and not path.is_file():
                    raise argparse.ArgumentError(
                        self, f"[{name}]: Not a file: {path}"
                    )

                if self.allowed_file_exts:
                    allowed_exts = set(self.allowed_file_exts)
                    suffix_compound = "".join(path.suffixes).lower().lstrip(".")
                    suffix_single = path.suffix.lower().lstrip(".")
                    if (suffix_compound not in allowed_exts) and (suffix_single not in allowed_exts):
                        expected_exts = ", ".join(self.allowed_file_exts)
                        actual_ext = "." + suffix_compound if suffix_compound else "(none)"
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
            raise argparse.ArgumentError(self, f"[{argument_name}]: Value cannot be None.")

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
        setattr(namespace, self.dest, path)

    # -------- convenience factories (avoid importing SpecPath explicitly) --------
    @classmethod
    def from_spec(cls, spec: SpecPath) -> type[argparse.Action]:
        """Factory returning a ``partial`` with a preset ``SpecPath``.

        Notes:
            - Validation semantics are the same as :class:`ActionPath`,
              including eager validation of non-``None`` defaults during parser
              construction.

        Args:
            spec: Path specification to enforce.

        Returns:
            A callable suitable for argparse ``action``.

        Examples:
            >>> parser.add_argument(
            ...     "--file_in",
            ...     action=ActionPath.from_spec(
            ...         spec=SpecPath(kind_entry="file", allowed_file_exts=("parquet",))
            ...     ),
            ... )
        """
        return cast(type[argparse.Action], partial(cls, spec=spec))

    @classmethod
    def make(
        cls,
        kind_entry: Literal["dir", "file", "exe"] = "file",
        *,
        allowed_file_exts: Iterable[str] = (),
        should_exist: bool = True,
        is_readable: bool = True,
        is_writable: bool = False,
    ) -> type[argparse.Action]:
        """Convenience factory that builds ``SpecPath`` from keyword inputs.

        Notes:
            - Validation semantics are the same as :class:`ActionPath`,
              including eager validation of non-``None`` defaults during parser
              construction.

        Args:
            kind_entry: Expected entry type.
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
            ...         kind_entry="file",
            ...         allowed_file_exts=("csv",),
            ...     ),
            ... )
        """
        return cls.from_spec(
            spec=SpecPath(
                kind_entry=kind_entry,
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
        *,
        exts: Iterable[str] = (),
        should_exist: bool = True,
        is_readable: bool = True,
        is_writable: bool = False,
    ) -> type[argparse.Action]:
        """Factory for file path validation.

        Notes:
            - Non-``None`` defaults are validated during parser construction.
            - If the default violates existence/extension/readability rules,
              parser construction fails and ``--help`` may not render.

        Args:
            exts: Allowed file extensions.
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
            kind_entry="file",
            allowed_file_exts=exts,
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
            - Non-``None`` defaults are validated during parser construction.
            - If the default violates existence/accessibility rules, parser
              construction fails and ``--help`` may not render.

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
            kind_entry="dir",
            should_exist=should_exist,
            is_readable=is_readable,
            is_writable=is_writable,
        )

    @classmethod
    def exe(cls) -> type[argparse.Action]:
        """Factory for executable path/command validation.

        Notes:
            - Non-``None`` defaults are validated during parser construction.
            - When used as ``action=ActionPath.exe(), default=\"some_tool\"``,
              if ``some_tool`` is not resolvable in current PATH, parser
              construction fails and ``--help`` may not render.

        Returns:
            A callable suitable for argparse ``action``.

        Examples:
            >>> parser.add_argument("--exe_rscript", action=ActionPath.exe(), default="Rscript")
        """
        return cls.make(kind_entry="exe")


_RE_ENV_ASSIGN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=.*$")


class ActionCommandPrefix(argparse.Action):
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

        # Normalize/validate default as well (argparse does not call Action for defaults).
        if getattr(self, "default", None) not in {None, argparse.SUPPRESS}:
            # Use an immutable default so it won't be mutated by later `extend`.
            self.default = tuple(
                self._tokenize_and_validate(
                    value=self.default,
                    argument_name=f"{dest} (default)",
                )
            )

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
        while env_assignment_count < len(tokens) and _RE_ENV_ASSIGN.match(tokens[env_assignment_count]):
            env_assignment_count += 1
        if env_assignment_count >= len(tokens):
            raise argparse.ArgumentError(
                self,
                f"[{argument_name}]: Must contain a command after env assignments. Yours: {input_text!r}",
            )

        command_head = tokens[env_assignment_count]
        if command_head in {"conda", "mamba", "micromamba"}:
            # forbid activate
            if env_assignment_count + 1 < len(tokens) and tokens[env_assignment_count + 1] == "activate":
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
        # If a default was supplied, argparse places it into the namespace
        # before parsing. On the first explicit user-specified occurrence we
        # should *replace* the default rather than append to it.
        if cur is None or isinstance(cur, tuple):
            setattr(namespace, self.dest, list(tokens))
            return

        if not isinstance(cur, list):
            raise argparse.ArgumentError(
                self, f"[{argument_name}]: Internal type error, expected list."
            )
        current_tokens = cast(list[str], cur)
        current_tokens.extend(tokens)


_RE_HEX6 = re.compile(r"#[0-9A-Fa-f]{6}$")


class ActionHexColor(argparse.Action):
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

        # Normalize/validate default as well (argparse does not call Action for defaults).
        if getattr(self, "default", None) not in {None, argparse.SUPPRESS}:
            self.default = self._normalize_hex(
                value=self.default,
                argument_name=f"{dest} (default)",
            )

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
        setattr(
            namespace,
            self.dest,
            self._normalize_hex(value=values, argument_name=argument_name),
        )


@dataclass(frozen=True, slots=True)
class SpecNumericRange:
    """Specification describing allowed numeric inputs.

    Attributes:
        kind_value: Numeric kind, either "int" or "float".
        min_value: Minimum allowed value or ``None``.
        max_value: Maximum allowed value or ``None``.
        allowed_values: Explicitly whitelisted values.
        should_include_min: Whether the lower bound is inclusive.
        should_include_max: Whether the upper bound is inclusive.
        is_finite: Whether floats must be finite.

    Examples:
        Strict positive integer:

        >>> SpecNumericRange(kind_value="int", min_value=0, should_include_min=False)

        Open-left, closed-right unit interval:

        >>> SpecNumericRange(
        ...     kind_value="float",
        ...     min_value=0.0,
        ...     max_value=1.0,
        ...     should_include_min=False,
        ...     should_include_max=True,
        ... )
    """

    kind_value: Literal["int", "float"] = "float"
    min_value: int | float | None = None
    max_value: int | float | None = None

    # if provided, values in this set are accepted immediately (even if outside min/max)
    allowed_values: tuple[int | float, ...] = ()

    should_include_min: bool = True
    should_include_max: bool = True

    # float-only: reject NaN/Inf by default
    is_finite: bool = True


class ActionNumericRange(argparse.Action):
    """Argparse action enforcing numeric value constraints.

    Parses an option as int/float, validates it against ``SpecNumericRange``,
    and normalizes defaults during parser construction.

    Examples:
        Directly provide a spec:

        >>> parser.add_argument(
        ...     "--learning_rate",
        ...     action=ActionNumericRange,
        ...     spec=SpecNumericRange(kind_value="float", min_value=0, should_include_min=False),
        ... )

        Use convenience factories:

        >>> parser.add_argument("--epochs", action=ActionNumericRange.non_negative(kind_value="int"))
        >>> parser.add_argument("--lr", action=ActionNumericRange.positive(kind_value="float"))
        >>> parser.add_argument("--p", action=ActionNumericRange.unit_interval())

    Notes:
        - ``allowed_values`` are accepted even if outside min/max.
        - When ``kind_value="float"`` and ``is_finite=True``, rejects NaN/Inf.
        - Defaults are validated eagerly; misconfigured defaults raise early.
    """

    def __init__(
        self,
        option_strings: list[str],
        dest: str,
        *,
        spec: SpecNumericRange,
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
            argparse.ArgumentError: If the default value fails validation.
        """
        super().__init__(option_strings, dest, **kwargs)

        if spec.kind_value not in ("int", "float"):
            raise ValueError(
                f"[{dest}]: spec.kind_value must be 'int'|'float', got {spec.kind_value!r}"
            )

        if spec.min_value is not None and spec.max_value is not None:
            if float(spec.min_value) > float(spec.max_value):
                raise ValueError(
                    f"[{dest}]: spec.min_value ({spec.min_value}) must be <= spec.max_value ({spec.max_value})"
                )

        self.spec = spec
        self._allowed_float: set[float] = {float(v) for v in spec.allowed_values}

        # Normalize/validate default as well (argparse does not call Action for defaults).
        if getattr(self, "default", None) not in {None, argparse.SUPPRESS}:
            self.default = self._parse_and_validate(
                value=self.default,
                argument_name=f"{dest} (default)",
            )

    # -------- convenience factories (avoid importing SpecNumericRange explicitly) --------
    @classmethod
    def from_spec(
        cls,
        spec: SpecNumericRange,
    ) -> type[argparse.Action]:
        """Core factory that uses an explicit ``SpecNumericRange``.

        Args:
            spec: Numeric range specification.

        Returns:
            A callable suitable for argparse ``action``.

        Examples:
            >>> parser.add_argument(
            ...     "--threads",
            ...     action=ActionNumericRange.from_spec(
            ...         spec=SpecNumericRange(kind_value="int", min_value=1)
            ...     ),
            ... )
        """
        return cast(type[argparse.Action], partial(cls, spec=spec))

    @classmethod
    def make(
        cls,
        kind_value: Literal["int", "float"] = "float",
        *,
        min_value: int | float | None = None,
        max_value: int | float | None = None,
        allowed_values: Sequence[int | float] = (),
        should_include_min: bool = True,
        should_include_max: bool = True,
        is_finite: bool = True,
    ) -> type[argparse.Action]:
        """Convenience factory for ``ActionNumericRange``.

        Args:
            kind_value: Numeric kind to parse.
            min_value: Minimum allowed value.
            max_value: Maximum allowed value.
            allowed_values: Explicitly whitelisted values.
            should_include_min: Whether ``min_value`` is inclusive.
            should_include_max: Whether ``max_value`` is inclusive.
            is_finite: Whether floats must be finite.

        Returns:
            A callable suitable for argparse ``action``.

        Examples:
            >>> parser.add_argument(
            ...     "--fold_change",
            ...     action=ActionNumericRange.make(
            ...         kind_value="float",
            ...         min_value=0.0,
            ...     ),
            ... )
        """
        return cls.from_spec(
            spec=SpecNumericRange(
                kind_value=kind_value,
                min_value=min_value,
                max_value=max_value,
                allowed_values=tuple(allowed_values),
                should_include_min=should_include_min,
                should_include_max=should_include_max,
                is_finite=is_finite,
            )
        )

    @classmethod
    def non_negative(
        cls,
        kind_value: Literal["int", "float"] = "float",
        *,
        is_finite: bool = True,
    ) -> type[argparse.Action]:
        """Factory for non-negative values ``[0, +inf)``.

        Args:
            kind_value: Numeric kind to parse.
            is_finite: Whether floats must be finite.

        Returns:
            A callable suitable for argparse ``action``.

        Examples:
            >>> parser.add_argument("--thr_count", action=ActionNumericRange.non_negative(kind_value="int"))
        """
        return cls.make(
            kind_value=kind_value,
            min_value=0,
            should_include_min=True,
            is_finite=is_finite,
        )

    @classmethod
    def positive(
        cls,
        kind_value: Literal["int", "float"] = "float",
        *,
        is_finite: bool = True,
    ) -> type[argparse.Action]:
        """Factory for positive values ``(0, +inf)``.

        Args:
            kind_value: Numeric kind to parse.
            is_finite: Whether floats must be finite.

        Returns:
            A callable suitable for argparse ``action``.

        Examples:
            >>> parser.add_argument("--learning_rate", action=ActionNumericRange.positive())
        """
        return cls.make(
            kind_value=kind_value,
            min_value=0,
            should_include_min=False,
            is_finite=is_finite,
        )

    @classmethod
    def unit_interval(
        cls,
        kind_value: Literal["int", "float"] = "float",
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
            kind_value=kind_value,
            min_value=0.0,
            max_value=1.0,
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
            return int(raw_value) if self.spec.kind_value == "int" else float(raw_value)

        # parse
        try:
            parsed_value: int | float = (
                int(raw_value) if self.spec.kind_value == "int" else float(raw_value)
            )
        except Exception:
            raise argparse.ArgumentError(
                self,
                f"[{argument_name}]: Cannot parse {raw_value!r} as {self.spec.kind_value}.",
            )

        if self.spec.kind_value == "float" and self.spec.is_finite:
            if not math.isfinite(float(parsed_value)):
                raise argparse.ArgumentError(
                    self,
                    f"[{argument_name}]: Must be finite, got {parsed_value!r}.",
                )

        numeric_value = float(parsed_value)

        if self.spec.min_value is not None:
            min_value = float(self.spec.min_value)
            is_valid = (
                numeric_value >= min_value
                if self.spec.should_include_min
                else numeric_value > min_value
            )
            if not is_valid:
                op = ">=" if self.spec.should_include_min else ">"
                raise argparse.ArgumentError(
                    self,
                    f"[{argument_name}]: Must be {op} {self.spec.min_value}, got {parsed_value!r}.",
                )

        if self.spec.max_value is not None:
            max_value = float(self.spec.max_value)
            is_valid = (
                numeric_value <= max_value
                if self.spec.should_include_max
                else numeric_value < max_value
            )
            if not is_valid:
                op = "<=" if self.spec.should_include_max else "<"
                raise argparse.ArgumentError(
                    self,
                    f"[{argument_name}]: Must be {op} {self.spec.max_value}, got {parsed_value!r}.",
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
        setattr(
            namespace,
            self.dest,
            self._parse_and_validate(value=values, argument_name=argument_name),
        )
