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
class PathSpec:
    """Specification for validating path-like CLI inputs.

    Use this as the contract for :class:`PathAction`. It encodes what kind of
    filesystem entry to accept and which validation rules to enforce.

    Attributes:
        kind_entry: Expected entry type, one of {"dir", "file", "exe"}.
        rule_file_exts: Allowed file extensions (lowercase, no leading dots).
        if_must_exist: Whether the path must already exist.
        if_readable: Whether the path must be readable (if it exists).
        if_writable: Whether the path must be writable (mainly for outputs).

    Examples:
        ``PathSpec(kind_entry="file", rule_file_exts=("tsv", "tsv.gz"))``
        enforces an existing readable TSV/TSV.GZ file.
    """

    kind_entry: Literal["dir", "file", "exe"] = "file"
    # file-only: allowed extensions (case-insensitive, without leading dots)
    rule_file_exts: tuple[str, ...] = ()
    # whether the target must already exist (useful for output paths if False)
    if_must_exist: bool = True
    # file/dir readability checks (only when if_must_exist=True)
    if_readable: bool = True
    # file/dir writability checks (mostly for outputs; only enforced when True)
    if_writable: bool = False


class PathAction(argparse.Action):
    """Validate and normalize path arguments for argparse.

    For ``file``/``dir`` kinds, resolves to absolute :class:`pathlib.Path` and
    enforces existence, readability, writability, and allowed extensions. For
    ``exe`` it resolves either an explicit path or a command via ``PATH``.

    Typical usage:
        ``parser.add_argument("--file_in", action=PathAction.file(exts=("tsv",)))``
        ``parser.add_argument("--dir_out", action=PathAction.dir(if_must_exist=False))``
        ``parser.add_argument("--path_rscript", action=PathAction.exe(), default="Rscript")``

    Notes:
        - Defaults are normalized during parser construction.
        - For executables, "activate" scripts are not checked; only existence
          and executability are validated.
    """

    def __init__(
        self,
        option_strings: list[str],
        dest: str,
        *,
        spec: PathSpec | None = None,
        kind_entry: Literal["dir", "file", "exe"] = "file",
        rule_file_exts: Iterable[str] | None = None,
        if_must_exist: bool = True,
        if_readable: bool = True,
        if_writable: bool = False,
        **kwargs: Any,
    ) -> None:
        """Construct a PathAction.

        Args:
            option_strings: Option strings received from argparse.
            dest: Namespace attribute name.
            spec: Prebuilt ``PathSpec``; overrides other spec params when set.
            kind_entry: Expected entry type when ``spec`` is not provided.
            rule_file_exts: Allowed file extensions for file inputs.
            if_must_exist: Whether the path must already exist.
            if_readable: Whether readability is enforced when the path exists.
            if_writable: Whether writability is enforced (mainly outputs).
            **kwargs: Forwarded to ``argparse.Action``.

        Raises:
            ValueError: If an invalid ``kind_entry`` is provided.
            argparse.ArgumentError: If the default value fails validation.
        """
        super().__init__(option_strings, dest, **kwargs)

        if spec is None:
            spec = PathSpec(
                kind_entry=kind_entry,
                rule_file_exts=tuple(
                    str(ext).lower().lstrip(".")
                    for ext in (rule_file_exts or ())
                    if ext and str(ext).strip()
                ),
                if_must_exist=if_must_exist,
                if_readable=if_readable,
                if_writable=if_writable,
            )

        if spec.kind_entry not in ("dir", "file", "exe"):
            raise ValueError(
                f"[{dest}]: `kind_entry` must be 'dir'|'file'|'exe', got {spec.kind_entry!r}"
            )

        self.spec = spec
        self.kind_entry = spec.kind_entry

        if spec.kind_entry == "file" and spec.rule_file_exts:
            self.rule_file_exts = tuple(
                str(ext).lower().lstrip(".")
                for ext in spec.rule_file_exts
                if ext and str(ext).strip()
            )
        else:
            self.rule_file_exts = None

        # Normalize/validate default as well (argparse does not call Action for defaults).
        if getattr(self, "default", None) not in {None, argparse.SUPPRESS}:
            self.default = self._normalize_one(
                value=self.default,
                c_name=f"{dest} (default)",
            )

    def _normalize_one(
        self, *, value: str | os.PathLike[str] | os.PathLike[bytes], c_name: str
    ) -> Path:
        """Normalize a single path-like value.

        Args:
            value: Input path-like value.
            c_name: Display name for error messages.

        Returns:
            An absolute ``Path`` instance.

        Raises:
            argparse.ArgumentError: If the value is empty, invalid, missing,
                not readable/writable, or violates extension rules.
        """
        c_raw = os.fsdecode(value)

        if not c_raw.strip():
            raise argparse.ArgumentError(self, f"[{c_name}]: Value cannot be empty.")

        if self.kind_entry in {"file", "dir"}:
            try:
                cls_path = Path(c_raw).expanduser().resolve()
            except Exception:
                raise argparse.ArgumentError(
                    self, f"[{c_name}]: Invalid path: {c_raw!r}"
                )

            if self.kind_entry == "file":
                if self.spec.if_must_exist and not cls_path.is_file():
                    raise argparse.ArgumentError(
                        self, f"[{c_name}]: Not a file: {cls_path}"
                    )

                if self.rule_file_exts:
                    set_exts = set(self.rule_file_exts)
                    c_suffix = "".join(cls_path.suffixes).lower().lstrip(".")
                    c_bare = cls_path.suffix.lower().lstrip(".")
                    if (c_suffix not in set_exts) and (c_bare not in set_exts):
                        c_expected = ", ".join(self.rule_file_exts)
                        c_got = "." + c_suffix if c_suffix else "(none)"
                        raise argparse.ArgumentError(
                            self,
                            f"[{c_name}]: Expected extension(s) in ({c_expected}), got {c_got}: {cls_path}",
                        )

                if (
                    self.spec.if_must_exist
                    and self.spec.if_readable
                    and not os.access(cls_path, os.R_OK)
                ):
                    raise argparse.ArgumentError(
                        self, f"[{c_name}]: File not readable: {cls_path}"
                    )

            else:
                if self.spec.if_must_exist and not cls_path.is_dir():
                    raise argparse.ArgumentError(
                        self,
                        f"[{c_name}]: Not a directory: {cls_path}",
                    )
                if (
                    self.spec.if_must_exist
                    and self.spec.if_readable
                    and not os.access(cls_path, os.R_OK | os.X_OK)
                ):
                    raise argparse.ArgumentError(
                        self,
                        f"[{c_name}]: Directory not accessible: {cls_path}",
                    )

            return cls_path

        # exe: path-ish vs command-ish
        c_raw_str = str(c_raw)
        b_has_sep = any(sep in c_raw_str for sep in (os.sep, os.altsep) if sep)
        b_is_absish = c_raw_str.startswith(("~", ".")) or os.path.isabs(c_raw_str)

        if b_has_sep or b_is_absish:
            cls_exe = Path(c_raw_str).expanduser()
            if not cls_exe.exists():
                raise argparse.ArgumentError(
                    self, f"[{c_name}]: Executable path not found: {cls_exe}"
                )
            if cls_exe.is_dir():
                raise argparse.ArgumentError(
                    self, f"[{c_name}]: Executable path is a directory: {cls_exe}"
                )
            if os.name != "nt" and not os.access(cls_exe, os.X_OK):
                raise argparse.ArgumentError(
                    self, f"[{c_name}]: Not executable: {cls_exe}"
                )
            return cls_exe.resolve()

        c_hit = shutil.which(c_raw_str)
        if not c_hit:
            raise argparse.ArgumentError(
                self,
                f"[{c_name}]: Executable not found in PATH: {c_raw_str!r} (consider passing an absolute path).",
            )
        return Path(c_hit).resolve()

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
        """
        c_name = option_string or self.dest

        if values is None:
            raise argparse.ArgumentError(self, f"[{c_name}]: Value cannot be None.")

        if isinstance(values, (list, tuple)):
            seq = cast(list[object] | tuple[object, ...], values)
            if len(seq) != 1:
                raise argparse.ArgumentError(
                    self, f"[{c_name}]: Expected a single value, got {values!r}."
                )
            values = seq[0]

        if not isinstance(values, (str, os.PathLike, Path)):
            raise argparse.ArgumentError(
                self,
                f"[{c_name}]: Expected a path-like value (str/Path), got {type(values).__name__}.",
            )

        values = cast(str | os.PathLike[str] | os.PathLike[bytes], values)
        cls_path = self._normalize_one(value=values, c_name=c_name)
        setattr(namespace, self.dest, cls_path)

    # -------- convenience factories (avoid importing PathSpec explicitly) --------
    @classmethod
    def build(cls, *, spec: PathSpec, **kwargs: Any):
        """Factory returning a ``partial`` with a preset ``PathSpec``.

        Args:
            spec: Path specification to enforce.
            **kwargs: Extra args forwarded to the constructor.

        Returns:
            A callable suitable for argparse ``action``.
        """
        return partial(cls, spec=spec, **kwargs)

    @classmethod
    def file(
        cls,
        *,
        exts: Iterable[str] = (),
        if_must_exist: bool = True,
        if_readable: bool = True,
        if_writable: bool = False,
        **kwargs: Any,
    ):
        """Factory for file path validation.

        Args:
            exts: Allowed file extensions.
            if_must_exist: Whether the file must already exist.
            if_readable: Whether readability is required.
            if_writable: Whether writability is required.
            **kwargs: Extra args forwarded to the constructor.

        Returns:
            A callable suitable for argparse ``action``.
        """
        return partial(
            cls,
            spec=PathSpec(
                kind_entry="file",
                rule_file_exts=tuple(
                    str(e).lower().lstrip(".") for e in exts if str(e).strip()
                ),
                if_must_exist=if_must_exist,
                if_readable=if_readable,
                if_writable=if_writable,
            ),
            **kwargs,
        )

    @classmethod
    def dir(
        cls,
        *,
        if_must_exist: bool = True,
        if_readable: bool = True,
        if_writable: bool = False,
        **kwargs: Any,
    ):
        """Factory for directory path validation.

        Args:
            if_must_exist: Whether the directory must already exist.
            if_readable: Whether readability/execute permission is required.
            if_writable: Whether writability is required.
            **kwargs: Extra args forwarded to the constructor.

        Returns:
            A callable suitable for argparse ``action``.
        """
        return partial(
            cls,
            spec=PathSpec(
                kind_entry="dir",
                rule_file_exts=(),
                if_must_exist=if_must_exist,
                if_readable=if_readable,
                if_writable=if_writable,
            ),
            **kwargs,
        )

    @classmethod
    def exe(cls, **kwargs: Any):
        """Factory for executable path/command validation.

        Args:
            **kwargs: Extra args forwarded to the constructor.

        Returns:
            A callable suitable for argparse ``action``.
        """
        return partial(
            cls,
            spec=PathSpec(kind_entry="exe"),
            **kwargs,
        )


_RE_ENV_ASSIGN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=.*$")


class CommandPrefixAction(argparse.Action):
    """Validate and tokenize shell-style command prefixes.

    Intended for prefixes like ``"micromamba run -n ENV"`` or
    ``"VAR=1 conda run -n ENV"``. Disallows activate-style commands and
    enforces the presence of an environment flag when using
    ``conda|mamba|micromamba run``.

    Example:
        ``parser.add_argument("--rule_exec_prefix", action=CommandPrefixAction)``
        allows values such as ``"VAR=1 micromamba run -n env"``.

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
        """Construct a CommandPrefixAction.

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
                    c_name=f"{dest} (default)",
                )
            )

    def _tokenize_and_validate(self, *, value: object | None, c_name: str) -> list[str]:
        """Parse and validate a command prefix string.

        Args:
            value: Raw value from argparse.
            c_name: Display name for error messages.

        Returns:
            Tokenized command prefix.

        Raises:
            argparse.ArgumentError: If the string is empty, invalid, lacks a
                command, points to a missing executable, or misuses env flags.
        """
        if value is None:
            raise argparse.ArgumentError(self, f"[{c_name}]: Value cannot be None.")

        if isinstance(value, str):
            c_in = value
        elif isinstance(value, (list, tuple)):
            seq = cast(list[object] | tuple[object, ...], value)
            c_in = " ".join(str(v) for v in seq)
        else:
            c_in = str(value)

        c_in = c_in.strip()
        if not c_in:
            raise argparse.ArgumentError(
                self, f"[{c_name}]: Cannot be empty/whitespace."
            )

        try:
            l_tokens = [t for t in shlex.split(c_in) if t.strip()]
        except ValueError as e:
            raise argparse.ArgumentError(
                self, f"[{c_name}]: Invalid shell string: {e}. Yours: {c_in!r}"
            )

        if not l_tokens:
            raise argparse.ArgumentError(
                self, f"[{c_name}]: Must contain at least one token. Yours: {c_in!r}"
            )

        # skip leading VAR=... assignments
        n_env = 0
        while n_env < len(l_tokens) and _RE_ENV_ASSIGN.match(l_tokens[n_env]):
            n_env += 1
        if n_env >= len(l_tokens):
            raise argparse.ArgumentError(
                self,
                f"[{c_name}]: Must contain a command after env assignments. Yours: {c_in!r}",
            )

        c_head = l_tokens[n_env]
        if c_head in {"conda", "mamba", "micromamba"}:
            # forbid activate
            if n_env + 1 < len(l_tokens) and l_tokens[n_env + 1] == "activate":
                raise argparse.ArgumentError(
                    self,
                    f"[{c_name}]: Do not use '... activate ...'. Use '... run -n <env>' style. Yours: {c_in!r}",
                )

        if shutil.which(c_head) is None:
            raise argparse.ArgumentError(
                self, f"[{c_name}]: Executable not in PATH: {c_head!r}"
            )

        # conda/mamba/micromamba run: require env
        if (
            c_head in {"conda", "mamba", "micromamba"}
            and n_env + 1 < len(l_tokens)
            and l_tokens[n_env + 1] == "run"
        ):
            set_env_flags = {"-n", "--name", "-p", "--prefix"}
            tail = l_tokens[n_env + 2 :]
            if not any(f in tail for f in set_env_flags):
                raise argparse.ArgumentError(
                    self,
                    f"[{c_name}]: '{c_head} run' must include -n/--name <env> or -p/--prefix <path>. Yours: {c_in!r}",
                )

            for f in set_env_flags:
                if f in tail:
                    idx = l_tokens.index(f, n_env + 2)
                    if idx == len(l_tokens) - 1 or l_tokens[idx + 1].startswith("-"):
                        raise argparse.ArgumentError(
                            self,
                            f"[{c_name}]: Missing env name/path after {f}. Yours: {c_in!r}",
                        )
                    break

        return list(l_tokens)

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
        c_name = option_string or self.dest
        l_tokens = self._tokenize_and_validate(value=values, c_name=c_name)

        cur = getattr(namespace, self.dest, None)
        # If a default was supplied, argparse places it into the namespace
        # before parsing. On the first explicit user-specified occurrence we
        # should *replace* the default rather than append to it.
        if cur is None or isinstance(cur, tuple):
            setattr(namespace, self.dest, list(l_tokens))
            return

        if not isinstance(cur, list):
            raise argparse.ArgumentError(
                self, f"[{c_name}]: Internal type error, expected list."
            )
        l_cur = cast(list[str], cur)
        l_cur.extend(l_tokens)


_RE_HEX6 = re.compile(r"#[0-9A-Fa-f]{6}$")


class HexColorAction(argparse.Action):
    """Parse and validate hex colors in ``#RRGGBB`` format.

    Example:
        ``parser.add_argument("--panel_border_color", action=HexColorAction)``

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
        """Construct a HexColorAction.

        Args:
            option_strings: Option strings received from argparse.
            dest: Namespace attribute name.
            **kwargs: Forwarded to ``argparse.Action``.
        """
        super().__init__(option_strings, dest, **kwargs)

        # Normalize/validate default as well (argparse does not call Action for defaults).
        if getattr(self, "default", None) not in {None, argparse.SUPPRESS}:
            self.default = self._normalize_hex(
                value=self.default, c_name=f"{dest} (default)"
            )

    def _normalize_hex(self, *, value: object | None, c_name: str) -> str:
        """Normalize a hex color string.

        Args:
            value: Raw value from argparse.
            c_name: Display name for error messages.

        Returns:
            Normalized uppercase hex string.

        Raises:
            argparse.ArgumentError: If the value is missing or not ``#RRGGBB``.
        """
        if value is None:
            raise argparse.ArgumentError(
                self, f"[{c_name}]: Color hex string cannot be None."
            )

        if isinstance(value, (list, tuple)):
            seq = cast(list[object] | tuple[object, ...], value)
            if len(seq) != 1:
                raise argparse.ArgumentError(
                    self, f"[{c_name}]: Expected a single value, got {value!r}."
                )
            value = seq[0]

        if not isinstance(value, str):
            raise argparse.ArgumentError(
                self,
                f"[{c_name}]: Expected a string in format '#RRGGBB', got {type(value).__name__}.",
            )

        c_val = value.strip()
        if not c_val:
            raise argparse.ArgumentError(
                self, f"[{c_name}]: Color hex string cannot be empty."
            )
        if not _RE_HEX6.fullmatch(c_val):
            raise argparse.ArgumentError(
                self,
                f"[{c_name}]: Color hex string must be in the format '#RRGGBB', got {c_val!r}.",
            )

        return f"#{c_val[1:].upper()}"

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: object | None,
        option_string: str | None = None,
    ) -> None:
        """Parse and set a validated hex color on the namespace."""
        c_name = option_string or self.dest
        setattr(namespace, self.dest, self._normalize_hex(value=values, c_name=c_name))


@dataclass(frozen=True, slots=True)
class NumericRangeSpec:
    """Specification describing allowed numeric inputs.

    Attributes:
        kind_value: Numeric kind, either "int" or "float".
        min_value: Minimum allowed value or ``None``.
        max_value: Maximum allowed value or ``None``.
        allowed_values: Explicitly whitelisted values.
        if_inclusive_min: Whether the lower bound is inclusive.
        if_inclusive_max: Whether the upper bound is inclusive.
        if_finite: Whether floats must be finite.

    Examples:
        ``NumericRangeSpec(kind_value="float", min_value=0, max_value=1, if_inclusive_min=False)``
        describes (0, 1] for floats.
    """

    kind_value: Literal["int", "float"] = "float"
    min_value: int | float | None = None
    max_value: int | float | None = None

    # if provided, values in this set are accepted immediately (even if outside min/max)
    allowed_values: tuple[int | float, ...] = ()

    if_inclusive_min: bool = True
    if_inclusive_max: bool = True

    # float-only: reject NaN/Inf by default
    if_finite: bool = True


class NumericRangeAction(argparse.Action):
    """Argparse action enforcing numeric value constraints.

    Parses an option as int/float, validates it against ``NumericRangeSpec``,
    and normalizes defaults during parser construction.

    Typical usage:
        ``parser.add_argument("--learning-rate", action=NumericRangeAction, spec=NumericRangeSpec(min_value=0, max_value=1))``
        ``parser.add_argument("--epochs", action=NumericRangeAction.build(kind_value="int", min_value=1))``

    Notes:
        - ``allowed_values`` are accepted even if outside min/max.
        - When ``kind_value="float"`` and ``if_finite=True``, rejects NaN/Inf.
        - Defaults are validated eagerly; misconfigured defaults raise early.
    """

    def __init__(
        self,
        option_strings: list[str],
        dest: str,
        *,
        spec: NumericRangeSpec,
        **kwargs: Any,
    ) -> None:
        """Construct a NumericRangeAction.

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
                value=self.default, c_name=f"{dest} (default)"
            )

    # -------- convenience factories (avoid importing NumericRangeSpec explicitly) --------
    @classmethod
    def build(
        cls,
        *,
        kind_value: Literal["int", "float"] = "float",
        min_value: int | float | None = None,
        max_value: int | float | None = None,
        allowed_values: Sequence[int | float] = (),
        if_inclusive_min: bool = True,
        if_inclusive_max: bool = True,
        if_finite: bool = True,
        **kwargs: Any,
    ):
        """Convenience factory for ``NumericRangeAction``.

        Args:
            kind_value: Numeric kind to parse.
            min_value: Minimum allowed value.
            max_value: Maximum allowed value.
            allowed_values: Explicitly whitelisted values.
            if_inclusive_min: Whether ``min_value`` is inclusive.
            if_inclusive_max: Whether ``max_value`` is inclusive.
            if_finite: Whether floats must be finite.
            **kwargs: Forwarded to the constructor.

        Returns:
            A callable suitable for argparse ``action``.
        """
        return partial(
            cls,
            spec=NumericRangeSpec(
                kind_value=kind_value,
                min_value=min_value,
                max_value=max_value,
                allowed_values=tuple(allowed_values),
                if_inclusive_min=if_inclusive_min,
                if_inclusive_max=if_inclusive_max,
                if_finite=if_finite,
            ),
            **kwargs,
        )

    @classmethod
    def p_value(cls, **kwargs: Any):
        """Factory for P-value ranges ``[0, 1]``.

        Args:
            **kwargs: Forwarded to the constructor.

        Returns:
            A callable suitable for argparse ``action``.
        """
        return cls.build(kind_value="float", min_value=0.0, max_value=1.0, **kwargs)

    def _parse_and_validate(self, *, value: Any, c_name: str) -> int | float:
        """Parse and validate a numeric argument against ``spec``.

        Args:
            value: Raw value from argparse.
            c_name: Display name for error messages.

        Returns:
            Parsed integer or float.

        Raises:
            argparse.ArgumentError: If the value is missing, cannot be parsed,
                is non-finite when disallowed, or violates bounds.
        """
        if value is None:
            raise argparse.ArgumentError(self, f"[{c_name}]: Value cannot be None.")

        if isinstance(value, (list, tuple)):
            seq = cast(list[object] | tuple[object, ...], value)
            if len(seq) != 1:
                raise argparse.ArgumentError(
                    self, f"[{c_name}]: Expected a single value, got {value!r}."
                )
            value = seq[0]

        if not isinstance(value, (str, int, float)):
            raise argparse.ArgumentError(
                self,
                f"[{c_name}]: Expected a number (int/float) or numeric string, got {type(value).__name__}.",
            )

        c_raw = str(value).strip()
        if not c_raw:
            raise argparse.ArgumentError(self, f"[{c_name}]: Value cannot be empty.")

        # fast path: allowed_values check by float membership
        try:
            n_try = float(c_raw)
        except Exception:
            n_try = None

        if n_try is not None and n_try in self._allowed_float:
            return int(c_raw) if self.spec.kind_value == "int" else float(c_raw)

        # parse
        try:
            n_val: int | float = (
                int(c_raw) if self.spec.kind_value == "int" else float(c_raw)
            )
        except Exception:
            raise argparse.ArgumentError(
                self,
                f"[{c_name}]: Cannot parse {c_raw!r} as {self.spec.kind_value}.",
            )

        if self.spec.kind_value == "float" and self.spec.if_finite:
            if not math.isfinite(float(n_val)):
                raise argparse.ArgumentError(
                    self, f"[{c_name}]: Must be finite, got {n_val!r}."
                )

        n_f = float(n_val)

        if self.spec.min_value is not None:
            n_min = float(self.spec.min_value)
            ok = n_f >= n_min if self.spec.if_inclusive_min else n_f > n_min
            if not ok:
                op = ">=" if self.spec.if_inclusive_min else ">"
                raise argparse.ArgumentError(
                    self,
                    f"[{c_name}]: Must be {op} {self.spec.min_value}, got {n_val!r}.",
                )

        if self.spec.max_value is not None:
            n_max = float(self.spec.max_value)
            ok = n_f <= n_max if self.spec.if_inclusive_max else n_f < n_max
            if not ok:
                op = "<=" if self.spec.if_inclusive_max else "<"
                raise argparse.ArgumentError(
                    self,
                    f"[{c_name}]: Must be {op} {self.spec.max_value}, got {n_val!r}.",
                )

        return n_val

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: str | Sequence[str] | None,
        option_string: str | None = None,
    ) -> None:
        """Parse, validate, and set a numeric value on the namespace."""
        c_name = option_string or self.dest
        setattr(
            namespace, self.dest, self._parse_and_validate(value=values, c_name=c_name)
        )
