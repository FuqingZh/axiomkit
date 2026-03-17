from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

DEFAULT_WORKSPACE_DIRS: tuple[str, ...] = (
    "in",
    "out",
    "tmp",
    "logs",
    "configs",
    "meta",
)


def _validate_relative_path(*, name: str, value: str, errors: list[str]) -> None:
    value_clean = value.strip()
    if not value_clean:
        errors.append(f"`{name}` must not be empty.")
        return

    path_relative = Path(value_clean)
    if path_relative.is_absolute():
        errors.append(f"`{name}` must be a relative path, got {value!r}.")
    if ".." in path_relative.parts:
        errors.append(f"`{name}` must not contain '..', got {value!r}.")


def _normalize_workspace_dirs(
    dirs: Sequence[str] | Mapping[str, str],
) -> dict[str, str]:
    if isinstance(dirs, str):
        raise TypeError("`dirs` must not be a plain string; pass a sequence or mapping.")

    if isinstance(dirs, Mapping):
        return {str(key): str(value) for key, value in dirs.items()}

    return {str(name): str(name) for name in dirs}


@dataclass(frozen=True, slots=True)
class WorkspacePaths:
    dir_root: Path
    dirs: Mapping[str, Path]

    def __getitem__(self, key: str) -> Path:
        return self.dirs[key]

    def iter_all(self) -> tuple[Path, ...]:
        return (self.dir_root, *self.dirs.values())


@dataclass(frozen=True, slots=True)
class SpecWorkspaceLayout:
    """Workspace layout contract with normalized directory mappings."""

    dirs: Sequence[str] | Mapping[str, str] = DEFAULT_WORKSPACE_DIRS

    def validate(self) -> tuple[str, ...]:
        errors: list[str] = []
        map_dirs = _normalize_workspace_dirs(self.dirs)
        if not map_dirs:
            errors.append("`dirs` must not be empty.")
            return tuple(errors)

        used_keys: set[str] = set()
        used_paths: set[str] = set()
        for key, relative_dir in map_dirs.items():
            key_clean = key.strip()
            if not key_clean:
                errors.append("`dirs` keys must not be empty.")
                continue
            if key_clean in used_keys:
                errors.append(f"`dirs` duplicated key: {key!r}.")
                continue
            used_keys.add(key_clean)

            path_clean = relative_dir.strip()
            if path_clean in used_paths:
                errors.append(f"`dirs` duplicated path: {relative_dir!r}.")
            else:
                used_paths.add(path_clean)

            _validate_relative_path(
                name=f"dirs[{key_clean!r}]",
                value=relative_dir,
                errors=errors,
            )

        return tuple(errors)

    def to_paths(self, dir_root: Path) -> WorkspacePaths:
        map_dirs = _normalize_workspace_dirs(self.dirs)
        return WorkspacePaths(
            dir_root=dir_root,
            dirs={key: dir_root / relative_dir for key, relative_dir in map_dirs.items()},
        )


@dataclass(frozen=True, slots=True)
class ReportWorkspaceCheck:
    ok: bool
    errors: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ReportWorkspaceApply:
    ok: bool
    paths: WorkspacePaths
    created: tuple[Path, ...] = ()
    existing: tuple[Path, ...] = ()
    errors: tuple[str, ...] = ()


class WorkspacePlan:
    """Workspace contract plan.

    Resolve and validate workspace paths in ``__init__``, then create directories
    with :meth:`apply`.
    """

    def __init__(
        self,
        dir_root: Path,
        *,
        spec: SpecWorkspaceLayout | None = None,
    ) -> None:
        self.spec = spec or SpecWorkspaceLayout()
        errors = list(self.spec.validate())
        self.paths = self.spec.to_paths(dir_root=dir_root)
        self.report_check = ReportWorkspaceCheck(
            ok=not errors,
            errors=tuple(errors),
        )

    def apply(self) -> ReportWorkspaceApply:
        if not self.report_check.ok:
            raise ValueError(
                "Workspace plan is invalid:\n"
                + "\n".join(f"- {_err}" for _err in self.report_check.errors)
            )

        created_paths: list[Path] = []
        existing_paths: list[Path] = []
        errors: list[str] = []

        for path in self.paths.iter_all():
            if path.exists():
                if path.is_dir():
                    existing_paths.append(path)
                else:
                    errors.append(f"{path}: path exists but is not a directory.")
                continue

            try:
                path.mkdir(parents=True, exist_ok=True)
                created_paths.append(path)
            except Exception as exc:
                errors.append(f"{path}: {exc}")

        return ReportWorkspaceApply(
            ok=not errors,
            paths=self.paths,
            created=tuple(created_paths),
            existing=tuple(existing_paths),
            errors=tuple(errors),
        )
