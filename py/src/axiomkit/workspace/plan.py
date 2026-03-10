from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path


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


@dataclass(frozen=True, slots=True)
class WorkspacePaths:
    dir_root: Path
    dir_in: Path
    dir_out: Path
    dir_tmp: Path
    dir_logs: Path
    dir_configs: Path
    dir_meta: Path
    dirs_extra: Mapping[str, Path] = field(default_factory=lambda: {})

    def iter_all(self) -> tuple[Path, ...]:
        return (
            self.dir_root,
            self.dir_in,
            self.dir_out,
            self.dir_tmp,
            self.dir_logs,
            self.dir_configs,
            self.dir_meta,
            *self.dirs_extra.values(),
        )


@dataclass(frozen=True, slots=True)
class SpecWorkspaceLayout:
    """Workspace layout contract with fixed and extension directories."""

    name_dir_in: str = "in"
    name_dir_out: str = "out"
    name_dir_tmp: str = "tmp"
    name_dir_logs: str = "logs"
    name_dir_configs: str = "configs"
    name_dir_meta: str = "meta"
    extra_dirs: Mapping[str, str] = field(default_factory=lambda: {})

    def validate(self) -> tuple[str, ...]:
        errors: list[str] = []
        for field_name, field_value in (
            ("name_dir_in", self.name_dir_in),
            ("name_dir_out", self.name_dir_out),
            ("name_dir_tmp", self.name_dir_tmp),
            ("name_dir_logs", self.name_dir_logs),
            ("name_dir_configs", self.name_dir_configs),
            ("name_dir_meta", self.name_dir_meta),
        ):
            _validate_relative_path(name=field_name, value=field_value, errors=errors)

        used_keys: set[str] = set()
        for extra_key, relative_dir in self.extra_dirs.items():
            key_clean = extra_key.strip()
            if not key_clean:
                errors.append("`extra_dirs` keys must not be empty.")
                continue

            if key_clean in used_keys:
                errors.append(f"`extra_dirs` duplicated key: {extra_key!r}.")
                continue

            used_keys.add(key_clean)
            _validate_relative_path(
                name=f"extra_dirs[{key_clean!r}]",
                value=relative_dir,
                errors=errors,
            )

        return tuple(errors)

    def to_paths(self, dir_root: Path) -> WorkspacePaths:
        return WorkspacePaths(
            dir_root=dir_root,
            dir_in=dir_root / self.name_dir_in,
            dir_out=dir_root / self.name_dir_out,
            dir_tmp=dir_root / self.name_dir_tmp,
            dir_logs=dir_root / self.name_dir_logs,
            dir_configs=dir_root / self.name_dir_configs,
            dir_meta=dir_root / self.name_dir_meta,
            dirs_extra={
                key: dir_root / relative_dir
                for key, relative_dir in self.extra_dirs.items()
            },
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
            except Exception as e:
                errors.append(f"{path}: {e}")

        return ReportWorkspaceApply(
            ok=not errors,
            paths=self.paths,
            created=tuple(created_paths),
            existing=tuple(existing_paths),
            errors=tuple(errors),
        )
