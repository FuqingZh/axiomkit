from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path


def _validate_relative_path(*, name: str, value: str, errors: list[str]) -> None:
    c_clean = value.strip()
    if not c_clean:
        errors.append(f"`{name}` must not be empty.")
        return

    cls_rel = Path(c_clean)
    if cls_rel.is_absolute():
        errors.append(f"`{name}` must be a relative path, got {value!r}.")
    if ".." in cls_rel.parts:
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
        for c_name, c_value in (
            ("name_dir_in", self.name_dir_in),
            ("name_dir_out", self.name_dir_out),
            ("name_dir_tmp", self.name_dir_tmp),
            ("name_dir_logs", self.name_dir_logs),
            ("name_dir_configs", self.name_dir_configs),
            ("name_dir_meta", self.name_dir_meta),
        ):
            _validate_relative_path(name=c_name, value=c_value, errors=errors)

        l_used_keys: set[str] = set()
        for c_key, c_rel in self.extra_dirs.items():
            c_key_clean = c_key.strip()
            if not c_key_clean:
                errors.append("`extra_dirs` keys must not be empty.")
                continue

            if c_key_clean in l_used_keys:
                errors.append(f"`extra_dirs` duplicated key: {c_key!r}.")
                continue

            l_used_keys.add(c_key_clean)
            _validate_relative_path(
                name=f"extra_dirs[{c_key_clean!r}]",
                value=c_rel,
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
                c_key: dir_root / c_rel for c_key, c_rel in self.extra_dirs.items()
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
        l_errors = list(self.spec.validate())
        self.paths = self.spec.to_paths(dir_root=dir_root)
        self.report_check = ReportWorkspaceCheck(
            ok=not l_errors,
            errors=tuple(l_errors),
        )

    def apply(self) -> ReportWorkspaceApply:
        if not self.report_check.ok:
            raise ValueError(
                "Workspace plan is invalid:\n"
                + "\n".join(f"- {_err}" for _err in self.report_check.errors)
            )

        l_created: list[Path] = []
        l_existing: list[Path] = []
        l_errors: list[str] = []

        for cls_path in self.paths.iter_all():
            if cls_path.exists():
                if cls_path.is_dir():
                    l_existing.append(cls_path)
                else:
                    l_errors.append(f"{cls_path}: path exists but is not a directory.")
                continue

            try:
                cls_path.mkdir(parents=True, exist_ok=True)
                l_created.append(cls_path)
            except Exception as e:
                l_errors.append(f"{cls_path}: {e}")

        return ReportWorkspaceApply(
            ok=not l_errors,
            paths=self.paths,
            created=tuple(l_created),
            existing=tuple(l_existing),
            errors=tuple(l_errors),
        )
