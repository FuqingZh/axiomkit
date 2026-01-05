from dataclasses import dataclass, fields
from pathlib import Path


@dataclass(frozen=True, slots=True)
class WorkspacePaths:
    dir_root: Path
    dir_in: Path
    dir_out: Path
    dir_tmp: Path
    dir_logs: Path
    dir_configs: Path
    dir_meta: Path


def prepare_workspace(dir_root: Path, subname: str | None = None) -> WorkspacePaths:
    dir_root = dir_root / subname if subname else dir_root
    cls_paths = WorkspacePaths(
        dir_root=dir_root,
        dir_in=dir_root / "in",
        dir_out=dir_root / "out",
        dir_tmp=dir_root / "tmp",
        dir_logs=dir_root / "logs",
        dir_configs=dir_root / "configs",
        dir_meta=dir_root / "meta",
    )

    for _field in fields(cls_paths):
        getattr(cls_paths, _field.name).mkdir(parents=True, exist_ok=True)
    return cls_paths
