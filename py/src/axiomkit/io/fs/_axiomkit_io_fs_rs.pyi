from collections.abc import Sequence

__bridge_abi__: int
__bridge_contract__: str
__bridge_transport__: str


class CopyErrorRecord:
    path: str
    exception: str


class CopyReport:
    cnt_matched: int
    cnt_scanned: int
    cnt_copied: int
    cnt_skipped: int
    warnings: list[str]
    errors: list[CopyErrorRecord]

    @property
    def error_count(self) -> int: ...

    @property
    def warning_count(self) -> int: ...

    def to_dict(self) -> dict[str, int]: ...
    def format(self, prefix: str = "[COPY]") -> str: ...


def copy_tree(
    dir_source: str,
    dir_destination: str,
    *,
    patterns_include_files: Sequence[str] | None = None,
    patterns_exclude_files: Sequence[str] | None = None,
    patterns_include_dirs: Sequence[str] | None = None,
    patterns_exclude_dirs: Sequence[str] | None = None,
    rule_pattern: str = "glob",
    rule_conflict_file: str = "skip",
    rule_conflict_dir: str = "skip",
    rule_symlink: str = "copy_symlinks",
    depth_limit: int | None = None,
    rule_depth_limit: str = "at_most",
    workers_max: int | None = None,
    should_keep_tree: bool = True,
    should_dry_run: bool = False,
) -> CopyReport: ...
