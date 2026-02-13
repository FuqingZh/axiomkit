from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

from axiomkit.io.fs._rs_bridge import is_rs_backend_available  # noqa: E402
from axiomkit.io.fs.copy import copy_tree  # noqa: E402
from axiomkit.io.fs.spec import EnumCopyDirectoryConflictStrategy  # noqa: E402


def _write_text(path: Path, text: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


@pytest.mark.skipif(not is_rs_backend_available(), reason="Rust fs backend unavailable")
@pytest.mark.skipif(os.name != "posix", reason="symlink tests require posix")
def test_copy_tree_rejects_symlink_destination_root(tmp_path: Path) -> None:
    src = tmp_path / "src"
    dst_real = tmp_path / "dst_real"
    dst_link = tmp_path / "dst_link"

    src.mkdir()
    dst_real.mkdir()
    _write_text(src / "a.txt")
    os.symlink(dst_real, dst_link)

    with pytest.raises(OSError):
        copy_tree(src, dst_link)


@pytest.mark.skipif(not is_rs_backend_available(), reason="Rust fs backend unavailable")
@pytest.mark.skipif(os.name != "posix", reason="symlink tests require posix")
def test_copy_tree_blocks_destination_symlink_escape_in_merge_mode(tmp_path: Path) -> None:
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    outside = tmp_path / "outside"

    _write_text(src / "escape" / "file.txt")
    dst.mkdir()
    outside.mkdir()
    os.symlink(outside, dst / "escape")

    report = copy_tree(
        src,
        dst,
        rule_conflict_dir=EnumCopyDirectoryConflictStrategy.MERGE,
    )

    assert report.error_count >= 1
    assert not (outside / "file.txt").exists()
