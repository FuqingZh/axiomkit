from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# Ensure src-layout imports work when running tests from repo checkout.
SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

from axiomkit.io.fs.copy import copy_tree  # noqa: E402
from axiomkit.io.fs.spec import (  # noqa: E402
    EnumCopyDepthLimitMode,
    EnumCopyDirectoryConflictStrategy,
    EnumCopyFileConflictStrategy,
    EnumCopyPatternMode,
    EnumCopySymlinkStrategy,
)


def _write_text(path: Path, text: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def _assert_exists(path: Path) -> None:
    assert path.exists(), f"Expected exists: {path}"


def _assert_symlink(path: Path) -> None:
    assert path.is_symlink(), f"Expected symlink: {path}"


def test_copy_tree_smoke_and_edges(tmp_path: Path) -> None:
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()

    _write_text(src / "root.txt")
    _write_text(src / "root.md")
    _write_text(src / "a" / "file1.txt")
    _write_text(src / "b" / "sub" / "file2.txt")
    _write_text(src / "skipme" / "file3.txt")

    try:
        os.symlink(src / "root.txt", src / "link_root.txt")
        os.symlink(src / "a", src / "link_dir")
        os.symlink(src / "missing.txt", src / "broken_link")
    except OSError as e:
        pytest.skip(f"symlink not supported: {e}")

    # 1) basic keep_tree
    report = copy_tree(src, dst)
    assert report.calculate_error_count == 0
    _assert_exists(dst / "root.txt")
    _assert_exists(dst / "a" / "file1.txt")
    _assert_exists(dst / "b" / "sub" / "file2.txt")

    # 2) keep_tree False + glob include
    dst2 = tmp_path / "dst2"
    report2 = copy_tree(
        src,
        dst2,
        if_keep_tree=False,
        patterns_include_files=["*.txt"],
        rule_pattern=EnumCopyPatternMode.GLOB,
    )
    assert report2.calculate_error_count == 0
    _assert_exists(dst2 / "root.txt")
    _assert_exists(dst2 / "file1.txt")
    _assert_exists(dst2 / "file2.txt")
    assert not (dst2 / "root.md").exists()

    # 3) depth exact only root-level files
    dst3 = tmp_path / "dst3"
    report3 = copy_tree(
        src,
        dst3,
        depth_limit=1,
        rule_depth_limit=EnumCopyDepthLimitMode.EXACT,
        if_keep_tree=True,
    )
    assert report3.calculate_error_count == 0
    _assert_exists(dst3 / "root.txt")
    assert not (dst3 / "a" / "file1.txt").exists()

    # 4) exclude dir pattern should prune
    dst4 = tmp_path / "dst4"
    report4 = copy_tree(
        src,
        dst4,
        patterns_exclude_dirs=["skipme"],
        rule_pattern=EnumCopyPatternMode.GLOB,
    )
    assert report4.calculate_error_count == 0
    assert not (dst4 / "skipme" / "file3.txt").exists()

    # 5) symlink behavior
    dst5 = tmp_path / "dst5"
    report5 = copy_tree(
        src,
        dst5,
        rule_symlink=EnumCopySymlinkStrategy.COPY_SYMLINKS,
    )
    assert report5.calculate_error_count == 0
    _assert_symlink(dst5 / "link_root.txt")
    _assert_symlink(dst5 / "link_dir")

    # 6) broken symlink with dereference should error
    dst6 = tmp_path / "dst6"
    report6 = copy_tree(
        src,
        dst6,
        rule_symlink=EnumCopySymlinkStrategy.DEREFERENCE,
    )
    assert report6.calculate_error_count >= 1

    # 7) conflict error on file
    dst7 = tmp_path / "dst7"
    dst7.mkdir()
    _write_text(dst7 / "root.txt")
    report7 = copy_tree(
        src,
        dst7,
        rule_conflict_file=EnumCopyFileConflictStrategy.ERROR,
        rule_conflict_dir=EnumCopyDirectoryConflictStrategy.SKIP,
    )
    assert report7.calculate_error_count >= 1

    # 8) overlap detection
    with pytest.raises(ValueError):
        copy_tree(src, src / "nested")
