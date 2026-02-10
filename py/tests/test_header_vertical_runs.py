from __future__ import annotations

# Ensure src-layout imports work when running tests from repo checkout.
import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

from axiomkit.io.xlsx.spec import SpecCellBorder  # noqa: E402
from axiomkit.io.xlsx.util import (  # noqa: E402
    _generate_vertical_runs,
    apply_vertical_run_text_blankout,
    plan_vertical_visual_merge_borders,
)


def test_iter_vertical_runs_empty_grid_yields_nothing() -> None:
    assert list(_generate_vertical_runs([])) == []


def test_iter_vertical_runs_single_row_yields_nothing() -> None:
    grid = [["A", "B", "C"]]
    assert list(_generate_vertical_runs(grid)) == []


def test_iter_vertical_runs_detects_only_contiguous_non_empty_runs() -> None:
    grid = [
        ["A", "X"],
        ["A", ""],
        ["A", "X"],
        ["", "X"],
        ["B", "X"],
        ["B", "Y"],
    ]
    # col 0: A run rows 0..2, B run rows 4..5
    # col 1: X run rows 2..4 (row 1 is empty so it breaks any earlier run)
    assert list(_generate_vertical_runs(grid)) == [
        (0, 0, 2, "A"),
        (0, 4, 5, "B"),
        (1, 2, 4, "X"),
    ]


def test_plan_vertical_visual_merge_borders_sets_top_bottom_borders_per_run() -> None:
    grid = [
        ["A"],
        ["A"],
        ["A"],
        [""],
        ["B"],
        ["B"],
    ]
    plan = plan_vertical_visual_merge_borders(grid)

    assert plan[(0, 0)] == SpecCellBorder(top=1, bottom=0, left=1, right=1)
    assert plan[(1, 0)] == SpecCellBorder(top=0, bottom=0, left=1, right=1)
    assert plan[(2, 0)] == SpecCellBorder(top=0, bottom=1, left=1, right=1)

    assert plan[(4, 0)] == SpecCellBorder(top=1, bottom=0, left=1, right=1)
    assert plan[(5, 0)] == SpecCellBorder(top=0, bottom=1, left=1, right=1)

    # Singletons should not appear in the plan.
    assert (3, 0) not in plan


def test_remove_vertical_run_text_blanks_non_top_cells_in_place() -> None:
    grid = [
        ["A", "B"],
        ["A", "B"],
        ["", "B"],
        ["C", "B"],
        ["C", ""],
    ]

    returned = apply_vertical_run_text_blankout(grid)
    assert returned is grid

    # col 0: A run rows 0..1 -> blank row 1; C run rows 3..4 -> blank row 4
    assert grid[0][0] == "A"
    assert grid[1][0] == ""
    assert grid[3][0] == "C"
    assert grid[4][0] == ""

    # col 1: B run rows 0..3 -> blank rows 1..3
    assert grid[0][1] == "B"
    assert grid[1][1] == ""
    assert grid[2][1] == ""
    assert grid[3][1] == ""
