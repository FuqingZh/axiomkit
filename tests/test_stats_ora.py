from __future__ import annotations

import polars as pl
import pytest

from axiomkit.stats.ora import OraComparison, OraOptions, calculate_ora


def _annotation_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ElementId": ["g1", "g2", "g3", "g4", "g5"],
            "TermId": ["t1", "t1", "t2", "t2", "t3"],
        }
    )


def test_calculate_ora_single_comparison_omits_comparison_id() -> None:
    df_result = calculate_ora(
        _annotation_frame(),
        comparisons=OraComparison(
            foreground_elements={"g1", "g2"},
        ),
        options=OraOptions(
            background_elements={"g1", "g2", "g3", "g4"},
            thr_bg_hits_min=0,
            thr_fg_hits_min=0,
            thr_p_value=1.0,
            thr_p_adjust=1.0,
        ),
    )

    assert _annotation_frame().columns == ["ElementId", "TermId"]
    assert "ComparisonId" not in df_result.columns
    assert "FgMembers" in df_result.columns
    assert "BgMembers" not in df_result.columns
    assert df_result.height == 2


def test_calculate_ora_can_drop_fg_members_and_keep_bg_members() -> None:
    df_result = calculate_ora(
        _annotation_frame(),
        comparisons=OraComparison(
            foreground_elements={"g1", "g2"},
        ),
        options=OraOptions(
            background_elements={"g1", "g2", "g3", "g4"},
            thr_bg_hits_min=0,
            thr_fg_hits_min=0,
            thr_p_value=1.0,
            thr_p_adjust=1.0,
            should_keep_fg_members=False,
            should_keep_bg_members=True,
        ),
    )

    assert "FgMembers" not in df_result.columns
    assert "BgMembers" in df_result.columns
    assert df_result.height == 2


def test_calculate_ora_multiple_comparisons_requires_comparison_id() -> None:
    with pytest.raises(ValueError, match="comparison_id"):
        calculate_ora(
            _annotation_frame(),
            comparisons=[
                OraComparison(
                    foreground_elements={"g1", "g2"},
                ),
                OraComparison(
                    comparison_id="cmp_b",
                    foreground_elements={"g2", "g4"},
                ),
            ],
        )


def test_calculate_ora_multiple_comparisons_include_comparison_id() -> None:
    df_result = calculate_ora(
        _annotation_frame(),
        comparisons=[
            OraComparison(
                comparison_id="cmp_a",
                foreground_elements={"g1", "g2"},
            ),
            OraComparison(
                comparison_id="cmp_b",
                foreground_elements={"g3", "g4"},
            ),
        ],
        options=OraOptions(
            background_elements={"g1", "g2", "g3", "g4"},
            thr_bg_hits_min=0,
            thr_fg_hits_min=0,
            thr_p_value=1.0,
            thr_p_adjust=1.0,
        ),
    )

    assert df_result.columns[0] == "ComparisonId"
    assert set(df_result["ComparisonId"].to_list()) == {"cmp_a", "cmp_b"}


def test_calculate_ora_option_override_inherits_unspecified_fields() -> None:
    df_result = calculate_ora(
        _annotation_frame(),
        comparisons=[
            OraComparison(
                comparison_id="cmp_a",
                foreground_elements={"g1", "g2"},
            ),
            OraComparison(
                comparison_id="cmp_b",
                foreground_elements={"g1", "g2"},
                option_override=OraOptions(
                    thr_bg_hits_max=None,
                    should_keep_bg_members=True,
                ),
            ),
        ],
        options=OraOptions(
            background_elements={"g1", "g2", "g3", "g4"},
            thr_bg_hits_min=0,
            thr_bg_hits_max=1,
            thr_fg_hits_min=0,
            thr_p_value=1.0,
            thr_p_adjust=1.0,
            should_keep_bg_members=False,
        ),
    )

    assert set(df_result["ComparisonId"].to_list()) == {"cmp_b"}
    assert "BgMembers" in df_result.columns
    df_t1 = df_result.filter(pl.col("TermId") == "t1")
    assert df_t1.height == 1
    assert set(df_t1.item(0, "BgMembers")) == {"g1", "g2"}


def test_calculate_ora_option_override_can_replace_shared_background() -> None:
    df_result = calculate_ora(
        _annotation_frame(),
        comparisons=[
            OraComparison(
                comparison_id="cmp_a",
                foreground_elements={"g1", "g2"},
            ),
            OraComparison(
                comparison_id="cmp_b",
                foreground_elements={"g1", "g2"},
                option_override=OraOptions(
                    background_elements={"g1", "g2"},
                    should_keep_bg_members=True,
                ),
            ),
        ],
        options=OraOptions(
            background_elements={"g1", "g2", "g3", "g4"},
            thr_bg_hits_min=0,
            thr_fg_hits_min=0,
            thr_p_value=1.0,
            thr_p_adjust=1.0,
            should_keep_bg_members=False,
        ),
    )

    df_cmp_b = df_result.filter(pl.col("ComparisonId") == "cmp_b")
    assert "BgMembers" in df_result.columns
    assert df_cmp_b.height == 1
    assert set(df_cmp_b.item(0, "BgMembers")) == {"g1", "g2"}


def test_calculate_ora_option_override_none_uses_inferred_universe() -> None:
    df_result = calculate_ora(
        _annotation_frame(),
        comparisons=[
            OraComparison(
                comparison_id="cmp_a",
                foreground_elements={"g1", "g2"},
            ),
            OraComparison(
                comparison_id="cmp_b",
                foreground_elements={"g1", "g2"},
                option_override=OraOptions(
                    background_elements=None,
                    should_keep_bg_members=True,
                ),
            ),
        ],
        options=OraOptions(
            background_elements={"g1", "g2", "g3", "g4"},
            thr_bg_hits_min=0,
            thr_fg_hits_min=0,
            thr_p_value=1.0,
            thr_p_adjust=1.0,
            should_keep_bg_members=False,
        ),
    )

    df_cmp_a = df_result.filter(pl.col("ComparisonId") == "cmp_a")
    df_cmp_b = df_result.filter(pl.col("ComparisonId") == "cmp_b")
    df_cmp_b_t3 = df_cmp_b.filter(pl.col("TermId") == "t3")

    assert "BgMembers" in df_result.columns
    assert "t3" not in df_cmp_a["TermId"].to_list()
    assert "t3" in df_cmp_b["TermId"].to_list()
    assert df_cmp_b_t3.height == 1
    assert set(df_cmp_b_t3.item(0, "BgMembers")) == {"g5"}


def test_ora_options_with_creates_derived_options() -> None:
    options_base = OraOptions(
        background_elements={"g1", "g2", "g3"},
        thr_p_value=0.05,
    )

    options_relaxed = options_base.with_(thr_p_value=1.0)

    assert options_base.thr_p_value == 0.05
    assert options_relaxed.thr_p_value == 1.0
    assert options_relaxed.background_elements == {"g1", "g2", "g3"}
