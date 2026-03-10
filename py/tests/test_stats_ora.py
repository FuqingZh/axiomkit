from __future__ import annotations

import polars as pl

from axiomkit.stats.ora import calculate_ora


def test_calculate_ora_keeps_only_fg_members_by_default() -> None:
    df_mapping = pl.DataFrame(
        {
            "ElementId": ["g1", "g2", "g3", "g4"],
            "TermId": ["t1", "t1", "t2", "t2"],
        }
    )

    df_result = calculate_ora(
        df_mapping,
        foreground_elements={"g1", "g2"},
        background_elements={"g1", "g2", "g3", "g4"},
        thr_bg_hits_min=0,
        thr_fg_hits_min=0,
        thr_p_value=1.0,
        thr_p_adjust=1.0,
    )

    assert "FgMembers" in df_result.columns
    assert "BgMembers" not in df_result.columns
    assert df_result.height == 2


def test_calculate_ora_can_drop_fg_members_and_keep_bg_members() -> None:
    df_mapping = pl.DataFrame(
        {
            "ElementId": ["g1", "g2", "g3", "g4"],
            "TermId": ["t1", "t1", "t2", "t2"],
        }
    )

    df_result = calculate_ora(
        df_mapping,
        foreground_elements={"g1", "g2"},
        background_elements={"g1", "g2", "g3", "g4"},
        thr_bg_hits_min=0,
        thr_fg_hits_min=0,
        thr_p_value=1.0,
        thr_p_adjust=1.0,
        should_keep_fg_members=False,
        should_keep_bg_members=True,
    )

    assert "FgMembers" not in df_result.columns
    assert "BgMembers" in df_result.columns
    assert df_result.height == 2
