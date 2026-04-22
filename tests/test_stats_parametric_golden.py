from __future__ import annotations

import math

import polars as pl

from axiomkit.stats import (
    TTestContrast,
    calculate_anova_one_way,
    calculate_anova_one_way_welch,
    calculate_anova_two_way,
    calculate_t_test_one_sample,
    calculate_t_test_paired,
    calculate_t_test_two_sample,
)


def _normalize_snapshot_value(value: object) -> object:
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        return round(value, 12)
    if isinstance(value, list):
        return [_normalize_snapshot_value(item) for item in value]
    if isinstance(value, dict):
        return {
            key: _normalize_snapshot_value(item) for key, item in value.items()
        }
    return value


def _assert_golden(df_result: pl.DataFrame, expected: list[dict[str, object]]) -> None:
    actual = [_normalize_snapshot_value(row) for row in df_result.to_dicts()]
    assert actual == expected


def test_parametric_stats_golden_one_sample() -> None:
    df_values = pl.DataFrame(
        {
            "FeatureId": ["f2", "f2", "f1", "f1", "f1"],
            "Value": [10.0, 12.0, 1.0, 2.0, 4.0],
        }
    )

    df_result = calculate_t_test_one_sample(
        df_values,
        col_feature="FeatureId",
        popmean=2.0,
        rule_p_adjust="bh",
    )

    _assert_golden(
        df_result,
        [
            {
                "FeatureId": "f2",
                "N": 2,
                "Mean": 11.0,
                "PopMean": 2.0,
                "MeanDiff": 9.0,
                "TStatistic": 9.0,
                "DegreesFreedom": 1.0,
                "PValue": 0.070446574955,
                "PAdjust": 0.140893149909,
            },
            {
                "FeatureId": "f1",
                "N": 3,
                "Mean": 2.333333333333,
                "PopMean": 2.0,
                "MeanDiff": 0.333333333333,
                "TStatistic": 0.377964473009,
                "DegreesFreedom": 2.0,
                "PValue": 0.741801110253,
                "PAdjust": 0.741801110253,
            },
        ],
    )


def test_parametric_stats_golden_paired() -> None:
    df_values = pl.DataFrame(
        {
            "FeatureId": [
                "f1",
                "f1",
                "f1",
                "f1",
                "f1",
                "f1",
                "f2",
                "f2",
                "f2",
                "f2",
                "f2",
                "f2",
            ],
            "PairId": [
                "p1",
                "p1",
                "p2",
                "p2",
                "p3",
                "p3",
                "p1",
                "p1",
                "p2",
                "p2",
                "p3",
                "p3",
            ],
            "Group": ["A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B"],
            "Value": [1.0, 2.0, 3.0, 5.0, 4.0, 6.0, 2.0, 5.0, 4.0, 7.0, 6.0, 10.0],
        }
    )

    df_result = calculate_t_test_paired(
        df_values,
        col_pair="PairId",
        col_feature="FeatureId",
        contrasts=TTestContrast(group_test="B", group_ref="A"),
        rule_p_adjust="bonferroni",
    )

    _assert_golden(
        df_result,
        [
            {
                "FeatureId": "f1",
                "ContrastId": ["B", "A"],
                "GroupTest": "B",
                "GroupRef": "A",
                "NGroupTest": 3,
                "NGroupRef": 3,
                "MeanGroupTest": 4.333333333333,
                "MeanGroupRef": 2.666666666667,
                "MeanDiff": 1.666666666667,
                "TStatistic": 5.0,
                "DegreesFreedom": 2.0,
                "PValue": 0.037749551351,
                "PAdjust": 0.075499102701,
            },
            {
                "FeatureId": "f2",
                "ContrastId": ["B", "A"],
                "GroupTest": "B",
                "GroupRef": "A",
                "NGroupTest": 3,
                "NGroupRef": 3,
                "MeanGroupTest": 7.333333333333,
                "MeanGroupRef": 4.0,
                "MeanDiff": 3.333333333333,
                "TStatistic": 10.0,
                "DegreesFreedom": 2.0,
                "PValue": 0.009852457023,
                "PAdjust": 0.019704914047,
            },
        ],
    )


def test_parametric_stats_golden_two_sample_comparison() -> None:
    df_values = pl.DataFrame(
        {
            "Comparison": [
                "B_vs_A",
                "B_vs_A",
                "B_vs_A",
                "B_vs_A",
                "C_vs_A",
                "C_vs_A",
                "C_vs_A",
                "C_vs_A",
                "C_vs_A",
                "C_vs_A",
                "C_vs_A",
                "C_vs_A",
            ],
            "FeatureId": [
                "T1",
                "T1",
                "T1",
                "T1",
                "T1",
                "T1",
                "T1",
                "T1",
                "T2",
                "T2",
                "T2",
                "T2",
            ],
            "Group": ["A", "A", "B", "B", "A", "A", "C", "C", "A", "A", "C", "C"],
            "Value": [10.0, 11.0, 20.0, 22.0, 10.0, 10.0, 0.0, 0.0, 4.0, 5.0, 7.0, 9.0],
            "IsValid": [
                True,
                True,
                True,
                True,
                False,
                False,
                False,
                False,
                True,
                True,
                True,
                True,
            ],
        }
    )

    df_result = calculate_t_test_two_sample(
        df_values,
        col_feature="FeatureId",
        col_comparison="Comparison",
        col_is_valid="IsValid",
        contrasts=[
            TTestContrast(group_test="B", group_ref="A"),
            TTestContrast(group_test="C", group_ref="A"),
        ],
        rule_p_adjust="bonferroni",
    )

    _assert_golden(
        df_result,
        [
            {
                "Comparison": "B_vs_A",
                "FeatureId": "T1",
                "ContrastId": ["B", "A"],
                "GroupTest": "B",
                "GroupRef": "A",
                "NGroupTest": 2,
                "NGroupRef": 2,
                "MeanGroupTest": 21.0,
                "MeanGroupRef": 10.5,
                "MeanDiff": 10.5,
                "TStatistic": 9.391485505499,
                "DegreesFreedom": 1.470588235294,
                "PValue": 0.027375128645,
                "PAdjust": 0.054750257289,
            },
            {
                "Comparison": "C_vs_A",
                "FeatureId": "T2",
                "ContrastId": ["C", "A"],
                "GroupTest": "C",
                "GroupRef": "A",
                "NGroupTest": 2,
                "NGroupRef": 2,
                "MeanGroupTest": 8.0,
                "MeanGroupRef": 4.5,
                "MeanDiff": 3.5,
                "TStatistic": 3.1304951685,
                "DegreesFreedom": 1.470588235294,
                "PValue": 0.129047835548,
                "PAdjust": 0.258095671097,
            },
        ],
    )


def test_parametric_stats_golden_one_way_comparison() -> None:
    df_values = pl.DataFrame(
        {
            "Comparison": [
                "cmp1",
                "cmp1",
                "cmp1",
                "cmp1",
                "cmp1",
                "cmp1",
                "cmp2",
                "cmp2",
                "cmp2",
                "cmp2",
                "cmp2",
                "cmp2",
                "cmp2",
                "cmp2",
                "cmp2",
                "cmp2",
                "cmp2",
                "cmp2",
            ],
            "FeatureId": [
                "T1",
                "T1",
                "T1",
                "T1",
                "T1",
                "T1",
                "T1",
                "T1",
                "T1",
                "T1",
                "T1",
                "T1",
                "T2",
                "T2",
                "T2",
                "T2",
                "T2",
                "T2",
            ],
            "Group": [
                "A",
                "A",
                "B",
                "B",
                "C",
                "C",
                "A",
                "A",
                "B",
                "B",
                "C",
                "C",
                "A",
                "A",
                "B",
                "B",
                "C",
                "C",
            ],
            "Value": [
                1.0,
                2.0,
                5.0,
                6.0,
                3.0,
                4.0,
                10.0,
                11.0,
                14.0,
                15.0,
                8.0,
                9.0,
                2.0,
                4.0,
                6.0,
                8.0,
                10.0,
                12.0,
            ],
            "IsValid": [
                True,
                True,
                True,
                True,
                True,
                True,
                False,
                False,
                False,
                False,
                False,
                False,
                True,
                True,
                True,
                True,
                True,
                True,
            ],
        }
    )

    df_result = calculate_anova_one_way(
        df_values,
        col_feature="FeatureId",
        col_comparison="Comparison",
        col_is_valid="IsValid",
    )

    _assert_golden(
        df_result,
        [
            {
                "Comparison": "cmp1",
                "FeatureId": "T1",
                "NumGroups": 3,
                "NTotal": 6,
                "DegreesFreedomBetween": 2.0,
                "DegreesFreedomWithin": 3.0,
                "FStatistic": 16.0,
                "PValue": 0.025094573304,
                "PAdjust": 0.025094573304,
            },
            {
                "Comparison": "cmp2",
                "FeatureId": "T2",
                "NumGroups": 3,
                "NTotal": 6,
                "DegreesFreedomBetween": 2.0,
                "DegreesFreedomWithin": 3.0,
                "FStatistic": 16.0,
                "PValue": 0.025094573304,
                "PAdjust": 0.025094573304,
            },
        ],
    )


def test_parametric_stats_golden_one_way_welch() -> None:
    df_values = pl.DataFrame(
        {
            "FeatureId": ["f2", "f2", "f2", "f2", "f1", "f1", "f1", "f1", "f1", "f1"],
            "Group": ["A", "A", "B", "B", "A", "A", "B", "B", "C", "C"],
            "Value": [2.0, 2.0, 2.0, 2.0, 1.0, 2.0, 5.0, 6.0, 3.0, 4.0],
        }
    )

    df_result = calculate_anova_one_way_welch(
        df_values,
        col_feature="FeatureId",
        rule_p_adjust="bh",
    )

    _assert_golden(
        df_result,
        [
            {
                "FeatureId": "f2",
                "NumGroups": 2,
                "NTotal": 4,
                "DegreesFreedomBetween": "NaN",
                "DegreesFreedomWithin": "NaN",
                "FStatistic": "NaN",
                "PValue": "NaN",
                "PAdjust": "NaN",
            },
            {
                "FeatureId": "f1",
                "NumGroups": 3,
                "NTotal": 6,
                "DegreesFreedomBetween": 2.0,
                "DegreesFreedomWithin": 2.0,
                "FStatistic": 12.0,
                "PValue": 0.076923076923,
                "PAdjust": 0.076923076923,
            },
        ],
    )


def test_parametric_stats_golden_two_way() -> None:
    df_values = pl.DataFrame(
        {
            "FeatureId": ["f2"] * 8 + ["f1"] * 8,
            "GroupA": ["A", "A", "A", "A", "B", "B", "B", "B"] * 2,
            "GroupB": ["X", "X", "Y", "Y", "X", "X", "Y", "Y"] * 2,
            "Value": [
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                2.0,
                2.0,
                3.0,
                4.0,
                5.0,
                7.0,
                8.0,
            ],
        }
    )

    df_result = calculate_anova_two_way(
        df_values,
        col_feature="FeatureId",
        rule_p_adjust="bh",
    )

    _assert_golden(
        df_result,
        [
            {
                "FeatureId": "f2",
                "NumGroupsA": 2,
                "NumGroupsB": 2,
                "NTotal": 8,
                "DegreesFreedomA": 1.0,
                "DegreesFreedomB": 1.0,
                "DegreesFreedomInteraction": 1.0,
                "DegreesFreedomWithin": 4.0,
                "FStatisticA": "NaN",
                "FStatisticB": "NaN",
                "FStatisticInteraction": "NaN",
                "PValueA": "NaN",
                "PValueB": "NaN",
                "PValueInteraction": "NaN",
                "PAdjustA": "NaN",
                "PAdjustB": "NaN",
                "PAdjustInteraction": "NaN",
            },
            {
                "FeatureId": "f1",
                "NumGroupsA": 2,
                "NumGroupsB": 2,
                "NTotal": 8,
                "DegreesFreedomA": 1.0,
                "DegreesFreedomB": 1.0,
                "DegreesFreedomInteraction": 1.0,
                "DegreesFreedomWithin": 4.0,
                "FStatisticA": 64.0,
                "FStatisticB": 16.0,
                "FStatisticInteraction": 4.0,
                "PValueA": 0.001323896909,
                "PValueB": 0.0161300899,
                "PValueInteraction": 0.116116523517,
                "PAdjustA": 0.001323896909,
                "PAdjustB": 0.0161300899,
                "PAdjustInteraction": 0.116116523517,
            },
        ],
    )
