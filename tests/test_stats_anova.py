from __future__ import annotations

import math

import numpy as np
import polars as pl
import pytest
from axiomkit.stats import (
    calculate_anova_one_way,
    calculate_anova_one_way_welch,
    calculate_anova_two_way,
)
from scipy import stats


def calculate_expected_two_way_balanced(
    cells: dict[tuple[str, str], list[float]],
) -> dict[str, float]:
    levels_a = sorted({factor_a for factor_a, _ in cells})
    levels_b = sorted({factor_b for _, factor_b in cells})
    cell_size = len(next(iter(cells.values())))
    values_all = [value for values in cells.values() for value in values]
    n_total = len(values_all)
    grand_total = sum(values_all)
    correction = grand_total**2 / n_total

    ss_cells = (
        sum((sum(values) ** 2) / len(values) for values in cells.values()) - correction
    )
    ss_within = sum(
        sum((value - (sum(values) / len(values))) ** 2 for value in values)
        for values in cells.values()
    )
    ss_a = (
        sum(
            (sum(sum(cells[(factor_a, factor_b)]) for factor_b in levels_b) ** 2)
            / (len(levels_b) * cell_size)
            for factor_a in levels_a
        )
        - correction
    )
    ss_b = (
        sum(
            (sum(sum(cells[(factor_a, factor_b)]) for factor_a in levels_a) ** 2)
            / (len(levels_a) * cell_size)
            for factor_b in levels_b
        )
        - correction
    )
    ss_interaction = ss_cells - ss_a - ss_b

    df_a = len(levels_a) - 1
    df_b = len(levels_b) - 1
    df_interaction = df_a * df_b
    df_within = n_total - len(levels_a) * len(levels_b)
    ms_within = ss_within / df_within

    f_a = (ss_a / df_a) / ms_within
    f_b = (ss_b / df_b) / ms_within
    f_interaction = (ss_interaction / df_interaction) / ms_within

    return {
        "DegreesFreedomA": float(df_a),
        "DegreesFreedomB": float(df_b),
        "DegreesFreedomInteraction": float(df_interaction),
        "DegreesFreedomWithin": float(df_within),
        "FStatisticA": float(f_a),
        "FStatisticB": float(f_b),
        "FStatisticInteraction": float(f_interaction),
    }


def calculate_expected_one_way_welch(
    groups: list[list[float]],
) -> dict[str, float]:
    n_group = np.array([len(group) for group in groups], dtype=np.float64)
    mean_group = np.array([np.mean(group) for group in groups], dtype=np.float64)
    var_group = np.array(
        [np.var(group, ddof=1) for group in groups],
        dtype=np.float64,
    )
    num_groups = float(len(groups))
    weight_group = n_group / var_group
    weight_total = np.sum(weight_group)
    mean_weighted = np.sum(weight_group * mean_group) / weight_total
    sum_term = np.sum(
        (1.0 / (n_group - 1.0)) * ((1.0 - (weight_group / weight_total)) ** 2)
    )
    degrees_freedom_between = num_groups - 1.0
    correction = 1.0 + ((2.0 * (num_groups - 2.0) / ((num_groups**2) - 1.0)) * sum_term)
    f_statistic = (
        np.sum(weight_group * ((mean_group - mean_weighted) ** 2))
        / degrees_freedom_between
    ) / correction
    degrees_freedom_within = ((num_groups**2) - 1.0) / (3.0 * sum_term)

    return {
        "DegreesFreedomBetween": float(degrees_freedom_between),
        "DegreesFreedomWithin": float(degrees_freedom_within),
        "FStatistic": float(f_statistic),
        "PValue": float(
            stats.f.sf(
                f_statistic,
                degrees_freedom_between,
                degrees_freedom_within,
            )
        ),
    }


def test_calculate_anova_one_way_matches_scipy() -> None:
    df_values = pl.DataFrame(
        {
            "Group": ["A", "A", "B", "B", "C", "C"],
            "Value": [1.0, 2.0, 5.0, 6.0, 3.0, 4.0],
        }
    )

    df_result = calculate_anova_one_way(df_values)

    assert df_result.columns == [
        "NumGroups",
        "NTotal",
        "DegreesFreedomBetween",
        "DegreesFreedomWithin",
        "FStatistic",
        "PValue",
        "PAdjust",
    ]
    assert df_result.height == 1

    row = df_result.row(0, named=True)
    expected = stats.f_oneway([1.0, 2.0], [5.0, 6.0], [3.0, 4.0])

    assert row["NumGroups"] == 3
    assert row["NTotal"] == 6
    assert row["DegreesFreedomBetween"] == pytest.approx(2.0)
    assert row["DegreesFreedomWithin"] == pytest.approx(3.0)
    assert row["FStatistic"] == pytest.approx(expected.statistic)
    assert row["PValue"] == pytest.approx(expected.pvalue)
    assert row["PAdjust"] == pytest.approx(expected.pvalue)


def test_calculate_anova_one_way_supports_feature_order_and_p_adjust() -> None:
    df_values = pl.DataFrame(
        {
            "FeatureId": ["f2", "f2", "f2", "f2", "f1", "f1", "f1", "f1", "f1", "f1"],
            "Group": ["A", "A", "B", "B", "A", "A", "B", "B", "C", "C"],
            "Value": [2.0, 2.0, 2.0, 2.0, 1.0, 2.0, 5.0, 6.0, 3.0, 4.0],
        }
    )

    df_result = calculate_anova_one_way(
        df_values.lazy(),
        col_feature="FeatureId",
        rule_p_adjust="bh",
    )

    assert df_result.columns == [
        "FeatureId",
        "NumGroups",
        "NTotal",
        "DegreesFreedomBetween",
        "DegreesFreedomWithin",
        "FStatistic",
        "PValue",
        "PAdjust",
    ]
    assert df_result["FeatureId"].to_list() == ["f2", "f1"]

    row_f2 = df_result.row(0, named=True)
    row_f1 = df_result.row(1, named=True)
    expected_f1 = stats.f_oneway([1.0, 2.0], [5.0, 6.0], [3.0, 4.0])

    assert math.isnan(row_f2["FStatistic"])
    assert math.isnan(row_f2["PValue"])
    assert math.isnan(row_f2["PAdjust"])
    assert row_f1["FStatistic"] == pytest.approx(expected_f1.statistic)
    assert row_f1["PValue"] == pytest.approx(expected_f1.pvalue)
    assert row_f1["PAdjust"] >= row_f1["PValue"]


def test_calculate_anova_one_way_supports_numeric_group_values() -> None:
    df_values = pl.DataFrame(
        {
            "Group": [0, 0, 1, 1, 2, 2],
            "Value": [1.0, 2.0, 5.0, 6.0, 3.0, 4.0],
        }
    )

    df_result = calculate_anova_one_way(df_values, col_group="Group")

    row = df_result.row(0, named=True)
    expected = stats.f_oneway([1.0, 2.0], [5.0, 6.0], [3.0, 4.0])
    assert row["FStatistic"] == pytest.approx(expected.statistic)
    assert row["PValue"] == pytest.approx(expected.pvalue)


def test_calculate_anova_one_way_supports_comparison_and_validity_gate() -> None:
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

    assert df_result.columns == [
        "Comparison",
        "FeatureId",
        "NumGroups",
        "NTotal",
        "DegreesFreedomBetween",
        "DegreesFreedomWithin",
        "FStatistic",
        "PValue",
        "PAdjust",
    ]
    assert df_result.select("Comparison", "FeatureId").rows() == [
        ("cmp1", "T1"),
        ("cmp2", "T2"),
    ]

    row_cmp1 = df_result.row(0, named=True)
    row_cmp2 = df_result.row(1, named=True)
    expected_cmp1 = stats.f_oneway([1.0, 2.0], [5.0, 6.0], [3.0, 4.0])
    expected_cmp2 = stats.f_oneway([2.0, 4.0], [6.0, 8.0], [10.0, 12.0])

    assert row_cmp1["PValue"] == pytest.approx(expected_cmp1.pvalue)
    assert row_cmp2["PValue"] == pytest.approx(expected_cmp2.pvalue)


def test_calculate_anova_one_way_supports_comparison_without_validity_gate() -> None:
    df_values = pl.DataFrame(
        {
            "Comparison": ["cmp1", "cmp1", "cmp1", "cmp1", "cmp2", "cmp2", "cmp2", "cmp2"],
            "FeatureId": ["f1", "f1", "f1", "f1", "f1", "f1", "f1", "f1"],
            "Group": ["A", "A", "B", "B", "A", "A", "B", "B"],
            "Value": [1.0, 2.0, 4.0, 5.0, 3.0, 3.5, 6.0, 7.0],
        }
    )

    df_result = calculate_anova_one_way(
        df_values,
        col_feature="FeatureId",
        col_comparison="Comparison",
    )

    assert df_result.select("Comparison", "FeatureId").sort("Comparison").rows() == [
        ("cmp1", "f1"),
        ("cmp2", "f1"),
    ]


def test_calculate_anova_one_way_keeps_feature_with_too_few_groups_as_nan() -> None:
    df_values = pl.DataFrame(
        {
            "FeatureId": ["f1", "f1", "f2", "f2", "f2"],
            "Group": ["A", "A", "A", "B", "B"],
            "Value": [1.0, 2.0, 10.0, 20.0, None],
        }
    )

    df_result = calculate_anova_one_way(
        df_values,
        col_feature="FeatureId",
        rule_p_adjust="bonferroni",
    )

    row_f1 = df_result.row(0, named=True)
    row_f2 = df_result.row(1, named=True)
    assert row_f1["NumGroups"] == 1
    assert row_f1["NTotal"] == 2
    assert math.isnan(row_f1["DegreesFreedomBetween"])
    assert math.isnan(row_f1["DegreesFreedomWithin"])
    assert math.isnan(row_f1["FStatistic"])
    assert math.isnan(row_f1["PValue"])
    assert math.isnan(row_f1["PAdjust"])
    assert row_f2["NumGroups"] == 2


def test_calculate_anova_one_way_keeps_feature_with_invalid_within_df_as_nan() -> None:
    df_values = pl.DataFrame(
        {
            "FeatureId": ["f1", "f1", "f2", "f2"],
            "Group": ["A", "B", "A", "A"],
            "Value": [1.0, 2.0, 10.0, 11.0],
        }
    )

    df_result = calculate_anova_one_way(
        df_values,
        col_feature="FeatureId",
    )

    row_f1 = df_result.row(0, named=True)
    row_f2 = df_result.row(1, named=True)
    assert row_f1["NumGroups"] == 2
    assert row_f1["NTotal"] == 2
    assert math.isnan(row_f1["DegreesFreedomBetween"])
    assert math.isnan(row_f1["DegreesFreedomWithin"])
    assert math.isnan(row_f1["FStatistic"])
    assert math.isnan(row_f1["PValue"])
    assert math.isnan(row_f1["PAdjust"])
    assert row_f2["NumGroups"] == 1


def test_calculate_anova_one_way_welch_matches_manual_formula() -> None:
    groups = {
        "A": [1.0, 2.0, 1.5],
        "B": [5.0, 7.0, 8.0],
        "C": [3.0, 4.0, 3.5, 4.5],
    }
    df_values = pl.DataFrame(
        {
            "Group": [
                "A",
                "A",
                "A",
                "B",
                "B",
                "B",
                "C",
                "C",
                "C",
                "C",
            ],
            "Value": groups["A"] + groups["B"] + groups["C"],
        }
    )

    df_result = calculate_anova_one_way_welch(df_values)

    assert df_result.columns == [
        "NumGroups",
        "NTotal",
        "DegreesFreedomBetween",
        "DegreesFreedomWithin",
        "FStatistic",
        "PValue",
        "PAdjust",
    ]
    row = df_result.row(0, named=True)
    expected = calculate_expected_one_way_welch(list(groups.values()))

    assert row["NumGroups"] == 3
    assert row["NTotal"] == 10
    assert row["DegreesFreedomBetween"] == pytest.approx(
        expected["DegreesFreedomBetween"]
    )
    assert row["DegreesFreedomWithin"] == pytest.approx(
        expected["DegreesFreedomWithin"]
    )
    assert row["FStatistic"] == pytest.approx(expected["FStatistic"])
    assert row["PValue"] == pytest.approx(expected["PValue"])
    assert row["PAdjust"] == pytest.approx(expected["PValue"])


def test_calculate_anova_one_way_welch_supports_feature_order_and_p_adjust() -> None:
    df_values = pl.DataFrame(
        {
            "FeatureId": [
                "f2",
                "f2",
                "f2",
                "f2",
                "f2",
                "f1",
                "f1",
                "f1",
                "f1",
                "f1",
                "f1",
                "f1",
                "f1",
                "f1",
                "f1",
            ],
            "Group": [
                "A",
                "A",
                "B",
                "B",
                "C",
                "A",
                "A",
                "A",
                "B",
                "B",
                "B",
                "C",
                "C",
                "C",
                "C",
            ],
            "Value": [
                1.0,
                1.0,
                2.0,
                2.0,
                3.0,
                1.0,
                2.0,
                1.5,
                5.0,
                7.0,
                8.0,
                3.0,
                4.0,
                3.5,
                4.5,
            ],
        }
    )

    df_result = calculate_anova_one_way_welch(
        df_values.lazy(),
        col_feature="FeatureId",
        rule_p_adjust="bh",
    )

    assert df_result["FeatureId"].to_list() == ["f2", "f1"]
    row_f2 = df_result.row(0, named=True)
    row_f1 = df_result.row(1, named=True)

    assert math.isnan(row_f2["FStatistic"])
    assert math.isnan(row_f2["PValue"])
    assert math.isnan(row_f2["PAdjust"])
    assert row_f1["PAdjust"] >= row_f1["PValue"]


def test_calculate_anova_one_way_welch_supports_numeric_group_values() -> None:
    df_values = pl.DataFrame(
        {
            "Group": [0, 0, 0, 1, 1, 1, 2, 2, 2, 2],
            "Value": [1.0, 2.0, 1.5, 5.0, 7.0, 8.0, 3.0, 4.0, 3.5, 4.5],
        }
    )

    df_result = calculate_anova_one_way_welch(df_values, col_group="Group")

    row = df_result.row(0, named=True)
    expected = calculate_expected_one_way_welch(
        [
            [1.0, 2.0, 1.5],
            [5.0, 7.0, 8.0],
            [3.0, 4.0, 3.5, 4.5],
        ]
    )
    assert row["FStatistic"] == pytest.approx(expected["FStatistic"])
    assert row["PValue"] == pytest.approx(expected["PValue"])


def test_calculate_anova_one_way_welch_keeps_invalid_feature_as_nan() -> None:
    df_values = pl.DataFrame(
        {
            "FeatureId": ["f1", "f1", "f1", "f1", "f2", "f2", "f2", "f2", "f2", "f2"],
            "Group": ["A", "A", "B", "C", "A", "A", "B", "B", "C", "C"],
            "Value": [1.0, 1.0, 3.0, 5.0, 1.0, 2.0, 5.0, 7.0, 3.0, 4.0],
        }
    )

    df_result = calculate_anova_one_way_welch(
        df_values,
        col_feature="FeatureId",
        rule_p_adjust="bonferroni",
    )

    row_f1 = df_result.row(0, named=True)
    row_f2 = df_result.row(1, named=True)
    assert row_f1["NumGroups"] == 3
    assert row_f1["NTotal"] == 4
    assert math.isnan(row_f1["DegreesFreedomBetween"])
    assert math.isnan(row_f1["DegreesFreedomWithin"])
    assert math.isnan(row_f1["FStatistic"])
    assert math.isnan(row_f1["PValue"])
    assert math.isnan(row_f1["PAdjust"])
    assert row_f2["NumGroups"] == 3
    assert row_f2["NTotal"] == 6
    assert not math.isnan(row_f2["FStatistic"])


def test_calculate_anova_one_way_welch_rejects_invalid_inputs() -> None:
    df_values = pl.DataFrame({"Group": ["A", "B"], "Value": [1.0, 2.0]})

    with pytest.raises(ValueError, match="must be different"):
        calculate_anova_one_way_welch(
            df_values,
            col_value="Group",
            col_group="Group",
        )

    with pytest.raises(
        pl.exceptions.InvalidOperationError,
        match=r"conversion from `str` to `f64` failed",
    ):
        calculate_anova_one_way_welch(
            pl.DataFrame(
                {
                    "Group": ["A", "A", "B", "B"],
                    "Value": ["low", "high", "higher", "highest"],
                }
            )
        )


def test_calculate_anova_one_way_rejects_invalid_column_layout() -> None:
    df_values = pl.DataFrame({"Group": ["A", "B"], "Value": [1.0, 2.0]})

    with pytest.raises(ValueError, match="must be different"):
        calculate_anova_one_way(
            df_values,
            col_value="Group",
            col_group="Group",
        )

    with pytest.raises(ValueError, match="must be different"):
        calculate_anova_one_way(
            df_values.rename({"Group": "FeatureId"}),
            col_group="FeatureId",
            col_feature="FeatureId",
        )

    with pytest.raises(ValueError, match="`col_feature` is required"):
        calculate_anova_one_way(
            df_values,
            col_comparison="Comparison",
        )

    with pytest.raises(ValueError, match="`col_comparison` must be different"):
        calculate_anova_one_way(
            df_values.rename({"Group": "Comparison"}),
            col_group="Comparison",
            col_feature="FeatureId",
            col_comparison="Comparison",
        )

    df_validity = pl.DataFrame(
        {
            "Comparison": ["cmp1", "cmp1", "cmp1", "cmp1"],
            "FeatureId": ["f1", "f1", "f1", "f1"],
            "Group": ["A", "A", "B", "B"],
            "Value": [1.0, 2.0, 3.0, 4.0],
            "IsValid": [True, False, True, False],
        }
    )
    with pytest.raises(ValueError, match="must be consistent within each"):
        calculate_anova_one_way(
            df_validity,
            col_feature="FeatureId",
            col_comparison="Comparison",
            col_is_valid="IsValid",
        )


def test_calculate_anova_one_way_rejects_non_numeric_value_column() -> None:
    df_values = pl.DataFrame(
        {
            "Group": ["A", "A", "B", "B"],
            "Value": ["low", "high", "higher", "highest"],
        }
    )

    with pytest.raises(
        pl.exceptions.InvalidOperationError,
        match=r"conversion from `str` to `f64` failed",
    ):
        calculate_anova_one_way(df_values)


def test_calculate_anova_two_way_matches_manual_balanced_result() -> None:
    cells = {
        ("A1", "B1"): [8.0, 10.0],
        ("A1", "B2"): [6.0, 8.0],
        ("A2", "B1"): [4.0, 5.0],
        ("A2", "B2"): [3.0, 6.0],
    }
    df_values = pl.DataFrame(
        {
            "GroupA": ["A1", "A1", "A1", "A1", "A2", "A2", "A2", "A2"],
            "GroupB": ["B1", "B1", "B2", "B2", "B1", "B1", "B2", "B2"],
            "Value": [8.0, 10.0, 6.0, 8.0, 4.0, 5.0, 3.0, 6.0],
        }
    )

    df_result = calculate_anova_two_way(
        df_values,
        col_group_a="GroupA",
        col_group_b="GroupB",
    )

    assert df_result.columns == [
        "NumGroupsA",
        "NumGroupsB",
        "NTotal",
        "DegreesFreedomA",
        "DegreesFreedomB",
        "DegreesFreedomInteraction",
        "DegreesFreedomWithin",
        "FStatisticA",
        "FStatisticB",
        "FStatisticInteraction",
        "PValueA",
        "PValueB",
        "PValueInteraction",
        "PAdjustA",
        "PAdjustB",
        "PAdjustInteraction",
    ]
    row = df_result.row(0, named=True)
    expected = calculate_expected_two_way_balanced(cells)

    assert row["NumGroupsA"] == 2
    assert row["NumGroupsB"] == 2
    assert row["NTotal"] == 8
    assert row["DegreesFreedomA"] == pytest.approx(expected["DegreesFreedomA"])
    assert row["DegreesFreedomB"] == pytest.approx(expected["DegreesFreedomB"])
    assert row["DegreesFreedomInteraction"] == pytest.approx(
        expected["DegreesFreedomInteraction"]
    )
    assert row["DegreesFreedomWithin"] == pytest.approx(
        expected["DegreesFreedomWithin"]
    )
    assert row["FStatisticA"] == pytest.approx(expected["FStatisticA"])
    assert row["FStatisticB"] == pytest.approx(expected["FStatisticB"])
    assert row["FStatisticInteraction"] == pytest.approx(
        expected["FStatisticInteraction"]
    )
    assert row["PValueA"] == pytest.approx(
        stats.f.sf(
            expected["FStatisticA"],
            expected["DegreesFreedomA"],
            expected["DegreesFreedomWithin"],
        )
    )
    assert row["PAdjustA"] == pytest.approx(row["PValueA"])


def test_calculate_anova_two_way_supports_feature_order_and_p_adjust() -> None:
    df_values = pl.DataFrame(
        {
            "FeatureId": [
                "f2",
                "f2",
                "f2",
                "f2",
                "f2",
                "f2",
                "f2",
                "f2",
                "f1",
                "f1",
                "f1",
                "f1",
                "f1",
                "f1",
                "f1",
                "f1",
            ],
            "GroupA": [
                "A1",
                "A1",
                "A1",
                "A1",
                "A2",
                "A2",
                "A2",
                "A2",
                "A1",
                "A1",
                "A1",
                "A1",
                "A2",
                "A2",
                "A2",
                "A2",
            ],
            "GroupB": [
                "B1",
                "B1",
                "B2",
                "B2",
                "B1",
                "B1",
                "B2",
                "B2",
                "B1",
                "B1",
                "B2",
                "B2",
                "B1",
                "B1",
                "B2",
                "B2",
            ],
            "Value": [
                5.0,
                5.0,
                5.0,
                5.0,
                5.0,
                5.0,
                5.0,
                5.0,
                8.0,
                10.0,
                6.0,
                8.0,
                4.0,
                5.0,
                3.0,
                6.0,
            ],
        }
    )

    df_result = calculate_anova_two_way(
        df_values.lazy(),
        col_group_a="GroupA",
        col_group_b="GroupB",
        col_feature="FeatureId",
        rule_p_adjust="bh",
    )

    assert df_result["FeatureId"].to_list() == ["f2", "f1"]
    row_f2 = df_result.row(0, named=True)
    row_f1 = df_result.row(1, named=True)

    assert math.isnan(row_f2["FStatisticA"])
    assert math.isnan(row_f2["PValueA"])
    assert math.isnan(row_f2["PAdjustA"])
    assert row_f1["PAdjustA"] >= row_f1["PValueA"]
    assert row_f1["PAdjustB"] >= row_f1["PValueB"]
    assert row_f1["PAdjustInteraction"] >= row_f1["PValueInteraction"]


def test_calculate_anova_two_way_keeps_unbalanced_feature_as_nan() -> None:
    df_values = pl.DataFrame(
        {
            "FeatureId": [
                "f1",
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
            ],
            "GroupA": [
                "A1",
                "A1",
                "A1",
                "A1",
                "A2",
                "A2",
                "A2",
                "A1",
                "A1",
                "A2",
                "A2",
            ],
            "GroupB": [
                "B1",
                "B1",
                "B2",
                "B2",
                "B1",
                "B1",
                "B2",
                "B1",
                "B2",
                "B1",
                "B2",
            ],
            "Value": [
                8.0,
                10.0,
                6.0,
                8.0,
                4.0,
                5.0,
                3.0,
                1.0,
                2.0,
                3.0,
                4.0,
            ],
        }
    )

    df_result = calculate_anova_two_way(
        df_values,
        col_group_a="GroupA",
        col_group_b="GroupB",
        col_feature="FeatureId",
    )

    row_f1 = df_result.row(0, named=True)
    row_f2 = df_result.row(1, named=True)
    assert math.isnan(row_f1["FStatisticA"])
    assert math.isnan(row_f1["FStatisticB"])
    assert math.isnan(row_f1["FStatisticInteraction"])
    assert math.isnan(row_f1["PValueA"])
    assert math.isnan(row_f1["PValueB"])
    assert math.isnan(row_f1["PValueInteraction"])
    assert row_f2["NumGroupsA"] == 2
    assert row_f2["NumGroupsB"] == 2


def test_calculate_anova_two_way_rejects_invalid_inputs() -> None:
    df_values = pl.DataFrame(
        {
            "GroupA": ["A1", "A2"],
            "GroupB": ["B1", "B2"],
            "Value": [1.0, 2.0],
        }
    )

    with pytest.raises(ValueError, match="must be different"):
        calculate_anova_two_way(
            df_values,
            col_value="GroupA",
            col_group_a="GroupA",
            col_group_b="GroupB",
        )

    with pytest.raises(
        pl.exceptions.InvalidOperationError,
        match=r"conversion from `str` to `f64` failed",
    ):
        calculate_anova_two_way(
            pl.DataFrame(
                {
                    "GroupA": ["A1", "A1", "A2", "A2"],
                    "GroupB": ["B1", "B2", "B1", "B2"],
                    "Value": ["low", "high", "higher", "highest"],
                }
            ),
            col_group_a="GroupA",
            col_group_b="GroupB",
        )
