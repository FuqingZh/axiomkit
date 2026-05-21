from __future__ import annotations

import math

import numpy as np
import polars as pl
import pytest
from axiomkit.stats import (
    ParametricComparison,
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


def test_anova_comparison_normalizes_inputs() -> None:
    comparison = ParametricComparison.anova_one_way(
        " cmp1 ",
        groups=["A", "B", "A", "1"],
    )

    assert comparison.comparison_id == "cmp1"
    assert comparison.groups == ("A", "B", "1")


def test_anova_comparison_rejects_invalid_inputs() -> None:
    with pytest.raises(ValueError, match="comparison_id"):
        ParametricComparison.anova_one_way(" ")

    with pytest.raises(ValueError, match="at least two"):
        ParametricComparison.anova_one_way("cmp1", groups=["A", "A"])


def test_parametric_comparison_factories_validate_inputs() -> None:
    one_sample = ParametricComparison.ttest_one_sample(" cmp1 ")
    one_way = ParametricComparison.anova_one_way("cmp2", groups=["A", "B", "A"])
    one_way_welch = ParametricComparison.anova_one_way_welch(
        "cmp3",
        groups=["A", "B"],
    )
    two_way = ParametricComparison.anova_two_way(
        "cmp4",
        groups_a=["A1", "A2", "A1"],
        groups_b=["B1", "B2"],
    )
    unscoped_ttest = ParametricComparison.ttest_two_sample(
        group_test="B",
        group_ref="A",
    )
    scoped_ttest = ParametricComparison.ttest_paired(
        group_test="B",
        group_ref="A",
        comparison_id="cmp5",
    )

    assert one_sample.comparison_id == "cmp1"
    assert one_sample.kind == "ttest_one_sample"
    assert unscoped_ttest.comparison_id is None
    assert scoped_ttest.comparison_id == "cmp5"
    assert one_way.comparison_id == "cmp2"
    assert one_way.groups == ("A", "B")
    assert one_way_welch.kind == "anova_one_way_welch"
    assert one_way_welch.groups == ("A", "B")
    assert two_way.kind == "anova_two_way"
    assert two_way.groups_a == ("A1", "A2")
    assert two_way.groups_b == ("B1", "B2")

    with pytest.raises(ValueError, match="group_test"):
        ParametricComparison.ttest_two_sample(
            group_test="A",
            group_ref="A",
        )

    with pytest.raises(ValueError, match="comparison_id"):
        ParametricComparison.anova_one_way(None)  # type: ignore[arg-type]


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


def test_calculate_anova_one_way_filters_declared_comparisons_and_groups() -> None:
    df_values = pl.DataFrame(
        {
            "Comparison": ["cmp1"] * 9 + ["cmp2"] * 6,
            "FeatureId": ["f1"] * 15,
            "Group": ["A", "A", "A", "B", "B", "B", "C", "C", "C"] + ["A", "A", "B", "B", "C", "C"],
            "Value": [
                1.0,
                2.0,
                3.0,
                6.0,
                7.0,
                8.0,
                20.0,
                21.0,
                22.0,
                10.0,
                11.0,
                14.0,
                15.0,
                30.0,
                31.0,
            ],
        }
    )

    df_result = calculate_anova_one_way(
        df_values,
        col_feature="FeatureId",
        col_comparison="Comparison",
        comparisons=ParametricComparison.anova_one_way("cmp1", groups=["A", "B"]),
    )

    assert df_result.select("Comparison", "FeatureId").rows() == [("cmp1", "f1")]
    row = df_result.row(0, named=True)
    expected = stats.f_oneway([1.0, 2.0, 3.0], [6.0, 7.0, 8.0])

    assert row["NumGroups"] == 2
    assert row["NTotal"] == 6
    assert row["FStatistic"] == pytest.approx(expected.statistic)
    assert row["PValue"] == pytest.approx(expected.pvalue)


def test_calculate_anova_one_way_accepts_parametric_comparison() -> None:
    df_values = pl.DataFrame(
        {
            "Comparison": ["cmp1"] * 6 + ["cmp2"] * 6,
            "FeatureId": ["f1"] * 12,
            "Group": ["A", "A", "B", "B", "C", "C"] * 2,
            "Value": [1.0, 2.0, 5.0, 6.0, 3.0, 4.0, 10.0, 11.0, 20.0, 21.0, 30.0, 31.0],
        }
    )

    df_result = calculate_anova_one_way(
        df_values,
        col_feature="FeatureId",
        col_comparison="Comparison",
        comparisons=ParametricComparison.anova_one_way(
            "cmp2",
            groups=["A", "B", "C"],
        ),
    )

    assert df_result.select("Comparison", "FeatureId").rows() == [("cmp2", "f1")]


def test_calculate_anova_one_way_keeps_missing_requested_group_as_nan() -> None:
    df_values = pl.DataFrame(
        {
            "Comparison": ["cmp1", "cmp1", "cmp1", "cmp1"],
            "FeatureId": ["f1", "f1", "f1", "f1"],
            "Group": ["A", "A", "B", "B"],
            "Value": [1.0, 2.0, 4.0, 5.0],
        }
    )

    df_result = calculate_anova_one_way(
        df_values,
        col_feature="FeatureId",
        col_comparison="Comparison",
        comparisons=ParametricComparison.anova_one_way("cmp1", groups=["A", "C"]),
        rule_p_adjust="bh",
    )

    row = df_result.row(0, named=True)
    assert row["Comparison"] == "cmp1"
    assert row["FeatureId"] == "f1"
    assert row["NumGroups"] == 1
    assert row["NTotal"] == 2
    assert math.isnan(row["FStatistic"])
    assert math.isnan(row["PValue"])
    assert math.isnan(row["PAdjust"])


def test_calculate_anova_one_way_adjusts_p_values_within_each_comparison() -> None:
    df_values = pl.DataFrame(
        {
            "Comparison": ["cmp1"] * 12 + ["cmp2"] * 12,
            "FeatureId": ["f1"] * 6 + ["f2"] * 6 + ["f1"] * 6 + ["f2"] * 6,
            "Group": ["A", "A", "B", "B", "C", "C"] * 4,
            "Value": [
                1.0,
                2.0,
                5.0,
                6.0,
                3.0,
                4.0,
                1.0,
                2.0,
                2.0,
                3.0,
                3.0,
                4.0,
                10.0,
                11.0,
                20.0,
                21.0,
                30.0,
                31.0,
                10.0,
                11.0,
                12.0,
                13.0,
                14.0,
                15.0,
            ],
        }
    )

    df_result = calculate_anova_one_way(
        df_values,
        col_feature="FeatureId",
        col_comparison="Comparison",
        rule_p_adjust="bh",
    )

    for comparison_id in ["cmp1", "cmp2"]:
        df_comparison = df_result.filter(pl.col("Comparison") == comparison_id)
        expected = stats.false_discovery_control(
            df_comparison["PValue"].to_numpy(),
            method="bh",
        )
        assert df_comparison["PAdjust"].to_list() == pytest.approx(expected)

    expected_global = stats.false_discovery_control(
        df_result["PValue"].to_numpy(),
        method="bh",
    )
    assert df_result["PAdjust"].to_list() != pytest.approx(expected_global)


def test_calculate_anova_one_way_excludes_invalid_units_from_p_adjust() -> None:
    df_values = pl.DataFrame(
        {
            "Comparison": ["cmp1"] * 12 + ["cmp2"] * 6,
            "FeatureId": ["f1"] * 6 + ["f2"] * 6 + ["f1"] * 6,
            "Group": ["A", "A", "B", "B", "C", "C"] * 3,
            "Value": [
                1.0,
                2.0,
                5.0,
                6.0,
                3.0,
                4.0,
                100.0,
                101.0,
                110.0,
                111.0,
                120.0,
                121.0,
                10.0,
                11.0,
                20.0,
                21.0,
                30.0,
                31.0,
            ],
            "IsValid": [True] * 6 + [False] * 6 + [True] * 6,
        }
    )

    df_result = calculate_anova_one_way(
        df_values,
        col_feature="FeatureId",
        col_comparison="Comparison",
        col_is_valid="IsValid",
        rule_p_adjust="bonferroni",
    )

    assert df_result.select("Comparison", "FeatureId").rows() == [
        ("cmp1", "f1"),
        ("cmp2", "f1"),
    ]
    assert df_result["PAdjust"].to_list() == pytest.approx(
        df_result["PValue"].to_list()
    )


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


def test_calculate_anova_one_way_welch_supports_comparisons_and_group_filter() -> None:
    df_values = pl.DataFrame(
        {
            "Comparison": ["cmp1"] * 6 + ["cmp2"] * 6,
            "FeatureId": ["f1"] * 12,
            "Group": ["A", "A", "B", "B", "C", "C"] * 2,
            "Value": [
                1.0,
                2.0,
                5.0,
                6.0,
                20.0,
                22.0,
                3.0,
                4.0,
                8.0,
                9.0,
                30.0,
                32.0,
            ],
        }
    )

    df_result = calculate_anova_one_way_welch(
        df_values,
        col_feature="FeatureId",
        col_comparison="Comparison",
        comparisons=ParametricComparison.anova_one_way_welch(
            "cmp1",
            groups=["A", "B"],
        ),
    )

    assert df_result.select("Comparison", "FeatureId").rows() == [("cmp1", "f1")]
    row = df_result.row(0, named=True)
    assert row["NumGroups"] == 2
    assert row["NTotal"] == 4
    assert not math.isnan(row["FStatistic"])


def test_calculate_anova_one_way_welch_missing_requested_group_stays_nan() -> None:
    df_values = pl.DataFrame(
        {
            "Comparison": ["cmp1", "cmp1", "cmp1", "cmp1"],
            "FeatureId": ["f1", "f1", "f1", "f1"],
            "Group": ["A", "A", "B", "B"],
            "Value": [1.0, 2.0, 5.0, 6.0],
        }
    )

    df_result = calculate_anova_one_way_welch(
        df_values,
        col_feature="FeatureId",
        col_comparison="Comparison",
        comparisons=ParametricComparison.anova_one_way_welch(
            "cmp1",
            groups=["A", "C"],
        ),
    )

    row = df_result.row(0, named=True)
    assert row["NumGroups"] == 1
    assert math.isnan(row["FStatistic"])
    assert math.isnan(row["PAdjust"])


def test_calculate_anova_one_way_welch_adjusts_p_values_within_comparison() -> None:
    df_values = pl.DataFrame(
        {
            "Comparison": ["cmp1"] * 9 + ["cmp2"] * 9,
            "FeatureId": ["f1"] * 18,
            "Group": ["A", "A", "A", "B", "B", "B", "C", "C", "C"] * 2,
            "Value": [
                1.0,
                2.0,
                1.5,
                5.0,
                7.0,
                8.0,
                3.0,
                4.0,
                3.5,
                2.0,
                3.0,
                2.5,
                8.0,
                10.0,
                11.0,
                5.0,
                6.0,
                5.5,
            ],
        }
    )

    df_result = calculate_anova_one_way_welch(
        df_values,
        col_feature="FeatureId",
        col_comparison="Comparison",
        comparisons=[
            ParametricComparison.anova_one_way_welch("cmp1"),
            ParametricComparison.anova_one_way_welch("cmp2"),
        ],
        rule_p_adjust="bh",
    )

    assert df_result["Comparison"].to_list() == ["cmp1", "cmp2"]
    assert df_result["PAdjust"].to_list() == pytest.approx(
        df_result["PValue"].to_list()
    )


def test_calculate_anova_one_way_welch_p_adjust_differs_from_global_scope() -> None:
    rows: list[tuple[str, str, str, float]] = []
    values_by_unit = {
        ("cmp1", "f1"): {
            "A": [1.0, 2.0, 1.5],
            "B": [5.0, 7.0, 8.0],
            "C": [3.0, 4.0, 3.5],
        },
        ("cmp1", "f2"): {
            "A": [1.0, 2.0, 1.5],
            "B": [2.0, 3.0, 2.5],
            "C": [3.0, 4.0, 3.5],
        },
        ("cmp2", "f1"): {
            "A": [2.0, 3.0, 2.5],
            "B": [8.0, 10.0, 11.0],
            "C": [5.0, 6.0, 5.5],
        },
        ("cmp2", "f2"): {
            "A": [10.0, 11.0, 10.5],
            "B": [12.0, 13.0, 12.5],
            "C": [14.0, 15.0, 14.5],
        },
    }
    for (comparison_id, feature_id), values_by_group in values_by_unit.items():
        for group, values in values_by_group.items():
            rows.extend((comparison_id, feature_id, group, value) for value in values)

    df_result = calculate_anova_one_way_welch(
        pl.DataFrame(
            rows,
            schema=["Comparison", "FeatureId", "Group", "Value"],
            orient="row",
        ),
        col_feature="FeatureId",
        col_comparison="Comparison",
        rule_p_adjust="bh",
    )

    for comparison_id in ["cmp1", "cmp2"]:
        df_comparison = df_result.filter(pl.col("Comparison") == comparison_id)
        expected = stats.false_discovery_control(
            df_comparison["PValue"].to_numpy(),
            method="bh",
        )
        assert df_comparison["PAdjust"].to_list() == pytest.approx(expected)

    expected_global = stats.false_discovery_control(
        df_result["PValue"].to_numpy(),
        method="bh",
    )
    assert df_result["PAdjust"].to_list() != pytest.approx(expected_global)


def test_calculate_anova_one_way_welch_excludes_invalid_units_from_p_adjust() -> None:
    rows: list[tuple[str, str, str, float, bool]] = []
    values_by_unit = {
        ("cmp1", "f1", True): {
            "A": [1.0, 2.0, 1.5],
            "B": [5.0, 7.0, 8.0],
            "C": [3.0, 4.0, 3.5],
        },
        ("cmp1", "f2", False): {
            "A": [100.0, 101.0, 100.5],
            "B": [110.0, 111.0, 110.5],
            "C": [120.0, 121.0, 120.5],
        },
        ("cmp2", "f1", True): {
            "A": [2.0, 3.0, 2.5],
            "B": [8.0, 10.0, 11.0],
            "C": [5.0, 6.0, 5.5],
        },
    }
    for (comparison_id, feature_id, is_valid), values_by_group in values_by_unit.items():
        for group, values in values_by_group.items():
            rows.extend(
                (comparison_id, feature_id, group, value, is_valid)
                for value in values
            )

    df_result = calculate_anova_one_way_welch(
        pl.DataFrame(
            rows,
            schema=["Comparison", "FeatureId", "Group", "Value", "IsValid"],
            orient="row",
        ),
        col_feature="FeatureId",
        col_comparison="Comparison",
        col_is_valid="IsValid",
        rule_p_adjust="bonferroni",
    )

    assert df_result.select("Comparison", "FeatureId").rows() == [
        ("cmp1", "f1"),
        ("cmp2", "f1"),
    ]
    assert df_result["PAdjust"].to_list() == pytest.approx(
        df_result["PValue"].to_list()
    )


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

    with pytest.raises(ValueError, match="`col_comparison` is required"):
        calculate_anova_one_way_welch(
            df_values,
            comparisons=ParametricComparison.anova_one_way_welch("cmp1"),
        )

    with pytest.raises(ValueError, match="Duplicate comparison ids"):
        calculate_anova_one_way_welch(
            pl.DataFrame(
                {
                    "Comparison": ["cmp1", "cmp1"],
                    "FeatureId": ["f1", "f1"],
                    "Group": ["A", "B"],
                    "Value": [1.0, 2.0],
                }
            ),
            col_feature="FeatureId",
            col_comparison="Comparison",
            comparisons=[
                ParametricComparison.anova_one_way_welch("cmp1"),
                ParametricComparison.anova_one_way_welch("cmp1"),
            ],
        )

    with pytest.raises(ValueError, match="anova_one_way_welch"):
        calculate_anova_one_way_welch(
            pl.DataFrame(
                {
                    "Comparison": ["cmp1", "cmp1"],
                    "FeatureId": ["f1", "f1"],
                    "Group": ["A", "B"],
                    "Value": [1.0, 2.0],
                }
            ),
            col_feature="FeatureId",
            col_comparison="Comparison",
            comparisons=ParametricComparison.anova_one_way("cmp1"),
        )

    with pytest.raises(ValueError, match="`col_feature` is required"):
        calculate_anova_one_way_welch(
            df_values,
            col_comparison="Comparison",
        )

    with pytest.raises(ValueError, match="`col_comparison` must be different"):
        calculate_anova_one_way_welch(
            df_values.rename({"Group": "Comparison"}),
            col_group="Comparison",
            col_feature="FeatureId",
            col_comparison="Comparison",
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

    with pytest.raises(ValueError, match="`col_comparison` is required"):
        calculate_anova_one_way(
            df_values,
            comparisons=ParametricComparison.anova_one_way("cmp1"),
        )

    with pytest.raises(ValueError, match="Duplicate comparison ids"):
        calculate_anova_one_way(
            pl.DataFrame(
                {
                    "Comparison": ["cmp1", "cmp1"],
                    "FeatureId": ["f1", "f1"],
                    "Group": ["A", "B"],
                    "Value": [1.0, 2.0],
                }
            ),
            col_feature="FeatureId",
            col_comparison="Comparison",
            comparisons=[
                ParametricComparison.anova_one_way("cmp1"),
                ParametricComparison.anova_one_way("cmp1"),
            ],
        )

    with pytest.raises(ValueError, match="anova_one_way"):
        calculate_anova_one_way(
            pl.DataFrame(
                {
                    "Comparison": ["cmp1", "cmp1"],
                    "FeatureId": ["f1", "f1"],
                    "Group": ["A", "B"],
                    "Value": [1.0, 2.0],
                }
            ),
            col_feature="FeatureId",
            col_comparison="Comparison",
            comparisons=ParametricComparison.ttest_one_sample("cmp1"),
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


def test_calculate_anova_two_way_supports_comparisons_and_group_filters() -> None:
    df_values = pl.DataFrame(
        {
            "Comparison": ["cmp1"] * 12 + ["cmp2"] * 12,
            "FeatureId": ["f1"] * 24,
            "GroupA": (["A1"] * 4 + ["A2"] * 4 + ["A3"] * 4) * 2,
            "GroupB": (["B1", "B1", "B2", "B2"] * 3) * 2,
            "Value": [
                8.0,
                10.0,
                6.0,
                8.0,
                4.0,
                5.0,
                3.0,
                6.0,
                30.0,
                31.0,
                32.0,
                33.0,
                9.0,
                11.0,
                7.0,
                9.0,
                5.0,
                6.0,
                4.0,
                7.0,
                40.0,
                41.0,
                42.0,
                43.0,
            ],
        }
    )

    df_result = calculate_anova_two_way(
        df_values,
        col_group_a="GroupA",
        col_group_b="GroupB",
        col_feature="FeatureId",
        col_comparison="Comparison",
        comparisons=ParametricComparison.anova_two_way(
            "cmp1",
            groups_a=["A1", "A2"],
            groups_b=["B1", "B2"],
        ),
    )

    assert df_result.select("Comparison", "FeatureId").rows() == [("cmp1", "f1")]
    row = df_result.row(0, named=True)
    assert row["NumGroupsA"] == 2
    assert row["NumGroupsB"] == 2
    assert row["NTotal"] == 8
    assert not math.isnan(row["FStatisticA"])


def test_calculate_anova_two_way_adjusts_p_values_within_comparison() -> None:
    df_values = pl.DataFrame(
        {
            "Comparison": ["cmp1"] * 8 + ["cmp2"] * 8,
            "FeatureId": ["f1"] * 16,
            "GroupA": ["A1", "A1", "A1", "A1", "A2", "A2", "A2", "A2"] * 2,
            "GroupB": ["B1", "B1", "B2", "B2", "B1", "B1", "B2", "B2"] * 2,
            "Value": [
                8.0,
                10.0,
                6.0,
                8.0,
                4.0,
                5.0,
                3.0,
                6.0,
                9.0,
                11.0,
                7.0,
                9.0,
                5.0,
                6.0,
                4.0,
                7.0,
            ],
        }
    )

    df_result = calculate_anova_two_way(
        df_values,
        col_group_a="GroupA",
        col_group_b="GroupB",
        col_feature="FeatureId",
        col_comparison="Comparison",
        comparisons=[
            ParametricComparison.anova_two_way("cmp1"),
            ParametricComparison.anova_two_way("cmp2"),
        ],
        rule_p_adjust="bh",
    )

    assert df_result["Comparison"].to_list() == ["cmp1", "cmp2"]
    assert df_result["PAdjustA"].to_list() == pytest.approx(
        df_result["PValueA"].to_list()
    )
    assert df_result["PAdjustB"].to_list() == pytest.approx(
        df_result["PValueB"].to_list()
    )
    assert df_result["PAdjustInteraction"].to_list() == pytest.approx(
        df_result["PValueInteraction"].to_list()
    )


def test_calculate_anova_two_way_p_adjust_differs_from_global_scope() -> None:
    rows: list[tuple[str, str, str, str, float]] = []
    cells_by_unit = {
        ("cmp1", "f1"): {
            ("A1", "B1"): [8.0, 10.0],
            ("A1", "B2"): [6.0, 8.0],
            ("A2", "B1"): [4.0, 5.0],
            ("A2", "B2"): [3.0, 6.0],
        },
        ("cmp1", "f2"): {
            ("A1", "B1"): [10.0, 11.0],
            ("A1", "B2"): [9.0, 10.0],
            ("A2", "B1"): [8.0, 9.0],
            ("A2", "B2"): [7.0, 8.0],
        },
        ("cmp2", "f1"): {
            ("A1", "B1"): [20.0, 22.0],
            ("A1", "B2"): [17.0, 19.0],
            ("A2", "B1"): [10.0, 12.0],
            ("A2", "B2"): [8.0, 11.0],
        },
        ("cmp2", "f2"): {
            ("A1", "B1"): [11.0, 12.0],
            ("A1", "B2"): [8.0, 9.0],
            ("A2", "B1"): [10.0, 11.0],
            ("A2", "B2"): [7.0, 8.0],
        },
    }
    for (comparison_id, feature_id), cells in cells_by_unit.items():
        for (group_a, group_b), values in cells.items():
            rows.extend(
                (comparison_id, feature_id, group_a, group_b, value)
                for value in values
            )

    df_result = calculate_anova_two_way(
        pl.DataFrame(
            rows,
            schema=["Comparison", "FeatureId", "GroupA", "GroupB", "Value"],
            orient="row",
        ),
        col_group_a="GroupA",
        col_group_b="GroupB",
        col_feature="FeatureId",
        col_comparison="Comparison",
        rule_p_adjust="bh",
    )

    for comparison_id in ["cmp1", "cmp2"]:
        df_comparison = df_result.filter(pl.col("Comparison") == comparison_id)
        for p_value_col, p_adjust_col in [
            ("PValueA", "PAdjustA"),
            ("PValueB", "PAdjustB"),
            ("PValueInteraction", "PAdjustInteraction"),
        ]:
            expected = stats.false_discovery_control(
                df_comparison[p_value_col].to_numpy(),
                method="bh",
            )
            assert df_comparison[p_adjust_col].to_list() == pytest.approx(expected)

    expected_global_a = stats.false_discovery_control(
        df_result["PValueA"].to_numpy(),
        method="bh",
    )
    assert df_result["PAdjustA"].to_list() != pytest.approx(expected_global_a)


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

    with pytest.raises(ValueError, match="`col_comparison` is required"):
        calculate_anova_two_way(
            df_values,
            col_group_a="GroupA",
            col_group_b="GroupB",
            comparisons=ParametricComparison.anova_two_way("cmp1"),
        )

    with pytest.raises(ValueError, match="`col_feature` is required"):
        calculate_anova_two_way(
            df_values,
            col_group_a="GroupA",
            col_group_b="GroupB",
            col_comparison="Comparison",
        )

    with pytest.raises(ValueError, match="Duplicate comparison ids"):
        calculate_anova_two_way(
            pl.DataFrame(
                {
                    "Comparison": ["cmp1", "cmp1"],
                    "FeatureId": ["f1", "f1"],
                    "GroupA": ["A1", "A2"],
                    "GroupB": ["B1", "B2"],
                    "Value": [1.0, 2.0],
                }
            ),
            col_group_a="GroupA",
            col_group_b="GroupB",
            col_feature="FeatureId",
            col_comparison="Comparison",
            comparisons=[
                ParametricComparison.anova_two_way("cmp1"),
                ParametricComparison.anova_two_way("cmp1"),
            ],
        )

    with pytest.raises(ValueError, match="anova_two_way"):
        calculate_anova_two_way(
            pl.DataFrame(
                {
                    "Comparison": ["cmp1", "cmp1"],
                    "FeatureId": ["f1", "f1"],
                    "GroupA": ["A1", "A2"],
                    "GroupB": ["B1", "B2"],
                    "Value": [1.0, 2.0],
                }
            ),
            col_group_a="GroupA",
            col_group_b="GroupB",
            col_feature="FeatureId",
            col_comparison="Comparison",
            comparisons=ParametricComparison.anova_one_way("cmp1"),
        )
