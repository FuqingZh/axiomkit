from __future__ import annotations

import polars as pl
import pytest
from axiomkit.stats.parametric.constant import COL_FEATURE_INTERNAL, COL_FEATURE_ORDER
from axiomkit.stats.parametric.spec import ParametricFrameAdapter


def test_parametric_frame_plan_supports_plain_mode_without_feature() -> None:
    plan = ParametricFrameAdapter(pl.DataFrame({"Value": [1.0, 2.0]}))
    plan.cast_cols(cols_float="Value").create_feature_key()
    schema_result = plan.create_result_schema({"Stat": pl.Float64})

    df_values = plan.lf.collect()
    lf_features = plan.create_feature_frame().collect()

    assert df_values.columns == ["Value", COL_FEATURE_INTERNAL]
    assert df_values[COL_FEATURE_INTERNAL].to_list() == [[None, None], [None, None]]
    assert schema_result == {"Stat": pl.Float64}
    assert lf_features.columns == [COL_FEATURE_ORDER, COL_FEATURE_INTERNAL]
    assert lf_features.rows() == [(0, [None, None])]


def test_parametric_frame_plan_supports_plain_mode_with_feature() -> None:
    plan = ParametricFrameAdapter(
        pl.DataFrame(
            {
                "FeatureId": ["f1", "f2"],
                "Value": [1.0, 2.0],
            }
        ),
        col_feature="FeatureId",
    )
    plan.cast_cols(cols_float="Value").create_feature_key()
    schema_result = plan.create_result_schema({"Stat": pl.Float64})

    df_values = plan.lf.collect()
    df_result = plan.create_result_frame(
        df_values.select(COL_FEATURE_INTERNAL)
        .unique(maintain_order=True)
        .with_columns(pl.lit(1.0).alias("Stat")),
        cols_selected=["Stat"],
    )

    assert df_values.columns == ["FeatureId", "Value", COL_FEATURE_INTERNAL]
    assert df_values[COL_FEATURE_INTERNAL].to_list() == [[None, "f1"], [None, "f2"]]
    assert schema_result == {"FeatureId": pl.String, "Stat": pl.Float64}
    assert df_result.columns == ["FeatureId", "Stat"]
    assert df_result.rows() == [("f1", 1.0), ("f2", 1.0)]


def test_parametric_frame_plan_supports_comparison_without_validity_gate() -> None:
    plan = ParametricFrameAdapter(
        pl.DataFrame(
            {
                "Comparison": ["cmp1", "cmp1", "cmp2", "cmp2"],
                "FeatureId": ["f1", "f1", "f1", "f1"],
                "Group": ["A", "B", "A", "B"],
                "Value": [1.0, 2.0, 3.0, 4.0],
            }
        ),
        col_feature="FeatureId",
        col_comparison="Comparison",
    )
    plan.cast_cols(cols_float="Value", cols_string="Group")
    plan.create_feature_key()
    schema_result = plan.create_result_schema({"Stat": pl.Float64})

    df_values = plan.lf.collect().sort([COL_FEATURE_INTERNAL, "Group"])
    assert df_values.columns == [
        "Comparison",
        "FeatureId",
        "Group",
        "Value",
        COL_FEATURE_INTERNAL,
    ]
    assert df_values.rows() == [
        ("cmp1", "f1", "A", 1.0, ["cmp1", "f1"]),
        ("cmp1", "f1", "B", 2.0, ["cmp1", "f1"]),
        ("cmp2", "f1", "A", 3.0, ["cmp2", "f1"]),
        ("cmp2", "f1", "B", 4.0, ["cmp2", "f1"]),
    ]
    assert schema_result == {
        "Comparison": pl.String,
        "FeatureId": pl.String,
        "Stat": pl.Float64,
    }
    df_features = plan.create_feature_frame().collect()
    assert df_features.rows() == [
        (0, ["cmp1", "f1"]),
        (1, ["cmp2", "f1"]),
    ]

    df_result = plan.create_result_frame(
        pl.DataFrame(
            {
                COL_FEATURE_INTERNAL: [["cmp1", "f1"], ["cmp2", "f1"]],
                "Stat": [10.0, 20.0],
            }
        ),
        cols_selected=["Stat"],
    )
    assert df_result.columns == ["Comparison", "FeatureId", "Stat"]
    assert df_result.rows() == [("cmp1", "f1", 10.0), ("cmp2", "f1", 20.0)]


def test_parametric_frame_plan_filters_invalid_comparison_feature_units() -> None:
    plan = ParametricFrameAdapter(
        pl.DataFrame(
            {
                "Comparison": ["cmp1", "cmp1", "cmp2", "cmp2"],
                "FeatureId": ["f1", "f1", "f1", "f1"],
                "Group": ["A", "B", "A", "B"],
                "Value": [1.0, 2.0, 3.0, 4.0],
                "IsValid": [True, True, False, False],
            }
        ),
        col_feature="FeatureId",
        col_comparison="Comparison",
        col_is_valid="IsValid",
    )
    plan.cast_cols(
        cols_float="Value",
        cols_string="Group",
        cols_boolean="IsValid",
    )
    plan.create_feature_key()

    df_values = plan.lf.collect().sort(["Comparison", "Group"])
    assert df_values.columns == [
        "Comparison",
        "FeatureId",
        "Group",
        "Value",
        "IsValid",
        COL_FEATURE_INTERNAL,
    ]
    assert df_values.rows() == [
        ("cmp1", "f1", "A", 1.0, True, ["cmp1", "f1"]),
        ("cmp1", "f1", "B", 2.0, True, ["cmp1", "f1"]),
        ("cmp2", "f1", "A", 3.0, False, ["cmp2", "f1"]),
        ("cmp2", "f1", "B", 4.0, False, ["cmp2", "f1"]),
    ]
    df_features = plan.create_feature_frame().collect()
    assert df_features.rows() == [(0, ["cmp1", "f1"])]


def test_parametric_frame_plan_rejects_inconsistent_validity_within_unit() -> None:
    plan = ParametricFrameAdapter(
        pl.DataFrame(
            {
                "Comparison": ["cmp1", "cmp1"],
                "FeatureId": ["f1", "f1"],
                "Group": ["A", "B"],
                "Value": [1.0, 2.0],
                "IsValid": [True, False],
            }
        ),
        col_feature="FeatureId",
        col_comparison="Comparison",
        col_is_valid="IsValid",
    )
    plan.cast_cols(cols_boolean="IsValid")

    with pytest.raises(ValueError, match="must be consistent within each"):
        plan.create_feature_frame().collect()
