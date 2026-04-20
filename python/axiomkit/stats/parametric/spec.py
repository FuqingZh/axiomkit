from typing import Self

import polars as pl
import polars.selectors as pl_sel
from polars._typing import SchemaDict

from .constant import COL_FEATURE_INTERNAL, COL_FEATURE_ORDER


class ParametricFrameAdapter:
    def __init__(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        *,
        col_feature: str | None = None,
        col_comparison: str | None = None,
        col_is_valid: str | None = None,
    ) -> None:
        self.df = df
        self.lf = self.df.lazy() if isinstance(self.df, pl.DataFrame) else self.df
        self.schema_in: SchemaDict = dict(self.lf.collect_schema())
        self.col_feature = col_feature
        self.col_comparison = col_comparison
        self.col_is_valid = col_is_valid

    def select_required_cols(self, cols_required: list[str]) -> Self:
        cols_in = set(self.schema_in.keys())
        cols_missing = set(cols_required) - cols_in
        if cols_missing:
            raise ValueError(
                f"Input `df` is missing required columns: {', '.join(cols_missing)}."
            )
        self.lf = self.lf.select(cols_required)

        return self

    def cast_cols(
        self,
        *,
        cols_float: str | list[str] | None = None,
        cols_string: str | list[str] | None = None,
        cols_boolean: str | list[str] | None = None,
    ) -> Self:
        exprs_cast: list[pl.Expr] = []
        if cols_float is not None:
            exprs_cast.append(pl_sel.by_name(cols_float).cast(pl.Float64))
        if cols_string is not None:
            exprs_cast.append(pl_sel.by_name(cols_string).cast(pl.String))
        if cols_boolean is not None:
            exprs_cast.append(pl_sel.by_name(cols_boolean).cast(pl.Boolean))
        if exprs_cast:
            self.lf = self.lf.with_columns(*exprs_cast)

        return self

    def create_feature_key(self) -> Self:
        expr_comparison = (
            pl.lit(None) if self.col_comparison is None else pl.col(self.col_comparison)
        )
        expr_feature = (
            pl.lit(None) if self.col_feature is None else pl.col(self.col_feature)
        )
        self.lf = self.lf.with_columns(
            pl.concat_list([expr_comparison, expr_feature]).alias(COL_FEATURE_INTERNAL)
        )

        return self

    def _validate_feature_units(self, lf_feature: pl.LazyFrame) -> None:
        if self.col_is_valid is None:
            return

        df_inconsistent = lf_feature.filter(pl.col("_ValidCount") > 1).collect()
        if df_inconsistent.height > 0:
            raise ValueError(
                "Arg `col_is_valid` must be consistent within each feature unit."
            )

    def create_feature_frame(self) -> pl.LazyFrame:
        if COL_FEATURE_INTERNAL not in self.lf.collect_schema():
            self.create_feature_key()

        if self.col_is_valid is not None:
            lf_feature = self.lf.group_by(
                COL_FEATURE_INTERNAL, maintain_order=True
            ).agg(
                pl.col(self.col_is_valid).n_unique().alias("_ValidCount"),
                pl.col(self.col_is_valid).first().alias(self.col_is_valid),
            )
            self._validate_feature_units(lf_feature)
            lf_feature = lf_feature.filter(
                pl.col(self.col_is_valid).fill_null(False)
            ).select(COL_FEATURE_INTERNAL)
        else:
            lf_feature = self.lf.select(COL_FEATURE_INTERNAL).unique(
                maintain_order=True
            )

        return lf_feature.with_row_index(COL_FEATURE_ORDER)

    def create_result_schema(self, schema_base: SchemaDict) -> SchemaDict:
        schema_result: SchemaDict = {}
        if self.col_comparison is not None:
            schema_result[self.col_comparison] = self.schema_in[self.col_comparison]
        if self.col_feature is not None:
            schema_result[self.col_feature] = self.schema_in[self.col_feature]
        schema_result.update(schema_base)

        return schema_result

    def create_result_frame(
        self,
        df_result: pl.DataFrame,
        cols_selected: list[str],
    ) -> pl.DataFrame:
        cols_output: list[str] = []
        exprs_output: list[pl.Expr] = []

        if self.col_comparison is not None:
            cols_output.append(self.col_comparison)
            exprs_output.append(
                pl.col(COL_FEATURE_INTERNAL).list.get(0).alias(self.col_comparison)
            )
        if self.col_feature is not None:
            cols_output.append(self.col_feature)
            exprs_output.append(
                pl.col(COL_FEATURE_INTERNAL).list.get(1).alias(self.col_feature)
            )
        if exprs_output:
            df_result = df_result.with_columns(*exprs_output)

        return df_result.select([*cols_output, *cols_selected])
