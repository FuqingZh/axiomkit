from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum


class ParametricComparisonKind(StrEnum):
    TTEST_ONE_SAMPLE = "ttest_one_sample"
    TTEST_TWO_SAMPLE = "ttest_two_sample"
    TTEST_PAIRED = "ttest_paired"
    ANOVA_ONE_WAY = "anova_one_way"
    ANOVA_ONE_WAY_WELCH = "anova_one_way_welch"
    ANOVA_TWO_WAY = "anova_two_way"


@dataclass(frozen=True, slots=True, init=False)
class ParametricComparison:
    """Declare a comparison unit for parametric statistical tests.

    For paired and two-sample t-tests, ``comparison_id=None`` declares an
    unscoped contrast. If ``col_comparison`` is provided to the calculation
    function, an unscoped contrast is evaluated independently inside every
    comparison layer. A non-null ``comparison_id`` scopes that contrast to the
    matching ``col_comparison`` value after string normalization.
    """

    comparison_id: str | None
    kind: ParametricComparisonKind
    group_test: str | None
    group_ref: str | None
    groups: tuple[str, ...] | None
    groups_a: tuple[str, ...] | None
    groups_b: tuple[str, ...] | None

    def __init__(
        self,
        comparison_id: str | None,
        kind: ParametricComparisonKind,
        *,
        group_test: str | None = None,
        group_ref: str | None = None,
        groups: Sequence[str] | None = None,
        groups_a: Sequence[str] | None = None,
        groups_b: Sequence[str] | None = None,
    ) -> None:
        comparison_id_normalized = (
            None if comparison_id is None else str(comparison_id).strip()
        )
        if comparison_id_normalized == "":
            raise ValueError("Arg `comparison_id` must be non-empty when provided.")

        object.__setattr__(self, "comparison_id", comparison_id_normalized)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(
            self,
            "group_test",
            None if group_test is None else str(group_test),
        )
        object.__setattr__(
            self,
            "group_ref",
            None if group_ref is None else str(group_ref),
        )
        object.__setattr__(
            self,
            "groups",
            None if groups is None else self._normalize_groups(groups),
        )
        object.__setattr__(
            self,
            "groups_a",
            None if groups_a is None else self._normalize_groups(groups_a),
        )
        object.__setattr__(
            self,
            "groups_b",
            None if groups_b is None else self._normalize_groups(groups_b),
        )
        self._validate_state()

    @classmethod
    def ttest_one_sample(cls, comparison_id: str) -> "ParametricComparison":
        return cls(comparison_id, ParametricComparisonKind.TTEST_ONE_SAMPLE)

    @classmethod
    def ttest_two_sample(
        cls,
        *,
        group_test: str,
        group_ref: str,
        comparison_id: str | None = None,
    ) -> "ParametricComparison":
        """Declare an independent two-sample t-test contrast.

        Args:
            group_test: Group label on the test side of the contrast.
            group_ref: Group label on the reference side of the contrast.
            comparison_id: Optional label used to bind this contrast to one
                ``col_comparison`` value. Leave it as ``None`` when
                ``col_comparison`` is a batch/layer column and the same contrast
                should be evaluated in every layer.
        """
        return cls(
            comparison_id,
            ParametricComparisonKind.TTEST_TWO_SAMPLE,
            group_test=group_test,
            group_ref=group_ref,
        )

    @classmethod
    def ttest_paired(
        cls,
        *,
        group_test: str,
        group_ref: str,
        comparison_id: str | None = None,
    ) -> "ParametricComparison":
        """Declare a paired t-test contrast.

        Args:
            group_test: Group label on the test side of the paired contrast.
            group_ref: Group label on the reference side of the paired contrast.
            comparison_id: Optional label used to bind this contrast to one
                ``col_comparison`` value. Leave it as ``None`` when
                ``col_comparison`` is a batch/layer column and the same contrast
                should be evaluated in every layer.
        """
        return cls(
            comparison_id,
            ParametricComparisonKind.TTEST_PAIRED,
            group_test=group_test,
            group_ref=group_ref,
        )

    @classmethod
    def anova_one_way(
        cls,
        comparison_id: str,
        *,
        groups: Sequence[str] | None = None,
    ) -> "ParametricComparison":
        return cls(
            comparison_id,
            ParametricComparisonKind.ANOVA_ONE_WAY,
            groups=groups,
        )

    @classmethod
    def anova_one_way_welch(
        cls,
        comparison_id: str,
        *,
        groups: Sequence[str] | None = None,
    ) -> "ParametricComparison":
        return cls(
            comparison_id,
            ParametricComparisonKind.ANOVA_ONE_WAY_WELCH,
            groups=groups,
        )

    @classmethod
    def anova_two_way(
        cls,
        comparison_id: str,
        *,
        groups_a: Sequence[str] | None = None,
        groups_b: Sequence[str] | None = None,
    ) -> "ParametricComparison":
        return cls(
            comparison_id,
            ParametricComparisonKind.ANOVA_TWO_WAY,
            groups_a=groups_a,
            groups_b=groups_b,
        )

    @staticmethod
    def _normalize_groups(groups: Sequence[str]) -> tuple[str, ...]:
        groups_seen: set[str] = set()
        groups_normalized: list[str] = []
        for group in groups:
            group_normalized = str(group)
            if group_normalized in groups_seen:
                continue
            groups_seen.add(group_normalized)
            groups_normalized.append(group_normalized)
        return tuple(groups_normalized)

    def _validate_state(self) -> None:
        if self.kind in {
            ParametricComparisonKind.TTEST_TWO_SAMPLE,
            ParametricComparisonKind.TTEST_PAIRED,
        }:
            if self.group_test is None or self.group_ref is None:
                raise ValueError(
                    "Args `group_test` and `group_ref` are required for t-test comparisons."
                )
            if self.group_test == self.group_ref:
                raise ValueError(
                    "Arg `group_test` must be different from `group_ref`, yours: "
                    f"{self.group_test!r}."
                )
            if self.groups is not None:
                raise ValueError("Arg `groups` is not valid for t-test comparisons.")
            if self.groups_a is not None or self.groups_b is not None:
                raise ValueError(
                    "Args `groups_a` and `groups_b` are not valid for t-test comparisons."
                )
            return

        if self.comparison_id is None:
            raise ValueError("Arg `comparison_id` is required for this comparison.")
        if self.group_test is not None or self.group_ref is not None:
            raise ValueError(
                "Args `group_test` and `group_ref` are only valid for t-test comparisons."
            )
        if self.kind in {
            ParametricComparisonKind.TTEST_ONE_SAMPLE,
            ParametricComparisonKind.ANOVA_ONE_WAY,
            ParametricComparisonKind.ANOVA_ONE_WAY_WELCH,
        }:
            if self.groups is not None and len(self.groups) < 2:
                raise ValueError(
                    "Arg `groups` must contain at least two unique groups."
                )
            if self.groups_a is not None or self.groups_b is not None:
                raise ValueError(
                    "Args `groups_a` and `groups_b` are only valid for two-way ANOVA comparisons."
                )
            return

        if self.kind == ParametricComparisonKind.ANOVA_TWO_WAY:
            if self.groups is not None:
                raise ValueError(
                    "Arg `groups` is not valid for two-way ANOVA comparisons."
                )
            if self.groups_a is not None and len(self.groups_a) < 2:
                raise ValueError(
                    "Arg `groups_a` must contain at least two unique groups."
                )
            if self.groups_b is not None and len(self.groups_b) < 2:
                raise ValueError(
                    "Arg `groups_b` must contain at least two unique groups."
                )
            return

        if (
            self.groups is not None
            or self.groups_a is not None
            or self.groups_b is not None
        ):
            raise ValueError("Group filters are only valid for ANOVA comparisons.")
