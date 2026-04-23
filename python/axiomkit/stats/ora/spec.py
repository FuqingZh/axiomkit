from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Self, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from axiomkit.stats.p_value import (
    PValueAdjustmentMode,
    PValueAdjustmentType,
    normalize_p_value_adjustment_mode,
)

from .constant import FIELDS_RESOLVED_ORA_OPTIONS


class _MissingType:
    __slots__ = ()


_OPTION_UNSET = _MissingType()


class OraOptions(BaseModel):
    """
    Query-level defaults and filtering policy for ORA.

    `OraOptions` is the main configuration bundle accepted by
    :func:`calculate_ora`. The same type is also reused as
    :attr:`OraComparison.option_override`, where only explicitly provided fields
    override the query-level defaults.

    Most users only need to set this once per ORA call. Reach for
    `OraComparison.option_override` only when one comparison needs to behave
    differently from the shared defaults.

    Attributes:
        background_elements:
            Shared background universe.
            - If `None`, the universe is inferred from the annotation mapping.
            - If provided, the universe is restricted to these elements.
            - `BgTotal` is `len(background_elements)` after resolution.
            - Provided elements may include ids that do not appear in the
              annotation mapping; those elements contribute to `BgTotal` but
              not to `BgHits`.
        rule_p_adjust:
            Method for p-value adjustment.
            - `"bh"`: (Default) Benjamini-Hochberg FDR
            - `"by"`: Benjamini-Yekutieli FDR
            - `"bonferroni"`: Bonferroni correction
            - `None`: no adjustment, so `PAdjust == PValue`
        thr_bg_hits_min:
            Minimum number of background hits required for a term.
            - Domain: `[0, ∞)`
            - Terms with `BgHits < thr_bg_hits_min` are discarded before
              significance filtering.
        thr_bg_hits_max:
            Maximum number of background hits allowed for a term.
            - Domain: `[thr_bg_hits_min, ∞) | None`
            - If `None`, no upper bound is applied.
            - Terms with `BgHits > thr_bg_hits_max` are discarded before
              significance filtering.
        thr_fg_hits_min:
            Minimum number of foreground hits required for a term.
            - Domain: `[0, ∞)`
            - Terms with `FgHits < thr_fg_hits_min` are discarded before
              significance filtering.
        thr_fg_hits_max:
            Maximum number of foreground hits allowed for a term.
            - Domain: `[thr_fg_hits_min, ∞) | None`
            - If `None`, no upper bound is applied.
            - Terms with `FgHits > thr_fg_hits_max` are discarded before
              significance filtering.
        thr_p_value:
            Raw p-value threshold for significance.
            - Domain: `[0.0, 1.0]`
            - Terms with `PValue > thr_p_value` are removed from the final
              result.
        thr_p_adjust:
            Adjusted p-value threshold for significance.
            - Domain: `[0.0, 1.0]`
            - Terms with `PAdjust > thr_p_adjust` are removed from the final
              result.
        should_keep_fg_members:
            Whether to retain foreground member lists in the result.
            - If `True`, output includes `FgMembers`.
            - If `False`, `FgMembers` is omitted from the returned table.
        should_keep_bg_members:
            Whether to retain background member lists in the result.
            - If `True`, output includes `BgMembers`.
            - If `False`, `BgMembers` is omitted from the returned table.

    Notes:
        Background handling follows this precedence:
        - `OraComparison.option_override.background_elements`, if explicitly
          provided for one comparison
        - `OraOptions.background_elements`, if provided at the query level
        - Otherwise, infer the universe from the annotation mapping

        Practical usage:
        - Set `background_elements` here when most or all comparisons should
          share the same universe.
        - Set `option_override=OraOptions(background_elements=...)` on one
          comparison when only that comparison should use a different universe.
        - Use `background_elements=None` as a real value when you want
          inference; it does not mean "leave unchanged" in override semantics.

    Examples:
        Use one shared background for all comparisons:

        >>> OraOptions(
        ...     background_elements={"g1", "g2", "g3", "g4"},
        ...     thr_p_value=0.05,
        ... )

        Start from one options object and change only one field:

        >>> OraOptions(thr_p_value=0.05).with_(thr_p_value=1.0)

    Raises:
        ValueError:
            If `thr_bg_hits_max` is smaller than `thr_bg_hits_min`, or if
            `thr_fg_hits_max` is smaller than `thr_fg_hits_min`.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    background_elements: set[str] | None = None
    rule_p_adjust: PValueAdjustmentType | str | None = "bh"
    thr_bg_hits_min: int = Field(default=0, ge=0)
    thr_bg_hits_max: int | None = None
    thr_fg_hits_min: int = Field(default=0, ge=0)
    thr_fg_hits_max: int | None = None
    thr_p_value: float = Field(default=0.05, ge=0.0, le=1.0)
    thr_p_adjust: float = Field(default=1.0, ge=0.0, le=1.0)
    should_keep_fg_members: bool = True
    should_keep_bg_members: bool = False

    @field_validator("rule_p_adjust", mode="after")
    @classmethod
    def _normalize_rule_p_adjust(
        cls,
        value: PValueAdjustmentType | str | None,
    ) -> PValueAdjustmentMode | None:
        return normalize_p_value_adjustment_mode(value)

    @model_validator(mode="after")
    def _validate_ranges(self) -> Self:
        if (
            self.thr_bg_hits_max is not None
            and self.thr_bg_hits_max < self.thr_bg_hits_min
        ):
            raise ValueError(
                "Arg `thr_bg_hits_max` must be in [`thr_bg_hits_min`, ∞) or None."
            )
        if (
            self.thr_fg_hits_max is not None
            and self.thr_fg_hits_max < self.thr_fg_hits_min
        ):
            raise ValueError(
                "Arg `thr_fg_hits_max` must be in [`thr_fg_hits_min`, ∞) or None."
            )
        return self

    def with_(
        self,
        *,
        background_elements: set[str] | None = cast(Any, _OPTION_UNSET),
        rule_p_adjust: PValueAdjustmentType | str | None = cast(Any, _OPTION_UNSET),
        thr_bg_hits_min: int = cast(Any, _OPTION_UNSET),
        thr_bg_hits_max: int | None = cast(Any, _OPTION_UNSET),
        thr_fg_hits_min: int = cast(Any, _OPTION_UNSET),
        thr_fg_hits_max: int | None = cast(Any, _OPTION_UNSET),
        thr_p_value: float = cast(Any, _OPTION_UNSET),
        thr_p_adjust: float = cast(Any, _OPTION_UNSET),
        should_keep_fg_members: bool = cast(Any, _OPTION_UNSET),
        should_keep_bg_members: bool = cast(Any, _OPTION_UNSET),
    ) -> Self:
        data = self.model_dump(mode="python")
        if background_elements is not _OPTION_UNSET:
            data["background_elements"] = background_elements
        if rule_p_adjust is not _OPTION_UNSET:
            data["rule_p_adjust"] = rule_p_adjust
        if thr_bg_hits_min is not _OPTION_UNSET:
            data["thr_bg_hits_min"] = thr_bg_hits_min
        if thr_bg_hits_max is not _OPTION_UNSET:
            data["thr_bg_hits_max"] = thr_bg_hits_max
        if thr_fg_hits_min is not _OPTION_UNSET:
            data["thr_fg_hits_min"] = thr_fg_hits_min
        if thr_fg_hits_max is not _OPTION_UNSET:
            data["thr_fg_hits_max"] = thr_fg_hits_max
        if thr_p_value is not _OPTION_UNSET:
            data["thr_p_value"] = thr_p_value
        if thr_p_adjust is not _OPTION_UNSET:
            data["thr_p_adjust"] = thr_p_adjust
        if should_keep_fg_members is not _OPTION_UNSET:
            data["should_keep_fg_members"] = should_keep_fg_members
        if should_keep_bg_members is not _OPTION_UNSET:
            data["should_keep_bg_members"] = should_keep_bg_members
        return self.__class__.model_validate(data)


class OraComparison(BaseModel):
    """
    One logical ORA comparison unit.

    `OraComparison` answers "what is the foreground set for this comparison?"
    rather than "how should ORA be run overall?". Put shared thresholds,
    p-adjust settings, and shared background defaults in :class:`OraOptions`.
    Use `option_override` only for comparison-specific exceptions.

    Attributes:
        comparison_id:
            Optional comparison identifier.
            - If `None` and this is the only comparison, the result omits
              `ComparisonId`.
            - If a string is provided for a single comparison, the result keeps
              `ComparisonId`.
            - If multiple comparisons are provided, every item must provide a
              unique non-empty identifier.
        foreground_elements:
            Foreground element set for this comparison.
            - This is the comparison-specific foreground before background
              resolution.
            - Effective foreground counts are computed against the resolved
              background universe.
        option_override:
            Optional comparison-level override of :class:`OraOptions`.
            - If `None`, the comparison uses the query-level defaults.
            - If provided, only explicitly set fields override the query-level
              defaults.

    Notes:
        Effective foreground counting:
        - The counted foreground is `foreground_elements ∩ background_elements`.
        - Background resolution uses:
          - `option_override.background_elements`, if explicitly set
          - Otherwise the query-level `OraOptions.background_elements`
          - Otherwise the inferred universe from the annotation mapping

    Examples:
        Minimal single-comparison input:

        >>> OraComparison(foreground_elements={"g1", "g2"})

        One comparison with a local override:

        >>> OraComparison(
        ...     comparison_id="cmp_b",
        ...     foreground_elements={"g1", "g2"},
        ...     option_override=OraOptions(
        ...         background_elements={"g1", "g2", "g3"},
        ...         should_keep_bg_members=True,
        ...     ),
        ... )

    Raises:
        ValueError:
            If `comparison_id` is provided but is empty after trimming
            whitespace.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    comparison_id: str | None = None
    foreground_elements: set[str]
    option_override: OraOptions | None = None

    @field_validator("comparison_id", mode="before")
    @classmethod
    def _normalize_comparison_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = str(value).strip()
        if not value:
            raise ValueError("`comparison_id` must be non-empty when provided.")
        return value


@dataclass(frozen=True, slots=True)
class ResolvedOraOptions:
    rule_p_adjust: PValueAdjustmentType | None
    thr_bg_hits_min: int
    thr_bg_hits_max: int | None
    thr_fg_hits_min: int
    thr_fg_hits_max: int | None
    thr_p_value: float
    thr_p_adjust: float
    should_keep_fg_members: bool
    should_keep_bg_members: bool

    @classmethod
    def from_options(cls, base: OraOptions, override: OraOptions | None) -> Self:
        if override is None:
            data = {_key: getattr(base, _key) for _key in FIELDS_RESOLVED_ORA_OPTIONS}
        else:
            data = {
                _key: (
                    getattr(override, _key)
                    if _key in override.model_fields_set
                    else getattr(base, _key)
                )
                for _key in FIELDS_RESOLVED_ORA_OPTIONS
            }

        return cls(**data)


@dataclass(frozen=True, slots=True)
class ResolvedOraComparison:
    comparison_id: str
    foreground_elements: frozenset[str]
    background_elements: frozenset[str] | None
    options: ResolvedOraOptions
