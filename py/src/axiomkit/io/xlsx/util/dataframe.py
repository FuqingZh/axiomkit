import polars as pl
from collections.abc import Sequence
from collections import defaultdict
from typing import Any

from ..conf import ColRef


def to_polars(df: Any) -> pl.DataFrame:
    return df if isinstance(df, pl.DataFrame) else pl.DataFrame(df)


def get_sorted_indices_from_refs(
    df: pl.DataFrame, refs: Sequence[ColRef] | None
) -> tuple[int, ...]:
    if not refs:
        return ()
    idx = {_resolve_col_index(df, _r) for _r in refs}
    return tuple(sorted(idx))


def assert_no_duplicate_columns(df: pl.DataFrame) -> None:
    l_cols = df.columns

    # fast path: no duplicates
    if len(l_cols) == len(set(l_cols)):
        return

    # slow path: collect details only when duplicates exist
    dict_pos: dict[str, list[int]] = defaultdict(list)
    for _idx, _val in enumerate(l_cols):
        dict_pos[_val].append(_idx)

    c_msg = "; ".join(
        f"{c_name!r} x{len(l_pos)} at indices {l_pos}"
        for c_name, l_pos in dict_pos.items()
        if len(l_pos) > 1
    )
    raise ValueError(f"Duplicate column names detected: {c_msg}")


def _resolve_col_index(df: pl.DataFrame, ref: ColRef) -> int:
    if isinstance(ref, int):
        return ref
    try:
        return df.columns.index(ref)
    except ValueError as e:
        raise KeyError(f"Column not found: {ref!r}") from e
