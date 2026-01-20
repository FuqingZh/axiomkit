import math
from typing import Any


def convert_nan_inf_to_str(x: float) -> str:
    if math.isnan(x):
        return "NaN"
    elif math.isinf(x):
        return "Inf" if x > 0 else "-Inf"
    else:
        raise ValueError("Input is neither NaN nor Inf.")


def convert_cell_value(value: Any, if_is_numeric_col: bool, if_keep_na: bool) -> object:
    if value is None:
        return None
    if not if_is_numeric_col:
        return str(value)
    if not math.isfinite(n_cell_float_value := float(value)):
        return convert_nan_inf_to_str(n_cell_float_value) if if_keep_na else None

    return n_cell_float_value
