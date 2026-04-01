from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True, slots=True)
class OneWayStatisticalResult:
    degrees_freedom_between: np.ndarray
    degrees_freedom_within: np.ndarray
    f_statistic: np.ndarray
