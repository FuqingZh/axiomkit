#' Quantification Matrix Class
#'
#' `QuantMatrix` stores a numeric double matrix.
#' Axis labels should be carried by `dimnames(mat)`.
#'
#' @param mat A numeric-like matrix (or matrix-coercible object).
#' @importFrom S7 class_any method method<- new_class new_generic new_object
#' @importFrom matrixStats colMedians rowMedians
#' @export
QuantMatrix <- new_class(
    "QuantMatrix",
    properties = list(
        mat = class_any
    ),
    constructor = function(mat) {
        if (!is.matrix(mat)) {
            mat <- as.matrix(mat)
        }
        storage.mode(mat) <- "double"

        new_object(
            QuantMatrix,
            mat = mat
        )
    },
    validator = function(self) {
        if (!is.matrix(self@mat)) {
            return("@mat must be a matrix.")
        }
        if (!is.numeric(self@mat)) {
            return("@mat must be numeric.")
        }
        if (!is.double(self@mat)) {
            return("@mat must be a double matrix.")
        }

        NULL
    }
)

#' Median-Center a Quantification Matrix
#'
#' Center by axis medians, with optional baseline re-alignment.
#'
#' @param x A `QuantMatrix` object.
#' @param rule_axis Axis for centering: `"col"` or `"row"`.
#'   - `"col"`: center each column by its median.
#'   - `"row"`: center each row by its median.
#' @param rule_baseline Baseline to add after centering.
#'   - `"global_median"`: add back the global median of axis medians.
#'   - `"zero"`: add back `0` (pure centering).
#'
#' @details
#' For each element `X[i, j]`, the transform is:
#' `X'[i, j] = X[i, j] - m_axis + b`
#' where `m_axis` is the row/column median selected by `rule_axis`,
#' and `b` is determined by `rule_baseline`.
#'
#' @return A new `QuantMatrix` object after median centering.
#' @export
center_median <- new_generic(
    "center_median",
    dispatch_args = "x",
    fun = function(
        x,
        rule_axis = c("col", "row"),
        rule_baseline = c("global_median", "zero")
    ) {
        S7::S7_dispatch()
    }
)

method(center_median, QuantMatrix) <- function(
    x,
    rule_axis = c("col", "row"),
    rule_baseline = c("global_median", "zero")
) {
    c_axis <- match.arg(rule_axis)
    c_baseline <- match.arg(rule_baseline)

    n_axis_medians <- switch(c_axis,
        "col" = colMedians(x@mat, na.rm = TRUE),
        "row" = rowMedians(x@mat, na.rm = TRUE)
    )
    n_baseline <- switch(c_baseline,
        "global_median" = median(n_axis_medians, na.rm = TRUE),
        "zero" = 0
    )

    mat_centered <- sweep(
        x@mat,
        if (c_axis == "col") 2 else 1,
        n_axis_medians,
        "-"
    ) + n_baseline

    QuantMatrix(mat = mat_centered)
}
