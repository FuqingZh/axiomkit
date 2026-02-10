# Coerce matrix-like input to a double matrix without lossy conversion
# from non-missing values.
.coerce_double_matrix <- function(mat) {
    if (!is.matrix(mat)) {
        mat <- as.matrix(mat)
    }

    c_values <- as.vector(mat)
    n_values <- suppressWarnings(as.double(c_values))
    b_bad_values <- !is.na(c_values) & is.na(n_values)
    if (any(b_bad_values)) {
        stop(
            sprintf(
                paste0(
                    "Arg `mat` must be numeric-like; ",
                    "%d value(s) cannot be converted to double."
                ),
                sum(b_bad_values)
            ),
            call. = FALSE
        )
    }

    matrix(
        n_values,
        nrow = nrow(mat),
        ncol = ncol(mat),
        dimnames = dimnames(mat)
    )
}

#' Quantification Matrix Class
#'
#' `QuantMatrix` stores a numeric double matrix.
#' Axis labels should be carried by `dimnames(mat)`.
#'
#' @param mat A matrix-like object coercible to a double matrix.
#'   Non-missing input values must be convertible to `double`.
#' @importFrom S7 class_double method method<- new_class
#'   new_generic new_object new_property
#' @importFrom matrixStats colMedians rowMedians
#' @export
QuantMatrix <- new_class(
    "QuantMatrix",
    properties = list(
        mat = new_property(
            class = class_double,
            validator = function(value) {
                if (!is.matrix(value)) {
                    return("@mat must be a matrix.")
                }

                NULL
            }
        )
    ),
    constructor = function(mat) {
        mat <- .coerce_double_matrix(mat)

        new_object(
            QuantMatrix,
            mat = mat
        )
    },
    validator = function(self) {
        if (!is.matrix(self@mat)) {
            return("@mat must be a matrix.")
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
