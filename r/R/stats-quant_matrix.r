# R/stats-quantify.r

.validate_axis_label <- function(label, len_expected, arg_name) {
    if (is.null(label)) {
        return(NULL)
    }

    n_len <- tryCatch(
        length(label),
        error = function(e) NA_integer_
    )
    if (!is.finite(n_len)) {
        cli_abort(sprintf("Arg `%s` must have a measurable length.", arg_name))
    }
    if (n_len != len_expected) {
        cli_abort(sprintf(
            "Arg `%s` length (%d) must equal %d.",
            arg_name,
            n_len,
            len_expected
        ))
    }

    label
}

#' Quantification Matrix Class
#'
#' `QuantMatrix` stores the numeric matrix and optional per-axis labels.
#'
#' @importFrom S7 class_any method method<- new_class new_generic new_object
#' @importFrom matrixStats colMedians rowMedians
#' @export
QuantMatrix <- new_class(
    "QuantMatrix",
    properties = list(
        mat = class_any,
        col_label = class_any,
        row_label = class_any
    ),
    constructor = function(mat, col_label = NULL, row_label = NULL) {
        if (!is.matrix(mat)) {
            mat <- as.matrix(mat)
        }
        storage.mode(mat) <- "double"

        col_label <- .validate_axis_label(
            label = col_label,
            len_expected = ncol(mat),
            arg_name = "col_label"
        )
        row_label <- .validate_axis_label(
            label = row_label,
            len_expected = nrow(mat),
            arg_name = "row_label"
        )

        new_object(
            QuantMatrix,
            mat = mat,
            col_label = col_label,
            row_label = row_label
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

        n_col <- ncol(self@mat)
        if (!is.null(self@col_label) && length(self@col_label) != n_col) {
            return("@col_label length must equal ncol(mat), or be NULL.")
        }

        n_row <- nrow(self@mat)
        if (!is.null(self@row_label) && length(self@row_label) != n_row) {
            return("@row_label length must equal nrow(mat), or be NULL.")
        }

        NULL
    }
)

#' Create a Quantification Matrix Object
#'
#' @param mat A numeric-like matrix (or matrix-coercible object).
#' @param col_label Optional vector/list of column labels with length `ncol(mat)`.
#' @param row_label Optional vector/list of row labels with length `nrow(mat)`.
#'
#' @return A `QuantMatrix` object.
#' @export
create_quant_matrix <- function(mat, col_label = NULL, row_label = NULL) {
    QuantMatrix(
        mat = mat,
        col_label = col_label,
        row_label = row_label
    )
}

#' Median-Center a Quantification Matrix
#'
#' Center by axis medians, with optional global-median alignment.
#'
#' @param x A `QuantMatrix` object.
#' @param rule_axis Axis for centering: `"col"` or `"row"`.
#'   - `"col"`: center each column by its median.
#'   - `"row"`: center each row by its median.
#' @param rule_align Whether to add back the global median of axis medians.
#'   - `"global_median"`: add back the global median of axis medians.
#'   - `"none"`: do not add back anything.
#'
#' @return A new `QuantMatrix` object after median centering.
#' @export
normalize_axis_median_center <- new_generic("normalize_axis_median_center", "x")

method(normalize_axis_median_center, QuantMatrix) <- function(
  x,
  rule_axis = c("col", "row"),
  rule_align = c("global_median", "none")
) {
    c_axis <- match.arg(rule_axis)
    c_align <- match.arg(rule_align)

    n_axis_medians <- switch(c_axis,
        "col" = colMedians(x@mat, na.rm = TRUE),
        "row" = rowMedians(x@mat, na.rm = TRUE)
    )
    n_global_median <- switch(c_align,
        "global_median" = median(n_axis_medians, na.rm = TRUE),
        "none" = 0
    )
    mat_norm <- sweep(
        x@mat,
        if (c_axis == "col") 2 else 1,
        n_axis_medians,
        "-"
    ) + n_global_median

    QuantMatrix(
        mat = mat_norm,
        col_label = x@col_label,
        row_label = x@row_label
    )
}
