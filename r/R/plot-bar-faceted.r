#' Plot Faceted Bar Chart
#'
#' @param df A tabular object.
#' @param col_x Column name used for x-axis.
#' @param col_y Column name used for y-axis.
#' @param col_y_name Column name used for facet label.
#'
#' @return Currently returns `NULL` (placeholder implementation).
#' @importFrom data.table as.data.table
#' @importFrom cli cli_abort
#' @export
plot_bar_faceted <- function(
    df,
    col_x = "X",
    col_y = "Y",
    col_y_name = "YName"
) {
    dt <- as.data.table(df)
    c_cols_dt <- colnames(dt)
    c_cols_args <- c(col_x, col_y, col_y_name)
    c_cols_inner <- intersect(c_cols_dt, c_cols_args)
    if (length(c_cols_inner) == 0) {
        cli_abort("None of the specified columns found in ``df``.")
    }
    if (!all(c(col_x, col_y) %in% c_cols_inner)) {
        cli_abort("All of ``col_x`` or ``col_y`` must be found in ``df``.")
    }
}
