plot_bar_faceted <- function(
  df,
  col_x = "X",
  col_y = "Y",
  col_y_name = "YName"
) {
    dt <- data.table::as.data.table(df)
    c_cols_dt <- colnames(dt)
    c_cols_args <- c(col_x, col_y, col_y_name)
    c_cols_inner <- intersect(c_cols_dt, c_cols_args)
    if (length(c_cols_inner) == 0) {
        cli::cli_abort("None of the specified columns found in ``df``.")
    }
    if (!all(c(col_x, col_y) %in% c_cols_inner)) {
        cli::cli_abort("All of ``col_x`` or ``col_y`` must be found in ``df``.")
    }
    
}
