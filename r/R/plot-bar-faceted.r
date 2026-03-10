#' Plot Faceted Bar Chart
#'
#' Thin wrapper around [plot_faceted()] that defaults to a column geometry.
#'
#' @param df A tabular object coercible to `data.table`.
#' @param col_x Column name used for x-axis values.
#' @param col_y Column name used for y-axis values.
#' @param col_facet Optional column name used for facet partitioning.
#' @param col_fill Optional column name mapped to the fill aesthetic.
#' @param col_group Optional column name mapped to the group aesthetic.
#' @param geom_layer A ggplot layer object. Defaults to `ggplot2::geom_col()`.
#' @param rule_facet_mode Facet mode: `"none"`, `"wrap"`, or `"grid"`.
#' @param rule_facet_direction Facet layout direction for grid mode: `"cols"` or `"rows"`.
#' @param rule_facet_scales Facet scale rule.
#' @param rule_facet_space Facet panel space rule.
#' @param rule_y_order Y ordering rule: `"none"`, `"global"`, or `"within_facet"`.
#' @param is_y_order_descending If `TRUE`, order y values by descending `x`.
#' @param is_facet_strip_use_palette If `TRUE`, style facet strips using `colors_facet_strip`.
#' @param colors_facet_strip Optional character vector of strip fill colors.
#'
#' @return A `ggplot` object.
#' @export
plot_bar_faceted <- function(
    df,
    col_x = "X",
    col_y = "Y",
    col_facet = NULL,
    col_fill = NULL,
    col_group = NULL,
    geom_layer = ggplot2::geom_col(),
    rule_facet_mode = if (is.null(col_facet)) "none" else "wrap",
    rule_facet_direction = "cols",
    rule_facet_scales = "fixed",
    rule_facet_space = "fixed",
    rule_y_order = "none",
    is_y_order_descending = FALSE,
    is_facet_strip_use_palette = FALSE,
    colors_facet_strip = NULL
) {
    plot_faceted(
        df = df,
        col_x = col_x,
        col_y = col_y,
        col_facet = col_facet,
        col_fill = col_fill,
        col_group = col_group,
        geom_layer = geom_layer,
        rule_facet_mode = rule_facet_mode,
        rule_facet_direction = rule_facet_direction,
        rule_facet_scales = rule_facet_scales,
        rule_facet_space = rule_facet_space,
        rule_y_order = rule_y_order,
        is_y_order_descending = is_y_order_descending,
        is_facet_strip_use_palette = is_facet_strip_use_palette,
        colors_facet_strip = colors_facet_strip
    )
}
