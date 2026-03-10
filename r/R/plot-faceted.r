# Normalize a faceted plotting column spec to a single string or `NULL`.
.normalize_plot_faceted_col <- function(
    col_name,
    arg_name,
    is_required = FALSE
) {
    if (is.null(col_name)) {
        if (is_required) {
            cli::cli_abort("Arg `{arg_name}` must not be `NULL`.")
        }
        return(NULL)
    }

    if (!is.character(col_name) || length(col_name) != 1L || is.na(col_name)) {
        cli::cli_abort("Arg `{arg_name}` must be a single non-missing string.")
    }

    col_name <- trimws(col_name)
    if (!nzchar(col_name)) {
        cli::cli_abort("Arg `{arg_name}` must not be an empty string.")
    }

    col_name
}

# Validate that selected columns exist in the input table.
.validate_plot_faceted_cols <- function(
    dt,
    col_x,
    col_y,
    col_facet = NULL,
    col_fill = NULL,
    col_group = NULL
) {
    cols_required <- c(
        col_x,
        col_y,
        col_facet,
        col_fill,
        col_group
    )
    cols_required <- unique(stats::na.omit(cols_required))
    cols_missing <- setdiff(cols_required, colnames(dt))
    if (length(cols_missing) > 0) {
        cli::cli_abort(c(
            "Input `df` is missing required columns for `plot_faceted()`.",
            "x" = paste(cols_missing, collapse = ", ")
        ))
    }
}

# Resolve supported runtime rules from explicit arguments.
.resolve_plot_faceted_options <- function(
    rule_facet_mode,
    rule_facet_direction,
    rule_facet_scales,
    rule_facet_space,
    rule_y_order,
    is_y_order_descending,
    is_facet_strip_use_palette,
    colors_facet_strip,
    facet_order_separator
) {
    facet_mode <- match.arg(
        rule_facet_mode,
        c("none", "wrap", "grid")
    )
    facet_direction <- match.arg(
        rule_facet_direction,
        c("cols", "rows")
    )
    facet_scales <- match.arg(
        rule_facet_scales,
        c("fixed", "free_x", "free_y", "free")
    )
    facet_space <- match.arg(
        rule_facet_space,
        c("fixed", "free_x", "free_y", "free")
    )
    y_order_rule <- match.arg(
        rule_y_order,
        c("none", "global", "within_facet")
    )
    is_y_order_descending <- isTRUE(is_y_order_descending)
    is_facet_strip_use_palette <- isTRUE(is_facet_strip_use_palette)

    if (
        !is.character(facet_order_separator) ||
            length(facet_order_separator) != 1L ||
            is.na(facet_order_separator) ||
            !nzchar(facet_order_separator)
    ) {
        cli::cli_abort(
            "Arg `facet_order_separator` must be a single non-empty string."
        )
    }

    if (
        !is.null(colors_facet_strip) &&
            (!is.character(colors_facet_strip) ||
                length(colors_facet_strip) == 0L)
    ) {
        cli::cli_abort(
            "Arg `colors_facet_strip` must be `NULL` or a non-empty character vector."
        )
    }

    list(
        rule_facet_mode = facet_mode,
        rule_facet_direction = facet_direction,
        rule_facet_scales = facet_scales,
        rule_facet_space = facet_space,
        rule_y_order = y_order_rule,
        is_y_order_descending = is_y_order_descending,
        is_facet_strip_use_palette = is_facet_strip_use_palette,
        colors_facet_strip = colors_facet_strip,
        facet_order_separator = facet_order_separator
    )
}

# Derive plotting-ready data with a stable internal schema.
.derive_faceted_plot_data <- function(
    df,
    col_x,
    col_y,
    col_facet = NULL,
    col_fill = NULL,
    col_group = NULL,
    rule_facet_mode,
    rule_facet_direction,
    rule_facet_scales,
    rule_facet_space,
    rule_y_order,
    is_y_order_descending,
    is_facet_strip_use_palette,
    colors_facet_strip,
    facet_order_separator
) {
    col_x <- .normalize_plot_faceted_col(col_x, "col_x", is_required = TRUE)
    col_y <- .normalize_plot_faceted_col(col_y, "col_y", is_required = TRUE)
    col_facet <- .normalize_plot_faceted_col(col_facet, "col_facet")
    col_fill <- .normalize_plot_faceted_col(col_fill, "col_fill")
    col_group <- .normalize_plot_faceted_col(col_group, "col_group")

    dt <- data.table::as.data.table(df)
    .validate_plot_faceted_cols(
        dt = dt,
        col_x = col_x,
        col_y = col_y,
        col_facet = col_facet,
        col_fill = col_fill,
        col_group = col_group
    )
    facet_options <- .resolve_plot_faceted_options(
        rule_facet_mode = rule_facet_mode,
        rule_facet_direction = rule_facet_direction,
        rule_facet_scales = rule_facet_scales,
        rule_facet_space = rule_facet_space,
        rule_y_order = rule_y_order,
        is_y_order_descending = is_y_order_descending,
        is_facet_strip_use_palette = is_facet_strip_use_palette,
        colors_facet_strip = colors_facet_strip,
        facet_order_separator = facet_order_separator
    )

    dt_plot <- data.table::data.table(
        X = dt[[col_x]],
        Y = dt[[col_y]],
        YPlot = dt[[col_y]]
    )
    if (!is.null(col_facet)) {
        dt_plot[, Facet := dt[[col_facet]]]
    }
    if (!is.null(col_fill)) {
        dt_plot[, Fill := dt[[col_fill]]]
    }
    if (!is.null(col_group)) {
        dt_plot[, Group := dt[[col_group]]]
    }

    cols_trim <- intersect(c("Y", "YPlot", "Facet", "Fill", "Group"), colnames(dt_plot))
    if (length(cols_trim) > 0) {
        for (col_name in cols_trim) {
            if (is.factor(dt_plot[[col_name]]) || is.character(dt_plot[[col_name]])) {
                dt_plot[, (col_name) := trimws(as.character(get(col_name)))]
            }
        }
    }

    cols_drop_na <- c("X", "Y")
    if (!is.null(col_facet) && facet_options$rule_facet_mode != "none") {
        cols_drop_na <- c(cols_drop_na, "Facet")
    }
    dt_plot <- dt_plot[stats::complete.cases(dt_plot[, ..cols_drop_na])]

    if (nrow(dt_plot) == 0) {
        cli::cli_abort("No valid rows remain after faceted plot normalization.")
    }

    dt_plot[, Y := as.character(Y)]
    dt_plot[, YPlot := as.character(YPlot)]
    if ("Facet" %in% colnames(dt_plot)) {
        dt_plot[, Facet := as.character(Facet)]
    }

    if (facet_options$rule_y_order == "global") {
        dt_order <- unique(dt_plot[, .(Y, X)])
        data.table::setorderv(
            dt_order,
            cols = c("X", "Y"),
            order = c(if (facet_options$is_y_order_descending) -1L else 1L, 1L)
        )
        dt_plot[, YPlot := factor(YPlot, levels = unique(dt_order$Y))]
    }

    if (facet_options$rule_y_order == "within_facet") {
        if (!("Facet" %in% colnames(dt_plot))) {
            cli::cli_abort(
                "Arg `rule_y_order = \"within_facet\"` requires `col_facet`."
            )
        }
        dt_order <- unique(dt_plot[, .(Facet, Y, X)])
        data.table::setorderv(
            dt_order,
            cols = c("Facet", "X", "Y"),
            order = c(1L, if (facet_options$is_y_order_descending) -1L else 1L, 1L)
        )
        dt_order[
            ,
            YFacet := paste(Facet, Y, sep = facet_options$facet_order_separator)
        ]
        dt_plot[
            ,
            YPlot := paste(Facet, YPlot, sep = facet_options$facet_order_separator)
        ]
        dt_plot[
            ,
            YPlot := factor(YPlot, levels = unique(dt_order$YFacet))
        ]
    }

    list(
        dt_plot = dt_plot,
        opts = facet_options
    )
}

# Build a stable aesthetic mapping from the normalized internal schema.
.create_faceted_mapping <- function(
    dt_plot,
    col_y_plot = "YPlot"
) {
    if (!identical(col_y_plot, "YPlot")) {
        cli::cli_abort("Internal arg `col_y_plot` currently only supports `\"YPlot\"`.")
    }

    if ("Fill" %in% colnames(dt_plot) && "Group" %in% colnames(dt_plot)) {
        return(ggplot2::aes(x = X, y = YPlot, fill = Fill, group = Group))
    }
    if ("Fill" %in% colnames(dt_plot)) {
        return(ggplot2::aes(x = X, y = YPlot, fill = Fill))
    }
    if ("Group" %in% colnames(dt_plot)) {
        return(ggplot2::aes(x = X, y = YPlot, group = Group))
    }

    ggplot2::aes(x = X, y = YPlot)
}

# Create a facet specification object for the normalized internal schema.
.create_facet_spec <- function(
    dt_plot,
    facet_options
) {
    if (!("Facet" %in% colnames(dt_plot)) || facet_options$rule_facet_mode == "none") {
        return(NULL)
    }

    strip_spec <- NULL
    if (facet_options$is_facet_strip_use_palette) {
        if (is.null(facet_options$colors_facet_strip)) {
            cli::cli_abort(
                "Arg `colors_facet_strip` must be provided when `is_facet_strip_use_palette = TRUE`."
            )
        }
        facet_levels <- unique(as.character(dt_plot$Facet))
        strip_colors <- rep(
            facet_options$colors_facet_strip,
            length.out = length(facet_levels)
        )
        strip_rect <- lapply(
            strip_colors,
            function(fill_color) ggplot2::element_rect(fill = fill_color, colour = NA)
        )
        strip_text <- lapply(
            seq_along(facet_levels),
            function(...) ggplot2::element_text(colour = "#111111", face = "bold")
        )
        if (facet_options$rule_facet_direction == "cols") {
            strip_spec <- ggh4x::strip_themed(
                background_x = strip_rect,
                text_x = strip_text
            )
        } else {
            strip_spec <- ggh4x::strip_themed(
                background_y = strip_rect,
                text_y = strip_text
            )
        }
    }

    if (facet_options$rule_facet_mode == "wrap") {
        if (!is.null(strip_spec)) {
            return(
                ggh4x::facet_wrap2(
                    ggplot2::vars(Facet),
                    scales = facet_options$rule_facet_scales,
                    strip = strip_spec
                )
            )
        }
        return(
            ggplot2::facet_wrap(
                ggplot2::vars(Facet),
                scales = facet_options$rule_facet_scales
            )
        )
    }

    if (facet_options$rule_facet_direction == "cols") {
        return(
            ggh4x::facet_grid2(
                rows = ggplot2::vars(),
                cols = ggplot2::vars(Facet),
                scales = facet_options$rule_facet_scales,
                space = facet_options$rule_facet_space,
                strip = strip_spec
            )
        )
    }

    ggh4x::facet_grid2(
        rows = ggplot2::vars(Facet),
        cols = ggplot2::vars(),
        scales = facet_options$rule_facet_scales,
        space = facet_options$rule_facet_space,
        strip = strip_spec
    )
}

#' Plot Faceted Geometry
#'
#' Build a ggplot object from a standardized x/y mapping with optional fill,
#' group, and facet semantics. The plotting layer itself is caller-supplied
#' through `geom_layer`, while this helper focuses on input normalization,
#' optional y-ordering, and facet construction.
#'
#' @param df A tabular object coercible to `data.table`.
#' @param col_x Column name used for x-axis values.
#' @param col_y Column name used for y-axis values.
#' @param col_facet Optional column name used for facet partitioning.
#' @param col_fill Optional column name mapped to the fill aesthetic.
#' @param col_group Optional column name mapped to the group aesthetic.
#' @param geom_layer A ggplot layer object such as `ggplot2::geom_col()`.
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
plot_faceted <- function(
    df,
    col_x,
    col_y,
    col_facet = NULL,
    col_fill = NULL,
    col_group = NULL,
    geom_layer,
    rule_facet_mode = if (is.null(col_facet)) "none" else "wrap",
    rule_facet_direction = "cols",
    rule_facet_scales = "fixed",
    rule_facet_space = "fixed",
    rule_y_order = "none",
    is_y_order_descending = FALSE,
    is_facet_strip_use_palette = FALSE,
    colors_facet_strip = NULL
) {
    if (missing(geom_layer) || is.null(geom_layer)) {
        cli::cli_abort("Arg `geom_layer` must be provided.")
    }

    obj_plot_data <- .derive_faceted_plot_data(
        df = df,
        col_x = col_x,
        col_y = col_y,
        col_facet = col_facet,
        col_fill = col_fill,
        col_group = col_group,
        rule_facet_mode = rule_facet_mode,
        rule_facet_direction = rule_facet_direction,
        rule_facet_scales = rule_facet_scales,
        rule_facet_space = rule_facet_space,
        rule_y_order = rule_y_order,
        is_y_order_descending = is_y_order_descending,
        is_facet_strip_use_palette = is_facet_strip_use_palette,
        colors_facet_strip = colors_facet_strip,
        facet_order_separator = "___AK_FACET___"
    )
    dt_plot <- obj_plot_data$dt_plot
    facet_options <- obj_plot_data$opts
    mapping <- .create_faceted_mapping(dt_plot = dt_plot)
    facet_spec <- .create_facet_spec(dt_plot = dt_plot, facet_options = facet_options)

    plot_gg <- ggplot2::ggplot(dt_plot, mapping) +
        geom_layer

    if (!is.null(facet_spec)) {
        plot_gg <- plot_gg + facet_spec
    }
    if (facet_options$rule_y_order == "within_facet") {
        sep_pattern <- paste0("^.*", facet_options$facet_order_separator)
        plot_gg <- plot_gg +
            ggplot2::scale_y_discrete(
                labels = function(x) sub(sep_pattern, "", x)
            )
    }

    plot_gg
}
