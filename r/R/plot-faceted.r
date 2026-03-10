# Internal plotting column used for the y-axis factor after ordering.
.COL_Y_ORDER_FACTOR <- "YOrderFactor"

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
  should_order_y_descending,
  should_use_facet_strip_palette,
  colors_facet_strip
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
    should_order_y_descending <- isTRUE(should_order_y_descending)
    should_use_facet_strip_palette <- isTRUE(should_use_facet_strip_palette)

    is_colors_facet_strip_null <- is.null(colors_facet_strip)
    is_colors_facet_strip_valid <- is.character(colors_facet_strip) &&
        length(colors_facet_strip) > 0L
    if (!is_colors_facet_strip_null && !is_colors_facet_strip_valid) {
        cli::cli_abort(
            "Arg `colors_facet_strip` must be `NULL` or a non-empty character vector."
        )
    }

    if (is_colors_facet_strip_null && should_use_facet_strip_palette) {
        cli::cli_abort(
            "Arg `colors_facet_strip` must be provided when `should_use_facet_strip_palette = TRUE`."
        )
    }

    list(
        rule_facet_mode = facet_mode,
        rule_facet_direction = facet_direction,
        rule_facet_scales = facet_scales,
        rule_facet_space = facet_space,
        rule_y_order = y_order_rule,
        should_order_y_descending = should_order_y_descending,
        should_use_facet_strip_palette = should_use_facet_strip_palette,
        colors_facet_strip = colors_facet_strip
    )
}

# Derive a globally ordered y-axis factor from the normalized plotting table.
.derive_y_order_global <- function(
  dt,
  should_order_y_descending
) {
    dt_order <- unique(dt[, .(Y, X)])
    data.table::setorderv(
        dt_order,
        cols = c("X", "Y"),
        order = c(
            if (should_order_y_descending) -1L else 1L,
            1L
        )
    )
    dt_order <- unique(dt_order, by = "Y")
    dt[
        ,
        YOrderFactor := factor(YOrderFactor, levels = dt_order$Y)
    ]

    list(
        dt = dt,
        labels_y = NULL
    )
}

# Derive a facet-local y-axis factor and display labels from the normalized
# plotting table.
.derive_y_order_within_facet <- function(
  dt,
  should_order_y_descending
) {
    if (!("Facet" %in% colnames(dt))) {
        cli::cli_abort(
            "Arg `rule_y_order = \"within_facet\"` requires `col_facet`."
        )
    }

    dt_order <- unique(dt[, .(Facet, Y, X)])
    data.table::setorderv(
        dt_order,
        cols = c("Facet", "X", "Y"),
        order = c(
            1L,
            if (should_order_y_descending) -1L else 1L,
            1L
        )
    )
    dt_order <- unique(dt_order, by = c("Facet", "Y"))
    dt_order[, YOrderKey := paste0("AKY", sprintf("%08d", seq_len(.N)))]

    labels_y <- stats::setNames(dt_order$Y, dt_order$YOrderKey)
    dt <- merge(
        dt,
        dt_order[, .(Facet, Y, YOrderKey)],
        by = c("Facet", "Y"),
        all.x = TRUE,
        sort = FALSE
    )
    dt[
        ,
        YOrderFactor := factor(YOrderKey, levels = dt_order$YOrderKey)
    ]
    dt[, YOrderKey := NULL]

    list(
        dt = dt,
        labels_y = labels_y
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
  should_order_y_descending,
  should_use_facet_strip_palette,
  colors_facet_strip
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
        should_order_y_descending = should_order_y_descending,
        should_use_facet_strip_palette = should_use_facet_strip_palette,
        colors_facet_strip = colors_facet_strip
    )

    dt_ <- data.table::data.table(
        X = dt[[col_x]],
        Y = dt[[col_y]],
        YOrderFactor = dt[[col_y]]
    )
    labels_y <- NULL
    if (!is.null(col_facet)) {
        dt_[, Facet := dt[[col_facet]]]
    }
    if (!is.null(col_fill)) {
        dt_[, Fill := dt[[col_fill]]]
    }
    if (!is.null(col_group)) {
        dt_[, Group := dt[[col_group]]]
    }

    cols_trim <- intersect(
        c("Y", .COL_Y_ORDER_FACTOR, "Facet", "Fill", "Group"),
        colnames(dt_)
    )
    if (length(cols_trim) > 0) {
        for (.col in cols_trim) {
            if (is.factor(dt_[[.col]]) || is.character(dt_[[.col]])) {
                dt_[, (.col) := get(.col) |> as.character() |> trimws()]
            }
        }
    }

    cols_drop_na <- c("X", "Y")
    if (!is.null(col_facet) && facet_options$rule_facet_mode != "none") {
        cols_drop_na <- c(cols_drop_na, "Facet")
    }
    dt <- dt_[stats::complete.cases(dt_[, ..cols_drop_na])]

    if (nrow(dt) == 0) {
        cli::cli_abort("No valid rows remain after faceted plot normalization.")
    }

    dt[
        ,
        c("Y", "YOrderFactor") := lapply(.SD, as.character),
        .SDcols = c("Y", "YOrderFactor")
    ]
    if ("Facet" %in% colnames(dt)) {
        dt[, Facet := as.character(Facet)]
    }

    order_result <- switch(facet_options$rule_y_order,
        none = list(
            dt = dt,
            labels_y = NULL
        ),
        global = .derive_y_order_global(
            dt = dt,
            should_order_y_descending = facet_options$should_order_y_descending
        ),
        within_facet = .derive_y_order_within_facet(
            dt = dt,
            should_order_y_descending = facet_options$should_order_y_descending
        ),
        cli::cli_abort(
            "Unsupported y-ordering rule: {facet_options$rule_y_order}"
        )
    )
    dt <- order_result$dt
    labels_y <- order_result$labels_y

    list(
        dt = dt,
        opts = facet_options,
        labels_y = labels_y
    )
}

# Build a stable aesthetic mapping from the normalized internal schema.
.create_faceted_mapping <- function(dt) {
    cols_dt <- colnames(dt)
    mapping_args <- list(
        x = quote(X),
        y = substitute(.data[[col_name]], list(col_name = .COL_Y_ORDER_FACTOR))
    )

    if ("Fill" %in% cols_dt) {
        mapping_args$fill <- quote(Fill)
    }
    if ("Group" %in% cols_dt) {
        mapping_args$group <- quote(Group)
    }

    do.call(ggplot2::aes, mapping_args)
}

# Create a facet specification object for the normalized internal schema.
.create_facet_spec <- function(
  dt,
  facet_options
) {
    is_facet_by_cols <- facet_options$rule_facet_direction == "cols"
    is_facet_by_rows <- !is_facet_by_cols
    is_facet_col_present <- "Facet" %in% colnames(dt)
    should_disable_facet <- facet_options$rule_facet_mode == "none"
    if (!is_facet_col_present || should_disable_facet) {
        return(NULL)
    }

    facet_strip <- NULL
    if (facet_options$should_use_facet_strip_palette) {
        facet_levels <- unique(as.character(dt$Facet))
        strip_colors <- rep(
            facet_options$colors_facet_strip,
            length.out = length(facet_levels)
        )
        strip_rect <- lapply(
            strip_colors,
            function(fill_color) ggplot2::element_rect(fill = fill_color, color = NA)
        )
        strip_text <- lapply(
            seq_along(facet_levels),
            function(...) ggplot2::element_text(color = "#111111", face = "bold")
        )

        facet_strip <- if (is_facet_by_cols) {
            ggh4x::strip_themed(
                background_x = strip_rect,
                text_x = strip_text
            )
        } else {
            ggh4x::strip_themed(
                background_y = strip_rect,
                text_y = strip_text
            )
        }
    }

    if (facet_options$rule_facet_mode == "wrap") {
        facet_args <- list(
            facets = ggplot2::vars(Facet),
            scales = facet_options$rule_facet_scales,
            dir = if (is_facet_by_cols) "h" else "v"
        )
        if (!is.null(facet_strip)) {
            facet_args$strip <- facet_strip
        }

        return(do.call(ggh4x::facet_wrap2, facet_args))
    }

    facet_rows <- if (is_facet_by_rows) ggplot2::vars(Facet) else NULL
    facet_cols <- if (is_facet_by_cols) ggplot2::vars(Facet) else NULL

    facet_args <- list(
        rows = facet_rows,
        cols = facet_cols,
        scales = facet_options$rule_facet_scales,
        space = facet_options$rule_facet_space
    )
    if (!is.null(facet_strip)) {
        facet_args$strip <- facet_strip
    }

    do.call(ggh4x::facet_grid2, facet_args)
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
#' @param rule_facet_mode Facet mode:
#'   - `"none"`: No faceting.
#'   - `"wrap"`: Wrap facets.
#'   - `"grid"`: Grid facets.
#' @param rule_facet_direction Facet layout direction:
#'   - `"cols"`: Facet by columns.
#'   - `"rows"`: Facet by rows.
#' @param rule_facet_scales Facet scale rule.
#' @param rule_facet_space Facet panel space rule.
#' @param rule_y_order Y ordering rule:
#'   - `"none"`: No ordering.
#'   - `"global"`: Order y values globally.
#'   - `"within_facet"`: Order y values within each facet.
#' @param should_order_y_descending Y ordering direction:
#'   - `TRUE`: Order y values in descending order.
#'   - `FALSE`: Order y values in ascending order.
#' @param should_use_facet_strip_palette Facet strip styling:
#'   - `TRUE`: Use `colors_facet_strip` for facet strip styling.
#'   - `FALSE`: Use default facet strip styling.
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
  should_order_y_descending = FALSE,
  should_use_facet_strip_palette = FALSE,
  colors_facet_strip = NULL
) {
    if (missing(geom_layer) || is.null(geom_layer)) {
        cli::cli_abort("Arg `geom_layer` must be provided.")
    }

    faceted_data <- .derive_faceted_plot_data(
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
        should_order_y_descending = should_order_y_descending,
        should_use_facet_strip_palette = should_use_facet_strip_palette,
        colors_facet_strip = colors_facet_strip
    )
    dt <- faceted_data$dt
    facet_options <- faceted_data$opts
    labels_y <- faceted_data$labels_y
    aes_mapping <- .create_faceted_mapping(dt)
    facet_spec <- .create_facet_spec(dt, facet_options = facet_options)

    gg_plot <- ggplot2::ggplot(dt, aes_mapping) + geom_layer

    if (!is.null(facet_spec)) {
        gg_plot <- gg_plot + facet_spec
    }
    if (!is.null(labels_y)) {
        gg_plot <- gg_plot +
            ggplot2::scale_y_discrete(
                labels = labels_y
            )
    }

    gg_plot
}
