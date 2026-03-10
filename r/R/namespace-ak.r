# Internal helper to construct the `ak` facade environment.
.create_ak_namespace <- function(ns = topenv()) {
    e_ak <- new.env(parent = emptyenv())
    e_stats <- new.env(parent = emptyenv())
    e_plot <- new.env(parent = emptyenv())

    c_stats_exports <- c(
        "QuantMatrix",
        "center_median"
    )
    for (c_name in c_stats_exports) {
        makeActiveBinding(
            c_name,
            local({
                c_export_name <- c_name
                function() get(c_export_name, envir = ns, inherits = FALSE)
            }),
            e_stats
        )
    }

    c_plot_exports <- c(
        "plot_faceted"
    )
    for (c_name in c_plot_exports) {
        makeActiveBinding(
            c_name,
            local({
                c_export_name <- c_name
                function() get(c_export_name, envir = ns, inherits = FALSE)
            }),
            e_plot
        )
    }

    e_ak$stats <- e_stats
    e_ak$plot <- e_plot

    lockEnvironment(e_stats, bindings = TRUE)
    lockEnvironment(e_plot, bindings = TRUE)
    lockEnvironment(e_ak, bindings = TRUE)

    e_ak
}

#' Unified AK Namespace Facade
#'
#' `ak` is a lightweight facade that groups package exports by domain.
#' Use `ak$stats$...` for statistics-related APIs and `ak$plot$...` for
#' plotting helpers.
#'
#' @format NULL
#' @usage ak
#' @export
ak <- .create_ak_namespace()
