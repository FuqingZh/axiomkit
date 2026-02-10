# Internal helper to construct the `ak` facade environment.
.create_ak_namespace <- function(ns = topenv()) {
    e_ak <- new.env(parent = emptyenv())
    e_stats <- new.env(parent = emptyenv())

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

    e_ak$stats <- e_stats

    lockEnvironment(e_stats, bindings = TRUE)
    lockEnvironment(e_ak, bindings = TRUE)

    e_ak
}

#' Unified AK Namespace Facade
#'
#' `ak` is a lightweight facade that groups package exports by domain.
#' Use `ak$stats$...` for statistics-related APIs.
#'
#' @format NULL
#' @usage ak
#' @export
ak <- .create_ak_namespace()
