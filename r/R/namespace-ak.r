# Internal helper to construct the `ak` facade environment.
.create_ak_namespace <- function(pkg = "axiomkit") {
    e_ak <- new.env(parent = emptyenv())
    e_stats <- new.env(parent = emptyenv())

    c_stats_exports <- c(
        "QuantMatrix",
        "create_quant_matrix",
        "normalize_axis_median_center"
    )
    for (c_name in c_stats_exports) {
        makeActiveBinding(
            c_name,
            local({
                c_export_name <- c_name
                function() getExportedValue(pkg, c_export_name)
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
ak <- NULL
