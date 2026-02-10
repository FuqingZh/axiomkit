.onLoad <- function(libname, pkgname) {
    e_ns <- asNamespace(pkgname)

    if (
        exists("ak", envir = e_ns, inherits = FALSE) &&
            bindingIsLocked("ak", e_ns)
    ) {
        unlockBinding("ak", e_ns)
    }
    obj_make_ak_namespace <- get(
        ".create_ak_namespace",
        mode = "function",
        envir = e_ns,
        inherits = FALSE
    )
    assign("ak", obj_make_ak_namespace(pkg = pkgname), envir = e_ns)
    lockBinding("ak", e_ns)

    invisible(NULL)
}
