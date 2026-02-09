.onLoad <- function(libname, pkgname) {
    e_ns <- asNamespace(pkgname)

    if (exists("ak", envir = e_ns, inherits = FALSE) && bindingIsLocked("ak", e_ns)) {
        unlockBinding("ak", e_ns)
    }
    assign("ak", .create_ak_namespace(pkg = pkgname), envir = e_ns)
    lockBinding("ak", e_ns)

    invisible(NULL)
}
