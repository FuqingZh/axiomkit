test_that("ak exposes plot helpers through the plot facade", {
    expect_true(exists("plot", envir = ak, inherits = FALSE))
    expect_true(exists("plot_faceted", envir = ak$plot, inherits = FALSE))
    expect_identical(ak$plot$plot_faceted, plot_faceted)
})
