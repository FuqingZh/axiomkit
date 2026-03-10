test_that("plot_faceted returns a ggplot object for minimal input", {
    df_plot <- data.frame(
        Score = c(2, 5, 3),
        Term = c("T1", "T2", "T3")
    )

    p <- plot_faceted(
        df = df_plot,
        col_x = "Score",
        col_y = "Term",
        geom_layer = ggplot2::geom_col()
    )

    expect_true(any(grepl("ggplot", class(p))))
    expect_silent(ggplot2::ggplot_build(p))
})

test_that("plot_faceted supports faceting and within-facet ordering", {
    df_plot <- data.frame(
        Score = c(2, 5, 3, 4),
        Term = c("T1", "T2", "T1", "T3"),
        Database = c("GO", "GO", "KEGG", "KEGG"),
        Direction = c("up", "down", "up", "down")
    )

    p <- plot_faceted(
        df = df_plot,
        col_x = "Score",
        col_y = "Term",
        col_facet = "Database",
        col_fill = "Direction",
        geom_layer = ggplot2::geom_col(),
        rule_y_order = "within_facet",
        rule_facet_scales = "free_y"
    )

    expect_true("Facet" %in% colnames(p$data))
    expect_true("Fill" %in% colnames(p$data))
    expect_true(is.factor(p$data$YPlot))
    expect_true(any(grepl("___AK_FACET___", levels(p$data$YPlot), fixed = TRUE)))
    expect_silent(ggplot2::ggplot_build(p))
})

test_that("plot_faceted validates required columns", {
    df_plot <- data.frame(
        Score = c(1, 2),
        Term = c("A", "B")
    )

    expect_error(
        plot_faceted(
            df = df_plot,
            col_x = "MissingX",
            col_y = "Term",
            geom_layer = ggplot2::geom_col()
        ),
        "missing required columns"
    )
})

test_that("plot_bar_faceted delegates to plot_faceted", {
    df_plot <- data.frame(
        X = c(1, 3),
        Y = c("A", "B")
    )

    p <- plot_bar_faceted(df_plot)

    expect_true(any(grepl("ggplot", class(p))))
    expect_silent(ggplot2::ggplot_build(p))
})
