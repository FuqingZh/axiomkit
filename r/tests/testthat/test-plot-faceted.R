test_that("plot_faceted returns a ggplot object for minimal input", {
    df <- data.frame(
        Score = c(2, 5, 3),
        Term = c("T1", "T2", "T3")
    )

    p <- plot_faceted(
        df = df,
        col_x = "Score",
        col_y = "Term",
        geom_layer = ggplot2::geom_col()
    )

    expect_true(any(grepl("ggplot", class(p))))
    expect_silent(ggplot2::ggplot_build(p))
})

test_that("plot_faceted renders to a raster device", {
    df <- data.frame(
        Score = c(2, 5, 3, 4),
        Term = c("T1", "T2", "T1", "T3"),
        Database = c("GO", "GO", "KEGG", "KEGG"),
        Direction = c("up", "down", "up", "down")
    )
    path_file_png <- tempfile(fileext = ".png")
    on.exit(unlink(path_file_png), add = TRUE)

    p <- plot_faceted(
        df = df,
        col_x = "Score",
        col_y = "Term",
        col_facet = "Database",
        col_fill = "Direction",
        geom_layer = ggplot2::geom_col(),
        rule_facet_mode = "wrap",
        rule_y_order = "within_facet",
        should_use_facet_strip_palette = TRUE,
        colors_facet_strip = c("#D9E8FB", "#FDE3C8")
    )

    current_device <- grDevices::dev.cur()
    grDevices::png(filename = path_file_png, width = 1200, height = 900, res = 144)
    on.exit(
        if (grDevices::dev.cur() != current_device) {
            grDevices::dev.off()
        },
        add = TRUE
    )
    print(p)
    grDevices::dev.off()

    expect_true(file.exists(path_file_png))
    expect_gt(file.info(path_file_png)$size, 0)
})

test_that("plot_faceted supports faceting and within-facet ordering", {
    df <- data.frame(
        Score = c(2, 5, 3, 4),
        Term = c("T1", "T2", "T1", "T3"),
        Database = c("GO", "GO", "KEGG", "KEGG"),
        Direction = c("up", "down", "up", "down")
    )

    p <- plot_faceted(
        df = df,
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
    expect_true(is.factor(p$data$YOrderFactor))
    expect_false(any(grepl("___AK_FACET___", levels(p$data$YOrderFactor), fixed = TRUE)))
    expect_true(all(unname(p$scales$get_scales("y")$labels) %in% df$Term))
    expect_silent(ggplot2::ggplot_build(p))
})

test_that("plot_faceted keeps row count stable for repeated terms within facet", {
    df <- data.frame(
        Score = c(2, 5, 3, 4, 1),
        Term = c("T1", "T1", "T1", "T2", "T2"),
        Database = c("GO", "GO", "KEGG", "KEGG", "KEGG")
    )

    p <- plot_faceted(
        df = df,
        col_x = "Score",
        col_y = "Term",
        col_facet = "Database",
        geom_layer = ggplot2::geom_col(),
        rule_y_order = "within_facet"
    )

    expect_equal(nrow(p$data), nrow(df))
    expect_silent(ggplot2::ggplot_build(p))
})

test_that("plot_faceted supports wrap faceting", {
    df <- data.frame(
        Score = c(2, 5, 3, 4),
        Term = c("T1", "T2", "T1", "T3"),
        Database = c("GO", "GO", "KEGG", "KEGG")
    )

    p <- plot_faceted(
        df = df,
        col_x = "Score",
        col_y = "Term",
        col_facet = "Database",
        geom_layer = ggplot2::geom_col(),
        rule_facet_mode = "wrap"
    )

    expect_silent(ggplot2::ggplot_build(p))
})

test_that("plot_faceted validates strip palette configuration", {
    df <- data.frame(
        Score = c(2, 5),
        Term = c("T1", "T2"),
        Database = c("GO", "KEGG")
    )

    expect_error(
        plot_faceted(
            df = df,
            col_x = "Score",
            col_y = "Term",
            col_facet = "Database",
            geom_layer = ggplot2::geom_col(),
            should_use_facet_strip_palette = TRUE
        ),
        "colors_facet_strip"
    )
})

test_that("plot_faceted validates required columns", {
    df <- data.frame(
        Score = c(1, 2),
        Term = c("A", "B")
    )

    expect_error(
        plot_faceted(
            df = df,
            col_x = "MissingX",
            col_y = "Term",
            geom_layer = ggplot2::geom_col()
        ),
        "missing required columns"
    )
})
