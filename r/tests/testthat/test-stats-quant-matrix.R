test_that("create_quant_matrix stores matrix and labels", {
    mat <- matrix(
        c(1, 2, 3, 4, 5, 6),
        nrow = 2,
        byrow = TRUE
    )
    col_label <- c("A", "B", "C")
    row_label <- c("R1", "R2")

    q <- create_quant_matrix(
        mat = mat,
        col_label = col_label,
        row_label = row_label
    )

    expect_true(inherits(q, "S7_object"))
    expect_true(any(grepl("QuantMatrix$", class(q))))
    expect_true(is.matrix(q@mat))
    expect_identical(dim(q@mat), c(2L, 3L))
    expect_identical(q@col_label, col_label)
    expect_identical(q@row_label, row_label)
})

test_that("create_quant_matrix validates label lengths", {
    mat <- matrix(c(1, 2, 3, 4), nrow = 2)

    expect_error(
        create_quant_matrix(mat = mat, col_label = c("A")),
        "col_label"
    )
    expect_error(
        create_quant_matrix(mat = mat, row_label = c("R1")),
        "row_label"
    )
})

test_that("normalize_axis_median_center works for column axis", {
    mat <- matrix(
        c(1, 2, 3, 4, 5, 6),
        nrow = 2,
        byrow = TRUE
    )
    q <- create_quant_matrix(
        mat = mat,
        col_label = c("A", "B", "C"),
        row_label = c("R1", "R2")
    )

    q_align <- normalize_axis_median_center(
        q,
        rule_axis = "col",
        rule_align = "global_median"
    )
    expect_equal(
        q_align@mat,
        matrix(c(2, 2, 2, 5, 5, 5), nrow = 2, byrow = TRUE)
    )
    expect_identical(q_align@col_label, q@col_label)
    expect_identical(q_align@row_label, q@row_label)

    q_none <- normalize_axis_median_center(
        q,
        rule_axis = "col",
        rule_align = "none"
    )
    expect_equal(
        q_none@mat,
        matrix(c(-1.5, -1.5, -1.5, 1.5, 1.5, 1.5), nrow = 2, byrow = TRUE)
    )
})

test_that("normalize_axis_median_center works for row axis", {
    mat <- matrix(
        c(1, 2, 3, 4, 5, 6),
        nrow = 2,
        byrow = TRUE
    )
    q <- create_quant_matrix(mat = mat)

    q_none <- normalize_axis_median_center(
        q,
        rule_axis = "row",
        rule_align = "none"
    )
    expect_equal(
        q_none@mat,
        matrix(c(-1, 0, 1, -1, 0, 1), nrow = 2, byrow = TRUE)
    )

    q_align <- normalize_axis_median_center(
        q,
        rule_axis = "row",
        rule_align = "global_median"
    )
    expect_equal(
        q_align@mat,
        matrix(c(2.5, 3.5, 4.5, 2.5, 3.5, 4.5), nrow = 2, byrow = TRUE)
    )
})

test_that("normalize_axis_median_center validates rules", {
    q <- create_quant_matrix(mat = matrix(c(1, 2, 3, 4), nrow = 2))

    expect_error(
        normalize_axis_median_center(q, rule_axis = "bad"),
        "arg"
    )
    expect_error(
        normalize_axis_median_center(q, rule_align = "bad"),
        "arg"
    )
})

test_that("ak stats facade exposes quant APIs", {
    expect_true(exists("ak", inherits = TRUE))
    expect_true(is.environment(ak))
    expect_true(is.environment(ak$stats))
    expect_true(is.function(ak$stats$create_quant_matrix))
    expect_true(is.function(ak$stats$normalize_axis_median_center))

    q <- ak$stats$create_quant_matrix(matrix(c(1, 2, 3, 4), nrow = 2))
    q2 <- ak$stats$normalize_axis_median_center(
        q,
        rule_axis = "col",
        rule_align = "global_median"
    )
    expect_true(is.matrix(q2@mat))
})
