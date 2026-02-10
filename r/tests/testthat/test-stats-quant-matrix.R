test_that("QuantMatrix stores double matrix and preserves dimnames", {
    mat <- matrix(
        c(1, 2, 3, 4, 5, 6),
        nrow = 2,
        byrow = TRUE,
        dimnames = list(c("R1", "R2"), c("A", "B", "C"))
    )

    q <- QuantMatrix(mat = mat)

    expect_true(inherits(q, "S7_object"))
    expect_true(any(grepl("QuantMatrix$", class(q))))
    expect_true(is.matrix(q@mat))
    expect_true(is.double(q@mat))
    expect_identical(dim(q@mat), c(2L, 3L))
    expect_identical(dimnames(q@mat), dimnames(mat))
})

test_that("QuantMatrix coerces matrix-like inputs to double matrix", {
    q <- QuantMatrix(mat = data.frame(a = 1:2, b = 3:4))

    expect_true(is.matrix(q@mat))
    expect_true(is.double(q@mat))
    expect_identical(dim(q@mat), c(2L, 2L))
})

test_that("center_median works for column axis", {
    mat <- matrix(
        c(1, 2, 3, 4, 5, 6),
        nrow = 2,
        byrow = TRUE,
        dimnames = list(c("R1", "R2"), c("A", "B", "C"))
    )
    q <- QuantMatrix(mat = mat)

    q_align <- center_median(
        q,
        rule_axis = "col",
        rule_baseline = "global_median"
    )
    expect_equal(
        q_align@mat,
        matrix(
            c(2, 2, 2, 5, 5, 5),
            nrow = 2,
            byrow = TRUE,
            dimnames = dimnames(mat)
        )
    )

    q_zero <- center_median(
        q,
        rule_axis = "col",
        rule_baseline = "zero"
    )
    expect_equal(
        q_zero@mat,
        matrix(
            c(-1.5, -1.5, -1.5, 1.5, 1.5, 1.5),
            nrow = 2,
            byrow = TRUE,
            dimnames = dimnames(mat)
        )
    )
})

test_that("center_median works for row axis", {
    mat <- matrix(
        c(1, 2, 3, 4, 5, 6),
        nrow = 2,
        byrow = TRUE,
        dimnames = list(c("R1", "R2"), c("A", "B", "C"))
    )
    q <- QuantMatrix(mat = mat)

    q_zero <- center_median(
        q,
        rule_axis = "row",
        rule_baseline = "zero"
    )
    expect_equal(
        q_zero@mat,
        matrix(
            c(-1, 0, 1, -1, 0, 1),
            nrow = 2,
            byrow = TRUE,
            dimnames = dimnames(mat)
        )
    )

    q_align <- center_median(
        q,
        rule_axis = "row",
        rule_baseline = "global_median"
    )
    expect_equal(
        q_align@mat,
        matrix(
            c(2.5, 3.5, 4.5, 2.5, 3.5, 4.5),
            nrow = 2,
            byrow = TRUE,
            dimnames = dimnames(mat)
        )
    )
})

test_that("center_median validates rules", {
    q <- QuantMatrix(mat = matrix(c(1, 2, 3, 4), nrow = 2))

    expect_error(
        center_median(q, rule_axis = "bad"),
        "arg"
    )
    expect_error(
        center_median(q, rule_baseline = "bad"),
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
