library(testthat)
library(ErasmusMCworkshop)

test_that("countPersons", {
  result <- countPersons(connectionDetails, cdmSchema) |>
    unlist() |>
    unname()

  expect_equal(result, 2694)
})


test_that("countDrug", {
  result <- countDrug(connectionDetails, cdmSchema, drugName) |>
    unlist() |>
    unname()

  expect_equal(result, 1844)
})


test_that("countDrugCondition", {
  result <- countDrugCondition(connectionDetails, cdmSchema, drugName, conditionId) |>
    unlist() |>
    unname()

  expect_equal(result, 41)
})
