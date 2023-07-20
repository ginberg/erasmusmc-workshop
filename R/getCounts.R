#' countPersons
#'
#' Counts the number of persons present in the person table of the OMOP CDM.
#'
#' @template param_connectionDetails
#' @template param_cdmSchema
#'
#' @return (`data.frame()`)
#' Returns a data frame with the count of persons.
#'
#' @export
#'
#' @examples
#' connectionDetails <- Eunomia::getEunomiaConnectionDetails()
#'
#' countPersons(connectionDetails, "main")
countPersons <- function(connectionDetails, cdmSchema) {
  assertions <- checkmate::makeAssertCollection()
  checkmate::assert_class(
    x = connectionDetails,
    classes = c("ConnectionDetails", "DefaultConnectionDetails"),
    add = assertions
  )
  checkmate::assert_character(
    x = cdmSchema,
    len = 1,
    null.ok = FALSE,
    add = assertions
  )
  checkmate::reportAssertions(assertions)

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  sql <- "SELECT COUNT(*) AS person_count FROM @cdm.person;"

  result <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = sql,
    cdm = cdmSchema
  )

  return(result)
}


#' countDrug
#'
#' Counts the amount of records of a specified drug.
#'
#' @template param_connectionDetails
#' @template param_cdmSchema
#' @template param_drugName
#'
#' @export
#'
#' @return (`data.frame()`)
#' Returns a data frame with the count of the drug.
#'
#' @examples
#' connectionDetails <- Eunomia::getEunomiaConnectionDetails()
#'
#' countDrug(connectionDetails, "main", "celecoxib")
countDrug <- function(connectionDetails, cdmSchema, drugName) {
  assertions <- checkmate::makeAssertCollection()
  checkmate::assert_class(
    x = connectionDetails,
    classes = c("ConnectionDetails", "DefaultConnectionDetails"),
    add = assertions
  )
  checkmate::assert_character(
    x = cdmSchema,
    len = 1,
    null.ok = FALSE,
    add = assertions
  )
  checkmate::assert_character(
    x = drugName,
    len = 1,
    null.ok = FALSE,
    add = assertions
  )
  checkmate::reportAssertions(assertions)

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  sql <- "
  SELECT COUNT(DISTINCT(person_id)) AS person_count
  FROM @cdm.drug_era
  INNER JOIN @cdm.concept ingredient
  ON drug_concept_id = ingredient.concept_id
  WHERE LOWER(ingredient.concept_name) = '@drugName'
    AND ingredient.concept_class_id = 'Ingredient'
    AND ingredient.standard_concept = 'S';"

  result <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = sql,
    cdm = cdmSchema,
    drugName = drugName
  )

  return(result)
}


#' countDrugCondition
#'
#' Counts the amount of records of a specified drug, within the bounds of a
#' specified condition.
#'
#' @template param_connectionDetails
#' @template param_cdmSchema
#' @template param_drugName
#' @template param_conditionId
#'
#' @export
#'
#' @return (`data.frame()`)
#' Returns a data frame with the count of the drug within the bounds of the
#' specified condition.
#'
#' @examples
#' connectionDetails <- Eunomia::getEunomiaConnectionDetails()
#'
#' countDrugCondition(connectionDetails, "main", drugName = "celecoxib", conditionId = 192671)
countDrugCondition <- function(connectionDetails, cdmSchema, drugName, conditionId) {
  assertions <- checkmate::makeAssertCollection()
  checkmate::assert_class(
    x = connectionDetails,
    classes = c("ConnectionDetails", "DefaultConnectionDetails"),
    add = assertions
  )
  checkmate::assert_character(
    x = cdmSchema,
    len = 1,
    null.ok = FALSE,
    add = assertions
  )
  checkmate::assert_character(
    x = drugName,
    len = 1,
    null.ok = FALSE,
    add = assertions
  )
  checkmate::assert_numeric(
    x = conditionId,
    lower = 0,
    len = 1,
    null.ok = FALSE,
    add = assertions
  )
  checkmate::reportAssertions(assertions)

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  sql <- "
  SELECT COUNT(*) AS diagnose_count
  FROM @cdm.drug_era
  INNER JOIN @cdm.concept ingredient
  ON drug_concept_id = ingredient.concept_id
  INNER JOIN @cdm.condition_occurrence
  ON condition_start_date >= drug_era_start_date
    AND condition_start_date <= drug_era_end_date
  INNER JOIN @cdm.concept_ancestor
  ON condition_concept_id =descendant_concept_id
  WHERE LOWER(ingredient.concept_name) = '@drugName'
    AND ingredient.concept_class_id = 'Ingredient'
    AND ingredient.standard_concept = 'S'
    AND ancestor_concept_id = @conditionId;"

  result <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = sql,
    cdm = cdmSchema,
    drugName = drugName,
    conditionId = conditionId
  )

  return(result)
}
