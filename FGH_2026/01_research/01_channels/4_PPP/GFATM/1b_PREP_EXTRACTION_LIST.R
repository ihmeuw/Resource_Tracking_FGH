#### #----#                        Docstring                         #----# ####
#' Project:         FGH
#'    
#' Purpose:         Intake & clean GFATM projects to extract
#------------------------------------------------------------------------------#

####################### #----# ENVIRONMENT SETUP #----# ########################
rm(list=ls())
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

## Source functions

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, openxlsx, magrittr, httr)

## Local CONSTANTS
GFATM_PREV_FIN <- get_path("GFATM", "fin", report_year = dah.roots$prev_report_year)
GF_API_SERVICE_ROOT <- "https://fetch.theglobalfund.org/v3.3/odata/"
#------------------------------------------------------------------------------#


######################## #----# HELPER FUNCTIONS #----# ########################
#' @title response_to_dt
#' @description Converts a REST API response object to a data.table
#' 
#' @param res [response] JSON-like response object, e.g. the successful result 
#' from a GET request
#' @return Returns a data.table with the parsed response data
response_to_dt <- function(res) {
  # Parse response to list-like data
  values <- content(res, as = "parsed")$value
  # Convert values to data.table rows
  dt <- rbindlist(values)
  # Assign data table column names
  names(dt) <- names(values[[1]])
  
  return ( dt )
}

#' @title update_iso3
#' @description Updates ISO3 portion of GFATM project ID-like string, if 
#' it does not match a supplied (up-to-date) reference ISO3
#'
#' @param project_id [character] GFATM PROJECT ID (aka 
#' GrantAgreementNumber)-like string. First 3 represent country ISO3 code
#' @param iso3 [character] Reference ISO3 code to compare project_id with
#' @return Returns project ID-like string
update_iso3 <- function (project_id, iso3) {
  # IF `project_id` has outdated ISO3 code, and `iso3` is valid code..
  if ((substr(project_id, 1, 3) != iso3) & (nchar(iso3) == 3)) {
    # THEN replace code within `project_id` with `iso3`
    return ( paste0(iso3, "-", substr(project_id, 5, nchar(project_id))) )
  } else {
    # Otherwise, just return the original `project_id`
    return ( project_id )
  }
}

#' @title clean_ids
#' @description Cleans and standardizes project ID string
#'
#' @param project_id [character] GFATM PROJECT ID (aka 
#' GrantAgreementNumber)-like string. First 3 represent country ISO3 code
#' @param iso3 [character] Reference ISO3 code to compare `project_id` with
#' @return Returns uppercase, trimmed, and updated project ID string
clean_ids <- function (project_id, iso3) {
  # To uppercase
  project_id <- toupper(project_id)
  # Remove trailing dash or space and trailing digits
  project_id <- str_replace(project_id, "( |-)[0-9]*$", "")
  # Fix grants with older ISO3 codes
  project_id <- update_iso3(project_id, iso3)
  
  return ( project_id ) 
}

#' @title clean_projects
#' @description Cleans and standardizes project data
#'
#' @param dt [data.table] Dataset to be cleaned
#' @param hfa_abbrev [character] "hiv", "tb", "mal", etc.
#' @return Returns unique data.table with cleaned PROJECT_ID column and column 
#' tagging HFA
clean_projects <- function (dt, hfa_abbrev) {
  dt[, PROJECT_ID := mapply(clean_ids, project_id = PROJECT_ID, iso3 = ISO3_RC)]
  # Remove rows where PROJECT_ID is duplicate
  dt <- unique(dt, by = "PROJECT_ID")
  # Remove unneeded ISO3 column
  dt[, ISO3_RC := NULL]
  # Set identifier for these rows (to distinguish them later when this dt is 
  # merged with others)
  dt[, eval(paste0("match_", hfa_abbrev)) := 1]
  
  return ( dt )
}

#' @title random_choice
#' @description Picks a with probability p or picks b with probability 1 - p
#'
#' @param a [character/numeric/integer/complex/logical] Some object
#' @param b [character/numeric/integer/complex/logical] Some object
#' @return Returns `a` with probability `p` or returns `b` with 
#' probability 1 - `p`
random_choice <- function (a, b, p = 0.5) {
  return ( ifelse(p >= runif(1), a, b) )
}


#' @title odata_get
#' @description Very very basic R wrapper for ODATA API GET request. For help 
#' with writing statements and syntax, this tutorial is a good start: 
#' https://www.odata.org/getting-started/basic-tutorial/
#' 
#' @param service_root  [character] API service root
#' @param resource_path [character] Resource path of interest, not prefixed 
#' with service root
#' @param select  [character] List of variable names to select
#' @param expand  [character] Valid ODATA $expand statement (see docs)
#' @param filter  [character] Valid ODATA $filter statement (see docs)
#' @param orderby [character] List of variable names to order response by
#' @param top     [integer]   Number of items to be included in result
#' @param skip    [integer]   Number of items to not include in result
#' @param desc    [logical]   Whether to sort results in descending order 
#' based on supplied `orderby` variable name(s)
#' @param count   [logical]   Whether to include a count of the items returned
#' along with the response
#' @return Returns a JSON-like response object, given a valid request
odata_get <- function (service_root, resource_path, select, expand, filter, 
                       orderby, top, skip, desc = FALSE, count = FALSE) {
  # Named list of function arguments
  ## Convert options arguments to valid ODATA query option strings
  # select
  select_stmt <- ifelse(missing(select), "", paste0("?$select=", paste(select, collapse = ",")))
  # expand
  expand_stmt <- ifelse(missing(expand), "", paste("?$expand=", expand, collapse = ","))
  # filter_stmt (this is very basic wrapping so filter items need to include valid operators)
  filter_stmt <- ifelse(missing(filter), "", paste("?$filter=", filter, collapse = ","))
  # order
  orderby_stmt <- ifelse(missing(orderby), "", paste("?$order=", orderby, collapse = ","))
  # top
  top_stmt <- ifelse(missing(top), "", paste0("?$top=", top))
  # desc
  orderby_stmt <- ifelse(missing(desc), orderby_stmt, paste0(orderby_stmt, " desc"))
  # count
  count_stmt <- ifelse(missing(count), "", paste0("?$count=", count))
  
  ## Build GET request URL
  url <- paste0(service_root, resource_path, select_stmt, expand_stmt, filter_stmt, orderby_stmt, top_stmt, count_stmt)
  
  ## GET request
  res <- GET(url)
  
  return ( res )
}
#------------------------------------------------------------------------------#


############################## #----# MAIN #----# ##############################
cat("\n\n")
cat(green(" ####################################\n"))
cat(green(" #### GFATM PREP EXTRACTION LIST ####\n"))
cat(green(" ####################################\n\n"))
cat(paste0("  Read in ", dah.roots$prev_report_year, " Malaria projects\n"))

allproj <- fread(paste0(GFATM_PREV_FIN, "P_GFATM_allprojects_master.csv"))
allproj <- clean_projects(allproj, "all")

#------------------------------------------------------------------------------#

#### #----#                      GFATM API data                      #----# ####
## GET Grant Agreements from The Global Fund API
## Docs: https://data-service.theglobalfund.org/api#tag/Views/operation/ViewGrantAgreement
## VGrantAgreements
#' https://fetch.theglobalfund.org/v3.3/odata/GrantAgreements
#' SELECT
#'  grantAgreementId
#'  grantAgreementNumber (-> grantAgreementDisbursements)
#'  geographicAreaName
#'  geographicAreaCode_ISO3
#'  componentId
#'  componentName
#'  grantAgreementTitle
#'  totalDisbursedAmount
cat("  Read in GFATM API data\n")
res <- odata_get(
  GF_API_SERVICE_ROOT,
  "VGrantAgreements",
  select = c(
    "grantAgreementId", "grantAgreementNumber", "geographicAreaName", 
    "geographicAreaCode_ISO3", "componentId", "componentName", 
    "grantAgreementTitle", "totalDisbursedAmount"
  )
)
# Use helper function to grab values from response and save to data.table
ga_dt <- response_to_dt(res)

## Rename our "ID" column
setnames(ga_dt, "grantAgreementNumber", "PROJECT_ID")

## Clean up Project IDs
cat("  Clean API Project ID column\n")
ga_dt[, PROJECT_ID := mapply(clean_ids, project_id = PROJECT_ID, 
                           iso3 = geographicAreaCode_ISO3)]
#------------------------------------------------------------------------------#

#### #----#                 Merge together datasets                  #----# ####
## Merge old project data with new API data
cat("  Merge datasets\n")
data <- merge(ga_dt, allproj, by="PROJECT_ID", all.x=TRUE)

## Project ID validation
proj_ids_ndiff <- length(! unique(ga_dt$PROJECT_ID) %in% allproj$PROJECT_ID)
if (proj_ids_ndiff != 0) {
  cat(paste0(red(proj_ids_ndiff),
                " project IDs from previous report ",
                "year not found in API response.\n"))
}
# New IDs count
cat(paste0(
  green(length(unique(data[is.na(match_all), PROJECT_ID]))), 
  " project IDs from API response not present in previous report year.\n\n"
))


# Keep only non-match rows in order to export NEW project IDs we don't already 
# have data for from previous years
dt_extract <- data[is.na(match_all), 
             c("PROJECT_ID", "componentName", 
               "geographicAreaCode_ISO3", "geographicAreaName")]
# Count of all new unique projects
cat(paste0(green(length(unique(dt_extract$PROJECT_ID))), " total new projects to extract.\n"))
#------------------------------------------------------------------------------#

#### #----#               Save pre-extractions dataset               #----# ####
cat("  Save Extractions Data\n")
write.xlsx(dt_extract, paste0(get_path("GFATM", "raw"), "projects_to_extract_", Sys.Date(), ".xlsx"))
write.xlsx(dt_extract, paste0(get_path("GFATM", "raw", is_backup = T), "projects_to_extract_", Sys.Date(), ".xlsx"))
#------------------------------------------------------------------------------#

#### #----#                    Project extraction                    #----# ####
### Components
data[, totalDisbursedAmount := as.numeric(totalDisbursedAmount)]

## RSSH
# Represents ~$1B in disbursements 
data[componentName == "RSSH", sum(totalDisbursedAmount)]
# Apparently we drop RSSH rows
data <- data[componentName != "RSSH", ]

## TB/HIV
# Represents ~$5.3B in disbursements 
data[componentName == "TB/HIV", sum(totalDisbursedAmount)]
# Randomly assign to TB or HIV. Supposedly doesn't matter, 
# according to HUB docs...
data[componentName == "TB/HIV", 
           componentName := random_choice("Tuberculosis", "HIV"), 
           by = seq_len(nrow(data[componentName == "TB/HIV", ]))]

## Multicomponent
# Represents ~$510M in disbursements 
data[componentName == "Multicomponent", sum(totalDisbursedAmount)]


# Apparently these used to be in RSSH and were not counted by FGH study. 
# However we seem to have enough information (keywords) in most cases from 
# grant titles to categorize.
data[, title_flag_hiv := ifelse(grantAgreementTitle %like% "HIV", 1, 0)]
data[, title_flag_mal := ifelse(grantAgreementTitle %like% "Mal", 1, 0)]
data[, title_flag_tb := ifelse(grantAgreementTitle %like% "TB|Tuberculosis", 1, 0)]
#------------------------------------------------------------------------------#

#### #----#        Project budgets by year and activity area         #----# ####
## grantAgreementDisbursements
#' https://fetch.theglobalfund.org/v3.3/odata/VGrantAgreementDisbursements
#' SELECT
#'  grantAgreementDisbursementId <- not needed most likely
#'  grantAgreementImplementationPeriodId (-> GrantAgreementImplementationPeriods)
select_entities <- c("grantAgreementNumber",
                     "grantAgreementImplementationPeriodId")
# TODO: speed
res <- odata_get(
  service_root  = GF_API_SERVICE_ROOT,
  resource_path = "VGrantAgreementDisbursements",
  select = select_entities
)
gad_dt <- response_to_dt(res)
# Not sure if we need disbursementId level of granularity, so removing dupe rows
gad_dt <- unique(gad_dt)

## VGrantAgreementImplementationPeriodDetailedBudgets
#' https://fetch.theglobalfund.org/v3.3/odata/GrantAgreementImplementationPeriodDetailedBudgets
#' SELECT
#'  grantAgreementImplementationPeriodDetailedBudgetId
#'  grantAgreementImplementationPeriodId
#'  activityAreaId
#'  budgetPeriodStartDate
#'  budgetPeriodEndDate
#'  budgetYear
#'  budgetAmount
select_entities <- c("grantAgreementImplementationPeriodDetailedBudgetId",
                     "grantAgreementImplementationPeriodId",
                     "activityAreaId",
                     "budgetPeriodStartDate",
                     "budgetPeriodEndDate",
                     "budgetYear",
                     "budgetAmount")
res <- odata_get(
  service_root  = GF_API_SERVICE_ROOT,
  resource_path = "grantAgreementImplementationPeriodDetailedBudgets",
  select = select_entities
)
ipdb_dt <- response_to_dt(res)

## ActivityAreas
#'  https://fetch.theglobalfund.org/v3.3/odata/ActivityAreas
#'  activityAreaId
#'  activityAreaType (e.g. "Module")
#'  activityAreaName
select_entities <- c("activityAreaId",
                     "activityAreaName",
                     "activityAreaType")
res <- odata_get(
  service_root  = GF_API_SERVICE_ROOT,
  resource_path = "ActivityAreas",
  select = select_entities
)
aa_dt <- response_to_dt(res)

## Associating our PROJECT_IDs with grant periods, budgets, and activity areas
grants_dt <- data %>%
  merge(gad_dt, by.x = "PROJECT_ID", by.y = "grantAgreementNumber") %>%
  merge(ipdb_dt, by = "grantAgreementImplementationPeriodId") %>%
  merge(aa_dt, by = "activityAreaId")
#------------------------------------------------------------------------------#

#### #----#                   Cleaning merged data                   #----# ####
grants_clean <- copy(grants_dt)
# Take year of budgetPeriodStartDate. Budget periods are at the quarter level
# (Jan-Mar, Apr-Jun, Jul-Sep, Oct-Dec) so start date year will always be same 
# as end date year
grants_clean[, year := as.numeric(tstrsplit(budgetPeriodStartDate, "-")[[1]])]
# Select categorical columns to keep along with annual budgetAmounts
id_cols <- c("geographicAreaName", "PROJECT_ID", "activityAreaName", "componentName", 
             "grantAgreementTitle", names(grants_clean)[names(grants_clean) %like% "title_flag_"],
             "year")
# Sum budget amount for each grant and year. Keep only annual budget and 
# `id_cols`
grants_clean <- grants_clean[, sum(budgetAmount), by = id_cols]
# Set column names for alignment with previous years
setnames(
  grants_clean, 
  c("geographicAreaName", "activityAreaName", "componentName", "grantAgreementTitle", "V1"),
  c("geo_name", "category", "component_name", "grant_agreement_title", "budget_annual")
)
#------------------------------------------------------------------------------#

#### #----#                        Save data                         #----# ####
## ALL Projects
save_dataset(
  grants_clean,
  paste0("All_Projects_FGH", dah.roots$report_year),
  "GFATM",
  "raw"
)
## NEW Projects (those not found in previous year's extractions)
grants_new <- grants_clean[(PROJECT_ID %in% unique(dt_extract$PROJECT_ID)) | (year >= dah.roots$prev_report_year), ]
save_dataset(
  grants_new,
  paste0("New_Projects_FGH", dah.roots$report_year),
  "GFATM",
  "raw"
)
#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#
