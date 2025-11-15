#### #----#                        Docstring                         #----# ####
#' Project:         FGH
#'    
#' Purpose:         From GFATM API, GET "All Program Documents" data and save
#'                  to .csv. Also, as the first script in the GFATM "pipeline", 
#'                  automatically creates the RAW/FGH_<report_year> directory 
#------------------------------------------------------------------------------#

####################### #----# ENVIRONMENT SETUP #----# ########################
## System Prep
rm(list=ls())
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

## Imports
pacman::p_load(httr)

## Source functions

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)
#------------------------------------------------------------------------------#


############################## #----# MAIN #----# ##############################
cat("\n\n")
cat(green(" ###########################\n"))
cat(green(" #### GFATM JSON TO CSV ####\n"))
cat(green(" ###########################\n\n"))

## Creates necessary directories if they don't exist
# RAW/FGH_<report_year>/ dir
ensure_dir(get_path("GFATM", "raw"))
# RAW/FGH_<report_year>/downloaded_files/ dir

## GET data from The Global Fund API
cat("  GETting JSON data\n")
res <- GET("https://fetch.theglobalfund.org/v3.3/odata/VProgramDocuments")
# Parse response to list-like data
input <- content(res, as = "parsed")$value

## Format input data
cat("  Formatting data for output\n")
cols <- names(input[[1]])
data <- data.table()
data[, eval(cols) := ""]

## Add each row into the output dataset
# Suppressing NA-assignment warnings for entries coming from the downloaded 
# data as null
options(warn = -1)
for (i in seq_along(input)) {
  data <- rbind(data, input[[i]])
}
options(warn = 0)

## Save dataset
cat("  Saving dataset\n")
save_dataset(
  data,
  paste0("VProgramDocuments_", Sys.Date()),
  channel = "GFATM",
  stage = "raw",
  folder = "downloaded_files"
)
#------------------------------------------------------------------------------#





