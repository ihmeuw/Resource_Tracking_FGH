#### #----#                        Docstring                         #----# ####
#' Project:         FGH
#'    
#' Purpose:         Cleaning of HIV, Malaria, TB, and Multicomponent project 
#'                  rows of data coming from The Global Fund's API
#------------------------------------------------------------------------------#

####################### #----# ENVIRONMENT SETUP #----# ########################
rm(list=ls())
if (!exists("code_repo")) {
  code_repo <- unname(ifelse(Sys.info()["sysname"] == "Windows",
                             "H:/repos/fgh/", 
                             paste0("/ihme/homes/", Sys.info()["user"][1],
                                    "/repos/fgh/")))
}


## Source functions
report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr, haven, testthat)

## Local CONSTANTS
GFATM_RAW_BASE <- paste0(
  dah.roots$j,
  "FILEPATH"
)
GFATM_2021_RAW <- paste0(
  GFATM_RAW_BASE,
  "FGH_", 
  2021,
  "/"
)
# List used to ensure all incoming data has similar columns
NEW_EXTRACT_COLUMN_NAMES <- c("geo_name", "PROJECT_ID", "currency", "category", 
                              "objective", "upper_SDA", "year", "budget_annual")
KW_SEARCH_COLS <- c("category", "objective", "upper_SDA")
UNIQUE_ID_COLS <- c("PROJECT_ID", "category", "objective", "upper_SDA", "year")
#------------------------------------------------------------------------------#


######################## #----# HELPER FUNCTIONS #----# ########################
#' @title clean_extraction_columns
#' @description Performs a standardized process of cleaning columns found in 
#' extractions datasets including string cleaning, removing of commas and 
#' converting budget columns to numeric, etc.
#' 
#' @param extractions_dt [data.table] The dataset with extractions columns to 
#' be cleaned
#' @return Returns a cleaned version of the extractions data.table
clean_extraction_columns <- function (extractions_dt) {
  ## DT copy dataset
  dt <- copy(extractions_dt)
  
  ## Check that expected column names are included
  if (!all(NEW_EXTRACT_COLUMN_NAMES %in% names(dt))) {
    stop("Check that column names contain standard extraction column names")
  }
  
  ## String substitutions
  dt[, geo_name := string_to_std_ascii(geo_name)]
  dt[, category := string_to_std_ascii(category)]
  dt[, PROJECT_ID := gsub("-00", "", PROJECT_ID)]
  
  ## Column-specific cleaning
  # Destring budget column
  dt[, budget_annual := 
       as.numeric(str_replace_all(budget_annual, ",", ""))]
  # Year to integer class
  dt[, year := as.integer(year)]
  # Make keyword search columns uppercase
  dt[, (KW_SEARCH_COLS) := lapply(.SD, toupper), .SDcols = KW_SEARCH_COLS]
  dt[grepl("OTHER THAN ITNS", upper_SDA), upper_SDA := "IRS"]
  # Replace keyword search column NAs with empty strings for the sake of 
  # standardization (added to match Stata encoding)
  dt[is.na(upper_SDA), upper_SDA := ""]
  dt[is.na(objective), objective := ""]
  
  return ( dt )
}
#------------------------------------------------------------------------------#


############################## #----# MAIN #----# ##############################
cat("  Prepare historical data\n")
#### #----#                       HIV Previous                       #----# ####
# Read in HIV extractions data for 2007-2021
# Through FGH 2021 project annual budgets were extracted 
# manually or via document parsing software from grant program document pdf 
# files downloaded one by one from The Global Fund's website. These extracted 
# budgets were added to .csv files, each year building on the last, so FGH 2021
# file should have all the outstanding extractions for 2021 and earlier
hiv_prev <- fread(paste0(
  GFATM_2021_RAW, 
  "HIV Extractions/New_HIV_Projects_FGH2021.csv"
))
# Clean Columns
# Rename cols to standardize with this year's data
setnames(hiv_prev, c("ID", "Grant", "total_"), 
         c("geo_name", "PROJECT_ID", "budget_annual"))
# Keep relevant columns
keep_cols <- c("geo_name", "PROJECT_ID", "category", "year", "budget_annual")
hiv_prev <- hiv_prev[, ..keep_cols]
# Add additional columns so that this data table will align with malaria (nmei)
# and TB data tables when concatenated
hiv_prev[, currency := "USD"]
hiv_prev[, objective := ""]
hiv_prev[, upper_SDA := ""]
# Reset columns order to align with other incoming extractions data
setcolorder(hiv_prev, NEW_EXTRACT_COLUMN_NAMES)
# Clean up columns to standardize with rest of database
hiv_prev <- clean_extraction_columns(hiv_prev)
# Add "HIV" as a flag to this table's rows
hiv_prev[, component_name := "HIV"]
#------------------------------------------------------------------------------#

#### #----#                     Malaria Previous                     #----# ####
# 2018-2021 data
nmei_list <- list()
i <- 1
for (year in 2017:2021) {
  if (year != 2017) {
    dt <- fread(paste0(GFATM_RAW_BASE, "FGH_", year, 
                       "/Malaria Extractions/New MAL Projects FGH", year, 
                       ".csv"))
  } else {
    dt <- setDT(read_dta(paste0(GFATM_RAW_BASE,
                          "FGH_2018/Malaria Extractions/",
                          "Malaria Extract Raw through FGH2017.dta")))
  }
  # Set column names to standard names
  names(dt) <- NEW_EXTRACT_COLUMN_NAMES
  # Clean up columns to standardize with rest of database
  dt <- clean_extraction_columns(dt)
  # Add to list of data tables
  nmei_list[[i]] <- dt
  i = i + 1
}
# Concatenate data.tables in `nmei_list` vertically
nmei_prev <- unique(rbindlist(nmei_list), by = UNIQUE_ID_COLS)

## Validation
# Check number of duplicate rows removed
# expect_equal(dim(nmei_prev)[1], sum(sapply(nmei_list, function (x) dim(x)[1])))
# Check that N columns stays the same
expect_true(all(dim(nmei_prev)[2] == sapply(nmei_list, dim)[2,]))

# Add "Malaria" as a flag to this table's rows
nmei_prev[, component_name := "Malaria"]
#------------------------------------------------------------------------------#

#### #----#                       TB Previous                        #----# ####
## 2017-2021 TB Data (FGH 2018 dataset appears to include 2017 values as well)
# Read in and bind
tb_list <- list()
i <- 1
for (year in 2018:2021) {
  if (year >= 2020) {
    dt <- fread(paste0(GFATM_RAW_BASE, "FGH_", year, 
                       "/TB Extractions/New TB Projects FGH", year, ".csv"))
  } else {
    dt <- setDT(read_excel(paste0(GFATM_RAW_BASE, "FGH_", year,
                                  "/TB Extractions/New TB Projects FGH",
                                  year, ".xlsx")))
  }
  # Set column names to standard names
  names(dt) <- NEW_EXTRACT_COLUMN_NAMES
  # Clean up columns to standardize with rest of database
  dt <- clean_extraction_columns(dt)
  # Add to list of data tables
  tb_list[[i]] <- dt
  i = i + 1
}

## 2003-2016 TB Data
# Read in and append to recent years
tb_old <- setDT(read_dta(paste0(
  dah.roots$j, 
  "FILEPATH/TB Aggregate 2003_2016.dta"
)))
# Collapse 
to_calc <- names(tb_old)[names(tb_old) %like% "total_"]
tb_old <- tb_old[, lapply(.SD, sum, na.rm=T), 
                 by=c("grant_number", "category",
                      "objective", "SDA", "country", "currency"),
                 .SDcols=to_calc]
# Reshape long
tb_old <- melt(tb_old, measure.vars = to_calc)
tb_old[, variable := as.integer(gsub("total_", "", as.character(variable)))]
# Change SDA to uppercase, drop original SDA column
tb_old[, upper_SDA := toupper(SDA)]
tb_old[, SDA := NULL]
# Reset names to align with other incoming extractions data
setcolorder(tb_old, c("country", "grant_number", "currency", "category", 
                      "objective", "upper_SDA", "variable", "value"))
names(tb_old) <- NEW_EXTRACT_COLUMN_NAMES
# 17M in DAH of whatever the currency is)
tb_old[currency %in% c("USD & EURO", "EUR"), currency := "EURO"]

## Bind 2003-2016 and 2017-2021 data
tb_list <- append(tb_list, list(tb_old))
# Concatenate data.tables in `tb_list` vertically
tb_prev <- unique(rbindlist(tb_list), by = UNIQUE_ID_COLS)

## Validation
# Check number of duplicate rows removed
# Check that N columns stays the same
expect_true(all(dim(tb_prev)[2] == sapply(tb_list, dim)[2,]))

# Add "Tuberculosis" as a flag to this table's rows
tb_prev[, component_name := "Tuberculosis"]
#------------------------------------------------------------------------------#

#### #----#                Combined previous dataset                 #----# ####
## Create full previous extractions dataset so the above never has to be done 
## again
all_prev_list <- list(hiv_prev, nmei_prev, tb_prev)
all_prev <- unique(rbindlist(all_prev_list), 
                  by = UNIQUE_ID_COLS)

all_prev <- unique(rbindlist(all_prev_list), 
                   by = UNIQUE_ID_COLS)

## Validation
# Check that N columns stays the same
expect_true(all(dim(all_prev)[2] == sapply(all_prev_list, dim)[2,]))

#------------------------------------------------------------------------------#

#### #----#              Current report year's download              #----# ####
## Read in current year data
cat("  Read in current data\n")
curr <- fread(paste0(
  get_path("GFATM", "raw"), 
  "New_Projects_FGH", 
  dah.roots$report_year, 
  ".csv"
))

## Data prep and cleaning
cat("  Prep & clean current data\n")
# Change agreement title col name to "category" to match with previous years' 
# extractions and keyword function's expectation
setnames(curr, "grant_agreement_title", "objective")
# Add additional columns so that this data table will align with previous
# years' extraction datasets when concatenated
curr[, currency := "USD"]
curr[, upper_SDA := ""]
# Reset columns order to align with other incoming extractions data
setcolorder(curr, c(NEW_EXTRACT_COLUMN_NAMES, "component_name",
                    names(curr)[names(curr) %like% "title_flag_"]))
# Clean up columns to standardize with rest of database
curr <- clean_extraction_columns(curr)
#------------------------------------------------------------------------------#

#### #----#       Combine previous + current datasets & clean        #----# ####
dt_all <- unique(rbind(all_prev, curr, fill = T), by = UNIQUE_ID_COLS)
cat(paste0(
  "  Combined extractions (all time) number of rows at the Project ID + year ",
  "grain: ",
  yellow(dim(dt_all)[1]),
  ".\n"
))
# Remove rows where budget is 0
dt_all <- dt_all[budget_annual != 0, ]
cat(paste0(
  "  Combined extractions (all time) number of rows at the Project ID + year ",
  "grain, with annual budget greater than zero: ",
  yellow(dim(dt_all)[1]),
  ".\n"
))
#------------------------------------------------------------------------------#

#### #----#                 Prep for Keyword Search                  #----# ####
cat("  Keyword search prep\n")
dt_all <- dt_all[year <= dah.roots$report_year, ]
cat(paste0("  Combined extractions (through ", yellow(dah.roots$report_year),
           ") number of rows at the Project ID + year ",
           "grain, with annual budget greater than zero: ",
           yellow(dim(dt_all)[1]), ".\n"))
cat(paste0("  Number of unique Project IDs (through ",
           yellow(dah.roots$report_year), "): ",
           yellow(length(unique(dt_all$PROJECT_ID))), ".\n"))

# Append component "flags" to `category` column for sake of keyword search in 
# cases where we have information from The Global Fund regarding component 
# (aka whether it is funding for HIV and/or Malaria and/or Tuberculosis)
dt_all[component_name %in% c("HIV", "Tuberculosis", "Malaria"), 
       category := paste(toupper(component_name), category)]
# When multicomponent and we have keywords in grant_agreement_title, append
# to `category` the components for which we have keywords.
dt_all[component_name == "Multicomponent" & title_flag_hiv == 1, 
       category := paste("HIV", category)]
dt_all[component_name == "Multicomponent" & title_flag_mal == 1, 
       category := paste("MALARIA", category)]
dt_all[component_name == "Multicomponent" & title_flag_tb == 1, 
       category := paste("TUBERCULOSIS", category)]

## Reconcile columns with what keyword search expects
drop_cols <- c("component_name", 
               names(dt_all)[names(dt_all) %like% "title_flag_"])
dt_all[, (drop_cols) := NULL]
#------------------------------------------------------------------------------#


### TMP Validation section
###
library(haven)
hiv_pre_kws_prev <- setDT(read_dta("FILEPATH/hiv_pre_kws.dta"))
tb_pre_kws_prev <- setDT(read_dta("FILEPATH/tb_pre_kws.dta"))
mal_pre_kws_prev <- setDT(read_dta("FILEPATH/mal_pre_kws.dta"))
length(unique(hiv_pre_kws_prev$Grant))
length(unique(tb_pre_kws_prev$grant_number))
length(unique(mal_pre_kws_prev$ID))
length(unique(dt_all$PROJECT_ID))
dim(hiv_pre_kws_prev)
dim(unique(hiv_pre_kws_prev, by = c("grant", "Grant", "category", "year")))

setorder(hiv_pre_kws_prev, grant, category, year)
setcolorder(hiv_pre_kws_prev, c("grant", "category", "year"))
subset(hiv_pre_kws_prev, duplicated(hiv_pre_kws_prev,
                                    by = c("grant", "Grant", "category", "year")))
###
###



#### #----#             Save dataset for keyword search              #----# ####
cat("  Save pre-keyword search Sdata\n")
save_dataset(dt_all, "all_pre_kws", "GFATM", "int")
save_dataset(dt_all, "all_pre_kws", "GFATM", "int", format = "dta")
#------------------------------------------------------------------------------#

check <- fread(get_path("GFATM", "int", "all_pre_kws.csv"))
