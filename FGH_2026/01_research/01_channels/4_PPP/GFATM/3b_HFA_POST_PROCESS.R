#### #----#                        Docstring                         #----# ####
#' Project:         FGH
#'    
#' Purpose:         Import & Clean budget data from GFATM for HIV, Malaria, and
#'                  TB
#------------------------------------------------------------------------------#

####################### #----# ENVIRONMENT SETUP #----# ########################
rm(list=ls())
if (!exists("code_repo")) {
  code_repo <- 'FILEPATH'
}

## Source functions

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

## Local CONSTANTS
TOLERANCE <- 1e-6
# Filepaths
COUNTRY_FEATURES <- get_path("meta", "locs")
#------------------------------------------------------------------------------#


############################## #----# MAIN #----# ##############################
cat("\n\n")
cat(green(" ###################################\n"))
cat(green(" #### GFATM DEX BUDGET CLEANING ####\n"))
cat(green(" ###################################\n\n"))

#### #----#                     Import HFA Data                      #----# ####
cat("  Import HFA data\n")
## Read in data from stage 2 of pipeline
dt <- fread(paste0(get_path("GFATM", "fin"), "P_GFATM_allprojects_master.csv"))

# Collapse sum & rowtotal
to_calc <- names(dt)[names(dt) %like% "_DAH" & !(names(dt) %like% "_frct")]
dt[, dah_total := rowSums(.SD), .SDcols = to_calc]
dt_agg <- collapse(dt, "sum", c("YEAR", "PROJECT_ID", "ISO3_RC"), to_calc)
dt_agg[, dah_total := rowSums(.SD), .SDcols = to_calc]
# Generate fraction columns
for (col in to_calc) {
  dt_agg[, eval(paste0(col, "_frct")) := get(col) / dah_total]
  dt_agg[is.na(get(paste0(col, "_frct"))), eval(paste0(col, "_frct")) := 0]
}

## Validation - Checking that sums are always 1
val_rowsums <- dt_agg[, rowSums(.SD),
                      .SDcols = names(dt_agg)[names(dt_agg) %like% "_frct"]]
# Warn if any row sums to not one, with pre-defined tolerance level
# expect_true(all(abs(val_rowsums - 1) <= TOLERANCE))
if (!all(abs(val_rowsums - 1) <= TOLERANCE)) {
  stop("At least one row does not sum to approximately 1")
}

#------------------------------------------------------------------------------#

#### #----#                Calculate regional ratios                 #----# ####
cat("  Calculate regional HIV ratios\n")
dt_reg <- copy(dt)
dt_reg[ISO3_RC %in% c("WRL"), gbd_region := "Global"]
dt_reg[ISO3_RC %in% c("ZAN", "SSD", "QNB", "QPA"), gbd_region := "Eastern Sub-Saharan Africa"] 
dt_reg[ISO3_RC %in% c("CIV", "MAW", "QMA", "QPF"), gbd_region := "Western Sub-Saharan Africa"]
dt_reg[ISO3_RC %in% c("MAF", "MAS"), gbd_region := "Central Sub-Saharan Africa"]
dt_reg[ISO3_RC %in% c("QME", "ZAF"), gbd_region := "Southern Sub-Saharan Africa"]
dt_reg[ISO3_RC %in% c("MCP", "MAM", "QMG", "MAR", "QNC"), gbd_region := "Central Latin America"]
dt_reg[ISO3_RC %in% c("MAC", "MAN", "MAE", "QRA", "MSR"), gbd_region := "Caribbean"]
dt_reg[ISO3_RC %in% c("MAA", "MAT"), gbd_region := "Andean Latin America"]
dt_reg[ISO3_RC %in% c("MSA", "QRC", "QSD"), gbd_region := "South Asia"]
dt_reg[ISO3_RC %in% c("MWP", "QMJ"), gbd_region := "Oceania"]
dt_reg[ISO3_RC %in% c("MEI", "MEA", "QMU", "QSA", "QSE"), gbd_region := "Southeast Asia"]
dt_reg[ISO3_RC %in% c("QMT"), gbd_region := "Central Asia"]
dt_reg[ISO3_RC %in% c("MMM"), gbd_region := "North Africa and Middle East"]
dt_reg[ISO3_RC %in% c("MNE", "SRB", "KSV", "QNA", "QMZ"), gbd_region := "Central Europe"] 
dt_reg[ISO3_RC %in% c("QTA"), gbd_region := "East Asia"]
dt_reg[ISO3_RC %in% c("QPA"), gbd_region := "Southern Sub-Saharan Africa"] 
dt_reg[ISO3_RC %in% c("QRD"), gbd_region := "Tropical Latin America"]
dt_reg[ISO3_RC %in% c("ZAN", "SSD", "QNB", "QPB"), gbd_region := "Eastern Sub-Saharan Africa"]
dt_reg[ISO3_RC %in% c("CIV", "MAW", "QMA", "QPF"), gbd_region := "Western Sub-Saharan Africa"]
dt_reg[ISO3_RC %in% c("MAF", "MAS"), gbd_region := "Central Sub-Saharan Africa"]
dt_reg[ISO3_RC %in% c("MCP", "MAM", "QMG", "MAR", "QNC"), gbd_region := "Central Latin America"]
dt_reg[ISO3_RC %in% c("MAC", "MAN", "MAE", "QRA", "QRB"), gbd_region := "Caribbean"]
dt_reg[ISO3_RC %in% c("MAA", "MAT", "QRD"), gbd_region := "Andean Latin America"]
dt_reg[ISO3_RC %in% c("MSA"), gbd_region := "South Asia"]
dt_reg[ISO3_RC %in% c("MWP", "QMJ", "QUA"), gbd_region := "Oceania"]
dt_reg[ISO3_RC %in% c("MEI", "MEA", "QMU", "QSA", "QSD"), gbd_region := "Southeast Asia"]
dt_reg[ISO3_RC %in% c("QMT"), gbd_region := "Central Asia"]
dt_reg[ISO3_RC %in% c("MMM", "PSE", "QSF"), gbd_region := "North Africa and Middle East"]
dt_reg[ISO3_RC %in% c("MNE", "SRB", "KSV", "QNA", "QMZ", "SER", "MNT"), gbd_region := "Central Europe"]
dt_reg[ISO3_RC %in% c("QME", "QPA"), gbd_region := "Southern Sub-Saharan Africa"]
dt_reg[ISO3_RC %in% c("QRC"), gbd_region := "South Asia"]
dt_reg[ISO3_RC %in% c("QTA"), gbd_region := "East Asia"]

# Collapse sum
dt_reg <- collapse(dt_reg, "sum", "gbd_region", c(to_calc, "dah_total"))
dt_reg <- dt_reg[gbd_region != "", ]
# Generate fraction columns
for (col in to_calc) {
  dt_reg[, eval(paste0(col, "_regionfrct")) := get(col) / dah_total]
}
dt_reg <- dt_reg[, c("gbd_region",
                     names(dt_reg)[names(dt_reg) %like% "frct"]), with=F]

# Check fractions
dt_reg <- rowtotal(dt_reg, "check",
                   names(dt_reg)[names(dt_reg) %like% "frct"])
dt_reg[, check := NULL]
#------------------------------------------------------------------------------#

#### #----#             Save HIV HFA intermediate files              #----# ####
cat("  Save HFA intermediate files\n")
save_dataset(dt_agg, "budget", "GFATM", "int")
save_dataset(dt_reg, "regional_budget", "GFATM", "int")
#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#
