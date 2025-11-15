#### #----#                        Docstring                         #----# ####
#' Project:         FGH 
#'    
#' Purpose:         HIV Keyword Search Cleanup
#------------------------------------------------------------------------------#

####################### #----# ENVIRONMENT SETUP #----# ########################
rm(list=ls())
if (!exists("code_repo")) {
  code_repo <- 'FILEPATH'
}

## Source functions

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr, haven, testthat)

## Local CONSTANTS
# For validation (epsilon)
TOLERANCE <- 1e-6
# Paths
FGH_BASE <- paste0(
  dah.roots$j,
  "FILEPATH/FGH_",
  dah.roots$report_year,
  "/"
)
COUNRTY_FEATURES <- get_path("meta", "locs")
XRATES <- paste0(
  get_path("meta", "rates"),
  "OECD_XRATES_NattoUSD_1950_",
  dah.roots$report_year,
  ".csv"
)
# Manually discovered these projects where the GEO is Indonesia but
# PROJECT ID includes India ISO3
INDONESIA_MISCLASSIFIED <- c("IND-H-MOH", "IND-S10-G17-H", "IND-809-G12-T",
                             "IND-102-G01-T-00", "IND-809-G10-T",
                             "IND-809-G11-T", "IND-T-MOH")
# TODO: add ISO3 fixes to an FGH central constants location
ISO_DICT <- c(
  BAN = "BGD",
  BOT = "BWA",
  BUL = "BGR",
  BUR = "BFA",
  CAM = "KHM",
  CEF = "CAF",
  DMR = "DOM",
  ERT = "ERI",
  GHN = "GHA",
  GUA = "GTM",
  GYA = "GUY",
  IDA = "IND",
  MAE = "DOM",
  MAL = "MLI",
  MAN = "DOM",
  MAS = "LSO",
  MAW = "BEN",
  MOL = "MDA",
  RWN = "RWA",
  UGD = "UGA",
  VTN = "VNM",
  WRL = "WLD",
  ZAM = "ZMB",
  ZAN = "TZA",
  ZAR = "COD",
  ZIM = "ZWE",
  DJB = "DJI",
  NGR = "NER",
  SUD = "SDN",
  MLW = "MWI",
  SNG = "SEN",
  TNZ = "TZA",
  MYN = "MMR",
  NEP = "NPL",
  NMB = "NAM",
  CDG = "COG",
  TMP = "TLS",
  PKS = "PAK",
  SLR = "LKA",
  SRI = "LKA",
  SRL = "LKA",
  TAJ = "TJK",
  COR = "CRI",
  INA = "IND",
  MNT = "MNE",
  MON = "MNG",
  MOR = "MAR",
  ROM = "ROU",
  SAF = "ZAF",
  TAN = "TZA",
  TKA = "TKM",
  KOS = "KSV"
)
#------------------------------------------------------------------------------#


######################## #----# HELPER FUNCTIONS #----# ########################
#' @title calc_share
#' @description Literally multiplies two numbers. Not sure why this isn't just 
#' called "multiply" or why it exists in the first place, but preserving for 
#' now.
calc_share <- function (fraction, total) {
  return ( fraction * total )
}
#------------------------------------------------------------------------------#


############################## #----# MAIN #----# ##############################
cat("\n\n")
cat(green(" ##########################################\n"))
cat(green(" #### GFATM KEYWORD SEARCH CLEANUP ####\n"))
cat(green(" ##########################################\n\n"))

#### #----#                  Read in post_kws data                   #----# ####
cat("  Read in post-keyword search data\n")
pkws <- setDT(read_dta(paste0(get_path("GFATM", "int"), "all_post_kws.dta")))
pkws <- pkws[, c("PROJECT_ID", "category", "objective", "upper_SDA", "currency",
                "year", "budget_annual", "geo_name",
                names(pkws)[str_detect(names(pkws), "final_")]),
            with = F]
# De-string annual budgets column
pkws[, budget_annual := gsub(",", "", budget_annual)]
pkws[, budget_annual := as.numeric(budget_annual)]
# Define set of final fractions' columns
final_fraction_cols <- names(pkws)[str_detect(names(pkws), 
                                              "final_")]
new_fraction_cols <- gsub("final_", "", final_fraction_cols)
# Reset names to remove "final_" from final_fraction_cols colnames
setnames(pkws, 
         final_fraction_cols,
         new_fraction_cols)
#------------------------------------------------------------------------------#

#### #----#                      Check HFA sums                      #----# ####
cat("  Check HFA sums\n")
## Validation
non_total_fraction_cols <- names(pkws)[str_detect(names(pkws),
                                                  "^(?!total).*_frct")]
# Row sum of non-total fraction cols should always be 1
val_rowsums <- pkws[, rowSums(.SD, na.rm = T),
                    .SDcols = non_total_fraction_cols]
# Warn if any row sums to not one, with pre-defined tolerance level
if (!all(abs(val_rowsums - 1) <= TOLERANCE)) {
  stop("At least one row does not sum to approximately 1")
}

## Adjust fractions in cases where row sums are sufficiently close to, but not 
## exactly 1
# !IMPORTANT! the above validation needs to pass before continuing with the 
# below adjustment
pkws[pkws[, abs(rowSums(.SD) - 1) - TOLERANCE != 0,
          .SDcols = non_total_fraction_cols],
     (non_total_fraction_cols) := .SD / rowSums(.SD),
     .SDcols = non_total_fraction_cols]
#------------------------------------------------------------------------------#

#### #----#           Collapse sum and split by tagged PAs           #----# ####
cat("  Collapse sum and split by tagged PAs\n")
split <- copy(pkws)
## Take shares of annual budgets using fraction columns and total
## annual budget column
budget_share_cols <- gsub("_frct", "_DAH", new_fraction_cols)
split[, (budget_share_cols) := Map(calc_share, fraction = .SD,
                                   total = list(budget_annual)),
      .SDcols = new_fraction_cols]
#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

#### #----#                     Currency Convert                     #----# ####
cat("  Currency convert\n")
split[currency == "", currency := "USD"]
# Clean up where currency is meant to be euros
split[currency == "EUR", currency := "EURO"]

## Merge with XRATES data for EURO rows
xrates <- fread(XRATES)
# Take OECD exchange rate of EA19 countries to USD for first year of 
# budgets for all projects with currency 
# listed as "EURO"
ea19_rates <- xrates[LOCATION == "EA19", c("TIME", "Value")]
setnames(ea19_rates, "TIME", "year")
split <- merge(split, ea19_rates, by = c("year"),  all.x = T)
# Take OECD exchange rate of EA19 countries to USD for first year of 
# budgets for all projects with currency 
# listed as "EURO"
split[currency == "EURO", (budget_share_cols) := .SD / Value,
    .SDcols = budget_share_cols]

# Take sum of budget share columns for each project/year (i.e. annual totals.
# gets rid of categories, objectives, upper_SDA descriptions)
# TODO: I guess we're totally done with fractions and project details?
split <- split[, lapply(.SD, sum), by = c("year", "PROJECT_ID"),
             .SDcols = budget_share_cols]
#------------------------------------------------------------------------------#

#### #----#                      Add ISO codes                       #----# ####
cat("  Add ISO codes\n")
isos <- fread(paste0(COUNRTY_FEATURES, "fgh_location_set.csv"))
isos <- isos[, c("ihme_loc_id", "location_name", "region_name")]
colnames(isos) <- c("iso3", "countryname_ihme", "gbd_region")
isos <- dplyr::distinct(isos)
# Fix columns before merging
split[, iso3 := tstrsplit(PROJECT_ID, "-", keep = 1)]
# Manually replace ISO3s missing in IHME locs with pre-defined replacements
split[, iso3 := str_replace_all(iso3, ISO_DICT)]
# Some Indonesia projects mistakenly classified to India
split[PROJECT_ID %in% INDONESIA_MISCLASSIFIED, iso3 := "IDN"]
# Exclude one Serbia project specified as Saudi Arabia GEO, but otherwise set
# "SER" to Serbia ISO3, "SRB"
split[iso3 == "SER" & PROJECT_ID == "SER-304-G02-T", iso3 := "SAU"]
split[iso3 == "SER", iso3 := "SRB"]
split[iso3 == "KOS" | iso3 == "Kosovo", iso3 := "KSV"]

## Merge with isos data.table to get IHME country names and regions
split_full <- merge(split, isos, by = "iso3", all.x = T)

## Reset names to standardize
setnames(split_full, c("year", "iso3", "countryname_ihme"),
         c("YEAR", "ISO3_RC", "RECIPIENT_COUNTRY"))

## Remove totals column
recent_all <- split_full[, total_DAH := NULL]
#------------------------------------------------------------------------------#

#### #----#                   HIV historical data                    #----# ####
cat("  Bring in HIV historical data\n")
## Read in data up to 2015(?)
hst <- setDT(read_dta(paste0(
  dah.roots$j,
  "Project/IRH/DAH/RESEARCH/CHANNELS/4_PPP/2_GFATM/DATA/FIN/FGH_2015/",
  "P_GFATM_INTPDB_1990_2015_FGH2015.dta"
)))
# Rename columns to match recent nomenclature
setnames(
  hst,
  c("hiv_unid_DAH", "hiv_hss_DAH"),
  c("hiv_other_DAH", "hiv_hss_other_DAH")
)

## Retain necessary ID columns, DISBURSEMENTS, and non-malaria, non-other
## DAH columns
id_cols <- c("YEAR", "PROJECT_ID", "ISO3_RC", "RECIPIENT_COUNTRY")
# TODO: Why include swap_hss and TB cols, but not other DAH?
value_cols <- c("DISBURSEMENT",
                names(hst)[names(hst) %like% "(tb|hiv|swap_hss).+_DAH"])
hst <- hst[, c(..id_cols, ..value_cols)]

## Recode historical HFAs
# Split out hiv_tb_HFA
# split is 1/2 of TB DAH
hst[, split := hiv_tb_DAH / 2]
# TB Other DAH is now 1/2 of HIV TB DAH
hst[, tb_other_DAH := split]
hst[!is.na(split), hiv_other_DAH := hiv_other_DAH + split]
# And remove the old hiv_tb_DAH col to avoid later confusion
hst[, hiv_tb_DAH := NULL]
hst[, hiv_total := rowSums(.SD, na.rm = T),
    .SDcols = names(hst)[names(hst) %like% "_DAH"]]
hst <- hst[hiv_total != 0, ]
# Generate fraction columns
to_calc <- names(hst)[names(hst) %like% "^hiv_.*_DAH$"]
for (col in to_calc) {
  hst[, eval(gsub("_DAH", "_frct", col)) := get(col) / hiv_total]
}
rm(to_calc, col)

## Add GBD Regions
hst <- merge(hst, isos[, !c("countryname_ihme")], by.x = "ISO3_RC",
             by.y = "iso3", all.x = T)

## Sum DAH columns and DISBURSEMENT column by ID vars, getting to budgets and
## disbursements at the year/project/country level
hst_hiv <- hst[, lapply(.SD, sum), by = id_cols,
               .SDcols = value_cols[value_cols != "hiv_tb_DAH"]]
#------------------------------------------------------------------------------#

#### #----#                   MAL historical data                    #----# ####
## Read in MEI 2016 data
hst <- setDT(read_dta(paste0(
  dah.roots$j,
  "FILEPATH",
  "GFATM_projects_58countries.dta"
)))
setnames(hst, "year", "YEAR")

## Load historical deflators
defl_old <- setDT(read_dta(paste0(
  dah.roots$j,
  "FILEPATH/",
  "imf_usgdp_deflators_2015.dta"
)))[, c("YEAR", "GDP_deflator_2015")]

## Merge malaria data and deflators
hst <- merge(hst, defl_old, by = "YEAR", all.x = T)

## Calculate to nominal USD
to_calc <- names(hst)[names(hst) %like% "MEI_"]
for (col in to_calc) {
  hst[, eval(col) := get(col) * GDP_deflator_2015]
}

## Edit HFA names to match recent data columns
setnames(
  hst,
  c("MEI_TREAT_DAH_15", "MEI_DIAG_DAH_15", "MEI_CON_NET_DAH_15",
    "MEI_CON_IRS_DAH_15"),
  c("mal_treat_DAH", "mal_diag_DAH", "mal_con_nets_DAH",
    "mal_con_irs_DAH")
)

## Manually assigned recent PAs based on aggregations of old categories
hst[, mal_comm_con_DAH := rowSums(
  hst[, c("MEI_OUTREACH_DAH_15", "MEI_PARTNER_DAH_15")], na.rm = T)]
hst[, mal_hss_other_DAH := rowSums(
  hst[, c("MEI_SURV_DAH_15", "MEI_MON_DAH_15", "MEI_INFRA_DAH_15",
          "MEI_PROCUR_DAH_15", "MEI_HSS_DAH_15")], na.rm=T)]

## Reset more malaria PA names
setnames(
  hst,
  c("MEI_HR_DAH_15", "MEI_CONTROL_OTHER_DAH_15", "MEI_unall_DAH_15", "Grant"),
  c("mal_hss_hrh_DAH", "mal_con_oth_DAH", "mal_other_DAH", "PROJECT_ID")
)

## Drop pre-2016 columns
to_drop <- names(hst)[names(hst) %like% "_15"]
hst[, eval(to_drop) := NULL]

## Calculate a malaria total column
## TODO: need this at all for HIV, MAL, TB?
mal_cols <- names(hst)[names(hst) %like% "^mal_"]
hst[, mal_total := rowSums(.SD, na.rm = T), .SDcols = mal_cols]

## Add GBD Regions
hst <- merge(hst, isos, by = "iso3", all.x = T)

## Final subset of columns to be bound to recent data rows
hst_mal <- hst[, c("YEAR", "PROJECT_ID", "iso3", "gbd_region", "RECIPIENT_COUNTRY",
               ..mal_cols)]

## Reconcile some column names
setnames(hst_mal, "iso3", "ISO3_RC")
#------------------------------------------------------------------------------#

#### #----#                Append datasets & collapse                #----# ####
cat("  Append datasets & collapse\n")

## Append recent post-kws data to HIV and MAL pre-2016 datasets
data <- recent_all %>%
  rbind(hst_mal, fill = T) %>%
  rbind(hst_hiv, fill = T)

## Fix regions for both recent and historical data
data <- merge(data[, -c("gbd_region")], isos, by.x = "ISO3_RC", by.y = "iso3", all.x = T)
data[ISO3_RC == "KSV", gbd_region := "Central Europe"]
data[ISO3_RC == "Cote d'Ivoire", gbd_region := "Western Sub-Saharan Africa"]

## Take sums of budget share columns by year, project, and recipient/region
data <- data[, lapply(.SD, sum, na.rm = T), 
             by = c("YEAR", "PROJECT_ID", "ISO3_RC", "RECIPIENT_COUNTRY",
                  "gbd_region"), 
             .SDcols = c(names(data)[names(data) %like% "_DAH" & 
                                     !(names(data) %like% "_frct")])]

## Re-create HFA total columns
for (hfa in c("hiv", "mal", "tb")) {
  hfa_cols <- names(data)[names(data) %like% paste0("^", hfa, "_.+_DAH$")]
  # Re-create HFA total columns
  data[, eval(paste0(hfa, "_total")) := rowSums(.SD, na.rm = T), 
             .SDcols = hfa_cols]
}

## Create dah_total sum column
data[, dah_total := rowSums(.SD, na.rm = T),
    .SDcols = names(data)[names(data) %like% "_DAH"]]
data <- data[dah_total != 0, ]

# Generate fraction columns
to_calc <- names(data)[names(data) %like% "_DAH$"]
for (col in to_calc) {
  data[, eval(gsub("_DAH", "_frct", col)) := get(col) / dah_total]
}

non_total_fraction_cols <- names(data)[str_detect(names(data),
                                                 "^(?!total).*_frct")]
# Row sum of non-total fraction cols should always be 1
val_rowsums <- data[, rowSums(.SD, na.rm = T),
                   .SDcols = non_total_fraction_cols]
# Warn if any row sums to not one, with pre-defined tolerance level
# expect_true(all(abs(val_rowsums - 1) <= TOLERANCE))
if (!all(abs(val_rowsums - 1) <= TOLERANCE)) {
  stop("At least one row does not sum to approximately 1")
}

## Set `type` column to "GFATM" for this channel
data[, type := "GFATM"]
#------------------------------------------------------------------------------#

#### #----#                        Validation                        #----# ####
fraction_cols <- names(data)[names(data) %like% "_frct$"]
for (col in fraction_cols) {
  badrows <- which(data[[col]] > 1 | data[[col]] < 0)
  if (length(badrows) > 0) {
    cat(paste0(yellow(col), " is not a valid fraction in at least one row.\n"))
    cat("  rows: ", paste(badrows, collapse = ", "), "\n")
  }
}
#------------------------------------------------------------------------------#

#### #----#                       Save dataset                       #----# ####
cat("  Save dataset\n")
save_dataset(data, "P_GFATM_allprojects_master", "GFATM", "fin")
#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#
