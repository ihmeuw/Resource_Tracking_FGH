#-----#    !---- DOCSTRING ----!    ####
# Project: FGH
# Purpose: Automating the cleaning/formatting process for the General Fund dataset
#--------------------------------------------------------------------------------------#
rm(list = ls(all.names = TRUE))
library(openxlsx)

code_repo <- 'FILEPATH'


report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))


gf_raw <- fread(get_path("WHO", "raw", "WHO_[prev_abrv_year]_VFHP_GENERAL_FUND_raw.csv"))
names(gf_raw) <- tolower(names(gf_raw))
if (! "donor_country" %in% names(gf_raw))
  gf_raw[, donor_country := ""]


#
# fill-in income_sector, income_type, and donor_country
#

# merge with previous years to fill in missing values
prev <- fread(get_path("WHO", "raw", "WHO_[prev_abrv_year]_General_fund.csv",
                       report_year = dah_cfg$prev_report_year,
                       prev_abrv_year = dah_cfg$prev_abrv_year - 1))
prev[income_sector == "", income_sector := NA_character_]
prev[income_type == "", income_type := NA_character_]
prev[donor_country == "", donor_country := NA_character_]
prev <- unique(prev[, .(donor, income_sector, income_type, donor_country)])

gf <- merge(gf_raw, prev, by = "donor", all.x = TRUE, suffixes = c("", "_prev"))
gf[income_sector == "", income_sector := income_sector_prev]
gf[income_type == "", income_type := income_type_prev]
gf[donor_country == "", donor_country := donor_country_prev]
gf[, grep("_prev", names(gf)) := NULL]


# merge with donor database
donors <- fread("FILEPATH/income_sector_and_type_assignments.csv",
                header = TRUE)
names(donors) <- tolower(names(donors))
donors <- unique(donors[, .(donor_name, income_sector, income_type, iso_code)])
locs <- fread(get_path("meta", "locs", "fgh_location_set.csv"))
donors <- merge(donors,
                locs[level == 3, .(iso_code = ihme_loc_id, donor_country = location_name)],
                by = "iso_code", all.x = TRUE)
donors[, donor_clean := string_to_std_ascii(donor_name)]
donors[, `:=`(iso_code = NULL, donor_name = NULL)]
donors[income_sector == "", income_sector := NA_character_]
donors[income_type == "", income_type := NA_character_]
donors[donor_country == "", donor_country := NA_character_]

gf[, donor_clean := string_to_std_ascii(donor)]
gf <- merge(gf, unique(donors),
            by = "donor_clean",
            all.x = TRUE, suffixes = c("", "_new"))
gf[is.na(income_sector), income_sector := income_sector_new]
gf[is.na(income_type), income_type := income_type_new]
gf[is.na(donor_country), donor_country := donor_country_new]
gf[, grep("_new", names(gf)) := NULL]
gf[, `:=`(
    donor = donor_clean,
    donor_clean = NULL
)]

## drop bad rows
gf <- gf[! trimws(donor) %in% c(
    "OTHER AND MISCELLANEOUS RECEIPTS NOTE 2",
    "PASS THROUGH FUNDING",
    "REFUNDS TO DONORS NOTE 3"
)]

## negative values are surrounded in parentheses like (1000) - so donor most
##  likely needs to be dropped unless there is some special case
stopifnot( gf[grep(")", total, fixed = TRUE), .N] == 0 )


val_cols <- "total"
gf[, (val_cols) := lapply(.SD, \(x) {
    # gsub non numeric characters except minus sign or period
    x <- gsub("[^0-9.-]", "", x)
    x[x == "-"] <- NA
    as.numeric(x)
}), .SDcols = val_cols]

names(gf) <- toupper(names(gf))
save_dataset(gf, 'WHO_[prev_abrv_year]_General_fund',
             'WHO', 'raw')
