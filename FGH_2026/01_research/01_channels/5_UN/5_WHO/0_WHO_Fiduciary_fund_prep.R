#-----#    !---- DOCSTRING ----!    ####
# Project: FGH
# Purpose: Automating the cleaning/formatting process for the Fiduciary Fund dataset
#--------------------------------------------------------------------------------------#
rm(list=ls(all.names = TRUE))
library(openxlsx)


code_repo <- 'FILEPATH'


report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))


ff_raw <- fread(get_path("WHO", "raw", "WHO_[prev_abrv_year]_VFHP_FIDUCIARY_FUND_raw.csv"))
names(ff_raw) <- tolower(names(ff_raw))
ff_raw <- ff_raw[, .(donor_name = contributor, total = `grand total (note 1)`)]
ff_raw <- ff_raw[!is.na(donor_name) &
                     tolower(donor_name) != "contributor" &
                     trimws(donor_name) != "" &
                     ! donor_name %ilike% "grand total" &
                     ! donor_name %ilike% "member states"]
ff_raw <- ff_raw[total != ""]


#
# fill in missing income_sector, income_type, donor_country
#

# load previous year for reference
prev <- fread(get_path("WHO", "raw", "WHO_[prev_abrv_year]_Fiduciary_fund.csv",
                       report_year = dah_cfg$prev_report_year,
                       prev_abrv_year = dah_cfg$prev_abrv_year - 1))
names(prev) <- tolower(names(prev))
prev[income_sector == "", income_sector := NA_character_]
prev[income_type == "", income_type := NA_character_]
prev[donor_country == "", donor_country := NA_character_]

prev <- unique(prev[, .(donor_name, income_sector, income_type, donor_country)])

# merge
ff <- merge(ff_raw, prev,
            by = "donor_name", all.x = TRUE)

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
donors[, iso_code := NULL]
donors[, donor_name := NULL]

ff[, donor_clean := string_to_std_ascii(donor_name)]
ff <- merge(ff, unique(donors),
            by = "donor_clean",
            all.x = TRUE, suffixes = c("", "_new"))
ff[is.na(income_sector), income_sector := income_sector_new]
ff[is.na(income_type), income_type := income_type_new]
ff[is.na(donor_country), donor_country := donor_country_new]
ff[, grep("_new", names(ff)) := NULL]
ff[, `:=`(
    donor_name = donor_clean,
    donor_clean = NULL
)]

## drop bad rows
ff <- ff[! trimws(donor_name) %in% c(
    "REFUNDS TO DONORS NOTE 3"
)]

## negative values are surrounded in parentheses like (1000) - so donor most
##  likely needs to be dropped unless there is some special case
stopifnot( ff[grep(")", total, fixed = TRUE), .N] == 0 )




val_cols <- "total"
ff[, (val_cols) := lapply(.SD, \(x) {
    # gsub non numeric characters except minus sign or period
    x <- gsub("[^0-9.-]", "", x)
    x[x == "-"] <- NA
    as.numeric(x)
}), .SDcols = val_cols]


save_dataset(ff, 'WHO_[prev_abrv_year]_Fiduciary_fund', 'WHO', 'raw')
