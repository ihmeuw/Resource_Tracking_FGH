#### #----#                    Docstring                    #----# ####
# Project:  FGH
# Purpose:  Clean bilateral data (step 3 and 4 of previous 1_CRS file)
#---------------------------------------------------------------------#

#----# Environment Prep #----# ####
# System prep
rm(list=ls(all.names = TRUE))

library(duckdb)
library(dplyr)


if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}
report_year <- 2024

source(paste0(code_repo, '/FGH_', report_year, '/utils.R'))
source(paste0(dah.roots$k, "FILEPATH/get_location_metadata.R"))
pacman::p_load(readstata13, crayon)
# Variable prep
update_mmyy <- get_dah_param('CRS', 'update_MMYY')

#---------------------------------------------------------------------#

cat('\n\n')
cat(green(' ####################################\n'))
cat(green(' #### CRS CLEAN ALL SECTORS DATA ####\n'))
cat(green(' ####################################\n\n'))


cat('  Load location metadata\n')
#----# Load location metadata #----# ####
locs <- fread(get_path("meta", "locs", "fgh_location_set.csv"))
locs <- locs[level == 3, .(ihme_loc_id, location_name)]
setnames(locs, "location_name", "donor_name")

# get recipient locs from previous channel update
prev <- get_dah_param("crs", "prev_update_mmyy")
recipient_locs <- unique(fread(
    get_path("CRS", "fin", "B_CRS_[crs.update_mmyy]_INTNEW.csv",
             crs.update_mmyy = prev),
    select = c("recipient_name", "ISO3_RC")
))

wb_group <- fread(get_path('meta', 'locs', "wb_historical_incgrps.csv"))
#---------------------------------------------------------------------#

cat('  Clean CRS data\n')
#----# Clean data #----# ####

# Load CRS via duckdb to avoid loading data into memory if we don't need to
con <- duckdb::dbConnect(duckdb::duckdb())

crs_parq_path <- get_path("crs", "raw", "CRS_1973_[crs.data_year].parquet")
crs_tbl <- dplyr::tbl(con, paste0("read_parquet('", crs_parq_path, "')"))


# Filter down to bilateral donors and official development assistance flows
bilat_codes <- c(1:12, 18, 20, 21, 22, 40, 50, 61, 68, 69, 75, 76, 82, 84,
                 301, 302, 701, 742, 801, 820, 576)
crs <- crs_tbl |>
    filter(donor_code %in% bilat_codes | donor_name == "EU Institutions",
           flow_code %in% c(11, 13, 19)) |>
    collect() |>
    as.data.table()

duckdb::dbDisconnect(con)

if (crs[, uniqueN(donor_code)] != length(bilat_codes) + 1) {
    stop("Ensure your data contains only bilateral donors")
}
if (crs[, uniqueN(donor_code)] != length(dah_cfg$crs$donors)) {
    stop("Ensure we are tracking the correct donors")
}


# Adjust recipient codes
crs[recipient_code %in% c(1033, 1034, 1035), recipient_code := 889 ] # oceania
crs[recipient_code %in% c(1030, 1027, 1028, 1029), recipient_code := 289 ] # Sub-sahara
crs[recipient_code %in% c(1032), recipient_code := 389 ] # North America


# Renaming monetary variables
setnames(crs,
         c("usd_commitment_defl", "usd_disbursement_defl", "usd_received_defl", "usd_adjustment_defl", "usd_amount_untied_defl", "usd_amount_partial_tied_defl", "usd_amounttied_defl"),
         c("commitment_constant", "disbursement_constant", "received_constant", "adjustment_constant", "amountuntied_constant", "amountpartialtied_constant", "amounttied_constant"))

setnames(crs,
         c("usd_commitment", "usd_disbursement", "usd_received", "usd_adjustment", "usd_amount_untied", "usd_amount_partial_tied", "usd_amount_tied", "usd_irtc", "usd_expert_commitment", "usd_expert_extended", "usd_export_credit", "usd_grant_equiv", "usd_interest", "usd_outstanding", "usd_arrears_principal", "usd_arrears_interest"),
         c("commitment_current", "disbursement_current", "received_current", "adjustment_current", "amountuntied_current", "amountpartialtied_current", "amounttied_current", "irtc_current", "expert_commitment_current", "expert_extended_current", "export_credit_current", "grantequiv_current", "interest_current", "outstanding_current", "arrears_principal_current", "arrears_interest_current"))

# merge on donor ISO codes
crs[donor_name == "Korea", donor_name := "S Korea"]
crs <- merge(crs, locs[, .(donor_name, ihme_loc_id)],
             by = "donor_name", all.x = TRUE)
setnames(crs, "ihme_loc_id", "isocode")

crs[donor_name == "EU Institutions", isocode := "EC"]
crs[donor_name == "United States", isocode := "USA"]
crs[donor_name == "S Korea", isocode := "KOR"]
crs[donor_name == "Slovak Republic", isocode := "SVK"]

if (crs[is.na(isocode), .N] != 0) {
    stop("There are missing donor iso codes")
}

# merge in recipient iso codes
crs <- merge(crs, recipient_locs, by = "recipient_name", all.x = TRUE)

crs[, ISO3_RC := fcase(
    recipient_name == "Aruba", "ABW",
    recipient_name == "Bahamas", "BHS",
    recipient_name == "Bermuda", "BMU",
    recipient_name == "Brunei Darussalam", "BRN",
    recipient_name == "Cayman Islands", "CYM",
    recipient_name == "Cyprus", "CYP",
    recipient_name == "Côte d'Ivoire", "CIV",
    recipient_name == "Hong Kong (China)", "HKG",
    recipient_name == "Israel", "ISR",
    recipient_name == "Kuwait", "KWT",
    recipient_name == "Macau (China)", "MAC",
    recipient_name == "Qatar", "QAT",
    recipient_name == "Singapore", "SGP",
    recipient_name == "Slovenia", "SVN",
    recipient_name == "Türkiye", "TUR",
    recipient_name == "United Arab Emirates", "ARE",
    rep_len(TRUE, .N), ISO3_RC
)]

crs[recipient_name %in% c("Melanesia, regional", "Micronesia, regional"),
    ISO3_RC := "QTA"] # same as oceania, regional

# fix some regional codes
crs[recipient_code == 1032, ISO3_RC := 'QNC']
crs[recipient_code %in% c(1027, 1028, 1029, 1030, 1033), ISO3_RC := 'QZA']

if (crs[is.na(ISO3_RC), .N] != 0) {
    stop("There are missing recipient iso codes")
}

# merge in World Bank income groups
crs <- merge(crs,
             wb_group[INC_GROUP != "", .(ISO3_RC, year = YEAR, INC_GROUP)],
             by = c("ISO3_RC", "year"), all.x = TRUE)

#---------------------------------------------------------------------#
cat('  Save datasets\n')
#----# Save datasets #----# ####

#full dataset
save_dataset(crs,
             'B_CRS_ALL_SECTORS_[crs.update_mmyy]',
             'CRS', 'int')

# save totals by donor and sector - used for DAC analysis in 1i_DAC_CREATE.R
crs_main <- crs[year >= 1990]
crs_main <- crs[, lapply(.SD, sum, na.rm = TRUE),
                by = c("isocode", "sector_code", "year"),
                .SDcols = c("commitment_current", "disbursement_current",
                            "commitment_constant", "disbursement_constant")]

save_dataset(crs_main,
             paste0('B_CRS_[crs.update_mmyy]_BILODA_BYDONOR_BYSECTOR'),
             'DAC', 'fin')
#---------------------------------------------------------------------#
cat('  Done\n')
