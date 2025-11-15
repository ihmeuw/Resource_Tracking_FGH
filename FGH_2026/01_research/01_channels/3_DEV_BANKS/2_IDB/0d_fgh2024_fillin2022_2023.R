#
# We did not receive 2022 or 2023 correspondent data, so we fill in the disbursements
#   using reported disbursements to IATI.
#
# These are appended onto the last round of correspondent data (for 2021), contained
# in IDB_2015_2021_disbursements.csv
# 
# Then the correspondent data received for 2024 is manually appended onto that
# in IDB_2015_2024_disbursements.csv
#
# This script won't be needed in future years, but is kept for reference
code_repo <- 'FILEPATH'


report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))




#
# Load IATI data to get disbursement data
#
dt <- fread(get_path("idb", "int", "iadb_iati_clean.csv"))

stopifnot(all(unique(dt$trans_currency) == "USD"))

dt <- dt[trans_type == "Disbursement"]
setorder(dt, trans_year)


#
# Extract IDB-reported sectors and filter to Health sector
#

## they report their own sectors in addition to OECD sectors.
## get position of reporting org sector
vocabs <- dt[, unique(sector_vocab)]
stopifnot(length(vocabs) == 1)
vocab_pos <- which(strsplit(vocabs, "<SEP>")[[1]] == "Reporting Organisation")

## extract
dt[, reporter_sector := unlist(lapply(
    strsplit(sector_narr, "<SEP>"),
    function(x) x[vocab_pos]
))]


health <- dt[reporter_sector == "HEALTH<SEP1>SALUD"] # (reported in en and es)



#
# Standardize health disbursements for combining with correspondent data
#

## identify COVID projects and COVID proportions
health[, srchstr := string_to_std_ascii(title_narr)]

covid_regex <- paste(toupper(dah_cfg$covid_intercept_keywords), collapse = "|")
health[, covid := grepl(covid_regex, srchstr)]
health[, covid_prop := as.integer(covid)]


## merge on iso3 codes and location names
isomap <- fread("FILEPATH/country_iso2to3.csv")
health <- merge(
    health,
    isomap,
    by.x = "recip_iso2",
    by.y = "alpha2",
    all.x = TRUE
)
stopifnot(health[is.na(alpha3), .N] == 0)

locs <- fread(get_path("meta", "locs", "fgh_location_set.csv"))
health <- merge(
    health,
    locs[, .(ihme_loc_id, location_name)],
    by.x = "alpha3",
    by.y = "ihme_loc_id",
    all.x = TRUE
)
stopifnot(health[is.na(location_name), .N] == 0)


## extract project id from iati_identifier
health[, proj_id := gsub("XI-IATI-IADB-", "", iati_identifier)]


## extract engligh title from english-spanish concatenation
health[, proj_name := unlist(lapply(
    strsplit(title_narr, "<SEP>"),
    function(x) x[1]
))]


## standardize data to match the data we receive from correspondent
health_fin <- health[trans_year %in% 2022:2023,
                     .(Disbursement = sum(trans_value)),
                     by = .(proj_id,
                            proj_name,
                            YEAR = trans_year,
                            ISO3_RC = alpha3,
                            RECIPIENT_COUNTRY = location_name,
                            covid_prop)]



#
# Append IATI Disbursements onto Correspondent Data
#
corr_data <- fread(get_path("idb", "raw", "IDB_2015_2021_disbursements.csv"))

corr_data <- rbind(
    corr_data,
    health_fin
)
setorder(corr_data, YEAR, proj_id)



#
# Save
#
fwrite(
    corr_data,
    get_path("idb", "raw", "IDB_2015_2023_disbursements.csv")
)

message("* Saved IDB_2015_2023_disbursements.csv")
