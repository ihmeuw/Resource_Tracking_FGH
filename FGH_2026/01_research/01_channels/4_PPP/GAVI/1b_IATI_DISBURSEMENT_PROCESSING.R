#----# Docstring #----# ###
# Project:  FGH 
# Purpose:  Process GAVI COVID data
#---------------------# 
library(dplyr)
library(tidyr)
rm(list = ls(all.names = TRUE))
code_repo <- 'FILEPATH'


# Variable Prep
report_year <- 2024
source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))

#
# Load and prep GAVI's IATI transaction data
#

iati <- fread(paste0(
    dah_cfg$j,
    "/FILEPATH/FGH_", report_year,
    "/iati_clean/gavi.csv"
))

# drop transactions with missing or zero value
iati <- iati[!is.na(trans_value) & trans_value != 0]
# keep only outgoing transactions
iati <- iati[outgoing_flag == TRUE]

if (! all(unique(iati$trans_currency) == "USD")) {
  stop("There are transactions reported in a currency other than USD. ",
       "Standardize currencies before aggregating transaction value.")
}

iati[, title_narr := string_to_std_ascii(title_narr)]

#Filter for correct rows
## Drop COVAX disbursements, which are estimated via a separate pipeline:
iati <- iati[! iati_identifier %like% "COVAX"]

iati[, COVID := 0]
iati[grepl("Covid|Coronavirus", title_narr, ignore.case = TRUE) | 
         grepl("Covid|Coronavirus", description_general_lang, ignore.case = TRUE),
     COVID := 1]
iati[iati_identifier %like% "COVAX", COVID := 1]
iati <- iati[COVID == 0]

dt <- iati[trans_year > 2021]


setnames(dt, old = "trans_year", new = "YEAR")
setnames(dt, old = "recip_iso3", new = "iso3_rc")
setnames(dt, old = "description_general_narr", new = "PURPOSE")
setnames(dt, old = "receiver", new = "RECIPIENT_AGENCY")
setnames(dt, old = "recip", new = "countryname")
dt[, PROGRAM := sub(".*-", "", iati_identifier)]
dt[, PROGRAM_TYPE := tolower(PROGRAM)]

# Aggregate to Project level rather than transaction level
dt <- dt[, keyby = .(YEAR, iso3_rc, PROGRAM, PROGRAM_TYPE, countryname, COVID, trans_type, RECIPIENT_AGENCY),
         .(trans_value = sum(trans_value))]


# Additional column fixes
dt[countryname == 'Bolivia', countryname := 'Bolivia (Plurinational State of)']
dt[countryname == 'Congo, (the Democratic Republic of the)', countryname := 'Democratic Republic of the Congo']
dt[countryname == 'Congo (the Democratic Republic of the)', countryname := 'Democratic Republic of the Congo']
dt[countryname == "Korea (the Democratic People's Republic of)", countryname := 'Republic of Korea']
dt[countryname == "Lao People's Democratic Republic (the)", countryname := 'Lao People\'s Democratic Republic']
dt[countryname == 'Moldova (the Republic of)', countryname := 'Republic of Moldova']
dt[countryname == 'Tanzania, the United Republic of', countryname := 'United Republic of Tanzania']
dt[countryname == 'Palestine, State of', countryname := 'Palestine']
dt[, countryname := gsub(" \\(the\\)", "", countryname)]


# region
region_codes <- fread(get_path("meta", "locs","fgh_location_set.csv"))[level==3, c('ihme_loc_id', 'location_name', 'region_name')]
dt <- merge(dt, region_codes[, .(location_name, region_name)], by.x = 'countryname', by.y = 'location_name', all.x = T)
setnames(dt, 'region_name', 'REGION')


#split
db <- dt[trans_type == "Disbursement"]
db <- db[, trans_type := NULL]
setnames(db, old = "countryname", new = "COUNTRY")
# Rename columns to have "YR_" prefix
dbwide <- db %>%
  mutate(YEAR = paste0("YR_", YEAR)) %>%
  pivot_wider(
    names_from = YEAR,
    values_from = trans_value
  ) %>%
  select(-iso3_rc)
save_dataset(dbwide, "Disbursements", "GAVI", "int", )

#Commitments
com <- dt[trans_type == "Outgoing Commitment"]
com <- com[, trans_type := NULL]
setnames(com, old = "countryname", new = "COUNTRY")
# Rename columns to have "YR_" prefix
comwide <- com %>%
  mutate(YEAR = paste0("YR_", YEAR)) %>%
  pivot_wider(
    names_from = YEAR,
    values_from = trans_value
  ) %>%
  select(-iso3_rc)
save_dataset(comwide, "Commitments", "GAVI", "int", )

