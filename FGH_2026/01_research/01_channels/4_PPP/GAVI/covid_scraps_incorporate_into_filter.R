#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Process GAVI COVID data
#---------------------# 

#----# Environment Prep #----# ####
rm(list=ls())


if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}


# Variable Prep
report_year <- 2024
source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
## check here: https://hub.ihme.washington.edu/spaces/GBDdirectory/pages/240552792/GBD+at-a-glance#GBDataglance-GBD2023
release_id <- 9
codes <- get_path("meta", "locs")
# hardcoded paths as we no longer reproduce it yearly--code availabe in archive
covid2020_path <- "FILEPATH/COVID_2023_data_pre_kws.csv"
covid2021_path <- "FILEPATH/COVID_2021_data_pre_kws.csv"

# Source Packages
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

# Countries
source("FILEPATH/get_location_metadata.R")
isos <-  get_location_metadata(location_set_id = 35, release_id = release_id)[level == 3]
iso_rc <- isos[, c('location_name', 'ihme_loc_id')]

#----------------------------# 

cat('\n\n')
cat(green(' ###################################\n'))
cat(green(' #### Read in 2020 and 2021 data ####\n'))
cat(green(' ###################################\n\n'))

dt_2020 <- fread(covid2020_path)
dt_2021 <- fread(covid2021_path)
dt_2021[, purpose := paste0('Vaccine: ', purpose)]
dt_update <- fread(get_path("GAVI", "int", paste0("COVID_", report_year, "data.csv")))
dt <- rbindlist(list(dt_2020, dt_2021, dt_update), fill = T)

# Final cleaning
dt[, INCOME_SHARE := NULL]

# Use a 1:1 disbursement commitment ratio to fill in amount for 2020 (done in 1b_COVID_PREP). We use the disbursement amount for 2021
dt[year %in% 2021:dah.roots$report_year & is.na(disbursement), disbursement := commitment]
dt[year %in% 2021:dah.roots$report_year, total_amt := disbursement]
dt[is.na(total_amt), total_amt := 0]
setnames(dt, 'disbursement', 'DISBURSEMENT')

dt[, GRANT_LOAN := "GRANT"]

# Merge on income group and fill in missing recipient iso codes
dt <- merge(dt, iso_rc, by.x = 'recipient_country', by.y = 'location_name', all.x = T)
dt[iso3_rc == '', iso3_rc := ihme_loc_id]

ingr <- fread(file.path(codes, 'wb_historical_incgrps.csv')
              )[YEAR == dah.roots$report_year, c('INC_GROUP', 'ISO3_RC')]
dt <- merge(dt, ingr, by.x = 'iso3_rc', by.y = 'ISO3_RC', all.x = T)

dtsave <- dt
#---------------------------#  

cat('\n\n')
cat(green(' ###################################\n'))
cat(green(' #### Run COVID KWS ####\n'))
cat(green(' ###################################\n\n'))

# some keyword cleaning

dt[, purpose := string_to_std_ascii(purpose)]
dt[, purpose := string_to_std_ascii(purpose_2)]
dt[, purpose := string_to_std_ascii(purpose_3)]

#------------------------------------# 

cat('  Save out COVID dataset\n')
#----# Save out COVID dataset #----# ####
dt[, `:=`(channel = 'GAVI')]
dt[, GRANT_LOAN := 'grant']
setnames(dt, 'commitment', 'COMMITMENT')
dt[is.na(COMMITMENT),  COMMITMENT := 0]
dt[is.na(DISBURSEMENT),  DISBURSEMENT := 0]

dt <- dt[, c('year', 'channel', 'DONOR_NAME','INCOME_SECTOR', 'INCOME_TYPE',
             'ISO_CODE', 'iso3_rc', 'recipient_country', 'recipient_agency', 
             'gbd_region', 'INC_GROUP', 'money_type', 'GRANT_LOAN', 'COMMITMENT', 
             'DISBURSEMENT','total_amt'), with=F]


save_dataset(dt, 'COVID_prepped', 'GAVI', 'fin')


#----------------------------------# 