#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Format WHO data for COVID estimates 
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

codes <- get_path("meta", "locs")
#----------------------------# ####


cat('\n\n')
cat(green(' ############################\n'))
cat(green(' #### LAUNCH NGO COVID FORMAT ####\n'))
cat(green(' ############################\n\n'))



cat('  Read in COVID WHO data\n')
#----# Read in COVID data #----# ####

covid_dir <- get_path("who", "raw", "covid")

who <- list()
for (file in list.files(covid_dir, pattern = "*.csv")) {
    message("Appending: ", file)
    who[[file]] <- fread(file.path(covid_dir, file))
}
who <- rbindlist(who)

who <- who[Award != "N/A",]
who <- who[!(Distributed == 0 & `Award Budget` == 0)]
#------------------------------# ####

cat('  Combine sub-projects\n')
#----# combine sub-projects #----# ####
names(who) <- tolower(names(who))
setnames(who, c('parent donor', 'pandemic flag', 'form of award', 
               'award classification', 'distributed office', 'distributed organization',
               'sprp code', 'sprp', 'distributed', 'planned cost', 'award budget', 'award agreement amount (usd)', 
              'agreement amount (lc)'),
         c('donor', 'pandemic_flag', 'award_form', 'award_type', 'distributed_office',
           'distributed_org', 'purpose_code', 'purpose', 'commitment','planned_cost', 'disbursement', 
           'award_total_usd', 'agreement_amt_lc'))

who[, c("year", "pandemic_flag") := tstrsplit(pandemic_flag, " ", fixed=TRUE)]
# Collapse spending variables to get unique values and match commitments and disbursements
comm <- copy(who)
comm <- collapse(comm, 'sum', 
                 c('year','donor', 'contributor', 'award', 'award_form', 'award_type', 'distributed_office', 'distributed_org','pandemic_flag'), 
                 c('commitment'))
comm <- comm[distributed_office != '']

disb <- copy(who)
disb <- collapse(disb, 'sum', 
                 c('year','donor', 'contributor', 'award', 'award_form', 'award_type', 'distributed_office', 'distributed_org', 'purpose_code', 'purpose', 'project','pandemic_flag'), 
                 c('disbursement'))
disb <- disb[distributed_office != '']
disb <- disb[project != '']

# Match commitments and disbursements (multiple disbursements per commitment)
dt <- merge(disb, comm, by = c('year','donor', 'contributor', 'award', 'award_form', 'award_type', 'distributed_office', 'distributed_org','pandemic_flag'), all.x = T)

rm(disb, comm)

cat('  Extract recipient countries\n')
#----# Extract recipient countries #----# ####

dt[, iso3_rc := tstrsplit(distributed_org, '_', keep=2)]
dt[is.na(iso3_rc), iso3_rc := tstrsplit(distributed_org, '/', keep=2)]

cat('  Merge metadata\n')
#----# Merge metadata #----# ####
# Recipient Location IDs
# clean first
dt[, iso3_rc := fcase(
    iso3_rc == "BAN", "BGD",
    iso3_rc == "BHU", "BTN",
    iso3_rc == "BUL", "BGR",
    iso3_rc == "CZH", "CZE",
    iso3_rc == "GRE", "GRC",
    iso3_rc == "INO", "IDN",
    iso3_rc == "IRA", "IRN",
    iso3_rc == "KOS", "KSV",
    iso3_rc == "KRD", "KOR",
    iso3_rc == "LEB", "LBN",
    iso3_rc == "LIY", "LBY",
    iso3_rc == "MAV", "MDV",
    iso3_rc == "MOR", "MAR",
    iso3_rc == "NEP", "NPL",
    iso3_rc == "OMA", "OMN",
    iso3_rc == "ROM", "ROU",
    iso3_rc == "SAA", "SAU",
    iso3_rc == "SRL", "LKA",
    iso3_rc == "SUD", "SDN",
    iso3_rc == "KUW", "KWT",
    rep_len(TRUE, .N), iso3_rc
)]

isos <- setDT(fread(paste0(codes, 'fgh_location_set.csv')))[level == 3, c('location_name', 'ihme_loc_id')]
dt <- merge(dt, isos, by.x='iso3_rc', by.y='ihme_loc_id', all.x = T)
setnames(dt, 'location_name', 'recipient_country')

dt[is.na(recipient_country), `:=` (iso3_rc = 'G', recipient_country = 'Global')]

# Donor Location IDs
dt[donor == "United Kingdom of Great Britain and Northern Ireland", donor := "United Kingdom"]
dt[donor == "Netherlands (Kingdom of the)", donor := "Netherlands"]

dt <- merge(dt, isos, by.x='donor', by.y='location_name', all.x = T)
setnames(dt, 'ihme_loc_id', 'iso_code')

# Assign income sector and income donors
dt[!is.na(iso_code), `:=` (INCOME_SECTOR = 'PUBLIC', INCOME_TYPE = 'CENTRAL')]

income_donors <- fread( paste0(dah.roots$j,'/Project/IRH/DAH/RESEARCH/CHANNELS/5_UN_AGENCIES/5_WHO/DATA/INT/',
                               'INCOME_SECTOR_TYPE_DONOR_NAMES.csv'), encoding = 'Latin-1')
income_donors[, upper_DONOR_NAME := string_to_std_ascii(DONOR_NAME, pad_char = "")]
dt[, upper_donor := string_to_std_ascii(donor, pad_char = "")]
income_donors <- unique(income_donors, by='upper_DONOR_NAME')
dt <- merge(dt, income_donors[,.(upper_DONOR_NAME, INCOME_SECTOR, INCOME_TYPE)],
            by.x = 'upper_donor', by.y = 'upper_DONOR_NAME', all.x = T)

dt[, `:=` (INCOME_SECTOR = ifelse(is.na(INCOME_SECTOR.x), INCOME_SECTOR.y, INCOME_SECTOR.x),
           INCOME_TYPE = ifelse(is.na(INCOME_TYPE.x), INCOME_TYPE.y, INCOME_TYPE.x))]
dt[,`:=` (INCOME_SECTOR.x=NULL, INCOME_TYPE.x=NULL,INCOME_SECTOR.y=NULL, INCOME_TYPE.y=NULL)]

# fixing some missing income sector and type
dt[upper_donor == 'ALLIANCE FOR INTERNATIONAL MEDICAL ACTION ALIMA', `:=` (INCOME_SECTOR = "PRIVATE", INCOME_TYPE="NGO")]
dt[upper_donor == 'APPLIED INFORMATION SCIENCES INC', `:=` (INCOME_SECTOR = "INK", INCOME_TYPE="CORP")]
dt[upper_donor == 'BIOMEDICAL RESEARCH AND TRAINING INSTITUTE ZIMBABWE', `:=` (INCOME_SECTOR = "PRIVATE", INCOME_TYPE="OTHER")]
dt[upper_donor == 'COVANTAS LLC', `:=` (INCOME_SECTOR = "INK", INCOME_TYPE="CORP")]
dt[upper_donor == 'COVID 19 SOLIDARITY RESPONSE FUND', `:=` (INCOME_SECTOR = "PRIVATE", INCOME_TYPE="OTHER")]
dt[upper_donor == 'COVID 19 STRATEGIC PREPAREDNESS AND RESPONSE PLAN MEMBER STATES POOL FUND', `:=` (INCOME_SECTOR = "PUBLIC", INCOME_TYPE="OTHER")]
dt[upper_donor == 'COVID 19 SUPPLY CHAIN SYSTEM BRIDGE FUND', `:=` (INCOME_SECTOR = "PRIVATE", INCOME_TYPE="OTHER")]
dt[upper_donor == 'FRENCH POLYNESIA', `:=` (INCOME_SECTOR = "PUBLIC", INCOME_TYPE="CENTRAL")]
dt[upper_donor == 'INTERNATIONAL MEDICAL CORPS', `:=` (INCOME_SECTOR = "PRIVATE", INCOME_TYPE="NGO")]
dt[upper_donor == 'ISLE OF MAN', `:=` (INCOME_SECTOR = "PUBLIC", INCOME_TYPE="CENTRAL")]
dt[upper_donor == 'NORWEGIAN REFUGEE COUNCIL NRC', `:=` (INCOME_SECTOR = "PRIVATE", INCOME_TYPE="NGO")]
dt[upper_donor == 'PERSIAN AMERICAN SOCIETY FOR HEALTH ADVANCEMENT PASHA', `:=` (INCOME_SECTOR = "PRIVATE", INCOME_TYPE="NGO")]
dt[upper_donor == 'PREMIERE URGENCE INTERNATIONALE PUI', `:=` (INCOME_SECTOR = "PRIVATE", INCOME_TYPE="NGO")]
dt[upper_donor == 'THE BIG HEART FOUNDATION TBHF', `:=` (INCOME_SECTOR = "PRIVATE", INCOME_TYPE="FOUND")]
dt[upper_donor == 'VEOLIA ENVIRONMENT FOUNDATION', `:=` (INCOME_SECTOR = "PRIVATE", INCOME_TYPE="FOUND")]
dt[upper_donor == 'VRIJE UNIVERSITEIT AMSTERDAM', `:=` (INCOME_SECTOR = "PRIVATE", INCOME_TYPE= "UNIV")]
dt[upper_donor == 'WHO CONTINGENCY FUND FOR EMERGENCIES', `:=` (INCOME_SECTOR = "PUBLIC", INCOME_TYPE="OTHER")]
dt[upper_donor == 'BIG HEART FOUNDATION TBHF', `:=`(INCOME_SECTOR = "PRIVATE", INCOME_TYPE="FOUND")]
dt[upper_donor == 'GLOBAL HEALTH DEVELOPMENT', `:=`(INCOME_SECTOR = "PRIVATE", INCOME_TYPE="OTHER")]
dt[upper_donor == 'UN MULTI PARTNER TRUST FUND OFFICE MPTF', `:=`(INCOME_SECTOR = "OTHER", INCOME_TYPE="UN")]
dt[upper_donor == 'WHO FOUNDATION', `:=`(INCOME_SECTOR = "PRIVATE", INCOME_TYPE="FOUND")]

dt[, upper_donor := NULL]
rm(isos, income_donors)

# Income groups
ingr <- setDT(readstata13::read.dta13(paste0(codes, 'wb_historical_incgrps.dta')))[YEAR == dah.roots$report_year, c('INC_GROUP', 'ISO3_RC')]
dt <- merge(dt, ingr, by.x='iso3_rc', by.y='ISO3_RC', all.x=T)
dt <- dt[INC_GROUP != 'H' | is.na(INC_GROUP), ]
# Income groups for donor countries
dt <- merge(dt, ingr, by.x='iso_code', by.y='ISO3_RC', all.x=T)
setnames(dt, old = c('INC_GROUP.x','INC_GROUP.y'), new = c("INC_GROUP","INC_GROUP_donor"))
rm(ingr)

#--------------------------# ####
cat('  Remove domestic spending\n')
#----# Remove domestic spending #----# ####

dt[iso3_rc == iso_code, domestic_tag := 1]
domestic_countries <- sort(unique(dt[INC_GROUP_donor %in% c('L','LM','UM')]$iso_code)) # getting all LMIC donors
dt[iso_code %in% domestic_countries & iso3_rc == "QZA", domestic_tag := 1]
dt[is.na(domestic_tag), domestic_tag := 0]
dt <- dt[domestic_tag == 0]

# Add variables for loans and grants
dt[, grant := disbursement]
dt[, loan := 0]

#--------------------------# ####
cat('  Save dataset \n')
#----# Save dataset #----# ####

save_dataset(dt, paste0('WHO_COVID_CLEAN_oct'), 'WHO', 'int')
