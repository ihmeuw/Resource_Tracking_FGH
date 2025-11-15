#----# Docstring #----# ####
# Project:  FGH 
# Purpose:  Intake + format CHINA COVID-19 data
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, openxlsx, magrittr)

# Variable prep
vacc_price <- 32 # value of vaccine in CHN currency 
report_yr <- dah.roots$report_year
abrv_yr <- dah.roots$abrv_year
#----------------------------# ####

cat(green('\n\n ##########################\n #### CHINA COVID PREP ####\n ##########################\n \n\n'))

cat('  Read in COVID data\n')
#----# Read in COVID data #----# ####
# UNOCHA data
dt <- fread(get_path("humanitarian_aid", "fin", "covid_all_collapsed.csv"))
dt <- dt[source == "BIL_CHN" & sector %ilike% "health"]
setnames(dt, "source_reported", "donor_agency")

# CIDCA Repurposed data
cidca <- setDT(fread(paste0(get_path('CHINA', 'int'), 'CIDCA_COVID.csv')))

# NHC Repurposed data
nhc <- setDT(fread(paste0(get_path('CHINA', 'int'), 'NHC_COVID.csv')))
#------------------------------# ####

cat('  Format UNOCHA data types\n')
#----# Format UNOCHA data types #----# ####
# new_money
dt[newmoney == TRUE, money_type := 'new_money']
dt[newmoney == FALSE, money_type := 'repurposed']
dt[, newmoney := NULL]
#------------------------------------# ####

cat('  Check for UNOCHA disb/comt/pledge project matches\n')
#----# Check for UNOCHA disb/comt/pledge project matches #----# ####
# Split funding types to merge together
disb <- dt[contributionstatus == "paid"]
setnames(disb, c("amount"), c("disb_amount"))
comt <- dt[contributionstatus == "comm"]
setnames(comt, c("amount"), c("comt_amount"))
all <- merge(disb[, -"contributionstatus"],
             comt[, -"contributionstatus"],
             by = grep("amount|contributionstatus",
                       names(disb), invert = TRUE, value = TRUE),
             all = TRUE)

# We prefer disbursement as actual amount, but when not available use commitment
all[, amount := fifelse(is.na(disb_amount), comt_amount, disb_amount)]
dt[, channel := 'CHINA']
rm(comt, disb)
cat('  Format NHC data\n')
#----# Format NHC data #----# ####
colnames(nhc) <- tolower(colnames(nhc))

# this is repurposed money taken from original NHC now all COVID related
nhc[, `:=`(donor_agency = 'China, Government of - National Health Commission',
           donor_agency_type = 'Government, National government',
           donor_country = 'China', recipient_agency = NA,
           recipient_country = 'UNALL', purpose = 'Case management and Communication',
           sector = 'Health', channel = 'CHINA', approval_date = NA,
           donor_name = NULL, amount = amount * 1e6)]

# 2.	Additional 3 medical teams worth of 0.425 million USD
med_teams = 0.425 * 1e6
add_nhc <- copy(nhc)
add_nhc[, `:=`(money_type = 'new_money', amount = 4854290.9)]
add_nhc[year==2021, `:=`(money_type = 'new_money', amount = med_teams)]

nhc <- rbind(nhc, add_nhc)
rm(add_nhc)
#---------------------------# ####

cat('  Format CIDCA data\n')
#----# Format CIDCA data #----# ####
colnames(cidca) <- tolower(colnames(cidca))

# this is repurposed money taken from original CIDCA now all COVID related
cidca[, `:=`(donor_agency = 'China, Government of - China International Development Cooperation Agency',
           donor_agency_type = 'Government, National government',
           donor_country = 'China', recipient_agency = NA,
           recipient_country = 'UNALL', purpose = 'Case management and Communication',
           sector = 'Health', channel = 'CHINA', approval_date = NA,
           donor_name = NULL, amount = amount * 1e6)]


#---------------------------# ####

cat('  Add PPE Estimate\n')
#----# Add PPE Estimate #----# ####
covid_years <- report_yr - 2019    # number of years for COVID
ppe <- data.table(year = seq(from=2020, to=report_yr),
                  donor_country = 'China', donor_agency =  'China, Government of',
                  donor_agency_type =  'Government, National government',
                  recipient_agency =  NA, recipient_country =  'UNALL',
                  money_type = 'new_money', purpose = 'PPE Donations',
                  sector =  'Health', approval_date =  NA, amount = NA_real_,
                  channel = 'CHINA')
setDT(ppe)
unit_cost <- 300000 
num_batches <- 200 
ppe[year == 2020, amount := unit_cost * num_batches]

# 3.	Vaccine donation 80 million * 32 RMB (~5 USD?)
exch_rates <- fread(get_path("meta", "rates", "OECD_XRATES_NattoUSD_1950_[report_year].csv"))
exch_rates <- exch_rates[LOCATION == "CHN" & TIME >= 1990, c("TIME", "Value")]

# load last year of Duke vaccine data - will never be updated again
duke_countries <- fread(file.path(
    dah_cfg$j,
    "FILEPATH/duke_covid_vaccine_donations_2023_05_05.csv"
))

## keep only donor rows from China
duke_china <- copy(duke_countries)
colnames(duke_china) <- tolower(gsub(" ", "_",names(duke_china)))
# keep only needed columns
duke_china <- duke_china[, .(date_last_updated,donating_entity,donating_entity_three_letter_country_code, donated_to , 
                             receiving_country_three_letter_code, receiving_country_economic_status,
                             donation_date, donation_amount, doses_delivered)]

colnames(duke_china) <- c('date_last_updated','donor_country', 'iso3', 'recipient_country', 'iso3_rc', 'inc_group', 
                          'donation_date', 'donation_amount', 'doses_delivered')

# assigning unknows (NAs) to be year of last update
duke_china[, paste0(c('date_last_updated'),c('_month', '_day', '_year')) := tstrsplit(date_last_updated, "/", fixed=TRUE)]
duke_china[, paste0(c('donation_date'),c('_month', '_day', '_year')) := tstrsplit(donation_date, "/", fixed=TRUE)]
duke_china[, year := donation_date_year]
duke_china[is.na(year)  & as.numeric(date_last_updated_month) > 7 & as.numeric(date_last_updated_year) > 2021, year := date_last_updated_year]
duke_china[is.na(year) | year < 2021, year := 2021]
duke_china[,c(paste0(c('date_last_updated'),c('_month', '_day', '_year')),paste0(c('donation_date'),c('_month', '_day', '_year'))) := NULL]
duke_china[year == "21*", year := "2021"]

duke_china[, c('donation_amount', 'doses_delivered','year') := lapply(.SD, function(x) as.integer(gsub(',|\\*','',x))), .SDcols = c('donation_amount', 'doses_delivered','year')]
# keeping only china donations to LMIC countries up to report year
duke_china <- duke_china[year <= report_yr & iso3 %like% 'CHN' & !(inc_group %like% 'High')]
agg_total_no_unknown <- sum(duke_china[!(recipient_country %like% 'Unknown') & year == 2021]$doses_delivered, na.rm = T)
duke_china[recipient_country %like% 'Unknown', recipient_country := "UNALL"]
duke_chn_ukw <- duke_china[recipient_country %like% 'UNALL']
duke_chn_ukw[year == 2021, `:=` (doses_delivered = 80000000 - agg_total_no_unknown)]
duke_china <- rbind(duke_chn_ukw, duke_china)
duke_china <- duke_china[, lapply(.SD, sum, na.rm = T), by = c(names(duke_chn_ukw)[names(duke_chn_ukw) %ni% 'doses_delivered']), .SDcols = c('doses_delivered')]

ppe_china <- duke_china[, .(year, donor_country, recipient_country, iso3_rc, vacc_donations = doses_delivered * vacc_price)]
ppe_china[, `:=` (donor_agency =  'China, Government of',
                  donor_agency_type =  'Government, National government',
                  money_type = 'new_money',sector =  'Health', approval_date =  NA, channel = 'CHINA',
                  purpose = "vaccine procurement/commodity")]

ppe_china <- merge(ppe_china, exch_rates, by.x='year', by.y='TIME', all.x = T)

ppe <- rbind(ppe, ppe_china, fill = T)
ppe[!is.na(Value), `:=`(amount = vacc_donations / Value)]
ppe <- ppe[!(is.na(amount) | amount == 0)]

## fixing recipient names and splitting unions
ppe[recipient_country %like% "West.*Bank.*Gaza", recipient_country := "Palestine"]
# african union into individual members
afr_uni <- ppe[recipient_country %like% "Afric.*Un"]
african_union_mem <- dah.roots$AFRICAN_ISOS
# Location IDs
# read in IHME location data
isos <- fread(get_path("meta", "locs", 'fgh_location_set.csv'))
isos <- isos[level == 3, c('location_name', 'ihme_loc_id')]
isos <- isos[ihme_loc_id %in% african_union_mem]      # keeping only african union members
afr_uni <- afr_uni[rep(seq_len(nrow(afr_uni)), 54), ] # duplicating 54 times for each member
afr_uni <- cbind(afr_uni, isos[, .(location_name)])
afr_uni[, recipient_country := location_name]
afr_uni[, location_name := NULL]
# splitting values equally amongst all members
afr_uni[, c('amount', 'vacc_donations') := lapply(.SD, function(x) x / 54), .SDcols = c('amount', 'vacc_donations')]
# appending back into ppe
ppe <-ppe[!(recipient_country %like% "Afric.*Un")]
ppe <- rbind(ppe, afr_uni)

ppe[,c('Value', 'vacc_donations', 'iso3_rc') := NULL]

rm(unit_cost, num_batches, med_teams, ppe_china, duke_china, duke_countries)
#----------------------------# ####

cat('  Append datasets\n')
#----# Append datasets #----# ####
dt <- rbind(dt, nhc, fill=T)
dt <- rbind(dt, cidca, fill=T)
dt <- rbind(dt, ppe, fill=T)
rm(nhc, ppe, cidca)
#---------------------------# ####

cat('  Clean locations\n')
#----# Clean locations #----# ####
dt[recipient_country == 'Eswatini', recipient_country := 'Swaziland']
dt[recipient_country == 'Palestine', recipient_country := 'West Bank and Gaza']
#---------------------------# ####

cat('  Merge metadata\n')
#----# Merge metadata #----# ####
# Read in ISOs
isos <- fread(get_path("meta", "locs", "countrycodes_official.csv"))
isos <- unique(isos[, c('country_lc', 'iso3'), with = F])
setnames(isos, 'iso3', 'ihme_loc_id')
dt[, iso3 := "CHN"]
dt[, donor_country := "CHN"]

# Recipient Location IDs
dt <- merge(dt, isos, by.x='recipient_country', by.y='country_lc', all.x = T)
isos <- fread(get_path("meta", "locs", 'fgh_location_set.csv'))[level == 3, c('location_name', 'ihme_loc_id', 'region_name')]
dt <- merge(dt, isos, by='ihme_loc_id', all.x = T)

setnames(dt, c('ihme_loc_id', 'region_name'), c('iso3_rc', 'gbd_region'))

dt[!is.na(location_name), recipient_country := location_name]

dt[recipient_country == 'Global', iso3_rc := 'GLOBAL']
dt[recipient_country == 'UNALL', iso3_rc := 'QZA']
rm(isos)

cat('  Save out COVID dataset\n')
#----# Save out COVID dataset #----# ####
dt <- dt[, c('year', 'channel', 'donor_agency', 'iso3', 'donor_country',
             'iso3_rc', 'recipient_country', 'amount'), with=F]
setnames(dt, 'donor_agency', 'donor_name')
colnames(dt) <- toupper(colnames(dt))
dt[, `:=`(INCOME_SECTOR = "PUBLIC", INCOME_TYPE = "CENTRAL")]

save_dataset(dt, 'COVID_prepped', 'CHINA', 'fin')
#----------------------------------# ####