#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Intake + format UNICEF COVID-19 data form UNOCHA for 2020
#---------------------# ###

#----# Environment Prep #----# ####
rm(list=ls())

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, openxlsx, magrittr)

# Variable prep
codes <- get_path("meta", "locs")
#----------------------------# ####


cat('\n\n')
cat(green(' ###########################\n'))
cat(green(' #### UNICEF COVID PREP ####\n'))
cat(green(' ###########################\n\n'))


cat('  Read in COVID data\n')
#----# Read in COVID data #----# ####
# UNOCHA data
unocha_files <- dir(get_path("unicef", "raw"))
unocha_files <- unocha_files[unocha_files %like% 'UNOCHA']
dt <- data.table()
for (file in unocha_files) {
  temp <- setDT(read.xlsx(get_path('UNICEF', 'raw', file), sheet = 'Export data'))
  dt <- rbind(dt, temp)
}

setnames(dt, c('Source.org.', 'Destination.org.', 'Description', 'Amount.(US$)', 'Funding.status', 'Destination.country', 
               'Source.org..type', 'Source.country', 'Flow.date', 'New.money', 'Sector'),
         c('donor_agency', 'recipient_agency', 'purpose', 'amount_usd', 'funding_status', 'recipient_country', 
           'donor_agency_type', 'donor_country', 'approval_date', 'new_money', 'sector'))

dt <- dt[tolower(sector) %like% 'health' | is.na(sector), c('donor_agency', 'donor_agency_type', 'donor_country', 'recipient_agency', 'recipient_country', 'amount_usd', 'funding_status',
                                                            'purpose', 'approval_date', 'new_money', 'sector')]
#------------------------------# ####

cat('  Format column data types\n')
#----# Format column data types #----# ####
# new_money
dt[new_money == 'Yes', money_type := 'new_money']
dt[new_money == 'No', money_type := 'repurposed']
dt[, new_money := NULL]
# approval_date
dt[, approval_date := format(as.Date(approval_date, '%d/%m/%Y'), format='%m/%d/%Y')]
dt[, year := as.numeric(format(as.Date(approval_date, '%m/%d/%Y'), format='%Y'))]
# funding_status
dt[funding_status == 'Paid Contribution', funding_status := 'disbursement']
dt[, funding_status := tolower(funding_status)]
# recipient agency
dt[, recipient_agency := 'UNICEF']
# channel
dt[, channel := 'UNICEF']
# donor agencies
dt[donor_agency == 'US Fund for UNICEF', donor_agency := 'UNICEF National Committee/United States of America']
#------------------------------------# ####

cat('  Check for disb/comt/pledge project matches\n')
#----# Check for disb/comt/pledge project matches #----# ####
# Split funding types to merge together
disb <- copy(dt[funding_status == 'disbursement'])
disb[, disbursement := amount_usd]
disb[, `:=`(amount_usd = NULL, funding_status = NULL)]

comt <- copy(dt[funding_status == 'commitment'])
comt[, commitment := amount_usd]
comt[, `:=`(amount_usd = NULL, funding_status = NULL)]

# Merge together
dt <- merge(disb, comt, by=c('year', 'channel', 'donor_agency', 'donor_agency_type', 'donor_country', 'recipient_agency', 
                             'recipient_country', 'money_type', 'purpose', 'sector', 'approval_date'), all=T) 

dt <- dt[, c('donor_agency', 'donor_country', 'recipient_agency', 'recipient_country', 'money_type',
             'purpose', 'approval_date', 'year', 'channel', 'sector', 'commitment', 'disbursement')]
rm(comt, disb)
#------------------------------------------------------# ####

cat('  Clean locations\n')
#----# Clean locations #----# ####
dt[donor_agency == 'Bill and Melinda Gates Foundation', donor_country := 'United States of America']
dt[donor_agency %like% 'United States', donor_country := 'United States of America']
dt[donor_agency %like% '/', donor_country := sub('.*/', '', donor_agency)] # get strings of countries from donor agency
dt[donor_country == 'occupied Palestinian territory', donor_country := 'Palestine']

dt[recipient_country == 'occupied Palestinian territory', recipient_country := 'Palestine']
dt[recipient_country == 'Venezuela, Bolivarian Republic of', recipient_country := 'Venezuela (Bolivarian Republic of)']
#---------------------------# ####
#remove donor where donor and recipient countries are the same
dt[donor_country == recipient_country, donor_country := ""]

## need to address multi country contributions
# currently on Brazil, Colombia, Equador, TT for venezuela migration
bcett <- copy(dt)[recipient_country %like% "\\|"]
dt <- dt[!(recipient_country %like% "\\|")]
num_cols <- max(stringr::str_count(bcett$recipient_country, "\\|")) + 1
bcett[, eval(paste0('country_', 1:num_cols)) := tstrsplit(recipient_country, "|", num_cols)]
bcett[, num_countries := stringr::str_count(recipient_country, "\\|") + 1]
bcett[, commitment := commitment / num_countries]
bcett_long <- melt(bcett, measure.vars = names(bcett)[names(bcett) %like% '^country_'] )
bcett_long[, recipient_country := trimws(value, which = 'both')]
bcett_long[, c('value', 'variable', 'num_countries') := NULL]

dt <- rbind(dt, bcett_long)
##
cat('  Merge metadata\n')
#----# Merge metadata #----# ####
# Donor Location IDs
isos1 <- setDT(fread(paste0(codes, 'fgh_location_set.csv')))[level == 3, c('location_name', 'ihme_loc_id', 'region_name')]
isos2 <- setDT(readstata13::read.dta13(paste0(codes, 'countrycodes_official.dta')))[, c('country_lc', 'countryname_ihme', 'iso3')]
setnames(isos2,c('iso3'), c('ihme_loc_id'))
isos <- merge(isos2, isos1, by.x = c('country_lc', 'ihme_loc_id'),by.y = c('location_name', 'ihme_loc_id'), all.x = T) 
isos <- isos[order(ihme_loc_id, countryname_ihme, region_name)]
isos[, region_name := region_name[1], .(cumsum(!is.na(region_name)))]

dt <- merge(dt, isos, by.x='donor_country', by.y='country_lc', all.x = T)
dt[donor_country == 'Hong Kong', `:=` (ihme_loc_id = 'CHN',
                                       donor_country = 'China')]
dt[donor_country == 'Liechtenstein', ihme_loc_id := 'LIE']
dt[, c('region_name', 'countryname_ihme') := NULL]
setnames(dt, 'ihme_loc_id', 'iso3')

# Recipient Location IDs # customize for new year
dt[recipient_country == "Korea, Democratic People's Republic of", recipient_country := "Democratic People's Republic of Korea"]
dt[recipient_country == "Türkiye", recipient_country := "Turkey"]
dt[recipient_country == "Palestine", recipient_country := "Occupied Palastinian Territory"]
dt[recipient_country == "Eswatini", recipient_country := "Swaziland"]
dt <- merge(dt, isos, by.x='recipient_country', by.y='country_lc', all.x = T)
dt[is.na(recipient_country), recipient_country := 'Global']
dt[recipient_country == "Global", ihme_loc_id := 'G']
setnames(dt, c('ihme_loc_id', 'region_name'), c('iso3_rc', 'gbd_region'))
rm(isos)

# Separating commitments and disbursements
dt[, total_amt := disbursement]
dt[is.na(total_amt), total_amt := commitment]
#--------------------------# ####

# Keep only 2020 (using UNICEF provided data for 2021)
dt <- dt[year != 2019 & year != 2021]

## assign income sector and income type
income_metadata <- fread(paste0(dirname(get_path('meta', 'donor')), '/income_sector_and_type_assignments_2021.csv'), encoding = 'Latin-1')
dt[, donor_name := donor_agency]
dt[grepl('Government of', donor_agency), donor_name := donor_country]
dt <- string_clean(dt, 'donor_name')
dt[, upper_donor_name := str_trim(upper_donor_name)]

dt <- merge(dt, income_metadata[, .(DONOR_NAME, INCOME_SECTOR, INCOME_TYPE)], by.x = 'upper_donor_name', by.y = 'DONOR_NAME', all.x = T)
dt[grepl('UNICEF National Committee', donor_name), INCOME_TYPE := 'UN']
dt[INCOME_TYPE %in% c('DEVBANK', 'EC', 'PPP', 'UN'), INCOME_SECTOR := 'MULTI']

cat('  Save out COVID dataset\n')
#----# Save out COVID dataset #----# ####
setnames(dt, c('iso3'), c('ISO_CODE'))
dt <- dt[, c('year', 'channel', 'donor_name', 'donor_country', 'ISO_CODE', 'INCOME_SECTOR', 'INCOME_TYPE', 'iso3_rc', 'recipient_country', 
             'disbursement', 'commitment', 'total_amt', 'money_type', 'purpose'), with = F]

save_dataset(dt, 'COVID_UNOCHA', 'UNICEF', 'int')
#----------------------------------# ####