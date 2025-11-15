#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Format UNFPA covid data and breakdown to PAs 
#---------------------# ####

#----# Environment Prep #----# ####
rm(list = ls())

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

# Variable prep
codes <- get_path("meta", "locs")
#----------------------------# ####


cat('  Read in COVID 2021 data\n')
## ----# Read in COVID 2021 data #----# ####
dt21 <- setDT(read_excel(paste0(dah.roots$j, "FILEPATH/", 
                              'UNFPA_COVID_GHRP_2021_26012022.xlsx'), sheet = 'Export data'))

names(dt21)
setnames(dt21, 
         c('Source org.', 'Destination org.', 'Description', 'Amount (US$)', 
           'Funding status', 'Destination country', 'Source org. type', 
           'Source country', 'Destination usage year', 'New money'),
         c('donor_agency', 'recipient_agency', 'purpose', 'amount_usd', 
           'funding_status', 'recipient_country', 'donor_agency_type', 
           'donor_country', 'YEAR', 'new_money'))
dt21 <- dt21[, c('donor_agency', 'donor_agency_type', 'donor_country', 'recipient_agency', 
             'recipient_country', 'amount_usd', 'funding_status',
             'purpose', 'YEAR', 'new_money')]

cat('  Read in COVID 2020 data\n')
## ----# Read in COVID 2020 data #----# ####
dt20 <- setDT(read_excel(paste0(dah.roots$j, "FILEPATH", 
                                'UNFPA_COVID_GHRP_2020_20210111.xlsx'), sheet = 'Export data'))

names(dt20)
setnames(dt20, 
         c('Source org.', 'Destination org.', 'Description', 'Amount (US$)', 
           'Funding status', 'Destination country', 'Source org. type', 
           'Source country', 'Destination usage year', 'New money'),
         c('donor_agency', 'recipient_agency', 'purpose', 'amount_usd', 
           'funding_status', 'recipient_country', 'donor_agency_type', 
           'donor_country', 'YEAR', 'new_money'))
dt20 <- dt20[, c('donor_agency', 'donor_agency_type', 'donor_country', 'recipient_agency', 
             'recipient_country', 'amount_usd', 'funding_status',
             'purpose', 'YEAR', 'new_money')]


# bind the two datasets
dt <- rbind(dt20, dt21, fill = T)

# cleaning donor_agency variables
dt[is.na(donor_agency), donor_agency := donor_country]

## ------------------------------------# ####
cat('  Format column data types\n')
## ----# Format column data types #----# ####
## new_money
dt[new_money == 'Yes', money_type := 'new money']
dt[new_money == 'No', money_type := 'repurposed money']
dt[, new_money := NULL]

dt[, grant_loan := 'grant']

## funding_status
dt[funding_status == 'Paid Contribution', funding_status := 'disbursement']
dt[, funding_status := tolower(funding_status)]
## purpose
dt[, purpose := toupper(purpose)]

## ------------------------------------------------------# ####
cat('  Check for disb/comt/pledge project matches\n')
## ----# Check for disb/comt/pledge project matches #----# ####
## Split funding types to merge together
disb <- copy(dt[funding_status == 'disbursement', ])
disb[, disbursement := amount_usd]
disb[, `:=`(amount_usd = NULL, funding_status = NULL)]
comt <- copy(dt[funding_status == 'commitment', ])
comt[, commitment := amount_usd]
comt[, `:=`(amount_usd = NULL, funding_status = NULL)]
pldg <- copy(dt[funding_status == 'pledge', ])
pldg[, pledge := amount_usd]
pldg[, `:=`(amount_usd = NULL, funding_status = NULL)]
## Merge together
dt <- merge(disb, comt, 
            by = c('YEAR', 'donor_agency', 'donor_agency_type', 'donor_country', 
                   'recipient_agency', 'recipient_country', 'money_type', 'purpose'), 
            all = T) %>% 
  merge(pldg, by = c('YEAR', 'donor_agency', 'donor_agency_type', 'donor_country', 
                   'recipient_agency', 'recipient_country', 'money_type', 'purpose'), 
        all = T)

dt <- dt[, c('YEAR', 'donor_agency', 'donor_agency_type', 'donor_country', 
             'recipient_agency', 'recipient_country', 'money_type', 'grant_loan', 
             'purpose', 'pledge', 'commitment', 'disbursement')]
rm(comt, disb, pldg)

dt[, total_amt := disbursement]
dt[is.na(total_amt), total_amt := commitment]
## -----------------------------------------------# ####
cat('  Clean locations\n')
#----# Clean locations #----# ####
print(unique(dt$donor_country))
dt[donor_country == 'Congo, The Democratic Republic of the', 
   donor_country := 'Democratic Republic of the Congo']
dt[donor_country == 'Korea, Republic of', donor_country := 'Republic of Korea']
dt[donor_country %like% 'Palestin', donor_country := 'Palestine']
dt[donor_country %like% "Iran", donor_country := "Iran (Islamic Republic of)"]
dt[donor_country %like% "Tanzania", donor_country := "United Republic of Tanzania"]

print(unique(dt$recipient_country))

dt[recipient_country %like% 'Bolivia', recipient_country := 'Bolivia (Plurinational State of)']
dt[recipient_country == 'Congo, The Democratic Republic of the', 
   recipient_country := 'Democratic Republic of the Congo']
dt[recipient_country %like% 'Lao', 
   recipient_country := "Lao People's Democratic Republic"]
dt[recipient_country %like% 'Moldova', 
   recipient_country := "Republic of Moldova"]
dt[recipient_country %like% 'Venezuela', 
   recipient_country := "Venezuela (Bolivarian Republic of)"]
dt[recipient_country %like% 'Palestin', recipient_country := 'Palestine']
dt[recipient_country %like% "Iran", recipient_country := "Iran (Islamic Republic of)"]
dt[recipient_country %like% "Tanzania", recipient_country := "United Republic of Tanzania"]
dt[recipient_country %like% "North Macedonia", recipient_country := "North Macedonia"]

# Donor Location IDs
isos <- setDT(fread(paste0(codes, 'fgh_location_set.csv')))[, c('location_name', 'ihme_loc_id')]
dt <- merge(dt, isos, by.x = 'donor_country', by.y = 'location_name', all.x = T)
setnames(dt, 'ihme_loc_id', 'iso_code')
stopifnot(nrow(unique(dt[!is.na(donor_country) & is.na(iso_code), .(donor_country, iso_code)])) == 0)
# Recipient Location IDs
dt <- merge(dt, isos, by.x = 'recipient_country', by.y = 'location_name', all.x = T)
setnames(dt, 'ihme_loc_id', 'iso3_rc')
stopifnot(nrow(unique(dt[!is.na(recipient_country) & is.na(iso3_rc), .(recipient_country, iso3_rc)])) == 0)

print(unique(dt[is.na(iso_code), .(donor_country, iso_code)]))
print(unique(dt[is.na(iso3_rc), .(recipient_country, iso3_rc)]))
rm(isos)

# Income groups
ingr <- setDT(read.dta13(paste0(codes, 'wb_historical_incgrps.dta')))[YEAR == dah.roots$report_year, c('INC_GROUP', 'ISO3_RC')]
dt <- merge(dt, ingr, by.x = 'iso3_rc', by.y = 'ISO3_RC', all.x = T)
print(dt[INC_GROUP == 'H', ])
rm(ingr)

cat('  Configure and launch keyword search\n')
#----# Configure and launch keyword search #----# ####
dt <- covid_kws(dataset = dt, keyword_search_colnames = 'purpose', 
                keep_clean = T, keep_counts = T, languages = c('english', 'spanish', 'french'))

dt[purpose %like% "GBV" & other_prop == 1 &  COVID_total_prop == 1, 
   `:=`(hss_prop = 1, other_prop = 0, hss = 1, other = 0)]

dt[purpose %like% "VIOLENCE" & other_prop == 1 & COVID_total_prop == 1, 
   `:=`(hss_prop = 1, other_prop = 0, hss = 1, other = 0)]

dt[purpose %like% "DERECHOS SEXUALES" & other_prop == 1 & COVID_total_prop == 1, 
   `:=`(hss_prop = 1, other_prop = 0, hss = 1, other = 0)]

dt[purpose %like% "VBG" & other_prop == 1 & COVID_total_prop == 1, 
   `:=`(hss_prop = 1, other_prop = 0, hss = 1, other = 0)]

print(dt[other == 1 & COVID_total == 1, upper_purpose])

dt_2020 <- dt[YEAR == 2020,]
dt_2021 <- dt[YEAR == 2021,]

covid_stats_report(dataset = dt, amount_colname = 'total_amt', 
                   recipient_iso_colname = 'iso3_rc', save_plot = T, 
                   output_path = get_path('UNFPA', 'output'))



cat('  Calculate amounts by HFA\n')
#----# Calculate amounts by HFA #----# ####
dt[, `:=`(COVID_total = NULL, COVID_total_prop = NULL)]

hfas <- gsub('_prop', '', names(dt)[names(dt) %like% '_prop'])
for (hfa in hfas) {
   dt[, eval(paste0(hfa, '_amt')) := total_amt * get(paste0(hfa, '_prop'))]
   dt[, eval(paste0(hfa, '_prop')) := NULL]
}

# Check sum still holds
dt <- rowtotal(dt, 'amt_test', names(dt)[names(dt) %like% '_amt'])
dt[round(total_amt, 2) == round(amt_test, 2), check := 1]
dt[, `:=`(amt_test = NULL, check = NULL)]

#------------------------------------# ####
cat('Get total amounts by year\n')

dt2020 <- setDT(fread(paste0(dah.roots$j, "FILEPATH", 
                             'COVID_prepped_R1.csv')))

# total dibursement
disb_sum2021 <- sum(dt$disbursement,na.rm=T)
print(disb_sum2021)

disb_sum2020 <- sum(dt2020[commitment == "disbursement", 'disbursement'])
print(disb_sum2020)

# total commitment
commit_sum2021 <- sum(dt$commitment, na.rm = T)
print(commit_sum2021)

commit_sum2020 <- sum(dt2020[commitment == "commitment", 'disbursement'])
print(commit_sum2020)

#-----------------------------------------------# ####

#------------------------------------# ####
cat('Graphs\n')

# melt dataset to have amounts by program area
# sum by program area and year for plot

dt_pa2021 <- melt(dt, measure.vars = c('rcce_amt',
                                       'sri_amt',
                                       'nl_amt',
                                       'ipc_amt',
                                       'cm_amt',
                                       'scl_amt',
                                       'hss_amt',
                                       'r&d_amt',
                                       'ett_amt',
                                       'other_amt'))



#-----------------------------------------------# ####

dt[, grant_loan  := "grant"]

sum(dt$disbursement, na.rm = T)
sum(dt$commitment, na.rm = T)
sum(dt$pledge, na.rm = T)


dt[!is.na(iso_code), `:=`(INCOME_SECTOR = 'PUBLIC', INCOME_TYPE = 'CENTRAL')]
dt[is.na(iso_code), `:=`(INCOME_SECTOR = 'OTHER', INCOME_TYPE = 'OTHER')]
dt[donor_agency %like% 'Private', `:=`(INCOME_SECTOR = 'PRIVATE')]
dt[donor_agency %like% 'European Commission', 
   `:=`(INCOME_SECTOR = 'PUBLIC', INCOME_TYPE = 'OTHER')]

unique(dt$INCOME_SECTOR)
unique(dt$INCOME_TYPE)

dt[is.na(grant_loan), grant_loan := 'grant']

# Updating variable names #
setnames(dt, 'donor_agency', 'donor_name')

fwrite(dt, paste0(get_path('UNFPA', 'fin'), 'COVID_prepped_20_21.csv'))

## End of Script ## 