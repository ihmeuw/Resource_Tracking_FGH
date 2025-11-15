#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Intake + format UNICEF COVID-19 data from UNICEF CDS dataset
#---------------------# ####

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

## read in IHME location data
locations <- setDT(readstata13::read.dta13(paste0(codes, "countrycodes_official.dta")))
locs <- locations[, .(country_lc, recipient_country=countryname_ihme, iso3_rc=iso3)]
# UNICEF CDS data
ta <- setDT(read.xlsx(paste0(get_path('UNICEF', 'raw'), '../FGH_2021/C19VFM_UNICEF_data_06302022.xlsx'), sheet = "UNICEF ACT-A HAC Allocations"))
ta[, eval(names(ta)[1:7]) := lapply(.SD, function(x) shift(x, n=1, type='lag')), .SDcols = names(ta)[1:7]]
if (min(which(!is.na(ta$X4))) == min(which(ta$`External.Funding.Disbursed.to.Countries.for.COVID-19.Vaccine.Delivery.via.UNICEF.ACT-A.HAC.(US$)` %like% "Country"))) {
  first_row <- min(which(ta$`External.Funding.Disbursed.to.Countries.for.COVID-19.Vaccine.Delivery.via.UNICEF.ACT-A.HAC.(US$)` %like% "Country"))
  last_row <- dim(ta)[1]
  ta1 <- copy(ta)[(first_row+1):last_row] 
  ta1 <- ta1[, c(1:2,28), with=T]  # keeping columns for isocode country name and GAVI TA to UNICEF 2021
  colnames(ta1) <- c('recipient_country', 'iso3_rc','GAVI_COVAX_TA_to_UNICEF')
  ta <- copy(ta1)
}

unicef_act <- setDT(read.xlsx(paste0(get_path('UNICEF', 'raw'), '../FGH_2021/C19VFM_UNICEF_data_06302022.xlsx'), sheet = "UNICEF ACT-A HAC"))
first_row <- min(which(unicef_act$ISO.Code %like% "ISO Code"))
last_row <- dim(unicef_act)[1]
uact <- copy(unicef_act)[(first_row+1):last_row] 
unicef_act <- copy(uact)
cols_keep <- c("ISO.Code", "Country.Name", "2021", "Allocation.Committee.proposal")
col_keep <- names(unicef_act)[names(unicef_act) %like% paste(cols_keep, collapse = "|")]
unicef_act <- unicef_act[!is.na(ISO.Code),..col_keep]
colnames(unicef_act) <- c('iso3_rc', 'recipient_country', 'UNICEF_ACT_Total','UNICEF_ACT_2021')

# cds dataset with UNICEF specific
cds_raw <- fread(paste0(get_path('UNICEF', 'raw'), "../FGH_2021/compiled_data_UNICEF.csv"))
unicef <- copy(cds_raw)
vax_col_names <- c("Supply Chain", "Technical assistance", "Evaluation", "Communications", "Distribution", "Human Resource", "Safety", "Commodity")
new_col_names <- paste0("Vaccine : ", vax_col_names, " ")
setnames(unicef, 
         old = c('Country', 'Cold Chain', 'Cross-cutting Technical Assistance for Planning, Coordination and Delivery', 
                        'Data Management, Monitoring & Evaluation, and Oversight', 'Demand Generation and Communications', 
                        'Vaccination Delivery', 'Vaccinators', 'Vaccine Safety Surveillance and Injection Safety', 
                        'Vaccine doses and related devices and supplies','Protecting Essential Health Services and Health Systems Strengthening',  'Total', 
                        'Region', 'AMC classification', 'CDS status', 'Amount requested (USD)', 'Amount approved (USD)', 'Amount disbursed (USD)', 
                        'Date of disbursement of funds'),
         new = c("country_lc", new_col_names, 'ESSENTIAL HEALTH SERVICE',
                 "total_amount", "cds_region", "amc_classification", "cds_status", "amount_requested_usd",
                 "amount_approved_usd", "amount_disbursed_usd", "date_disbursed"))


#------------------------------# ####

#### ## Clean CDS Data ## ####
cat(paste0(" Clean CDS Data \n"))

# clean TA data
ta[, purpose := 'COVID Vaccine: Technical Assistance']
setnames(ta, 'GAVI_COVAX_TA_to_UNICEF', 'disbursement')
ta[, disbursement := as.numeric(disbursement)]

# cleaning names
unicef[country_lc %like% "Palestinian", country_lc := "Occupied Palestinian Territories"]
unicef[country_lc == "Eswatini", country_lc := "Swaziland"]

# getting iso codes
unicef <- merge(unicef, locs, by = "country_lc", all.x = T)
unicef[, year := year(as.Date(date_disbursed, format = "%m/%d/%Y"))]
unicef[, `Vaccine : Other` := amount_approved_usd - total_amount]

unicef[, c("cds_region","amc_classification","cds_status","amount_requested_usd","country_lc","date_disbursed", "total_amount","amount_approved_usd") := NULL]

#  convert to long format with vax columns as purpose
cds <- melt.data.table(unicef,
                       id.vars = c("recipient_country", "iso3_rc", "year"), 
                       value.name = 'disbursement',
                       variable.name = 'purpose',
                       variable.factor = F)

# clean UNICEF ACT data
unicef_act[, purpose := 'COVID Vaccine: Technical Assistance']
setnames(unicef_act, 'UNICEF_ACT_2021', 'disbursement')
unicef_act[, disbursement := as.numeric(disbursement)]
unicef_act <- unicef_act[!is.na(iso3_rc), .(iso3_rc, recipient_country, disbursement)]

# combine 
dt <- rbind(cds, ta, unicef_act, fill = T)
# Add additional variables
dt[, `:=` (channel = 'UNICEF')]
dt[is.na(year), `:=` (year = 2021)]
dt[is.na(disbursement), disbursement := 0]

#### Income share create ------- ####
# Read in and clean donor data
inc <- fread(paste0(get_path('UNICEF', 'raw'), '../FGH_2021/COVID_2021_response_donor_shares.csv'), encoding = 'Latin-1')
inc[`Donation amount` %like% 'million', amount2 := 'million']

inc[, `Donation amount` := gsub(',', '', `Donation amount`)]
inc[, `Donation amount` := gsub('million', '', `Donation amount`)]
inc[, amount := as.numeric(`Donation amount`)]
inc[amount2 == 'million', amount := amount * 1e6]

inc$total <- inc[, lapply(.SD, sum), .SDcols = 'amount']
inc[, income_share := amount / total]

setnames(inc, c('Donor', 'Donation amount'), c('donor_name', 'donation_amount'))

# add metadata
income_metadata <- fread(paste0(dirname(get_path('meta', 'donor')), '/income_sector_and_type_assignments_2021.csv'), encoding = 'Latin-1')
inc <- string_clean(inc, 'donor_name')
inc[, upper_donor_name := str_trim(upper_donor_name)]

inc[upper_donor_name == 'AFRICAN DEVELOPMENT BANK AS SECONDARY DONORS', upper_donor_name := 'AFRICAN DEVELOPMENT BANK']
inc[upper_donor_name == 'UNDP USA', upper_donor_name := 'UNDP']
inc[upper_donor_name == 'UNOPS NEW YORK', upper_donor_name := 'UNOPS']
inc[upper_donor_name == 'WFP ITALY', upper_donor_name := 'WFP']
inc[upper_donor_name == 'WORLD BANK AS SECONDARY DONOR', upper_donor_name := 'WORLD BANK']

inc <- merge(inc, income_metadata, by.x = 'upper_donor_name', by.y = 'DONOR_NAME', all.x = T)
inc[donor_name %like% "African Development Bank", `:=` (INCOME_SECTOR = "OTHER", INCOME_TYPE = "DEVBANK", ISO_CODE = '')]
inc[upper_donor_name == "PRIVATE SECTOR", `:=` (INCOME_SECTOR = 'PRIVATE', INCOME_TYPE = 'OTHER', ISO_CODE = '')]
inc <- inc[, c('year', 'donor_name', 'INCOME_SECTOR', 'INCOME_TYPE', 'ISO_CODE', 'income_share')]

inc[INCOME_TYPE == 'CENTRAL', donor_country := donor_name]
inc[donor_name == 'Global Partnership for Education', INCOME_TYPE := 'PPP']
inc[INCOME_TYPE %in% c('DEVBANK', 'PPP', 'EC', 'UN'), INCOME_SECTOR := 'MULTI']


# merge and add donor information
dt_inc <- merge(dt, inc, by = 'year', allow.cartesian = T)
dt_inc[, disbursement := disbursement * income_share]
dt_inc[, income_share := NULL]
dt_inc[, commitment := 0]
dt_inc[, money_type := 'new_money']
dt_inc[, total_amt := disbursement]

cat('  Append 2020 and 2021 data\n')
#----# Append 2020 and 2021 data #----# ####
unocha_data <- fread(paste0(get_path('UNICEF', 'int'), '/COVID_UNOCHA.csv'))
unocha_data[is.na(disbursement), disbursement := 0]
dt_tot <- rbind(unocha_data, dt_inc, fill = T)

cat('  Configure and launch keyword search\n')
#----# Configure and launch keyword search #----# ####
dt_tot <- covid_kws(dataset = dt_tot, keyword_search_colnames = 'purpose', 
                    keep_clean = F, keep_counts = F, languages = 'english')

save_dataset(dt_tot, 'COVID_descriptions_with_hfas_UNICEF_CDS', 'UNICEF', 'fin')

covid_stats_report(dataset = dt_tot, amount_colname = 'total_amt',
                   recipient_iso_colname = 'iso3_rc', save_plot = T,
                   output_path = get_path('UNICEF', 'output'))

#-----------------------------------------------# ####

cat('  Calculate amounts by HFA\n')
#----# Calculate amounts by HFA #----# ####
dt_tot[, `:=`(COVID_total = NULL, COVID_total_prop = NULL)]

hfas <- gsub('_prop', '', names(dt_tot)[names(dt_tot) %like% '_prop'])
for (hfa in hfas) {
  dt_tot[, eval(paste0(hfa, '_amt')) := total_amt * get(paste0(hfa, '_prop'))]
  dt_tot[, eval(paste0(hfa, '_prop')) := NULL]
}

# Check sum still holds
dt_tot <- rowtotal(dt_tot, 'amt_test', names(dt_tot)[names(dt_tot) %like% '_amt'])
dt_tot[round(total_amt, 2) == round(amt_test, 2), check := 1]
dt_tot[, `:=`(amt_test = NULL, check = NULL)]
#------------------------------------# ####

#----# Creating GRANT_LOAN column before saving #----#

dt_tot[, `:=`(GRANT_LOAN = 'grant')]

cat('  Save out COVID dataset\n')
#----# Save out COVID dataset #----# ####

dt_tot <- dt_tot[, lapply(.SD, sum), 
                 by = c('year', 'channel', 'donor_name', 'donor_country', 'ISO_CODE', 'INCOME_SECTOR',
                        'INCOME_TYPE', 'recipient_country', 'iso3_rc', 'money_type', 'GRANT_LOAN'), 
                 .SDcols = c('commitment', 'disbursement', names(dt_tot)[names(dt_tot) %like% '_amt'])]

save_dataset(dt_tot, 'COVID_prepped', 'UNICEF', 'fin')
#----------------------------------# ####
