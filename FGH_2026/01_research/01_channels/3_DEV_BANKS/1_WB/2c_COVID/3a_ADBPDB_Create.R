#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))
start.time <- Sys.time()
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

# Variable prep
codes <- get_path("meta", "locs")
#----------------------------# ####

## read in data
data <- fread(paste0(get_path('WB', 'int'), 'COVID_cleaned.csv'))

## get IDA income
cat('  Read in IDA income\n')
#----# Read in IDA income #----# ####
dac_income <- fread(get_path('WB', 'raw', 'WB_IDA_INCOME_BYDONOR.csv'))
dac_income <- dac_income[year < dah.roots$prev_report_year]
setnames(dac_income, c('value', 'amounttype'), c('income', 'income_type'))
dac_income[income_type %like% 'Constant', income_type := 'income_16']
dac_income[income_type %like% 'Current', income_type := 'income_current']
dac_income <- dcast.data.table(dac_income, formula = 'donor + year ~ income_type', value.var = 'income')
dac_income[, `:=`(INCOME_SECTOR = "PUBLIC", INCOME_TYPE = "CENTRAL", CHANNEL = "WB_IDA", 
                  DONOR_NAME = donor, DONOR_COUNTRY = donor)]
setnames(dac_income, 'donor', 'country_lc')


# Merge on ISO3 codes
isos <- setDT(readstata13::read.dta13(paste0(codes, 'countrycodes_official.dta')))[, c('country_lc', 'iso3')]
dac_income <- merge(dac_income, isos, by = 'country_lc', all.x = T)
setnames(dac_income, 'country_lc', 'countryname')
rm(isos, codes)

dac_income[, total_inc := sum(income_current, na.rm=T), by='countryname']
dac_income <- dac_income[total_inc != 0]
dac_income[, total_inc := NULL]

setnames(dac_income, c('iso3', 'year', 'income_current'), c('ISO_CODE', 'YEAR', 'INCOME_ALL'))
dac_income <- dac_income[, c('INCOME_SECTOR', 'INCOME_TYPE', 'CHANNEL', 'DONOR_NAME', 'DONOR_COUNTRY', 'ISO_CODE', 'YEAR', 'INCOME_ALL')]

#------------------------------#

cat('  Read income from loans & IBRD transfers\n')
#----# Read income from loans & IBRD transfers #----# ####
inc_other <- setDT(fread(paste0(get_path('WB', 'raw'), 'WB_IDA_INCOME_OTHER.csv')))
inc_other[, INCOME_SECTOR := 'DEBT']
inc_other <- inc_other[YEAR < dah.roots$prev_report_year]
inc_other[, INCOME_TYPE := as.character(INCOME_TYPE)]
inc_other[, INCOME_TYPE := '']
# Append dac_income
inc_other <- rbind(inc_other, dac_income, fill=T)
inc_other[, INCOME_TOTAL_YR := sum(INCOME_ALL, na.rm=T), by='YEAR']
inc_other[, INCOME_ALL_SHARE := INCOME_ALL / INCOME_TOTAL_YR]


inc_other <- dcast.data.table(inc_other,
                              CHANNEL + INCOME_SECTOR + INCOME_TYPE + DONOR_NAME + DONOR_COUNTRY + ISO_CODE ~ YEAR,
                              value.var = 'INCOME_ALL_SHARE')
inc_other[, eval(paste0(c(2016:(dah.roots$report_year-2)))) := lapply(.SD, function(x) ifelse(is.na(x),0,x)),
          .SDcols = paste0(c(2016:(dah.roots$report_year-2)))]


inc_other[, `2022` := `2019` / 6 + `2020` / 3 + `2021` / 2]
inc_other[, `2023` := `2020` / 6 + `2021` / 3 + `2022` / 2]
inc_other <- melt(inc_other, id.vars =c('CHANNEL', 'INCOME_SECTOR', 'INCOME_TYPE', 'DONOR_NAME', 'DONOR_COUNTRY', 'ISO_CODE'), value.name  = 'INCOME_ALL_SHARE', variable.name = 'YEAR')
inc_other[, YEAR :=as.numeric(as.character(YEAR))]
inc_other <- inc_other[YEAR >= 2020]

# Bring in total disbursements
temp_ida <- data[agreementtype == 'IDA',
                 .(DISB_TOTAL = sum(get(paste0('DAH_',dah.roots$abrv_year)), na.rm = TRUE) / 1000000),
                 by = 'YEAR']

## merge income on
inc_other <- merge(inc_other, temp_ida, by = 'YEAR')
rm(temp_ida, dac_income)

inc_other[, DISBURSEMENT := DISB_TOTAL * INCOME_ALL_SHARE]
inc_other[, OUTFLOW := DISBURSEMENT]
inc_other <- inc_other[DISBURSEMENT != 0]

cat('  Calculate IBRD income\n')
#----# Calculate IBRD income #----# ####
temp_ibrd <- data[agreementtype == 'IBRD',
                  .(DISB_TOTAL = sum(get(paste0('DAH_',dah.roots$abrv_year)), na.rm = TRUE) / 1000000),
                  by = 'YEAR']
temp_ibrd[, `:=` (CHANNEL = "WB_IBRD", INCOME_SECTOR = "DEBT", INCOME_TYPE = "", 
                  DONOR_NAME = "Bond Issuance", DONOR_COUNTRY = "NA", ISO_CODE = "NA",
                  OUTFLOW = DISB_TOTAL)]
#------------------------------#


#----# Append datasets #----# ####
wb_adb <- rbind(inc_other, temp_ibrd, fill=T)

wb_adb[, `:=`(GHI = 'WB', DISB = DISBURSEMENT, DISBURSEMENT = DISBURSEMENT * 1000000, 
              OUTFLOW = OUTFLOW * 1000000, DISB_TOTAL = NULL)]

# Collapse yearly disbursements
setnames(data, 'agreementtype', 'CHANNEL')
data[, CHANNEL := paste0('WB_', CHANNEL)]
data[, VAC_TOTAL := rowSums(.SD, na.rm = T), .SDcols=names(data)[names(data) %like% "vax" & names(data) %like% '_DAH' & !(names(data) %like% 'component_')]]
data[, is_vax := ifelse(VAC_TOTAL > 0, 1, 0)]
to_fix <- names(data)[names(data) %like% "vax" & names(data) %like% "_DAH" | names(data) %like% "VAC" | names(data) %like% "COVID_total_DAH"]
data[, vac_adj := ifelse(VAC_TOTAL>0,1,1)]
data[,eval(to_fix) := lapply(.SD, function(x) x * vac_adj), .SDcols=to_fix]
dt <- collapse(data, 'sum', c('YEAR', 'ISO3_RC', 'CHANNEL', 'money_type', 'grant_loan','is_vax'),
               c('DAH', 'disbursement','commitment_vax', names(data)[names(data) %like% '_DAH' & !(names(data) %like% 'component_')]))

## Merge datasets
dt <- merge(dt, wb_adb, by = c('YEAR', 'CHANNEL'), all = T, allow.cartesian = T)

## Allocate funding based on income shares
to_eval <- c('DAH', 'disbursement','commitment_vax', names(dt)[names(dt) %like% '_DAH'])
dt[CHANNEL == 'WB_IDA',
   c(to_eval) := .SD * INCOME_ALL_SHARE,
   .SDcols = to_eval]

dt[, `:=`(RECIPIENT_AGENCY_SECTOR = 'GOV', LEVEL = 'COUNTRY')]
dt[ISO3_RC %in% c("QMA", "QMD", "QME", "QNA", "QNB", "QNC", "QNE", "QRA", "QRB", "QRC", "QRD",
                  "QRE", "QRS", "QSA", "QTA"), LEVEL := 'REGIONAL']
dt[ISO3_RC %in% c("QZA", "WLD"), LEVEL := 'GLOBAL']
dt[, gov := 0]
dt[LEVEL == "COUNTRY" & RECIPIENT_AGENCY_SECTOR == "GOV", gov := 1]
dt[LEVEL == "REGIONAL" & RECIPIENT_AGENCY_SECTOR == "GOV", gov := 1]
dt[gov == 0, gov := NA]
dt[, COMMITMENT := commitment_vax]

## keep and rearrange important columns
dt <- dt[, .SD, .SDcols = c('CHANNEL', 'YEAR', 'INCOME_SECTOR', 'INCOME_TYPE', 'DONOR_NAME', 'DONOR_COUNTRY', 'ISO_CODE', 'INCOME_ALL_SHARE',
                            'DISBURSEMENT', 'OUTFLOW', 'GHI', 'ISO3_RC', 'DISB', 'DAH', 'COMMITMENT', 'money_type', 'grant_loan',
                            names(dt)[names(dt) %like% '_DAH'], 'RECIPIENT_AGENCY_SECTOR', 'LEVEL', 'gov')]

# fix covid discrepancy
# remove "not_dah" columns
dt[, not_DAH := NULL]
to_fix <- names(dt)[names(dt) %like% "_DAH" & !(names(dt) %like% "^COVID_")]
dt[,test := rowSums(.SD,na.rm = T), .SDcols = to_fix]
dt[, eval(paste0(to_fix, 'prop')) := lapply(.SD, function(x) x / test), .SDcols = to_fix]
dt[, eval(paste0(to_fix)) := lapply(.SD, function(x) x * COVID_total_DAH), .SDcols = paste0(to_fix, 'prop')]
dt[, eval(paste0(to_fix, 'prop')) := NULL]
dt[,test2 := rowSums(.SD,na.rm = T), .SDcols = to_fix]
dt[, diff := COVID_total_DAH - test2]
dt[, `:=` (test=NULL,test2=NULL, diff=NULL)]
dt[CHANNEL == 'WB_', CHANNEL := 'WB_IDA']

# finalizing setup code from Compile 

dt[, `:=`( INCOME_ALL_SHARE = NULL, LEVEL = NULL, RECIPIENT_AGENCY_SECTOR = NULL,
           GHI = NULL, DISBURSEMENT = NULL, DAH = NULL, REPORTING_AGENCY = 'WB')]

dt[INCOME_SECTOR != 'PUBLIC', INCOME_TYPE := 'OTHER']
dt[INCOME_SECTOR %ni% c('PUBLIC','DEBT'), INCOME_SECTOR := 'OTHER']
dt <- dt[YEAR <= dah.roots$report_year, c('YEAR', 'CHANNEL', 'REPORTING_AGENCY', 'ISO3_RC', 'money_type', 
                        'grant_loan', 'COMMITMENT', 'INCOME_SECTOR', 'INCOME_TYPE', 'ISO_CODE', 
                        'DONOR_NAME', 'DONOR_COUNTRY',names(dt)[names(dt) %like% '_DAH']),    with = F]

#------------------------------#

# FIX NAMES
names(dt) <- gsub("_DAH$", "_amt", names(dt))
setnames(dt, c("COVID_total_amt"), c("total_amt"))


cat('  Save covid prepped dataset\n')
#----# Save dataset #----# ####
save_dataset(dt, 'COVID_prepped', 'WB', 'fin')
#-------------------# ####
