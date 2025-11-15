#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Create WB INC dataset
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))
start.time <- Sys.time()
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, dplyr)

# Variable prep
codes <- get_path("meta", "locs")
#----------------------------# ####


cat('\n\n')
cat(green(' #######################\n'))
cat(green(' #### WB INC CREATE ####\n'))
cat(green(' #######################\n\n'))


cat('  Read in IDA income\n')
#----# Read in IDA income #----# ####
dac_income <- setDT(fread(paste0(get_path('WB', 'raw'), 'WB_IDA_INCOME_BYDONOR.csv')))
dac_income <- dac_income[year < dah.roots$report_year]
setnames(dac_income, c('value', 'amounttype'), c('income', 'income_type'))
dac_income[income_type %like% 'Constant', income_type := 'income_16']
dac_income[income_type %like% 'Current', income_type := 'income_current']
dac_income <- dcast.data.table(dac_income,
                               formula = 'donor + year ~ income_type',
                               value.var = 'income')
dac_income[, `:=`(INCOME_SECTOR = "PUBLIC", INCOME_TYPE = "CENTRAL", CHANNEL = "WB_IDA", 
                  DONOR_NAME = donor, DONOR_COUNTRY = donor)]
setnames(dac_income, 'donor', 'country_lc')

# Read in ISO codes
isos <- setDT(readstata13::read.dta13(paste0(codes, 'countrycodes_official.dta')))[, c('country_lc', 'iso3')]
isos[, u_m := 2]
dac_income[, m_m := 1]
# Merge
dac_income <- merge(dac_income, isos, by='country_lc', all=T)
dac_income[, merge := rowSums(dac_income[, c('u_m', 'm_m')], na.rm=T)]
dac_income <- dac_income[merge == 3, !c('merge', 'u_m', 'm_m')]
setnames(dac_income, 'country_lc', 'countryname')
rm(isos, codes)

dac_income[, total_inc := sum(income_current, na.rm=T), by='countryname']
dac_income <- dac_income[total_inc != 0]
dac_income[, total_inc := NULL]

setnames(dac_income, c('iso3', 'year', 'income_current'), c('ISO_CODE', 'YEAR', 'INCOME_ALL'))
dac_income <- dac_income[, c('INCOME_SECTOR', 'INCOME_TYPE', 'CHANNEL', 'DONOR_NAME', 'DONOR_COUNTRY', 'ISO_CODE', 'YEAR', 'INCOME_ALL')]
#------------------------------# ####

cat('  Read income from loans & IBRD transfers\n')
#----# Read income from loans & IBRD transfers #----# ####
inc_other <- setDT(fread(paste0(get_path('WB', 'raw', 'WB_IDA_INCOME_OTHER.csv'))))
inc_other <- inc_other[YEAR < dah.roots$report_year]
inc_other[, INCOME_TYPE := as.character(INCOME_TYPE)]
inc_other[, INCOME_TYPE := '']
# Append dac_income
inc_other <- rbind(inc_other, dac_income, fill=T)
inc_other[, INCOME_TOTAL_YR := sum(INCOME_ALL, na.rm=T), by='YEAR']
inc_other[, INCOME_ALL_SHARE := INCOME_ALL / INCOME_TOTAL_YR]
# Bring in total disbursements
temp_ida <- setDT(fread(paste0(get_path('WB', 'int'), 'total_disb.csv')))[, c('YEAR', 'disb_ida')]
setnames(temp_ida, 'disb_ida', 'DISB_TOTAL')
temp_ida[, DISB_TOTAL := DISB_TOTAL / 1000000]
temp_ida[, u_m := 2]
# Merge
inc_other[, m_m := 1]
inc_other <- merge(inc_other, temp_ida, by='YEAR', all=T)
inc_other[, merge := rowSums(inc_other[, c('u_m', 'm_m')], na.rm=T)]
inc_other <- inc_other[merge == 3, !c('merge', 'u_m', 'm_m')]
rm(temp_ida, dac_income)

# project income forward one year in case we have report-year project data
inc_other[order(YEAR),
          pr_income := (1/2) * INCOME_ALL +
              (1/3) * shift(INCOME_ALL, n = 1) +
              (1/6) * shift(INCOME_ALL, n = 2),
          by = .(CHANNEL, INCOME_SECTOR, INCOME_TYPE, DONOR_NAME, DONOR_COUNTRY, ISO_CODE)]

inc_ry <- inc_other[YEAR == dah.roots$prev_report_year]
if (report_year == 2024) {
    inc_fr <- inc_other[DONOR_NAME == "France"][YEAR == max(YEAR)]
    inc_ry <- rbind(inc_ry, inc_fr)
    rm(inc_fr)
}

inc_ry[, `:=`(
    YEAR = dah.roots$report_year,
    INCOME_ALL = pr_income,
    INCOME_TOTAL_YR = sum(pr_income, na.rm=TRUE)
)]
inc_ry[, INCOME_ALL_SHARE := INCOME_ALL / INCOME_TOTAL_YR]

disb <- unique(inc_other[, .(YEAR, DISB_TOTAL)])
disb[order(YEAR), pr_disb := (1/2) * DISB_TOTAL +
    (1/3) * shift(DISB_TOTAL, n = 1) +
    (1/6) * shift(DISB_TOTAL, n = 2)]
disb <- disb[YEAR == dah.roots$prev_report_year,]
inc_ry[, DISB_TOTAL := disb$pr_disb]

inc_other <- rbind(inc_other, inc_ry)
inc_other[, pr_income := NULL]

#---------------------------------------------------# ####

cat('  Calculate IDA inkind\n')
#----# Calculate IDA inkind #----# ####
inkind_ratio_ida <- setDT(fread(paste0(get_path('WB', 'int'), 'inkind_ratio_ida.csv')))
inkind_disb_ida <- merge(inc_other, inkind_ratio_ida, by=c('YEAR', 'CHANNEL'), all.x=T)
inkind_disb_ida[, DISBURSEMENT := DISB_TOTAL * INKIND_RATIO]
inkind_disb_ida[, DISBURSEMENT := DISBURSEMENT * INCOME_ALL_SHARE]
inkind_disb_ida[, `:=`(OUTFLOW = DISBURSEMENT, INKIND = 1, INCOME_ALL = 0)]
# Match & append
inc_other[, DISBURSEMENT := DISB_TOTAL * INCOME_ALL_SHARE]
inc_other[, OUTFLOW := DISBURSEMENT]
inc_other <- rbind(inc_other, inkind_disb_ida, fill=T)
inc_other <- inc_other[!is.na(DISBURSEMENT) & !is.na(OUTFLOW)]
rm(inkind_disb_ida, inkind_ratio_ida)
#--------------------------------# ####

cat('  Calculate IBRD income\n')
#----# Calculate IBRD income #----# ####
temp_ibrd <- setDT(fread(paste0(get_path('WB', 'int'), 'total_disb.csv')))[, c('YEAR', 'disb_ibrd')]
setnames(temp_ibrd, 'disb_ibrd', 'DISB_TOTAL')
temp_ibrd[, DISB_TOTAL := DISB_TOTAL / 1000000]
temp_ibrd[, `:=`(CHANNEL = "WB_IBRD", INCOME_SECTOR = "OTHER", INCOME_TYPE = "", 
                 DONOR_NAME = "Bond Issuance", DONOR_COUNTRY = "NA", ISO_CODE = "NA",
                 OUTFLOW = DISB_TOTAL)]
# Calculate inkind
inkind_disb_ibrd <- copy(temp_ibrd)
inkind_ratios <- setDT(fread(paste0(get_path('WB', 'int'), 'inkind_ratios.csv')))
inkind_disb_ibrd <- merge(inkind_disb_ibrd, inkind_ratios, by=c('CHANNEL', 'YEAR'), all.x=T)
inkind_disb_ibrd[, DISBURSEMENT := DISB_TOTAL * INKIND_RATIO]
inkind_disb_ibrd[, `:=`(OUTFLOW = DISBURSEMENT, INKIND = 1)]
rm(inkind_ratios)
#---------------------------------# ####

cat('  Append datasets\n')
#----# Append datasets #----# ####
wb_adb <- rbind(inc_other, temp_ibrd, fill=T)
wb_adb <- rbind(wb_adb, inkind_disb_ibrd, fill=T)

wb_adb[, `:=`(GHI = 'WB', INCOME_ALL = INCOME_ALL * 1000000,
              DISBURSEMENT = DISBURSEMENT * 1000000, 
              OUTFLOW = OUTFLOW * 1000000)]
wb_adb <- wb_adb[YEAR >= 1990]
rm(inc_other, temp_ibrd, inkind_disb_ibrd)
#---------------------------# ####

cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(wb_adb, 'wb_adb', 'WB', 'int')
save_dataset(wb_adb, paste0('WB_INC_DISB_FINAL_1990_', dah.roots$report_year), 'WB', 'fin')
#------------------------# ####
