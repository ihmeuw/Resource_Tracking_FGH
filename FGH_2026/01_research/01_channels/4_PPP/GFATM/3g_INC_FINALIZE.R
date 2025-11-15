#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Finalize INC database
#---------------------#

#----# Environment Prep #----# ####
rm(list=ls())

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

# Variable prep
defl <- get_path("meta", "defl")
#----------------------------#


cat('\n\n')
cat(green(' ############################\n'))
cat(green(" #### GFATM INC FINALIZE ####\n"))
cat(green(' ############################\n\n'))


cat('  Read in noneg data & calc yearly disbursement\n')
#----# Read in noneg data & calc yearly disbursement #----# ####
temp_disb <- setDT(fread(paste0(get_path('GFATM', 'int'), 'noneg.csv')))
setnames(temp_disb, 'DAH', 'DISBURSEMENT')
temp_disb <- collapse(temp_disb, 'sum', 'YEAR', 'DISBURSEMENT')
#---------------------------------------------------------#

cat('  Import INCOME data & Exchange Rates\n')
#----# Import INCOME data & Exchange Rates #----# ####
income_all <- setDT(fread(paste0(get_path('GFATM', 'raw'), 'P_GFATM_INCOME_ALL.csv')))
# Import exchange rates
xrates <- fread(get_path("meta", "rates",
                         paste0("OECD_XRATES_NattoUSD_1950_", dah.roots$report_year, ".csv")))
xrates[, YEAR := as.numeric(substr(TIME, 1, 4))]
xrates <- collapse(xrates, 'mean', c('YEAR', 'LOCATION', 'Country'), 'Value')
setnames(xrates, c('Value', 'LOCATION'), c('EXCHRATE', 'ISO3'))
xrates[ISO3 == 'EA19', ISO3 := 'FRA']
# Generate currency column
xrates[ISO3 == 'AUS', CURRENCY := 'AUD']
xrates[ISO3 == 'CAN', CURRENCY := 'CAD']
xrates[ISO3 == 'CHE', CURRENCY := 'CHF']
xrates[ISO3 == 'DNK', CURRENCY := 'DKK']
xrates[ISO3 == 'FRA', CURRENCY := 'EUR']
xrates[ISO3 == 'GBR', CURRENCY := 'GBP']
xrates[ISO3 == 'JPN', CURRENCY := 'JPY']
xrates[ISO3 == 'NOR', CURRENCY := 'NOK']
xrates[ISO3 == 'SWE', CURRENCY := 'SEK']
xrates[ISO3 == 'NZL', CURRENCY := 'NZD']
xrates[ISO3 == 'USA', CURRENCY := 'USD']
xrates[ISO3 == 'IDN', CURRENCY := 'IDR']
xrates[ISO3 == 'KOR', CURRENCY := 'KRW']
xrates <- xrates[CURRENCY != '' & YEAR > 2013, ]
#-----------------------------------------------#

cat('  Merge & currency convert\n')
#----# Merge & currency convert #----# ####
xrates[, u_m := 2]
income_all[, m_m := 1]
income_all <- merge(income_all, xrates, by=c('CURRENCY', 'YEAR'), all=T)
income_all[, `:=`(ISO3.y = NULL, EXCHRATE.y = NULL, Country.y = NULL)]
setnames(income_all, c('ISO3.x', 'EXCHRATE.x', 'Country.x'), c('ISO3', 'EXCHRATE', 'Country'))
income_all[, merge := rowSums(income_all[, c('u_m', 'm_m')], na.rm=T)]
income_all[, `:=`(u_m = NULL, m_m = NULL)]
income_all[ISO3 == 'SGP' & YEAR == 2017, EXCHRATE := 1.36]
income_all[ISO3 == 'SGP' & YEAR == 2018, EXCHRATE := 1.38]
income_all[CURRENCY == 'USD', EXCHRATE := 1]
income_all <- income_all[merge %in% c(1,3), !c('merge')]
# Deflate
for (col in c('INCOME_REG_PAID', 'INCOME_REG_PLEDGED')) {
  income_all[YEAR > 2013 & CURRENCY != '' & !is.na(EXCHRATE), 
             eval(col) := get(col) / EXCHRATE]
  income_all[is.na(get(col)), eval(col) := 0]
}
# Keep obs & rename
income_all <- income_all[!(INCOME_REG_PLEDGED == 0 & INCOME_REG_PAID == 0), ]
income_all[, INCOME_REG := INCOME_REG_PAID]
income_all[, INCOME_ALL := INCOME_REG]
income_all[, INCOME_TOTAL_YR := sum(INCOME_ALL, na.rm=T), by='YEAR']
income_all[, INCOME_ALL_SHARE := INCOME_ALL / INCOME_TOTAL_YR]

rm(xrates)
#------------------------------------#
 
cat('  Add total disbursements by year\n')
#----# Add total disbursements by year #----# ####
inc <- merge(income_all, temp_disb, by='YEAR', all=T)
setnames(inc, c('DISBURSEMENT', 'ISO3'), c('DISB_TOTAL', 'ISO_CODE'))
inc[, DISBURSEMENT := DISB_TOTAL * INCOME_ALL_SHARE]
inc[, OUTFLOW := DISBURSEMENT]
inc[DONOR_COUNTRY == "" & INCOME_SECTOR == 'PUBLIC', DONOR_COUNTRY := "NA"]
inc[ISO_CODE == "" & INCOME_SECTOR == "PUBLIC", ISO_CODE := "NA"]
rm(income_all, temp_disb)
#-------------------------------------------#

cat('  Calculate inkind again\n')
#----# Calculate inkind again #----# ####
inkind <- setDT(fread(paste0(get_path('GFATM', 'raw'), 'P_GFATM_INKIND.csv')))
# Merge
inkind_final <- merge(inc, inkind, by='YEAR', all=T)
inkind_final[, `:=`(CHANNEL.x = NULL, CHANNEL.y = NULL, CHANNEL = 'GFATM')]
# Calculate inkind
inkind_final[, DISBURSEMENT := (INKIND_RATIO * DISB_TOTAL) * INCOME_ALL_SHARE]
inkind_final[, OUTFLOW := DISBURSEMENT]
inkind_final[, `:=`(INKIND_RATIO = NULL, EXP_GRANTS = NULL, EXP_OPER = NULL, SOURCE = NULL)]
inkind_final[, `:=`(INKIND = 1, INCOME_ALL = 0)]
# Append
inc <- rbind(inc, inkind_final, fill=T)
inc[is.na(INKIND), INKIND := 0]
inc[, GHI := 'GFATM']
rm(inkind, inkind_final, col, defl)
#----------------------------------#

cat('  Save database\n')
#----# Save database #----# ####
save_dataset(inc, paste0('P_GFATM_INC_FGH', dah.roots$report_year), 'GFATM', 'fin')
#-------------------------#