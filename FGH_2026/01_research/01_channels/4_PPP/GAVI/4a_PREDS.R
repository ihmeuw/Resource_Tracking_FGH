#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Generate GAVI Predictions
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls())

pacman::p_load(haven)

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

# Variable prep
income_update_year <- get_dah_param('GAVI', 'income_update_year')
disb_update_year <- get_dah_param('GAVI', 'disb_update_year')
contributions_update_tag <- get_dah_param('GAVI', 'contributions_update_tag')
DEFL <- get_path("meta", "defl")
defl_series <- setDT(read_dta(paste0(DEFL, 'imf_usgdp_deflators_', dah.roots$defl_MMYY, '.dta'))[,c('YEAR', paste0('GDP_deflator_', dah.roots$report_year))])

includes_double_counting <- FALSE # Should be True unless rerunning for noDC fixes
#----------------------------# ####


cat('\n\n')
cat(green(' ######################################\n'))
cat(green(' #### GENERATE GENERAL PREDICTIONS ####\n'))
cat(green(' ######################################\n\n'))


cat('  Reading GAVI ADB_PDB\n')
#----# Read GAVI ADB_PDB #----# ####
if (includes_double_counting) {
  dah <- setDT(fread(paste0(get_path('GAVI', 'fin'), 'P_GAVI_ADB_PDB_FGH', dah.roots$report_year, '.csv')))
} else {
  dah <- setDT(read.dta13(paste0(get_path('GAVI', 'fin'), '/GAVI_ADB_PDB_FGH', dah.roots$report_year, '_noDC.dta')))
}

setnames(dah, 'DAH', 'OUTFLOW')
dah[, OUTFLOW := OUTFLOW - oid_covid_DAH]
dah[, oid_covid_DAH := NULL]
dah <- dah[YEAR <= dah.roots$report_year,
           .(OUTFLOW = sum(OUTFLOW, na.rm = T)),
           by = .(YEAR)]
#-----------------------------# ####

cat('  Reading GAVI INT_PDB\n')
#----# Read GAVI INT_PDB #----# ####
intpdb <- fread(paste0(get_path('GAVI', 'fin'), 'P_GAVI_INTPDB_FGH', dah.roots$report_year, '.csv'))
intpdb <- intpdb[oid_covid == 0, ]
intpdb[, oid_covid := NULL]
intpdb[PURPOSE == 'In Kind', INKIND := DISBURSEMENT]
to_sum <- c('DISBURSEMENT', 'DISBURSEMENT_yr_paid', 'INVESTMENT', 'ADMIN_WRKPLAN', 'INKIND')
intpdb <- intpdb[, lapply(.SD, sum, na.rm = T), by = .(YEAR), .SDcols = to_sum]
intpdb <- merge(intpdb, dah, by = 'YEAR', all = T)
intpdb[, total := DISBURSEMENT_yr_paid + INVESTMENT + ADMIN_WRKPLAN + INKIND]
#-----------------------------# ####

cat('  Format Donor Pledges\n') 
#----# Format Donor Pledges #----# ####
pledges <- fread(paste0(get_path('GAVI', 'raw'), 'Contributions-Proceeds-to-Gavi-', contributions_update_tag, '.csv'))
# Keep all present r,y,yr,yrs-prefixed columns <= dah.roots$report_year, the Donors column, and the last two columns
yr_donor <- dah.roots$report_year
prev_yr_donor <- dah.roots$prev_report_year
toKeep <- c('Donor', paste0('r', c(2011:yr_donor)), paste0('y', c(2008:2020)), paste0('yr', c(2000:yr_donor)),
            paste0('yrs', c(2006:yr_donor)), names(pledges)[(length(names(pledges))-1):length(names(pledges))])
pledges <- pledges[Donor == 'TOTAL PLEDGED:', eval(toKeep), with = F]
# Destring
for (col in names(pledges)[names(pledges) %like% '20']) {
  pledges[, eval(col) := str_replace_all(string = get(col), pattern = ",", replacement = "")]
  pledges[, eval(col) := as.numeric(get(col))]
}
# Rowtotal each year
for (year in 2000:dah.roots$report_year) {
  pledges[, eval(paste0('yr_', year)) := rowSums(pledges[, names(pledges)[names(pledges) %like% year], with=F], na.rm=T) * 1000000]
}
# Reshape Long
pledges <- melt.data.table(pledges,
                           measure.vars = c(paste0('yr_', c(2000:dah.roots$report_year))),
                           variable.factor = F,
                           variable.name = 'YEAR',
                           value.name = 'PLEDGE')
pledges[, YEAR := as.numeric(substr(x = YEAR, start = 4, stop = length(YEAR)))]
pledges <- pledges[, .(YEAR, PLEDGE)]
#--------------------------------# ####

cat('  Combine Above Datasets\n')
#----# Combine #----# ####
pre_preds <- copy(intpdb)
pre_preds <- merge(pre_preds, pledges, by = 'YEAR', all = T)
# Replace DAH with NA for the report year since it's not complete data. New comment for 2021: keeping it and not replace since we received report year.
pre_preds[YEAR == dah.roots$report_year, DISBURSEMENT := NA]
pre_preds[YEAR == dah.roots$prev_report_year, DISBURSEMENT := NA]



est_weights <- pre_preds[, weighted_fraction := 
                           (0.5 * shift(DISBURSEMENT/PLEDGE, 1, type = "lag") +
                           1/3 * shift(DISBURSEMENT/PLEDGE, 2, type = "lag") +
                           1/6 * shift(DISBURSEMENT/PLEDGE, 3, type = "lag"))*PLEDGE]
est_weights <- est_weights[YEAR == 2022, DISBURSEMENT := weighted_fraction]
est_weights <- est_weights[, weighted_fraction := 
                           (0.5 * shift(DISBURSEMENT/PLEDGE, 1, type = "lag") +
                              1/3 * shift(DISBURSEMENT/PLEDGE, 2, type = "lag") +
                              1/6 * shift(DISBURSEMENT/PLEDGE, 3, type = "lag"))*PLEDGE]
pre_preds <- est_weights[YEAR == 2023, DISBURSEMENT := weighted_fraction]


# Deflate
pre_preds <- merge(pre_preds, defl_series, by = 'YEAR')
for (col in c('DISBURSEMENT', 'DISBURSEMENT_yr_paid', 'INVESTMENT', 'ADMIN_WRKPLAN', 'INKIND', 'total', 'PLEDGE')) {
  pre_preds[, eval(paste0(col, '_', dah.roots$abrv_year)) := get(col) / get(paste0('GDP_deflator_', dah.roots$report_year))]
}
#-------------------# ####

cat(paste0('  Predict GAVI DAH for ', dah.roots$report_year, '\n'))
#----# Predict GAVI DAH #----# ####
preds <- copy(pre_preds[order(YEAR)])
# Simple regression over disbursement (even though we end up using the raw pledges anyways)
temp <- lm(paste0('DISBURSEMENT_', dah.roots$abrv_year, ' ~ PLEDGE_', dah.roots$abrv_year, ' + YEAR'), preds)
preds[, eval(paste0('fit_DISB_', dah.roots$abrv_year)) := predict(temp, preds)]
preds[, final_prediction := get(paste0('fit_DISB_', dah.roots$abrv_year))]
rm(intpdb, pledges, temp, col, toKeep, year)
#----------------------------# ####

cat('  Save GAVI PREDS\n')
#----# Save GAVI PREDS #----# ####
if (includes_double_counting) {
  save_dataset(preds, paste0('P_GAVI_PREDS_DAH_1990_', dah.roots$report_year), 'GAVI', 'fin')
} else {
  save_dataset(preds, paste0('P_GAVI_PREDS_DAH_1990_', dah.roots$report_year, '_noDC'), 'GAVI', 'fin')
}
#---------------------------# ####
