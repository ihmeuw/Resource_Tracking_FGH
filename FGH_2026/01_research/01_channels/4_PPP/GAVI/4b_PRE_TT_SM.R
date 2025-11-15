#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Generate GAVI Predictions by Source
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls())

pacman::p_load(haven)

if (!exists('code_repo'))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

# Variable prep
income_update_year <- get_dah_param('GAVI', 'income_update_year')
disb_update_year <- get_dah_param('GAVI', 'disb_update_year')
DEFL <- get_path("meta", "defl")
defl_series <- setDT(read.dta13(paste0(DEFL, 'imf_usgdp_deflators_', dah.roots$defl_MMYY, '.dta'))[,c('YEAR', paste0('GDP_deflator_', dah.roots$report_year))])

includes_double_counting <- FALSE # Should be True unless rerunning for noDC fixes
#----------------------------# ####


cat('\n\n')
cat(green(' ########################################\n'))
cat(green(' #### GENERATE PREDICTIONS BY SOURCE ####\n'))
cat(green(' ########################################\n\n'))


cat('  Calculate GAVI Predictions by Source\n')
#----# Format Data #----# ####
if (includes_double_counting) {
  fractions <- fread(paste0(get_path('GAVI', 'fin'), 'P_GAVI_ADB_PDB_FGH', dah.roots$report_year, '.csv'))
} else {
  fractions <- setDT(read.dta13(paste0(get_path('GAVI', 'fin'), 'GAVI_ADB_PDB_FGH', dah.roots$report_year, '_noDC.dta')))
}

fractions <- fractions[oid_covid_DAH == 0, ]
fractions[, oid_covid_DAH := NULL]

# Fill income sector
fractions[DONOR_NAME == 'BMGF' | CHANNEL == 'BMGF', INCOME_SECTOR := 'BMGF']
fractions[INCOME_SECTOR == 'PUBLIC' & ISO_CODE == 'GBR', INCOME_SECTOR := 'UK']
fractions[INCOME_SECTOR == 'MULTI' | INCOME_SECTOR == 'UNSP' | INCOME_SECTOR == 'DEVBANK' | INCOME_SECTOR == 'NA', INCOME_SECTOR := 'OTHER']
fractions[INCOME_SECTOR == 'INK', INCOME_SECTOR := 'PRIVATE_INK']
fractions[INCOME_SECTOR == 'OTHER_PRIV', INCOME_SECTOR := 'PRIVATE']

for (iso in c('AUS','BRA','CAN','CHN','DEU','DNK','ESP','FRA','IND','IRL','JPN','KOR',
              'KWT','LUX','MCO','NLD','NOR','OMN','QAT','RUS','SAU','SWE','USA','ZAF')) {
  fractions[INCOME_SECTOR == 'PUBLIC' & ISO_CODE == iso, INCOME_SECTOR := iso]
}

# Collapse sum and reshape
toCollapse <- names(fractions)[names(fractions) %like% "_DAH" & !names(fractions) %like% '_TOT']
fractions <- fractions[, lapply(.SD, sum, na.rm=T), by=c('YEAR', 'INCOME_SECTOR'), .SDcols=toCollapse]
fractions <- melt.data.table(fractions, 
                             measure.vars = toCollapse,
                             variable.factor = F,
                             variable.name = 'hfa',
                             value.name = 'DAH')
fractions[, hfa := substr(hfa, 1, nchar(hfa) - 4)]

# Calculate fractions of total
fractions[, tot := sum(DAH, na.rm=T), by='YEAR']
fractions[, frct := DAH / tot]
fractions[, `:=`(DAH = NULL, tot = NULL)]
rm(iso, toCollapse)
#-----------------------# ####

cat('  Adding Observed DAH Data\n')
#----# Read in actual DAH data #----# ####
# We need to do this because we want to apply hfa and income_sector fractions to the TOTAL yearly DAH amounts (with double counted subtracted out)
# If we tried to simply subtract out health focus area double counting there would be a lot of negative values 
if (includes_double_counting) {
  pbs <- fread(paste0(get_path('GAVI', 'fin'), 'P_GAVI_ADB_PDB_FGH', dah.roots$report_year, '.csv'))
} else {
  pbs <- setDT(read.dta13(paste0(get_path('GAVI', 'fin'), 'GAVI_ADB_PDB_FGH', dah.roots$report_year, '_noDC.dta')))
}

pbs <- pbs[oid_covid_DAH == 0, ]
pbs[, oid_covid_DAH := NULL]

# this drops the double counting by adding in the negative DAH values from compiling
pbs <- pbs[, .(DAH = sum(DAH, na.rm = T)),
           by = .(YEAR)]

# Merge with fractions
pbs <- merge(pbs, fractions, by = 'YEAR', all = T)
#-----------------------------------# ####

cat('  Deflate & Reshape\n')
#----# Deflate & Reshape #----# ####
pbs <- merge(pbs, defl_series, by='YEAR')
# Generate report year DAH amounts
pbs[, eval(paste0('DAH_', dah.roots$abrv_year)) := DAH / get(paste0('GDP_deflator_', dah.roots$report_year))]
pbs[, eval(paste0('DAH_', dah.roots$abrv_year)) := get(paste0('DAH_', dah.roots$abrv_year)) * frct]
# Remove cols and reshape
pbs <- pbs[, c('YEAR', 'INCOME_SECTOR', 'hfa', paste0('DAH_', dah.roots$abrv_year)), with=F]
pbs <- dcast.data.table(pbs, formula = 'YEAR + INCOME_SECTOR ~ hfa', value.var = eval(paste0('DAH_', dah.roots$abrv_year)))
pbs <- dcast.data.table(pbs, formula = 'YEAR ~ INCOME_SECTOR', value.var = names(pbs)[3:length(pbs)])
#-----------------------------# ####

cat('  Append Predicted DAH\n')
#----# Append predicted DAH #----# ####
if (includes_double_counting) {
  temp <- fread(paste0(get_path('GAVI', 'fin'), 'P_GAVI_PREDS_DAH_1990_', dah.roots$report_year, '.csv'))
} else {
  temp <- fread(paste0(get_path('GAVI', 'fin'), 'P_GAVI_PREDS_DAH_1990_', dah.roots$report_year, '_noDC.csv'))
}

temp <- temp[, c('YEAR', paste0('DISBURSEMENT_', dah.roots$abrv_year), 'final_prediction'), with=F]
pbs <- merge(pbs, temp, by='YEAR', all=T)

pbs[YEAR == dah.roots$prev_report_year - 1, eval(paste0('DISBURSEMENT_', dah.roots$abrv_year)) := final_prediction]
pbs[YEAR == dah.roots$prev_report_year, eval(paste0('DISBURSEMENT_', dah.roots$abrv_year)) := final_prediction]
pbs[YEAR == dah.roots$report_year, eval(paste0('DISBURSEMENT_', dah.roots$abrv_year)) := final_prediction]
rm(temp)
#--------------------------------# ####
 
cat('  Saving Pre-TT_Smooth Data\n')
#----# Save data for TT_smooth #----# ####
save_dataset(pbs, 'pre_tt_smooth', 'GAVI', 'int', format = "dta")
#-----------------------------------# ####