#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Post TT_smooth finalization
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls())

if (!exists('code_repo'))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

# Variable prep
DEFL <- get_path("meta", "defl")
defl_series <- setDT(read.dta13(paste0(DEFL, 'imf_usgdp_deflators_', dah.roots$defl_MMYY, '.dta'))[, c('YEAR', paste0('GDP_deflator_', dah.roots$report_year))])
#----------------------------# ####
includes_double_counting <- FALSE 


cat('\n\n')
cat(green(' #################################\n'))
cat(green(' #### GAVI COVID REALLOCATION ####\n'))
cat(green(' #################################\n\n'))


if (includes_double_counting) {
  stop("  HALT! This script should never be run on double-counting included data! If you are NOT rerunning GAVI for double-counting elimination, do NOT run this script!")
}


cat('  Read in datasets\n')
#----# Read in datasets #----# ####
full <- fread(paste0(get_path('GAVI', 'fin'), 'P_GAVI_PREDS_DAH_BY_SOURCE_1990_', dah.roots$report_year, '_noDC.csv'))
full_copy <- copy(full)
covid <- fread(paste0(get_path('GAVI', 'fin'), 'COVID_prepped.csv'))
#----------------------------# ####


cat('  Deflate Covid and fill income sector\n')
#----# Deflate Covid and fill income sector #----# ####

## Merge
to_deflate <- c(names(covid)[names(covid) %like% '_amt'], 'COMMITMENT', 'DISBURSEMENT')
covid <- merge(covid, defl_series, by.x = 'year', by.y = 'YEAR', all.x = T)

## Deflate
covid[, c(to_deflate) := .SD / get(paste0('GDP_deflator_', dah.roots$report_year)), .SDcols = to_deflate]
covid[, eval(paste0('GDP_deflator_', dah.roots$report_year)) := NULL]

# Fill income sector
covid[grepl('Gates Foundation|BMGF', DONOR_NAME, ignore.case = T), INCOME_SECTOR := 'BMGF']
covid[INCOME_SECTOR == 'PUBLIC' & ISO_CODE == 'GBR', INCOME_SECTOR := 'UK']
covid[INCOME_SECTOR %in% c('MULTI', 'UNSP', 'DEVBANK', 'NA'), INCOME_SECTOR := 'OTHER']
covid[grepl('european commission', DONOR_NAME, ignore.case = T), INCOME_SECTOR := 'PUBLIC']
covid[INCOME_SECTOR == 'INK', INCOME_SECTOR := 'PRIVATE_INK']

covid[INCOME_SECTOR == 'PUBLIC' & ISO_CODE %in% c('AUS','BRA','CAN','CHN','DEU','DNK','ESP','FRA','IND','IRL','JPN','KOR',
                                                  'KWT','LUX','MCO','NLD','NOR','OMN','QAT','RUS','SAU','SWE','USA','ZAF'),
      INCOME_SECTOR := ISO_CODE]

covid[INCOME_SECTOR == 'UNALL', INCOME_SECTOR := 'OTHER']

## Collapse
cov <- copy(covid)
cov <- cov[money_type == 'repurposed']
cov <- collapse(cov, 'sum', c('INCOME_SECTOR', 'year'), 'total_amt')


cat('  Reallocate money\n')
#----# Reallocate money #----# ####
setnames(cov, c('total_amt', 'year'), c('repurposed_amt', 'YEAR'))

## Subset and sum HFA values
dt <- copy(full)
dt <- dt[YEAR %in% unique(cov$YEAR)]
to_sum <- names(dt)[names(dt) %like% '_DAH_']
dt[, DAH := rowSums(.SD), .SDcols = to_sum]

# merge
dt <- merge(dt, cov, by = c('INCOME_SECTOR', 'YEAR'), all.x = T)
dt[is.na(repurposed_amt), repurposed_amt := 0]
dt[repurposed_amt > DAH, repurposed_remainder := repurposed_amt - DAH]
dt[!is.na(repurposed_remainder), repurposed_amt := repurposed_amt - repurposed_remainder]
dt[INCOME_SECTOR == 'OTHER', repurposed_amt := repurposed_amt + sum(dt$repurposed_remainder, na.rm = T)] 
dt[, repurposed_remainder := NULL]

# Calculate proportion allocated to COVID to be removed
dt[, covid_proportion := repurposed_amt / DAH]
dt[is.na(covid_proportion), covid_proportion := 0]

rm(cov)

# Separate other (has negative disbursements)
dt_other <- copy(dt)
dt_other <- dt[INCOME_SECTOR == 'OTHER']
dt <- dt[INCOME_SECTOR != 'OTHER']

## Remove from non-other
dt[, c(to_sum) := .SD * (1 - covid_proportion), .SDcols = to_sum]

## Remove from other (only remove from positive PA values)
dt_other[, DAH := nch_cnv_DAH_23 + nch_hss_other_DAH_23]
dt_other[, covid_proportion := repurposed_amt / DAH]
dt_other[, nch_cnv_DAH_23 := nch_cnv_DAH_23 * (1 - covid_proportion)]
dt_other[, nch_hss_other_DAH_23 := nch_hss_other_DAH_23 * (1 - covid_proportion)]

## Rebind
dt <- rbind(dt, dt_other)

## Remove intermediate columns
dt[, `:=` (DAH = NULL, repurposed_amt = NULL, covid_proportion = NULL)]

#----------------------------# ####

cat('  Re-append new data\n')
#----# Re-append new data #----# ####
full <- full[!YEAR %in% unique(dt$YEAR)]
full <- rbind(full, dt)
rm(dt, dt_other)
#------------------------------# ####

cat('  Re-save PREDS dataset\n')
#----# Re-save PREDS dataset #----# ####
save_dataset(full, paste0('P_GAVI_PREDS_DAH_BY_SOURCE_1990_', dah.roots$report_year, '_noDC'), 'GAVI', 'fin')
#---------------------------------# ####