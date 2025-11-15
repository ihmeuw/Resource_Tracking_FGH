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
includes_double_counting <- FALSE  # Should be True unless rerunning for noDC fixes
#----------------------------# ####


cat('\n\n')
cat(green(' #######################\n'))
cat(green(' #### RUN TT_SMOOTH ####\n'))
cat(green(' #######################\n\n'))


cat('  Read in Post-TT_smooth data\n')
#----# Read in Post-TT_smooth #----# ####
data <- setDT(read.dta13(paste0(get_path('GAVI', 'int'), 'post_tt_smooth.dta')))
#----------------------------------# ####

cat('  Update report year predictions for NCD, NCD_HSS, and NCH_CNV\n')
#----# Update report year #----# ####
# HFAs to keep
toKeep <- names(data)[names(data) %like% 'ncd_' | names(data) %like% 'nch_hss_' | names(data) %like% 'nch_cnv' |
                        names(data) %like% 'pr_ncd_' | names(data) %like% 'pr_nch_hss_' | names(data) %like% 'pr_nch_cnv']

# HFAs to correct
toCorrect <- toKeep[toKeep %like% "pr_"]
# Update report year HFAs
for (col in toCorrect) {
  hfa <- gsub('pr_', '', col)
  data[YEAR %in% c(dah.roots$report_year, dah.roots$prev_report_year), eval(hfa) := get(col)]
}

# Drop other "pr_" columns
toDrop <- names(data)[names(data) %like% 'pr_']
data[, eval(toDrop) := NULL]
#------------------------------# ####

cat('  Reshape long\n')
#----# Reshape Long #----# ####
data <- melt.data.table(data, 
                        measure.vars = names(data)[names(data) %like% '_' & (names(data) %ni% c(paste0('DISBURSEMENT_', dah.roots$abrv_year), 'final_prediction'))],
                        variable.name = 'HFA',
                        variable.factor = F,
                        value.name = 'amt')

# Add income sector column
data[, INCOME_SECTOR := str_extract(HFA, "[[:upper:]].+$")]

# Remove income sector piece from HFAs
data[, HFA := gsub("_[[:upper:]].+$", '', HFA)]

# Rename HFAs
data[, HFA := paste0(HFA, '_DAH_', dah.roots$abrv_year)]

# Reshape wide
data <- collapse(dataset = data, agg_function = 'sum', group_cols = c('YEAR', 'INCOME_SECTOR', 'HFA', paste0('DISBURSEMENT_', dah.roots$abrv_year), 'final_prediction'), calc_cols = c('amt'))
data <- dcast.data.table(data, formula = paste0('YEAR + INCOME_SECTOR + final_prediction + DISBURSEMENT_', dah.roots$abrv_year, ' ~ HFA'), value.var = 'amt')

# Add channel name and rename cols
data[, CHANNEL := 'GAVI']
#------------------------# ####

cat('  Save PREDS by SOURCE\n')
#----# Save GAVI PREDS by SOURCE #----# ####
if (includes_double_counting) {
  save_dataset(data, paste0('P_GAVI_PREDS_DAH_BY_SOURCE_1990_', dah.roots$report_year), 'GAVI', 'fin')
} else {
  save_dataset(data, paste0('P_GAVI_PREDS_DAH_BY_SOURCE_1990_', dah.roots$report_year, '_noDC'), 'GAVI', 'fin')
}
#-------------------------------------# ####