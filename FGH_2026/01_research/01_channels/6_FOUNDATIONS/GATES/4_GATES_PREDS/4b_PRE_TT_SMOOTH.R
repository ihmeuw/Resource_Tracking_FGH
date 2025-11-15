#----# Docstring #----# ####
# Project:  FGH 
# Purpose:  PRE_TT_SMOOTH
#---------------------#

#----------NOTES----------#
# uses collapse and row total which should be deprecated in favor of data.table
# lapply() and rowSums() syntax
#-------------------------------#

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)
#----------------------------#


cat('\n\n')
cat(green(' ############################\n'))
cat(green(' #### GF PRE_TT_SMOOTH ####\n'))
cat(green(' ############################\n\n'))


cat('  Read in ADB_PDB\n')
#----# Read in ADB_PDB #----# ####
dt <- setDT(fread(paste0(get_path('BMGF', 'fin'), 'BMGF_ADB_PDB_FGH_', dah.roots$report_year, '.csv')))
dt <- dt[YEAR < dah.roots$report_year]
dt <- dt[ELIM_CH == 0]

dt <- collapse(dt, 'sum', 'YEAR', c('DAH', names(dt)[names(dt) %like% '_DAH']))

dt <- rbind(dt, data.frame('YEAR'=dah.roots$report_year), fill=T)
dt <- dt[order(YEAR), ]
#---------------------------# 

cat('  Merge GF PREDS\n')
#----# Merge GF PREDS #----# ####
preds <- setDT(fread(paste0(get_path('BMGF', 'fin'), 'BMGF_PREDS_DAH_1990_', dah.roots$report_year, '.csv')))[, c('YEAR', paste0('OUTFLOW_final_', dah.roots$abrv_year), paste0('GDP_deflator_', dah.roots$report_year)), with=F]

dt <- merge(dt, preds, by='YEAR', all=T)

for(col in names(dt)[names(dt) %like% '_DAH']) {
  dt[, eval(paste0(col, '_', dah.roots$abrv_year)) := get(col) / get(paste0('GDP_deflator_', dah.roots$report_year))]
}

dt[, eval(paste0('GDP_deflator_', dah.roots$report_year)) := NULL]
dt <- rowtotal(dt, 'new_tot', names(dt)[names(dt) %like% paste0('_DAH_', dah.roots$abrv_year)])

dt[YEAR < dah.roots$report_year, eval(paste0('OUTFLOW_final_', dah.roots$abrv_year)) := new_tot]
rm(preds)
#----------------------------#

cat('  Save pre-tt_smooth dataset\n')
#----# Save pre-tt_smooth dataset #----# ####
save_dataset(dt, 'pre_tt_smooth', 'BMGF', 'int', format = "dta")
#--------------------------------------#
