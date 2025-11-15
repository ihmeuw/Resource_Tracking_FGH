#----# Docstring #----# ####
# Project:  FGH 
# Purpose:  Finalize BMGF PREDS by HFA
#---------------------#

#----# Environment Prep #----# ####
rm(list=ls())

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)
#----------------------------#


cat('\n\n')
cat(green(' #############################\n'))
cat(green(' #### GF PREDS FINALIZE ####\n'))
cat(green(' #############################\n\n'))


cat('  Read in post-TT_smooth data\n')
#----# Read in post-TT_smooth data #----# ####
dt <- setDT(read.dta13(paste0(get_path('BMGF', 'int'), 'post_tt_smooth.dta')))
#---------------------------------------# 

cat('  Update report_year estimates\n')
#----# Update report_year estimates #----# ####
to_calc <- names(dt)[names(dt) %like% paste0('_DAH_', dah.roots$abrv_year) & !(names(dt) %like% 'pr_')] %>% str_replace_all(paste0('_DAH_', dah.roots$abrv_year), '')
for (col in to_calc) {
  dt[YEAR >= dah.roots$report_year, eval(paste0(col, '_DAH_', dah.roots$abrv_year)) := get(paste0('pr_', col, '_DAH_', dah.roots$abrv_year))]
}

dt <- dt[, c('YEAR', names(dt)[names(dt) %like% paste0('_DAH_', dah.roots$abrv_year) & !(names(dt) %like% 'pr_')]), with=F]
setnames(dt, names(dt)[names(dt) %like% paste0('_DAH_', dah.roots$abrv_year)], paste0('pr_', names(dt)[names(dt) %like% paste0('_DAH_', dah.roots$abrv_year)]))

dt <- rowtotal(dt, paste0('DAH_', dah.roots$abrv_year), names(dt)[names(dt) %like% paste0('_DAH_', dah.roots$abrv_year)])
#----------------------------------------#

cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(dt, paste0('BMGF_PREDS_DAH_1990_', dah.roots$report_year, '_BY_HFA'), 'BMGF', 'fin')
#------------------------# 