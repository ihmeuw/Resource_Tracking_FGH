#----# Docstring #----# ####
# Project:  FGH
# Purpose:  LAUNCH_TT_SMOOTH
#---------------------# ####

#-----------NOTES-----------#
# Launches TT_smooth which is a stata function. To fully move off stata
# we'd have to transition these scripts as well.
#----------------#

#----# Environment Prep #----# ####
rm(list=ls())

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(readstata13, crayon)
#----------------------------# 

NUM_YRS_FORECAST <- 2 # typically 1, but 2 for FGH 2024 since predicting 2024 and 2025


cat('\n\n')
cat(green(' ###############################\n'))
cat(green(' #### BMGF LAUNCH TT_SMOOTH ####\n'))
cat(green(' ###############################\n\n'))


cat('  Create TT_Smooth config\n')
#----# Create TT_Smooth config #----# ####
data <- setDT(read.dta13(paste0(get_path('BMGF', 'int'), 'pre_tt_smooth.dta')))

create_TT_config(data_path = get_path('BMGF', 'int'), channel_name = 'BMGF', total_colname = paste0('OUTFLOW_final_', dah.roots$abrv_year), 
                 year_colname = 'YEAR',
                 num_yrs_forecast = NUM_YRS_FORECAST,
                 is_test = 0, 
                 hfa_list = names(data)[names(data) %like% paste0('_DAH_', dah.roots$abrv_year)])
#-----------------------------------# 

cat('  Launch TT_Smooth\n')
#----# Launch TT_Smooth #----# ####
launch_TT_smooth(channel_name = 'BMGF', queue = 'all.q')
#----------------------------# 