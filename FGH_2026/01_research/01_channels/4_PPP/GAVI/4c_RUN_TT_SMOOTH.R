#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Run TT_smooth
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls())

if (!exists('code_repo'))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)
#----------------------------# ####


cat('\n\n')
cat(green(' #######################\n'))
cat(green(' #### RUN TT_SMOOTH ####\n'))
cat(green(' #######################\n\n'))


cat('  Prepare config file\n')
#----# Make Config #----# ####
# Read in pre-tt_smooth dataset
pbs <- setDT(read.dta13(paste0(get_path('GAVI', 'int'), 'pre_tt_smooth.dta')))
a <- pbs[, ]


args <- c()
# # Foreach column that has at least two observed years of data (any less and TT_smooth gets mad), add it to the list
for (col in names(pbs)[names(pbs) %ni% c('YEAR', paste0('DISBURSEMENT_', dah.roots$abrv_year), 'final_prediction')]) {
  if (nrow(pbs[!is.na(pbs[[col]])]) >= 2) {
    args <- append(args, col)
  }
}

# Create TT_smooth config file
create_TT_config(data_path = get_path('GAVI', 'int'), channel_name = 'gavi', total_colname = paste0('DISBURSEMENT_', dah.roots$abrv_year),
                 year_colname = 'YEAR', num_yrs_forecast = 2, is_test = 0, hfa_list = args)
#-----------------------# ####

cat('  Run TT_smooth\n')
#----# Run TT_smooth #----# ####
launch_TT_smooth(channel_name = 'gavi', queue = 'all.q')
#-------------------------# ####