#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Launch BMGF Health_ADO
#---------------------# 

#-----------NOTES----------#
# depends on FUNCTIONS/utils.r:launch_health_ADO which in turn launches
# FUNCTIONS/launch_Health_ADO.ado. Will need to be part of stata transition
# to remove these dependencies, which also affects utils.R
#--------------------------#

#----# Environment Prep #----# ####
rm(list=ls())

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon)
#----------------------------# 


cat('\n\n')
cat(green(' #############################\n'))
cat(green(' #### BMGF KEYWORD SEARCH ####\n'))
cat(green(' #############################\n\n'))


cat('  Configure BMGF Health_ADO\n')
#----# Configure BMGF Health_ADO #----# ####
# `bmgf_`pre_kws.dta is created in phase 1 of the pipeline (script 1b)
create_Health_config(data_path = paste0(get_path('BMGF', 'int'), 'bmgf_'), channel_name = 'bmgf', 
                     varlist = c('long_description', 'short_description'), language = 'english', 
                     function_to_run = 1)
#-------------------------------------#

cat('  Launch health_ADO\n')
#----# Launch Health_ADO #----# ####
launch_Health_ADO(channel_name = 'bmgf', runtime = '00:30:00', job_threads = 6, queue = 'all.q')
#-----------------------------# 