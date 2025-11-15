#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Launch WB keyword search
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls())

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon)
#----------------------------# ####


cat('\n\n')
cat(green(' #######################\n'))
cat(green(' #### WB LAUNCH KWS ####\n'))
cat(green(' #######################\n\n'))

cat('  Create keyword search config\n')
#----# Create keyword search config #----# ####
create_Health_config(data_path = get_path('WB', 'int'),
                     channel_name = 'WB',
                     varlist = c('project_name'),
                     language = 'english',
                     function_to_run = 1)
#----------------------------------------# ####

cat('  Launch keyword search\n')
#----# Launch keyword search #----# ####
launch_Health_ADO(channel_name = 'WB')
#---------------------------------# ####