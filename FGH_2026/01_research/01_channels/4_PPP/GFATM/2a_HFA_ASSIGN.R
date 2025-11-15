#### #----#                        Docstring                         #----# ####
#' Project:         FGH
#'    
#' Purpose:         HIV Keyword Search
#------------------------------------------------------------------------------#

####################### #----# ENVIRONMENT SETUP #----# ########################
rm(list=ls())
if (!exists("code_repo")) {
  code_repo <- 'FILEPATH'
}

## Source functions
report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)
#------------------------------------------------------------------------------#


############################## #----# MAIN #----# ##############################
cat("\n\n")
cat(green(" ##################################\n"))
cat(green(" ###### GFATM KEYWORD SEARCH ######\n"))
cat(green(" ##################################\n\n"))

#### #----#               Create keyword search config               #----# ####
cat("  Create keyword search config\n")
create_Health_config(data_path = paste0(get_path("GFATM", "int"), "all_"), 
                     channel_name = "GFATM_all", 
                     varlist = c("category", "objective", "upper_SDA"), 
                     language = "english", function_to_run = 1)
#------------------------------------------------------------------------------#

#### #----#                  Launch keyword search                   #----# ####
cat("  Launch keyword search\n")
launch_Health_ADO(channel_name = "GFATM_all", queue = "long.q")
#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#
