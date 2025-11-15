#### #----#                        Docstring                         #----# ####
#' Project:         FGH
#'    
#' Purpose:         Finalize CEPI INC_EXP and ADB_PDB
#------------------------------------------------------------------------------#

#####-------------------------# enviro setup #------------------------------####

# system prep
rm(list=ls())
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

# source functions

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)
source("FILEPATH/get_location_metadata.R")


#####---------------------# save final datasets #---------------------------####


cat('  Save final datasets\n')

# read in stage 2a data
cepi_inc_exp <- setDT(fread(paste0(get_path('CEPI', 'int'), '2a_HFA_ASSIGN.csv')))


# merge isocodes
cc <- get_location_metadata(location_set_id = 35,
                            release_id = 9)[level == 3, ]
cc <- cc[, c("location_name", "ihme_loc_id")]
cc[,location_name := toupper(location_name)]
cepi_inc_exp <- merge(cepi_inc_exp, cc, by.x = "DONOR_COUNTRY", by.y = "location_name", all.x = T)
cepi_inc_exp[ISO_CODE == "", ISO_CODE := ihme_loc_id]
cepi_inc_exp[DONOR_NAME == "LIECHTENSTEIN", ISO_CODE := "LIE"]
cepi_inc_exp[,ihme_loc_id := NULL]

# save out
save_dataset(cepi_inc_exp, paste0("CEPI_INC_EXP_",dah.roots$report_year), 'CEPI', 'fin')

# add recipient iso_code as WLD
cepi_inc_exp[, ISO3_RC := 'WLD']

# recode non-countries as "other"
cepi_inc_exp[INCOME_SECTOR != "PUBLIC", DONOR_NAME := 'OTHER']

# save as adp_bdb
save_dataset(cepi_inc_exp, paste0("CEPI_ADB_PDB_", dah.roots$report_year), 'CEPI', 'fin')

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#