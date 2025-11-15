#----# Docstring #----# ####
# Project:  FGH 
# Purpose:  Assign CHINA CIDCA HFAs
#---------------------# ####

#*********************************************************************#
####-----                       NOTES                         -----####
# Decided all post 2020 values to be given to COVID and moved the COVID tracking from 1b to 2b
# This will keep consistency with NHC files where COVID is removed post HFA allocation in 2C



#----# Environment Prep #----# ####
# System prep
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, dplyr)

#----------------------------# ####


cat('\n\n')
cat(green(' ##########################################\n'))
cat(green(' #### BEGIN CHINA CIDCA HFA ASSIGNMENT ####\n'))
cat(green(' ##########################################\n\n'))


cat('  Read Stage 1b Data\n')
#----# Read stage 1b #----# ####
cidca <- fread(get_path('CHINA', 'int', '1b_CIDCA.csv'))
#-------------------------# ####

cat('  Assign HFAs\n')
#----# Assign HFAs #----# ####
# Assign HFAs

cidca[, `:=`(swap_hss_other_DAH = (CIDCA_DAH)/XRATE)]

hfas <- c(paste0("swap_hss_other_DAH_", dah.roots$abrv_year))
cidca[, eval(hfas) := 0]
cidca[, eval(paste0("swap_hss_other_DAH_", dah.roots$abrv_year)) := get(paste0("CIDCA_DAH_", dah.roots$abrv_year))]

#---- Removing COVID
# Update envelope to be all COVID after 2020 (i.e. drop it and pick it up in COVID_PREP.R)
cat(paste0(yellow('    NOTE: Dropping years in interval [2020, 2022] and picking it back up in COVID_PREP.R. Decision made that all',
                  'CIDCA DAH after 2020 was COVID-related, so removing it here.\n')))
# Subset COVID data, any year after 2019
covid_out <- cidca[between(YEAR, 2020, 2022) & inkind == 0,
                   .(YEAR, amount = CIDCA_DAH_USD)]
covid_out[, `:=`(CHANNEL = "BIL_CHN",
                 DONOR_NAME = "CHN_CIDCA",
                 money_type = 'repurposed')]

covid_out[YEAR == 2022,
          amount := amount * as.numeric(get_dah_param("china", "covid_frac_2022"))]
# Write dataset
save_dataset(covid_out, "CIDCA_COVID", 'CHINA', 'int')
rm(covid_out)

# Remove years after 2020 values from NHC data since they are COVID
cidca <- cidca[!between(YEAR, 2020, 2021), ]
dahcols <- c("CIDCA_DAH", paste0("CIDCA_DAH_", dah.roots$abrv_year), "CIDCA_DAH_USD")
cidca[YEAR == 2022, (dahcols) := lapply(.SD, \(x) {
    x * (1 - as.numeric(get_dah_param("china", "covid_frac_2022")))
}), .SDcols = dahcols]


# Rename data
CHINA_CIDCA <- copy(cidca)
#-----------------------# ####

cat('  Save Stage 2b Dataset\n')
#----# Save Stage 2b Dataset #----# ####
save_dataset(CHINA_CIDCA, "2b_CIDCA", 'CHINA', 'int')
#---------------------------------# ####