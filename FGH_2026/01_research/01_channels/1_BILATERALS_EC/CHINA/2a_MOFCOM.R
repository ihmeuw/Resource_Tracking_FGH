#----# Docstring #----# ####
# Project:  FGH 
# Purpose:  Assign CHINA MOFCOM HFAs
#---------------------# ####

#----# Environment Prep #----# ####
# System prep
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr, dplyr)



cat('\n\n')
cat(green(' ###########################################\n'))
cat(green(' #### BEGIN CHINA MOFCOM HFA ASSIGNMENT ####\n'))
cat(green(' ###########################################\n\n'))


cat('  Read in Stage 1a Data\n')
#----# Read Stage 1a #----# ####
mofcom <- fread(get_path('CHINA', 'int', '1a_MOFCOM.csv'))
#-------------------------# ####

cat('  Assign HFAs & Deflate\n')
#----# Assign HFAs and Deflate #----# ####
# Add HFA - assign HFA based on literature review
mofcom[, swap_hss_other_DAH := E_MOFCOM_DAH]

# Deflation
deflators <- fread(get_path("meta", "defl", "imf_usgdp_deflators_[defl_MMYY].csv"))
deflators <- deflators[, c("YEAR", paste0("GDP_deflator_", dah.roots$report_year)),
                       with = FALSE]
mofcom <- merge(mofcom, deflators, by="YEAR", all.x = TRUE)

# Change to USD
exch_rates <- fread(get_path("meta", "rates", "OECD_XRATES_NattoUSD_1950_[report_year].csv"))
exch_rates <- exch_rates[LOCATION == "CHN" & TIME >= 1990, c("TIME", "Value")]
setnames(exch_rates, c("TIME", "Value"), c("YEAR", "XRATE"))
mofcom <- merge(mofcom, exch_rates, by="YEAR", all.x = TRUE)
mofcom[is.na(E_MOFCOM_DAH_USD), E_MOFCOM_DAH_USD := E_MOFCOM_DAH / XRATE]
mofcom[, swap_hss_other_DAH_USD := E_MOFCOM_DAH_USD]

mofcom[, paste0("E_MOFCOM_DAH_", dah.roots$abrv_year) := E_MOFCOM_DAH_USD / get(paste0("GDP_deflator_", dah.roots$report_year))]
mofcom[, paste0("swap_hss_other_DAH_", dah.roots$abrv_year) := get(paste0("E_MOFCOM_DAH_", dah.roots$abrv_year))]
#-----------------------------------# ####

cat('  Save Stage 2a Data\n')
#----# Save 2a #----# ####
save_dataset(mofcom, "2a_MOFCOM", 'CHINA', 'int')
#-------------------# ####