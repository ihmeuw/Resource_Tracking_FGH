#----# Docstring #----# ####
# Project:  FGH 
# Purpose:  Assign CHINA EXIM BANK HFAs
#---------------------# ####

#----# Environment Prep #----# ####
# System prep
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

#----------------------------# ####


cat('\n\n')
cat(green(' ##############################################\n'))
cat(green(' #### BEGIN CHINA EXIM BANK HFA ASSIGNMENT ####\n'))
cat(green(' ##############################################\n\n'))


cat('  Read in Stage 1d Data\n')
#----# Read in Stage 1d Data #----# ####
exim <- fread(get_path('CHINA', 'int', '1e_EXIM_BANK.csv'))
#---------------------------------# ####

cat('  Assign HFAs\n')
#----# Assign HFAs #----# ####
# add HFA - assign HFA based on literature review
exim[, swap_hss_other_DAH := E_EXIM_DAH]
#-----------------------# ####

cat('  Deflate\n')
#----# Deflate #----# ####
# Deflation
deflators <- fread(get_path("meta", "defl", "imf_usgdp_deflators_[defl_MMYY].csv"))
deflators <- deflators[, c("YEAR", paste0("GDP_deflator_", dah.roots$report_year)),
                       with = FALSE]

exim <- merge(exim, deflators, by = "YEAR", all.x = TRUE)

exim[, `:=`(E_EXIM_DAH_USD = E_EXIM_DAH / XRATE,
            swap_hss_other_DAH_USD = swap_hss_other_DAH / XRATE)]
exim[, eval(paste0("E_EXIM_DAH_", dah.roots$abrv_year)) := E_EXIM_DAH_USD / get(paste0("GDP_deflator_", dah.roots$report_year))]
exim[, eval(paste0("swap_hss_other_DAH_", dah.roots$abrv_year)) := swap_hss_other_DAH_USD / get(paste0("GDP_deflator_", dah.roots$report_year))]

# Rename data
CHINA_EXIM <- copy(exim)
#-------------------# ####

cat('  Save Stage 2e Data\n')
#----# Save Stage 2e Data #----# ####
save_dataset(CHINA_EXIM, "2e_EXIM_BANK", 'CHINA', 'int')
#------------------------------# ####