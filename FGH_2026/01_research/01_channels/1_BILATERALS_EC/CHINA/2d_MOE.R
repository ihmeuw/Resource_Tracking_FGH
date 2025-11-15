#----# Docstring #----# ####
# Project:  FGH 
# Purpose:  Assign CHINA MOE HFAs
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
cat(green(' ########################################\n'))
cat(green(' #### BEGIN CHINA MOE HFA ASSIGNMENT ####\n'))
cat(green(' ########################################\n\n'))


cat('  Read in Stage 1d Data\n')
#----# Read in Stage 1d Data #----# ####
moe <- fread(get_path('CHINA', 'int', '1d_MOE.csv'))
#---------------------------------# ####

cat('  Assign HFAs\n')
#----# Assign HFAs #----# ####
# add HFA - assign HFA based on literature review
moe[, swap_hss_hrh_DAH := E_MOE_DAH]
#-----------------------# ####

cat('  Deflate and Currency Convert\n')
#----# Deflate and Currency Convert #----# ####
exch_rates <- fread(get_path("meta", "rates", "OECD_XRATES_NattoUSD_1950_[report_year].csv"))
exch_rates <- exch_rates[LOCATION == "CHN" & TIME >= 1990, c("TIME", "Value")]
setnames(exch_rates, c("TIME", "Value"), c("YEAR", "XRATE"))

deflators <- fread(get_path("meta", "defl", "imf_usgdp_deflators_[defl_MMYY].csv"))
deflators <- deflators[, c("YEAR", paste0("GDP_deflator_", dah.roots$report_year)),
                       with = FALSE]

# Deflation & exchange rates
moe <- merge(moe, deflators, by = "YEAR", all.x = TRUE)
moe <- merge(moe, exch_rates, by = "YEAR", all.x = TRUE)

moe[, `:=`(E_MOE_DAH_USD = E_MOE_DAH / XRATE,
           swap_hss_hrh_DAH_USD = swap_hss_hrh_DAH / XRATE)]
moe[, eval(paste0("E_MOE_DAH_", dah.roots$abrv_year)) := E_MOE_DAH_USD / get(paste0("GDP_deflator_", dah.roots$report_year))]
moe[, eval(paste0("swap_hss_hrh_DAH_", dah.roots$abrv_year)) := swap_hss_hrh_DAH_USD / get(paste0("GDP_deflator_", dah.roots$report_year))]

# Rename data
CHINA_MOE <- copy(moe)
#----------------------------------------# ####

cat('  Save Stage 2d Data\n')
#----# Save Stage 2d Data #----# ####
save_dataset(CHINA_MOE, "2d_MOE", 'CHINA', 'int')
#------------------------------# ####
