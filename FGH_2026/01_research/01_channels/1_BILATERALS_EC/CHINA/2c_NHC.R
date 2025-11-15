#----# Docstring #----# ####
# Project:  FGH 
# Purpose:  Assign CHINA NHC HFAs
#---------------------# ####

#*********************************************************************#
####-----                       NOTES                         -----####
# Decided all post 2020 values to be given to COVID

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
cat(green(' #### BEGIN CHINA NHC HFA ASSIGNMENT ####\n'))
cat(green(' ########################################\n\n'))


cat('  Read in Stage 1c Data\n')
#----# Read in Stage 1c #----# ####
nhc <- fread(get_path('CHINA', 'int', '1c_NHC.csv'))
#----------------------------# ####

cat('  Assign HFAs\n')
#----# Assign HFAs #----# ####
# Add HFA - assign HFA based on literature review
nhc[, `:=`(swap_hss_hrh_DAH = E_NHC_DAH,
           other_DAH = 0)]

nhc[YEAR == 2015, other_DAH := E_NHC_DAH * .0150254]
nhc[YEAR == 2015, swap_hss_hrh_DAH := E_NHC_DAH - other_DAH]
nhc[YEAR == 2017, other_DAH := E_NHC_DAH * .00930449]
nhc[YEAR == 2017, swap_hss_hrh_DAH := E_NHC_DAH - other_DAH]
#-----------------------# ####

cat('  Deflate and Currency Convert\n')
#----# Deflate and Currency Convert #----# ####
exch_rates <- fread(get_path("meta", "rates", "OECD_XRATES_NattoUSD_1950_[report_year].csv"))
exch_rates <- exch_rates[LOCATION == "CHN" & TIME >= 1990, c("TIME", "Value")]
setnames(exch_rates, c("TIME", "Value"), c("YEAR", "XRATE"))

deflators <- fread(get_path("meta", "defl", "imf_usgdp_deflators_[defl_MMYY].csv"))
deflators <- deflators[, c("YEAR", paste0("GDP_deflator_", dah.roots$report_year)),
                       with = FALSE]

# Deflation & Exchange rates
nhc <- merge(nhc, deflators, by="YEAR", all.x = TRUE)
nhc <- merge(nhc, exch_rates, by="YEAR", all.x = TRUE)

nhc[, `:=`(E_NHC_DAH_USD = E_NHC_DAH / XRATE,
           swap_hss_hrh_DAH_USD = swap_hss_hrh_DAH / XRATE,
           other_DAH_USD = other_DAH / XRATE)]
nhc[, eval(paste0("E_NHC_DAH_", dah.roots$abrv_year)) := E_NHC_DAH_USD / get(paste0("GDP_deflator_", dah.roots$report_year))]
nhc[, eval(paste0("swap_hss_hrh_DAH_", dah.roots$abrv_year)) := swap_hss_hrh_DAH_USD / get(paste0("GDP_deflator_", dah.roots$report_year))]
nhc[, eval(paste0("other_DAH_", dah.roots$abrv_year)) := other_DAH_USD / get(paste0("GDP_deflator_", dah.roots$report_year))]

# Update 2020 envelope to be all COVID (i.e. drop it and pick it up in COVID_PREP.R)
cat(paste0(yellow('    NOTE: Dropping years in interval [2020, 2022] and picking it back up in COVID_PREP.R. Decision made that all',
                  'NCH DAH after 2020 was COVID-related, so removing it here.\n')))
# Subset COVID data, any year after 2019
covid_out <- nhc[between(YEAR, 2020, 2022) & inkind == 0,
                 .(YEAR, amount = E_NHC_DAH_USD)]
covid_out[, `:=`(CHANNEL = "BIL_CHN",
                 DONOR_NAME = "CHN_NHC",
                 money_type = 'repurposed')]

covid_out[YEAR == 2022,
          amount := amount * as.numeric(get_dah_param("china", "covid_frac_2022"))]
# Write dataset
save_dataset(covid_out, 'NHC_COVID','CHINA', 'int')
rm(covid_out)

# Remove years after 2020 values from NHC data since they are COVID
nhc <- nhc[!between(YEAR, 2020, 2021) ]
dahcols <- grep("DAH", names(nhc), value = TRUE)
nhc[YEAR == 2022, (dahcols) := lapply(.SD, \(x) {
    x * (1 - as.numeric(get_dah_param("china", "covid_frac_2022")))
}), .SDcols = dahcols]

# Rename data
CHINA_NHC <- copy(nhc)
#----------------------------------------# ####

cat('  Save Stage 2c Data\n')
#----# Save Stage 2c Data #----# ####
save_dataset(CHINA_NHC, "2c_NHC", 'CHINA', 'int')
#------------------------------# ####