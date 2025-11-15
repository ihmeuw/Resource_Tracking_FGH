#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Reads in sorted DAH and reformats for Stata pipeline
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls())

if (!exists("code_repo"))  {
  code_repo <- "FILEPATH"
}
source(paste0(code_repo, 'FGH_2024/utils.R'))
report_year <- dah.roots$report_year
prev_year <- dah.roots$prev_report_year
data_yr <- report_year - 2
#----------------------------# ####
# Read in CSV
dt <- fread("FILEPATH")
dt_prev <- fread("FILEPATH")

#----------# Clean and format data table------------------# ####
# Find starting place to increment primary grant key
maxgrant <- max(dt_prev$grant_key)

cleandt <- dt %>%
  rename(gm_name = Foundation) %>%
  rename(amount = Amount) %>%
  rename(description = Purpose) %>%
  rename(recip_country = Country) %>%
  select(gm_name, Recipient, description, Address, amount, recip_country, Year) %>%
  mutate(Keep = "SE") %>%
  rename(activity_override = Keep) %>%
  mutate(recip_subject_code = activity_override) %>%
  mutate(intl_geotree_tran = recip_country) %>%
  rename(intl_countries_tran = recip_country) %>%
  mutate(grant_key = maxgrant + 1:n()) %>%
  rename(yr_issued = Year) %>%
  rename(recip_name = Recipient) %>%
  mutate(covid = ifelse(grepl('COVID|CORONAVIRUS', description), 1, 0))

# Saves out dataset
save_dataset(cleandt, paste0("FC_output_", data_yr), "US_FOUNDS", "int")


