########################################################################################
## UNICEF Recipient Fix using CRS data
## Description: Read in COVID_prepped data and ADB_PDB. Merge & Collapse COVID total
## onto ADB_PDB and save same filename but _COVID_fix appended 
## to be used in compile to avoid double counts
########################################################################################
rm(list = ls())

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}


report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, openxlsx, magrittr)
asn_date <- format(Sys.time(), "%Y%m%d")

# Variable prep
codes <- get_path("meta", "locs")
#--------------# Read in data #--------------#
adb_pdb <- setDT(readstata13::read.dta13(paste0(get_path('UNICEF', 'fin'), "UNICEF_ADB_PDB_FGH", dah.roots$report_year, "_ebola_fixed_includesDC.dta")))
covid <- fread(paste0(get_path('UNICEF','fin'),'COVID_prepped.csv'), encoding = 'Latin-1')

#add 0 for inkind
covid[, INKIND := 0]

#--------# reformat and sum by shared columns #--------#
setnames(covid, 
         old = c("year", "channel", 'donor_name', 'donor_country', "total_amt", 'iso3_rc'), 
         new = c('YEAR', 'CHANNEL', 'DONOR_NAME', 'DONOR_COUNTRY', 'TOTAL_AMT', 'ISO3_RC'))

covid <- covid[, .(oid_covid_DAH = sum(TOTAL_AMT, na.rm = T)),
               by = .(YEAR, CHANNEL, DONOR_NAME, DONOR_COUNTRY, ISO_CODE, INCOME_SECTOR, INCOME_TYPE, ISO3_RC, INKIND)]

#clean column names for better matching later
covid <- string_clean(covid, 'DONOR_NAME')
adb_pdb <- string_clean(adb_pdb, 'DONOR_NAME')
covid[, upper_DONOR_NAME := trimws(upper_DONOR_NAME)]
adb_pdb[, upper_DONOR_NAME := trimws(upper_DONOR_NAME)]

# fix covid donor name for binding/aggregating with adbpdb data
covid[upper_DONOR_NAME %in% c('KOREA REPUBLIC OF', 'SOUTH KOREA'), upper_DONOR_NAME := 'REPUBLIC OF KOREA']
covid[grepl('AFRICAN DEVELOPMENT BANK', upper_DONOR_NAME), upper_DONOR_NAME := 'AFDB']
covid[grepl('ASIAN DEVELOPMENT BANK', upper_DONOR_NAME), upper_DONOR_NAME := 'ASDB']
covid[upper_DONOR_NAME == 'GAVI ALLIANCE', upper_DONOR_NAME := 'GAVI THE VACCINE ALLIANCE']
covid[upper_DONOR_NAME == 'UNDP USA', upper_DONOR_NAME := 'UNDP NEW YORK']
covid[upper_DONOR_NAME == 'UNESCO', upper_DONOR_NAME := 'THE UNITED NATIONS EDUCATIONAL SCIENTIFIC AND CULTURAL ORGANIZATION UNESCO']
covid[upper_DONOR_NAME == 'UNITED NATIONS MULTI PARTNER TRUST', upper_DONOR_NAME := 'UNITED NATIONS MULTI PARTNER TRUST FUND']
covid[upper_DONOR_NAME == 'UNITED STATES OF AMERICA', upper_DONOR_NAME := 'UNITED STATES']
covid[upper_DONOR_NAME == 'UNOCHA', upper_DONOR_NAME := 'OFFICE FOR THE COORDINATION OF HUMANITARIAN AFFAIRS OCHA']
covid[grepl('UNOPS', upper_DONOR_NAME), upper_DONOR_NAME := 'UNITED NATIONS OFFICE FOR PROJECT SERVICES UNOPS']
covid[grepl('WFP', upper_DONOR_NAME), upper_DONOR_NAME := 'WORLD FOOD PROGRAMME WFP']
covid[upper_DONOR_NAME == 'WORLD HEALTH ORGANIZATION', upper_DONOR_NAME := 'WHO']
covid[grepl('WORLD BANK', upper_DONOR_NAME), upper_DONOR_NAME := 'WB']


# fix adbpdb donor name for binding/aggregating with covid data
adb_pdb[grepl('UNITED NATIONS EDUCATIONAL SCIENTIFIC AND CULTURAL ORGANIZATION', upper_DONOR_NAME),
        upper_DONOR_NAME := 'THE UNITED NATIONS EDUCATIONAL SCIENTIFIC AND CULTURAL ORGANIZATION UNESCO']
adb_pdb[upper_DONOR_NAME == 'GAVI THE VACCINE ALLIANCE', SOURCE_CH := 'GAVI']

# source channels for covid
covid[, SOURCE_CH := '']
covid[upper_DONOR_NAME %in% c('WHO', 'UNFPA', 'UNAIDS', 'PAHO'), SOURCE_CH := upper_DONOR_NAME]
covid[upper_DONOR_NAME == 'GAVI THE VACCINE ALLIANCE', SOURCE_CH := 'GAVI']
covid[upper_DONOR_NAME == 'ASDB', SOURCE_CH := 'AsDB']

# adjusting columns in covid to assist with bind
inkind_ratios <- unique(adb_pdb[, .(YEAR, INKIND_RATIO)])
covid <- merge(covid, inkind_ratios, by = 'YEAR', all.x = T)
covid[, DAH := oid_covid_DAH]
covid[, gov := 0]
covid[, INCOME_TYPE := NULL]
covid <- covid[oid_covid_DAH > 0]

# assign hfa values for covid data to zero
to_add <- colnames(adb_pdb)[colnames(adb_pdb) %like% '_DAH']
covid[, c(to_add) := 0]

# bind covid to adb_pdb
adb_pdb[, oid_covid_DAH := 0]
adb_pdb <- rbind(adb_pdb, covid, fill = T)

# collapse
to_sum <- colnames(adb_pdb)[colnames(adb_pdb) %like% 'DAH']
to_group <- colnames(adb_pdb)[!(colnames(adb_pdb) %in% to_sum)]
dt <- collapse(adb_pdb,
               agg_function = 'sum',
               group_cols = to_group,
               calc_cols = to_sum)


#-------# save out dataset #-------#
save_dataset(dt, paste0("UNICEF_ADB_PDB_FGH_", dah.roots$report_year, "_includesDC_COVID_fix"), 'UNICEF', 'fin')

