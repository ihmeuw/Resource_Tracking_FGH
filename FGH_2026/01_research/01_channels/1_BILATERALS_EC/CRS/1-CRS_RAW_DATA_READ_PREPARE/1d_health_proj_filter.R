#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Keep only projects we consider DAH - health projects and emergency
#   projects tagged by previous script
#---------------------# ####

#----# Environment Prep #----# ####
# System prep
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, '/FGH_', report_year, '/utils.R'))
pacman::p_load(crayon, readstata13)
#----------------------------# ####

cat('\n\n')
cat(green(' ###################################\n'))
cat(green(' #### CRS HEALTH PROJECT FILTER ####\n'))
cat(green(' ###################################\n\n'))


cat('  Read input data\n')
#----# Read input data #----# ####

# Load full CRS (all sectors) and filter down to the sectors that contribute to
#  ODA for Health (i.e., DAH)
crs <- fread(get_path('CRS', 'int', 'B_CRS_ALL_SECTORS_[crs.update_mmyy].csv'))
## we keep sectors "Health, General", "Basic Health", "NCDs", and "Pop/Repr. Health"
crs <- crs[sector_code %in% c(121, 122, 123, 130)]
## convert date-columns to character
datecols <- grep("date", names(crs), value = TRUE)
crs[, (datecols) := lapply(.SD, as.character), .SDcols = datecols]


# Load emergency response dah (from other sector) to combine with health projects
emergency <- fread(get_path("crs", "int", "crs_emergency.csv"))
## convert date-columns to character
datecols <- grep("date", names(emergency), value = TRUE)
emergency[, (datecols) := lapply(.SD, as.character), .SDcols = datecols]

# add emergency to crs
crs[, emergency_response := NA_character_]
crs <- rbind(crs, emergency)


#------------------------------------------------# ####

cat('  Final cleaning\n')

#----# Final cleaning #----# ####
# Drop high-income recipient country-years
crs <- crs[INC_GROUP != "H"]
# high income countries pre-1990
crs <- crs[! ISO3_RC %in% c("ISR", "SGP", "HKG", "BHS", "ABW", "MAC")]


# Clean donor agency name
crs[, agency_name_clean := string_to_std_ascii(agency_name, pad_char = NULL)]

# adjust SIDA (kept for back compatibility)
crs[agency_name_clean == "SIDA" & donor_name == "Sweden",
    agency_name_clean := "SWEDISH INTERNATIONAL DEVELOPMENT AUTHORITY"]

# generate donor agency code
crs[, donor_agency := paste(isocode, agency_name_clean, sep="_")]
crs[, agency_name_clean := NULL]
#--------------------------# ####

cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(crs,
             paste0('B_CRS_[crs.update_mmyy]_HEALTH_BIL_ODA_1'),
             'CRS', 'int')
#------------------------# ####
