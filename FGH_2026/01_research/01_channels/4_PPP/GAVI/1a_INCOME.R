#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Intake and Format GAVI Data
#---------------------# 

# First step here is to go into dah_parameters.yml and update the date tags under the GAVI section!

#----# Environment Prep #----# ####
rm(list=ls())


if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr, haven, zoo)

# Variable prep
income_update_year <- get_dah_param('GAVI', 'income_update_year')
disb_update_year <- get_dah_param('GAVI', 'disb_update_year')
crs_mmyy <- get_dah_param('CRS', 'update_MMYY')

CODES <- get_path("meta", "locs")
country_codes <- fread(file.path(CODES, "countrycodes_official.csv"),
                       select = c("iso3", "country_lc"))
country_codes[country_lc == 'Côte d\'Ivoire', country_lc := 'Cote d\'Ivoire']
#----------------------------# 


cat('\n\n')
cat(green(' ####################################\n'))
cat(green(' #### GAVI BEGIN DATA FORMATTING ####\n'))
cat(green(' ####################################\n\n'))


cat('  Formatting GAVI Contributions\n')
#----# GAVI Contributions #----# ####
# Import dataset
gavi_contrib <- fread(paste0(get_path('GAVI', 'raw'), 'P_GAVI_CONTRIBUTIONS_2000_', dah.roots$report_year, '_', income_update_year, '.csv'))

# Clean colnames & remove unwanted rows/cols
# note: unneeded rows w/ no funds (such as notes rows) will be removed later
setnames(gavi_contrib, 'DONOR', 'DONOR_NAME')
gavi_contrib <- gavi_contrib[!(DONOR_NAME %like% c('Total contributions|Donor Governments and EC|Sub-total|IFFIm Proceeds|AMC Proceeds|Foundations, organisations, corporations and institutions|Private Contributions')) & DONOR_NAME != '']
gavi_contrib[, DONOR_NAME := gsub('[0-9.]', '', DONOR_NAME)]

# Destring columns
yr_cols <- names(gavi_contrib)[names(gavi_contrib) %like% '20'] # Search for 2000's columns
gavi_contrib[, (yr_cols) := lapply(.SD, \(x) {
    as.numeric(gsub(",", "", x))
}), .SDcols = yr_cols]
# Collapse sum of yr_* cols
gavi_contrib <- gavi_contrib[, lapply(.SD, sum, na.rm=T), by=DONOR_NAME, .SDcols=yr_cols]
# Replace generated 0's with NA again
for (col in yr_cols) {
  gavi_contrib[get(col) == 0, eval(col) := NA]
}
# Fix three instances of true zero
gavi_contrib[DONOR_NAME %in% c('Spain', 'Sweden'), yr_2012 := 0]

# Reshape long, rename, clean strings + multiply income, drop NA vals
gavi_contrib <- melt.data.table(gavi_contrib, 
                                measure = yr_cols,
                                variable.factor = F,
                                variable.name = 'YEAR',
                                value.name = 'INCOME_REG')
gavi_contrib[, is_covax := grepl("covax", YEAR)]
gavi_contrib[, YEAR := gsub("covax_", "", YEAR)]
gavi_contrib[, `:=`(YEAR = str_replace(YEAR, 'yr_', ''),
                    INCOME_REG = INCOME_REG * 1000000)]
gavi_contrib <- gavi_contrib[!is.na(INCOME_REG)]

# Add in World Bank data from 2006-2007
temp_wb <- data.table(DONOR_NAME = rep('World Bank', 2),
                      YEAR = c(2006, 2007),
                      INCOME_REG = c(1000000, 500000),
                      is_covax = FALSE)
gavi_contrib <- rbind(gavi_contrib, temp_wb)
rm(temp_wb, col, yr_cols)

# Edit data to match previous years
gavi_contrib[DONOR_NAME == 'Kingdom of Saudi Arabia', DONOR_NAME := 'Saudi Arabia']
gavi_contrib[DONOR_NAME == 'Bill & Melinda Gates Foundation', DONOR_NAME := 'BMGF']
gavi_contrib[DONOR_NAME == "\"\"la Caixa\"\" Foundation", DONOR_NAME := 'La Caixa Foundation']
gavi_contrib[DONOR_NAME == 'Scotland', DONOR_NAME := 'United Kingdom']

# Assign income types
gavi_contrib[, INCOME_TYPE := ""]
gavi_contrib[DONOR_NAME %in% c('Australia', 'Austria', 'Brazil', 'Bahrain', 'Croatia', 'Greece',
                               'Belgium', 'Canada', 'China', 'Italy', 'Sweden', 'Denmark', 'Switzerland', 
                               'Ireland', 'Luxembourg', 'Republic of Korea', 'France', 'Germany', 'Kuwait', 
                               'Monaco', 'Japan', 'Netherlands', 'Norway', 'Spain', 'United Kingdom', 'Qatar', 
                               'Saudi Arabia', 'United States', 'Oman', 'India', 'Iceland', 'Finland', 'Burkina Faso',
                               'Philippines', 'Poland', 'Portugal', 'Singapore', 'Vietnam',
                               'Niger', 'Russia', 'Colombia', 'Estonia', 'New Zealand'), INCOME_TYPE := 'CENTRAL']
gavi_contrib[DONOR_NAME %in% c('World Bank'), INCOME_TYPE := 'DEVBANK']
gavi_contrib[DONOR_NAME %ilike% 'european commission', INCOME_TYPE := 'EC']
gavi_contrib[DONOR_NAME %in% c('Alwaleed Philanthropies', 'ELMA Vaccines and Immunization Foundation','La Caixa Foundation', 
                               'Rockefeller Foundation',
                               'Analog Devices Foundation',
                               'Asia Philanthropy Circle',
                               'Charities Trust',
                               'Coca-Cola Foundation', 'Thistledown Foundation',
                               'UBS Optimus Foundation',
                               'Gates Philanthropy Partners',
                               'Visa Foundation',
                               'WHO Foundation - Go Give One Campaign',
                               'Children\'s Investment Fund Foundation (CIFF)', 'Wellcome Trust', 'BMGF'), 
             INCOME_TYPE := 'FOUND']
gavi_contrib[DONOR_NAME %in% c('Reed Hastings and Patty Quillin'),
             INCOME_TYPE := 'INDIV']
gavi_contrib[DONOR_NAME %in% c('International Federation of Pharmaceutical Wholesalers (IFPW)',
                               'Gamers Without Borders', 'Vaccine Forward Initiative',
                               'KS Relief',
                               'OPEC Fund for International Development (OFID)', 'His Highness Sheikh Mohamed bin Zayed Al Nahyan', 
                               'The Church of Jesus Christ of Latter-day Saints', 'Other private'), 
             INCOME_TYPE := 'OTHER']
gavi_contrib[DONOR_NAME %in% c('Unilever', 'Al Ansari Exchange',
                               'TikTok', 'UPS', 'Mastercard', 'Arm Limited',
                               'Cisco', 'Googleorg', 'Portuguese private sector',
                               'Proctor & Gamble', 'Salesforce', 'Spotify',
                               'Stanley Black & Decker', 'Toyota Tsusho', 'Twilio',
                               'Shell International BV'),
             INCOME_TYPE := 'CORP']

# Drop blank INCOME_TYPE observations (covid fund)
# ATTN: Make sure to check what these are in case of new donors/funders
stopifnot( gavi_contrib[INCOME_TYPE == "", .N] == 0 )

# Generate INCOME_SECTOR column
gavi_contrib[, INCOME_SECTOR := ""]
gavi_contrib[INCOME_TYPE %in% c('EC', 'DEVBANK'), INCOME_SECTOR := 'MULTI']
gavi_contrib[INCOME_TYPE == 'OTHER', INCOME_SECTOR := 'OTHER']
gavi_contrib[INCOME_TYPE %in% c('INDIV', 'FOUND') | DONOR_NAME == 'Other private',
             INCOME_SECTOR := 'PRIVATE']
gavi_contrib[INCOME_TYPE == 'CENTRAL', INCOME_SECTOR := 'PUBLIC']
gavi_contrib[INCOME_TYPE == 'CORP', INCOME_SECTOR := 'INK']

# Warning message if there are any blank INCOME_SECTOR
if ("" %in% unique(gavi_contrib$INCOME_SECTOR)) {
  cat(red('  WARNING: SOME BLANK INCOME_SECTORs! Check data and ensure all rows have appropriate INCOME_SECTOR'))
}

# Generate DONOR_COUNTRY column
gavi_contrib[INCOME_TYPE == 'CENTRAL', DONOR_COUNTRY := DONOR_NAME]

gavi_contrib[DONOR_NAME %in% c('BMGF','ELMA Vaccines and Immunization Foundation',
                               'Rockefeller Foundation', 'UPS', 'Mastercard',
                               'The Church of Jesus Christ of Latter-day Saints'), 
             DONOR_COUNTRY := 'United States']

gavi_contrib[DONOR_NAME == 'La Caixa Foundation', DONOR_COUNTRY := "Spain"]
gavi_contrib[DONOR_NAME == 'TikTok', DONOR_COUNTRY := "China"]
gavi_contrib[DONOR_NAME %in% c('Al Ansari Exchange','His Highness Sheikh Mohamed bin Zayed Al Nahyan'), DONOR_COUNTRY := "United Arab Emirates"]
gavi_contrib[DONOR_NAME %in% c('Unilever', 'Arm Limited', 'Wellcome Trust', 'Children\'s Investment Fund Foundation (CIFF)'), DONOR_COUNTRY := "United Kingdom"]
gavi_contrib[DONOR_NAME == 'Other private', DONOR_COUNTRY := 'unallocable']

gavi_contrib[DONOR_NAME %in% c('European Commission (EC)',
                               'OPEC Fund for International Development (OFID)','Alwaleed Philanthropies',
                               'International Federation of Pharmaceutical Wholesalers (IFPW)', 'World Bank'),
             DONOR_COUNTRY := '']

# Generate other columns
gavi_contrib[, `:=`(SOURCE_DOC = paste0('P_GAVI_CONTRIBUTIONS_2000-', dah.roots$report_year, '_', income_update_year),
                    CHANNEL = 'GAVI')]

#------------------------------# 

cat('  Formatting GAVI IFFIm Pledges\n') 
#----# GAVI IFFIm Pledges #----# ####
iffim <- setDT(fread(paste0(get_path('GAVI', 'raw'), 'P_GAVI_IFFIm_Pledges_Disbursements_2006-', dah.roots$report_year, '_fullsummary_stata_tabs.csv')))
iffim[, `:=`(DONOR_NAME = paste0(DONOR_COUNTRY, ' - IFFIm'),
             INCOME_SECTOR = 'PUBLIC',
             INCOME_TYPE = 'CENTRAL')]
iffim[, `GAVI RECEIVED` := NULL]
#------------------------------#  

cat('  Formatting GAVI AMC Pledges\n') 
#----# GAVI AMC Pledges #----# ####
amc <- setDT(fread(paste0(get_path('GAVI', 'raw'), 'P_GAVI_AMC_Pledges_Disbursements_2009-', dah.roots$report_year, '_fullsumary_stata_tabs.csv')))
amc[, `:=`(DONOR_NAME = paste0(DONOR_COUNTRY, ' - AMC'),
           INCOME_SECTOR = 'PUBLIC',
           INCOME_TYPE = 'CENTRAL')]
amc[DONOR_COUNTRY == 'BMGF', `:=`(INCOME_SECTOR = 'PRIVATE',
                                  INCOME_TYPE = 'FOUND',
                                  ISO3 = "USA")]
amc[, `GAVI RECEIVED` := NULL]
#----------------------------#  

cat('  Formatting GAVI Investment Income\n')
#----# GAVI Investment Income #----# ####
inv_inc <- setDT(fread(paste0(get_path('GAVI', 'raw'), 'P_GAVI_Investment_Income_2000_', dah.roots$report_year, '.csv')))
inv_inc[, `:=`(DONOR_COUNTRY = NULL,
               ISO3 = NULL)]
#----------------------------------# 

cat('  Appending formatted data\n') 
#----# Append formatted data #----# ####
income <- rbind(gavi_contrib, iffim, amc, inv_inc, fill = T)
rm(gavi_contrib, iffim, amc, inv_inc)
income[, is_covax := as.integer(is_covax)]
setnafill(income, fill = 0, cols = "is_covax")

# Merge on ISO codes
setnames(income, 'DONOR_COUNTRY', 'country_lc')
income[, ISO3 := NULL]
income <- merge(income, country_codes, by = 'country_lc', all.x = T)
# setnames(income, c('country_lc', 'iso3'), c('DONOR_COUNTRY', 'ISO3'))
income <- income[!is.na(DONOR_NAME)]
setnames(income, c('country_lc', 'iso3'), c('DONOR_COUNTRY', 'ISO3'))

income[, INCOME_EB := 0]
income[is.na(INCOME_REG), INCOME_REG := 0]
income[is.na(INCOME_OTHER), INCOME_OTHER := 0]
num_cols <- c("INCOME_EB", "INCOME_REG", "INCOME_OTHER")
income[, (num_cols) := lapply(.SD, \(x) gsub(",", "", x)), .SDcols = num_cols]
income[, (num_cols) := lapply(.SD, as.numeric), .SDcols = num_cols]


income[is_covax == 0, INCOME_ALL := INCOME_REG + INCOME_EB + INCOME_OTHER] 
income[is_covax == 1, INCOME_ALL := INCOME_REG]
income <- income[INCOME_ALL >= 0]
income[, INCOME_TOTAL_YR := sum(INCOME_ALL), by = .(YEAR, is_covax)]
income[, INCOME_ALL_SHARE := INCOME_ALL/INCOME_TOTAL_YR]
income[, YEAR := as.numeric(YEAR)]

inc24 <- copy(income)
inc24 <- inc24[YEAR == 2023]
inc24 <- inc24[,YEAR := 2024]
income <- rbind(income, inc24)
#---------------------------------#  
cat('  Save out GAVI income\n')
#----# Output income data #----# ####
save_dataset(income[is_covax == 0], paste0('GAVI_income'), 'GAVI', 'int')
save_dataset(income[is_covax == 1], paste0('GAVI_covax_income'), 'GAVI', 'int')
