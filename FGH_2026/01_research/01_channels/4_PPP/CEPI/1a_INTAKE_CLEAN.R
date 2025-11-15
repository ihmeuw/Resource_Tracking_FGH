#### #----#                        Docstring                         #----# ####
#' Project:         FGH 
#'    
#' Purpose:         Intake CEPI data and clean
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
source(paste0(dah.roots$k, "FILEPATH/get_location_metadata.R"))

# variable prep
report_year <- get_dah_param('report_year')
prev_report_year <- get_dah_param('prev_report_year')

#####---------------------------# read in data #----------------------------####
# country codes
country_codes <- fread(get_path("meta", "locs", "countrycodes_official.csv"),
                       select = c("country_lc", "iso3"))
setnames(country_codes, "country_lc", "DONOR_COUNTRY")

# raw expenditure
cepi_expenditure <- fread(paste0(get_path('CEPI', 'raw'), "CEPI_EXPENDITURE_BY_HFA_", dah.roots$report_year, ".csv"), header=TRUE, sep=',')

# income data
cepi_income <- fread(paste0(get_path('CEPI', 'raw'), "CEPI_INCOME_2017_", dah.roots$report_year, ".csv"), header=TRUE, sep=',')


#####---------------------# format raw expenditure #------------------------####

cat('  Format raw expenditure\n')

# rename columns
setnames(cepi_expenditure,
         c("YEAR", "DISEASE", "EXPENDITURE", "HFA"),
         c("YEAR", "DESCRIPTION", "DAH", "HFA"))

# drop COVID-related HFAs
cepi_expenditure <- cepi_expenditure[HFA != 'drop_in_regular_channel_work', ]
# Post 2023 is left in because there are no longer special covid funding sources
# so it is just treated like any other project would be

# To be consistent with 2020-2023 covid data though, need to apply ODA fraction
# to COVID DAH. 
cepi_expenditure[HFA == "oid_covid" & YEAR == 2024, DAH := DAH * 0.88]

# take sum of expenditure by year and HFA
cepi_expenditure <- cepi_expenditure[, lapply(.SD, sum, na.rm = TRUE),
                                     by = c('YEAR', 'HFA'), .SDcols = 'DAH']
  
# reshape wide
cepi_expenditure <- dcast(cepi_expenditure, YEAR ~ HFA, value.var = 'DAH')

#####-----------------------# format income data #--------------------------####

cat('  Format Income\n')

# rename columns
colnames(cepi_income) <- toupper(colnames(cepi_income))

# drop the covid values
cepi_income <- cepi_income[!(PURPOSE %like% 'COVID'), ]

# drop the existing income_sector and income_type
inc_donors <- cepi_income[, c('INCOME_SECTOR', 'INCOME_TYPE') := NULL]


# match income_sector and income_type using central functions
inc_donors <- standardize_donors(inc_donors, 'DONOR_NAME', get_path("meta", "donor", "income_sector_and_type_assignments.csv"))



# check the donors
not_match <- inc_donors[[2]]
match <- inc_donors[[1]]

cepi_income <- inc_donors[[1]]
cepi_income[DONOR_NAME %like% "Wellcome/DFID" & DONOR_COUNTRY == "United Kingdom",
            `:=`(IHME_NAME = "WELLCOME TRUST",
                 INCOME_SECTOR = "PRIVATE",
                 INCOME_TYPE = "FOUND")]
cepi_income[DONOR_NAME %like% "Government of the United Kingdom",
            `:=`(IHME_NAME = "UNITED KINGDOM",
                 DONOR_COUNTRY = "United Kingdom",
                 INCOME_SECTOR = "PUBLIC",
                 INCOME_TYPE = "CENTRAL")]
cepi_income[toupper(DONOR_NAME) %like% "SAUDI ARABIA",
            `:=`(IHME_NAME = "Saudi Arabia",
                 DONOR_COUNTRY = "Saudi Arabia",
                 INCOME_SECTOR = "PUBLIC",
                 INCOME_TYPE = "CENTRAL")]

cepi_income[DONOR_NAME %like% "Bill and Melinda Gates",
            `:=`(IHME_NAME = "BMGF",
                 DONOR_COUNTRY = "Government of the United States of America",
                 INCOME_SECTOR = "PUBLIC",
                 INCOME_TYPE = "CENTRAL")]

cepi_income[DONOR_NAME == "Government of Lithuania",
            `:=`(
                IHME_NAME = "Lithuania",
                DONOR_COUNTRY = "Lithuania",
                INCOME_SECTOR = "PUBLIC",
                INCOME_TYPE = "CENTRAL")]

cepi_income[, c('checked_upper_DONOR_NAME', 'DONOR_NAME') := NULL]
setnames(cepi_income, 'IHME_NAME', 'DONOR_NAME')

# add the donor countries where missing
cepi_income[DONOR_NAME == "WELLCOME TRUST", DONOR_COUNTRY := "United Kingdom"]

cepi_income[DONOR_COUNTRY == "" | is.na(DONOR_COUNTRY),
            DONOR_COUNTRY := DONOR_NAME]

# divide individual donor contributions by the total in that year

cepi_income[,FUNDS := as.numeric(gsub(",", "", FUNDS))]
cepi_income[, TOTAL_FUNDS := sum(FUNDS, na.rm = TRUE), by = YEAR]
cepi_income[, FUNDS_frct := FUNDS / TOTAL_FUNDS]


#####-------------------# merge income and expenditure #---------------------####

# merge income and expenditure data
cepi_inc_exp <- merge(cepi_income, cepi_expenditure, by = c('YEAR'), all.x = T)

# set vector for hfas
hfas <- c("oid_hss_other", "oid_other", "swap_hss_pp", "oid_ebz", 'oid_covid')
hfafrcts <- paste0(hfas, "_disb_frct")
# multiply budget fractions by HFA disbursement
cepi_inc_exp[, (hfafrcts) := lapply(.SD, \(x) FUNDS_frct * x),
             .SDcols = hfas]

cepi_inc_exp[, total_expend := rowSums(.SD, na.rm = TRUE),
             .SDcols = hfafrcts]


# drop the columns we don't need
toKeep <- c("YEAR", "DONOR_NAME", "INCOME_SECTOR", "INCOME_TYPE", "DONOR_COUNTRY",
            "total_expend", paste0(hfas, "_disb_frct"))
cepi_inc_exp <- cepi_inc_exp[, eval(toKeep), with = F]

# add iso codes
cepi_inc_exp <- merge(cepi_inc_exp, country_codes,
                      by = "DONOR_COUNTRY", all.x = TRUE)
setnames(cepi_inc_exp, 'iso3', 'ISO_CODE')
cepi_inc_exp[DONOR_COUNTRY == "EC", ISO_CODE := "EC"]



#####--------------------------# save dataset #-----------------------------####

cat('  Save out stage 1a data\n')

# write csv file
save_dataset(cepi_inc_exp, paste0('1a_INTAKE_CLEAN_precovid'), 'CEPI', 'int')
check <- fread(get_path("CEPI", "INT", "1a_INTAKE_CLEAN_precovid.csv"))
#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#