#----# Docstring #----# ####
#
# Purpose:  Cleans donor column, creates income sector and type, and calculates
# fraction of total donated by each donor.
#---------------------# 

#----# Environment Prep #----# ####
# System prep
rm(list=ls())
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

cat('  Read in COVID data/n')
#----# Read in COVID data #----# ####
dt <- fread(get_path('PAHO', 'raw', c("covid", "paho_donor_funds_covid.csv")))
dt <- dt[tolower(Donor) != "donations channeled through who"]

#----# Cleaning amount_to_date variable #----# ####
# cleaning amount-to-date column
setnames(dt, old = 'Amount_USD', new = 'amount_to_date')
dt[, amount_to_date := gsub('\\,', '', amount_to_date)]
dt[, amount_to_date := as.numeric(amount_to_date)]

# cleaning the donor column
dat <- dt

# Changing Korea to South Korea
dat[Donor == 'Korea', Donor := 'South Korea'] # Cleaning country name so it merges with income sector/type dataset
dat[, upper_Donor := toupper(Donor)]
dat[, Donor := NULL]
cat('  Read in income and sector type assignments/n')
#----# Read in income and sector type assignments #----# ####
inc <- fread(paste0(dah.roots$j, 'FILEPATH/income_sector_and_type_assignments_2021.csv'), encoding = 'Latin-1')

#----# Cleaning donor name variable #----# ####
inc$DONOR_NAME <- iconv(inc$DONOR_NAME, from = "Latin1", to = "UTF-8", sub = "byte")
inc_c <- inc
inc_c[, upper_DONOR_NAME := toupper(DONOR_NAME)]
inc_c[, DONOR_NAME := NULL] # Deleting old column
inc_c <- unique(inc_c) # Making sure there are no duplicates in dataset

cat('  Merging data with income and sector type assignments/n')
#----# Merging data with income and sector type assignments #----# ####

data <- merge(dat, inc_c,
              by.x = c('upper_Donor'), 
              by.y = c('upper_DONOR_NAME'), 
              all.x = T)

#----# Estimating fractions donated by each donor #----# ####
# estimating total donated to date by all donors
data[, total_amt := sum(amount_to_date)] # as of March 26, 2022. (Date when PAHO data was last updated)

cat('  Estimate fractions donated by each donor /n')
data[, fraction := amount_to_date/total_amt]

##
### amount that PAHO reports spending for COVID in 2020
##
dah_2020 <- 109374000 # page 24 of the PAHO 2020 financial report (https://iris.paho.org/bitstream/handle/10665.2/54488/9789275373620_eng.pdf?sequence=1&isAllowed=y)
rev_2020 <- 263000000 # page 3 of covid-19 paho response summary

# estimating dah to date
dah_to_date <- sum(data$amount_to_date)

# estimating dah 2021
## calculated in fgh2021
rev_21 <- 233675175 / .66

dis_ratio_21 <- 0.66 # page 3 of covid-19 paho response summary

dah_2021 <- rev_21 * dis_ratio_21 # Using 2020 disbursement ratio for 2021

#
# estimate 2022
rev_2022 <- 75 * 1e5 # "Pan American Health Organization Response to COVID-19 2022"
dah_2022 <- 75 * 1e6 # disbursements not available, so assuming 100% disbursement


#----# Estimating donated amounts #----# ####

# commitments
data[, commitment_2020 := rev_2020 * fraction]
data[, commitment_2021 := rev_21 * fraction]
data[, commitment_2022 := rev_2022 * fraction]

# disbursements
data[, disbursement_2020 := dah_2020 * fraction]
data[, disbursement_2021 := dah_2021 * fraction]
data[, disbursement_2022 := dah_2022 * fraction]

#----# Deleting duplicated columns #----# ####
data[, amount_to_date := NULL]
data[, total_amt := NULL]
data[, fraction := NULL]

#----# Saving data #----# ####
save_dataset(data, 
             paste0('1.COVID_clean_donors'), 
             'PAHO', 'int')
