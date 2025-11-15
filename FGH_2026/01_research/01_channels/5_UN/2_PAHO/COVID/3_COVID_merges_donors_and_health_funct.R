#----# Docstring #----# ####
#
# Project:  FGH
# Purpose:  Reads cleaned donor and health function files and merges them into
# a single file. 
#     
#----------------------------# 

#----# Environment Prep #----# ####
# System prep
rm(list=ls())
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, dplyr)

#----------------------------# 
cat('  Read in donor data\n')
#----# Read in donor data #----# ####
do <- setDT(fread(paste0(get_path('PAHO', 'int'), '1.COVID_clean_donors.csv')))

# deleting these columns because we want to use the disbursement column from file 2.  
do[, disbursement_2020 := NULL]
do[, disbursement_2021 := NULL]
do[, disbursement_2022 := NULL]

# Estimating total commitment by year
com_20 <- sum(do$commitment_2020)
com_21 <- sum(do$commitment_2021)
com_22 <- sum(do$commitment_2022)

#----# Estimating fraction that each donor donated to #----# ####
# making data set to estimate fraction of total committed by each donor
frac <- melt(do, id.vars = c('upper_Donor', 'INCOME_SECTOR', 'INCOME_TYPE', 'ISO_CODE'))
setnames(frac, old = 'value', new = 'commitment')
frac[, YEAR := tstrsplit(variable, '_', keep = 2)]
frac[, YEAR := as.numeric(YEAR)]

# We need to estimate the fraction of the total that was commitment by each donor 
# to apply it to disbursements 
frac[YEAR == 2020, FRACTION := commitment/com_20]
frac[YEAR == 2021, FRACTION := commitment/com_21]
frac[YEAR == 2022, FRACTION := commitment/com_22]

# deleting unnecessary columns
frac[, variable := NULL]
frac[, commitment := NULL]

cat('  Read in health functions data\n')
#----# Read in health functions data #----# ####
hf <- setDT(fread(paste0(get_path('PAHO', 'int'), '2.COVID_health_functions.csv')))
names(hf) <- toupper((names(hf)))

#----# Bringing data set together #----# ####
dat <-  merge(frac, hf, by = 'YEAR', allow.cartesian = T)

# Making a list of program areas (in dollar amounts)
hfas <- gsub('_AMT', '', names(dat)[names(dat) %like% '_AMT'])

# applying the fraction committed by each donor to each program area column
for (hfa in hfas) {
  print(hfa)
  dat[, eval(paste0(hfa, '_AMT')) := FRACTION * get(paste0(hfa, '_AMT'))]
}

# applying the fraction committed by each donor to each program area column
dat[, DISBURSEMENT := DISBURSEMENT*FRACTION]

# Removing fraction column because we no longer need it
dat[, FRACTION := NULL]

#----# Cleaning file #----#
data <- copy(dat)
data[, `:=`(MONEY_TYPE = 'new_money', GRANT_LOAN = 'grant')]
data[, TOTAL_AMT := DISBURSEMENT]
data[, COMMITMENT := DISBURSEMENT/.66] # LOOK AT SCRIPT 1 FOR EXPLANATION ON THIS FRACTION

cat('Printing summary for visual inspection')
#----# Printing commitments and disbursements for visual inspection #----# ####
cat('Commitments \n')
data[, sum(COMMITMENT)/1e6, by = 'YEAR']

cat('Dibursements \n')
data[, sum(TOTAL_AMT)/1e6, by = 'YEAR']

#----# Cleaning variables names #----#
setnames(data, 'upper_Donor', 'DONOR_NAME') 


#----# Saving data set #----# ####
save_dataset(data, 
             paste0('COVID_prepped'), 
             'PAHO', 'fin') 
