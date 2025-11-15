#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Process CEPI COVID data
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls())

if (!exists("code_repo"))  {
    code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr, dplyr)

#----------------------------# ####


cat('\n\n')
cat(green(' ###################################\n'))
cat(green(' #### CEPI CLEAN COVID PROJECTS ####\n'))
cat(green(' ###################################\n\n'))


cat('  Read in data\n')
#----# Read in data #----# ####
dt_raw <- setDT(fread(paste0(get_path('CEPI', 'raw'), 'CEPI_INCOME_2017_', dah.roots$report_year, '.csv')))
dt_raw <- dt_raw[tolower(PURPOSE) %like% 'covid' & FUNDS != 0 & YEAR >= 2020, ]
correspond <- setDT(read_excel(get_path('CEPI', 'raw', 'raw_COVID_data_from_collaborator_2024.xlsx')))
cepi_expenditure <- fread(paste0(get_path('CEPI', 'raw'), "CEPI_EXPENDITURE_BY_HFA_", dah.roots$report_year, ".csv"), header=TRUE, sep=',')

#------------------------# ####
cat('  Format data\n')
#----# Format data #----# ####
correspond <- correspond[grant_loan != 'all' ,]
correspond <- correspond[, `:=` (commitment = commitment*1e6,
                                 disbursement = disbursement*1e6)]

## Disbursements
# find fraction of funds (updated to pool 2020 and 2021 ones)
# use only 2020 donors for 2020
dt_raw[,FUNDS := as.numeric(gsub(",", "", FUNDS))]
dt <- copy(dt_raw)

dt <- dt[, total := sum(FUNDS), by = "YEAR"]
dt_2020 <- copy(dt)[YEAR==2020, ]
dt_2020[, fraction := FUNDS/total]


# use 2020 and 2021 to create fractions for 2021
dt_2021 <- copy(dt_raw)[YEAR %in% c(2020, 2021)]
dt_2021 <- dt_2021[, lapply(.SD, sum), by = c('DONOR_NAME', 'PURPOSE'), .SDcols = c('FUNDS')]
dt_2021 <- dt_2021[, total := sum(FUNDS)]
dt_2021[, YEAR := 2021]
dt_2021[, fraction := FUNDS/total]
dt_inc <- rbind(dt_2020, dt_2021, fill = T) 

dt_inc <- dt_inc[, FUNDS := NULL]

# for years after 2020 and 2021
dt <- dt_raw[YEAR > 2021, ]
dt[, total := sum(FUNDS, na.rm = TRUE), by = YEAR]
dt[, `:=`(
    fraction = FUNDS / total,
    FUNDS = NULL
)]
dt_inc <- rbind(dt_inc, dt)


dt <- merge(dt_inc, correspond, by = 'YEAR', allow.cartesian =  TRUE)
dt <- dt[, -c('total_commitment', 'total_disbursement', 'total_loan', 'oda_frac', 'commitment')]
dt[, disbursement := disbursement*fraction] # only calculate disbursements this way since we need to keep year by year fractions for commitments
dt <- dt[, `:=` (total = NULL, fraction = NULL)]

## Commitments - add separately
# NOTE: 5/27 Since the total commitments is larger than the total funds for covid summed together, we need to take 
# out the difference and attribute it to unallocable so that we only have the funds from raw data attributed to 
# the donors. Using the same method as disbursements will lead to larger amount than donors funded.
correspond_comm <- copy(correspond)[, -c('total_commitment', 'total_disbursement', 'total_loan', 'oda_frac', 'disbursement')]
correspond_comm$total <- ave(correspond_comm$commitment, correspond_comm$YEAR, FUN = sum) 
correspond_comm[, grant_loan_pct := commitment / total] # get grant loan percent to apply to new unallocable and donor totals

dt_comm <- copy(dt_raw)
dt_comm <- dt_comm[, lapply(.SD,sum), by = c('YEAR'), .SDcols = 'FUNDS'] # get sum of money that is covered by donor funds
diff <-  sum(correspond_comm$commitment, na.rm = TRUE) - sum(dt_comm$FUNDS) # Get the difference we need to add to unallocable for 2021 only (since the sum of 2020 Funds is more than the 2020 commitment)
correspond_comm[, unallocable := diff]
correspond_comm[YEAR == 2020, unallocable := 0]
correspond_comm[YEAR == 2021, unallocable := unallocable * grant_loan_pct]
correspond_comm[, commitment := commitment - unallocable]

# Get separate unallocable amounts
unall <- copy(correspond_comm)[YEAR == 2021, -c('total', 'grant_loan_pct')]
unall[, commitment := unallocable]
unall[, unallocable := NULL]
unall[, PURPOSE := 'COVID-19']

# Merge commitments with donor fractions
commitments <- merge(dt_inc, correspond_comm[, -c('unallocable', 'total', 'grant_loan_pct')], allow.cartesian = T)
commitments[, commitment := commitment*fraction]
commitments <- commitments[, `:=` (total = NULL, fraction = NULL)]

# Merge commitments disbursement and append unallocable
dt <- merge(dt, commitments, by = c('YEAR', 'DONOR_NAME', 'INCOME_SECTOR', 'INCOME_TYPE', 'DONOR_COUNTRY', 'PURPOSE', 'CHANNEL', 'source', 'grant_loan'), all = T)
dt <- rbind(dt, unall, fill = T)
dt[is.na(disbursement), disbursement := 0]

# Check
stopifnot(round(sum(correspond$commitment, na.rm = TRUE), 0) == round(sum(dt$commitment, na.rm = TRUE), 0))
stopifnot(round(sum(correspond$disbursement, na.rm = TRUE), 0) == round(sum(dt$disbursement, na.rm = TRUE), 0))

dt <- collapse(dt, 'sum', c('YEAR', 'DONOR_NAME', 'INCOME_SECTOR', 'INCOME_TYPE', 'DONOR_COUNTRY', 'PURPOSE', 'grant_loan', 'CHANNEL'), c('commitment', 'disbursement'))

dt[, `:=`(PURPOSE = 'Vaccine R&D', RECIPIENT_COUNTRY = 'Global',
          ISO3_RC = 'WLD',
          money_type = 'new_money')]

# Update donor names
dt[tolower(DONOR_NAME) %like% 'government', temp := tstrsplit(DONOR_NAME, ' of ', keep=2)]
dt[tolower(DONOR_NAME) %like% 'government', DONOR_NAME := paste0(temp, ', Government of')]
dt[DONOR_NAME %in% c('United Kingdom', 'The United Kingdom'), DONOR_NAME := 'United Kingdom, Government of']
dt[, temp := NULL]

# Rename donor_country to merge nicely with country codes
dt[DONOR_COUNTRY == 'Czech Republic', DONOR_COUNTRY := 'Czechia']

# Merge with other data
dt[, INCOME_SECTOR := NULL]
dt[, INCOME_TYPE := NULL]

dt$upper_DONOR_NAME <- toupper(dt$DONOR_NAME)
dt[, upper_DONOR_NAME := trimws(upper_DONOR_NAME, which = 'both')]

donors <- fread("FILEPATH/income_sector_and_type_assignments.csv")
new.donors <- data.frame(
  DONOR_NAME = c("THE UNITED STATES GOVERNMENT OF", "NA"),
  INCOME_SECTOR = c("PUBLIC", "UNALL"),
  INCOME_TYPE = c("CENTRAL", "UNALL"),
  ISO_CODE = c("USA", ""),
  IHME_NAME = c("LITHUANIA", "Unallocable")
)
donors <- rbind(donors, new.donors)
save_dataset(donors, "income_sector_and_type_assignments.csv", "meta", "donor")


# match income_sector and income_type using central functions
withdonor <- standardize_donors(dt, 'DONOR_NAME', get_path("meta", "donor", "income_sector_and_type_assignments.csv"))

# check the donors
not_match <- withdonor[[2]]
match <- withdonor[[1]]
dt <- match



# fix unallocable rows
dt[is.na(DONOR_NAME), `:=` (DONOR_NAME = 'Unallocable difference', INCOME_SECTOR = 'UNALL', INCOME_TYPE = 'UNALL')]
dt[, total_expend := disbursement]
dt[, oid_covid_disb_frct := total_expend]

cat('  Save out COVID dataset\n')
#----# Save out COVID dataset #----# ####
dt <- dt[, c('DONOR_COUNTRY', 'YEAR', 'DONOR_NAME', 'INCOME_SECTOR', 'INCOME_TYPE', 'total_expend', 'oid_covid_disb_frct', 'ISO_CODE')]
save_dataset(dt, 'COVID_prepped', 'CEPI', 'fin')
reg <- fread(get_path("CEPI", "INT", "1a_INTAKE_CLEAN_precovid.csv"))
all <- bind_rows(reg, dt)

save_dataset(all, '1a_INTAKE_CLEAN', 'CEPI', 'int')

#----------------------------------# ####