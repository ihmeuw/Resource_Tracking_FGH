#----# Docstring #----# ####
# Project:  FGH 
# Purpose:  Finalize BMGF ADB_PDB Dataset
#---------------------# 

#---------NOTES----------#
# uses both rowtotal and collapse which should be depricated in favor of
# data.table lapply() and rowSums() syntax.
#

#----# Environment Prep #----# ####
rm(list=ls())
library(dplyr)

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)
#----------------------------# 


cat('\n\n')
cat(green(' ##############################\n'))
cat(green(' #### BMGF ADBPDB FINALIZE ####\n'))
cat(green(' ##############################\n\n'))


cat('  Read in INTPDB\n')
#----# Read in INTPDB #----# ####
dt <- fread(paste0(get_path('BMGF', 'fin'), 'BMGF_INTPDB_1999_', dah.roots$report_year, '_update.csv'))
#--------------------------# 
cat('  Create new cols, fix ISOs, generate gov\n')
#----# Create new cols, fix ISOs, generate gov #----# ####
dt[, LEVEL := 'COUNTRY']
dt[RECIPIENT_COUNTRY == "Asia" & ISO3_RC != 'N/A', ISO3_RC := 'QRA'] 
dt[RECIPIENT_COUNTRY == "North America" & ISO3_RC != 'N/A', ISO3_RC := 'QNC']
dt[RECIPIENT_COUNTRY == "South America" & ISO3_RC != 'N/A', ISO3_RC := 'QNE'] 
dt[RECIPIENT_COUNTRY == "Africa" & ISO3_RC != 'N/A', ISO3_RC := 'QMA']
dt[RECIPIENT_COUNTRY=="Global" , ISO3_RC := 'WLD']

dt[ISO3_RC %in% c("QMA", 'QMD', 'QME', 'QNA', 'QNB', 'QNC', 'QNE', 'QRA', 'QRB',
                  'QRC', 'QRD', 'QRE', 'QRS', 'QSA', 'QTA'), LEVEL := 'REGIONAL']
dt[ISO3_RC == "WLD", LEVEL := 'GLOBAL']

dt[, gov := as.numeric(NA)]
dt[LEVEL== "COUNTRY" & RECIPIENT_AGENCY_SECTOR=="GOV", gov := 1]
dt[LEVEL== "REGIONAL" & RECIPIENT_AGENCY_SECTOR=="GOV", gov := 1]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY=="PUBLIC SECTOR INSTITUTIONS", gov := 1]
dt[LEVEL== "COUNTRY" & RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% 'Government', gov := 1]

dt[LEVEL=='COUNTRY' & RECIPIENT_AGENCY_SECTOR=="CORP" | RECIPIENT_AGENCY_SECTOR=="CSO" | RECIPIENT_AGENCY_SECTOR=="IGO" | 
     RECIPIENT_AGENCY_SECTOR=="PPP" | RECIPIENT_AGENCY_SECTOR=="UNIV", 
   gov := 2]
dt[LEVEL== "REGIONAL" & RECIPIENT_AGENCY_SECTOR=="CORP" | RECIPIENT_AGENCY_SECTOR=="CSO" | RECIPIENT_AGENCY_SECTOR=="IGO" | 
     RECIPIENT_AGENCY_SECTOR=="PPP" | RECIPIENT_AGENCY_SECTOR=="UNIV", 
   gov := 2]

dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "NGO", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "Institute", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "University", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "Asian Development", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "Food", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "Vaccines", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "Red Cross", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "NETWORKS", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "MULTILATERAL", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "PPP", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "UNITED NATIONS", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "United Nations", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "World Food Programme", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "African", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "European Commission", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "FAMILY HEALTH INTERNATIONAL", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "Family Health International", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "BANK", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "bank", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "Bank", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "SAVE THE CHILDREN", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "Save the Children", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "Clean Technology Fund", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "IPAS", gov := 2]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% "International Atomic Energy Agency", gov := 2]

dt[LEVEL== "GLOBAL" & is.na(gov), gov := 0]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% 'OTHER' & is.na(gov), gov := 0]
dt[RECIPIENT_AGENCY_SECTOR=="" & RECIPIENT_AGENCY %like% 'Other' & is.na(gov), gov := 0]

dt[is.na(gov), gov := 0]

# Rename & collapse
setnames(dt, 'DISBURSEMENT', 'DAH')

data <- collapse(dt, 'sum', c('YEAR', 'ISO3_RC', 'gov', 'ELIM_CH', 'CHANNEL', 'SOURCE', 'INKIND', names(dt)[names(dt) %like% 'INCOME_']),
                 c('DAH', names(dt)[names(dt) %like% '_DAH']))
data[is.na(ELIM_CH), ELIM_CH := 0]
rm(dt)
#---------------------------------------------------#

cat('  Settle negative disbursements not fixed at project-level\n')
#----# Settle negative disbursements not fixed at project-level #----# ####
data <- data %>% group_by(CHANNEL, ISO3_RC, gov, INKIND) %>% setDT()
setnames(data, 'DAH', 'allDAH')

to_calc <- names(data)[names(data) %like% '_DAH'] %>% str_replace_all('_DAH', '')
data <- data[order(YEAR), ]
lst <- list()
for (col in to_calc) {
  data[get(paste0(col, '_DAH')) < 0, eval(paste0('neg_', col)) := 1]
  lst[[paste0('neg_', col)]] <- data[get(paste0('neg_', col)) == 1, c('YEAR', 'ISO3_RC', paste0(col, '_DAH')), with=F]
  data[get(paste0('neg_', col)) == 1, other_DAH := other_DAH + get(paste0(col, '_DAH'))]
  data[get(paste0('neg_', col)) == 1, eval(paste0(col, '_DAH')) := 0]
}

data <- data[order(CHANNEL, ISO3_RC, gov, INKIND, YEAR), ]

for (year in dah.roots$report_year:2000) { 
  data[(YEAR==year-1) & shift(other_DAH, n=1, type='lead') < 0, other_DAH := other_DAH + shift(other_DAH, n=1, type='lead')]
  data[YEAR == year & other_DAH < 0, other_DAH := 0]
}

data[YEAR==1999 & other_DAH<0, other_DAH := 0]

data <- rowtotal(data, 'new_tot', names(data)[names(data) %like% '_DAH'])

data[, allDAH := new_tot]
setnames(data, 'allDAH', 'DAH')

data <- data[, !c('new_tot', names(data)[names(data) %like% 'neg_']), with=F]
data[, DONOR_NAME := 'BILL & MELINDA GATES FOUNDATION']
#--------------------------------------------------------------------#
check <- data %>%
  group_by(YEAR) %>%
  filter(ELIM_CH == 0) %>%
  summarize(OUTFLOW = sum(DAH, na.rm = T)) %>%
  mutate(fgh = "new")
plot(check$YEAR, check$OUTFLOW, type = "l")
old <- fread(get_path("BMGF", "fin", "BMGF_ADB_PDB_FGH_2023.csv", report_year = 2023))
check2 <- old %>%
  group_by(YEAR) %>%
  filter(ELIM_CH == 0) %>%
  summarize(OUTFLOW = sum(DAH, na.rm = T)) %>%
  mutate(fgh = "old")

plot(check$YEAR, check$OUTFLOW, type = "l", col = "blue", xlab = "X", ylab = "Values")
lines(check2$YEAR, check2$OUTFLOW, col = "red")


cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(data, paste0('BMGF_ADB_PDB_FGH_', dah.roots$report_year), 'BMGF', 'fin')
#------------------------#