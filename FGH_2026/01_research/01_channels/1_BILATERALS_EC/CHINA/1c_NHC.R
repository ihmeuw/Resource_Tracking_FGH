#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Format CHINA NHC Data
#---------------------# ####

#----# Environment Prep #----# ####
# System prep
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr, dplyr)

#----------------------------# ####

cat('\n\n')
cat(green(' #########################################\n'))
cat(green(' #### BEGIN CHINA NHC DATA FORMATTING ####\n'))
cat(green(' #########################################\n\n'))


cat('  Read in NHC Data\n')
#----# Read NHC Data #----# ####
# Pull in raw data
nhc <- as.data.table(read_excel(get_path('CHINA', 'raw', "CHINA_NHC_ESTIMATE_1990_[report_year].xlsx")))
#-------------------------# ####

cat('  Calculate disbursement\n')
#----# Calc Disbursement #----# ####
# Calculate fraction of disbursement/budget to predict 2018 disbursement
nhc[, BUDGET_FRCT := NHC_DAH_RAW_MLN_CNY / NHC_DAH_BUDGET_MLN_CNY]

# Generate weighted fraction with three-year weighted average
nhc[, WGT_AVG_FRCT := ((1/2) * data.table::shift(BUDGET_FRCT, 1L, type = "lag")) +
      ((1/3) * data.table::shift(BUDGET_FRCT, 2L, type = "lag")) +
      ((1/6) * data.table::shift(BUDGET_FRCT, 3L, type = "lag"))]

# Estimate budget amount for report_yr if budget not produced
budg_frct <- data.frame("YEAR" = 1990:dah.roots$report_year, 
                        "budg_frct" =
                            ((1/2) * data.table::shift(nhc$NHC_DAH_BUDGET_MLN_CNY, 1L, type = "lag")) +
                            ((1/3) * data.table::shift(nhc$NHC_DAH_BUDGET_MLN_CNY, 2L, type = "lag")) +
                            ((1/6) * data.table::shift(nhc$NHC_DAH_BUDGET_MLN_CNY, 3L, type = "lag"))
                        )
if(is.na(nhc[YEAR == dah.roots$report_year, "NHC_DAH_BUDGET_MLN_CNY"])) {
  nhc[YEAR == dah.roots$report_year, "NHC_DAH_BUDGET_MLN_CNY"] <- budg_frct[budg_frct$YEAR == dah.roots$report_year, "budg_frct"]
}
rm(budg_frct)
#-----------------------------# ####

cat('  Calculate budget\n')
#----# Calc Budget #----# ####
# Replace 2018 budget number with predicted weighted disbursement
nhc[is.na(NHC_DAH_RAW_MLN_CNY), NHC_DAH_RAW_MLN_CNY := NHC_DAH_BUDGET_MLN_CNY * WGT_AVG_FRCT]

# Merge with china aid budget
china_aid <- as.data.table(read_excel(get_path('CHINA', 'raw', "CHINA_AID_BUDGET_ESTIMATE_1990_[report_year].xlsx")))
nhc <- merge(nhc, china_aid, by="YEAR", all.x=T)

# Calculate the proportion of NHC over all central government aid budget
nhc[, NHC_PROPORTION := NHC_DAH_RAW_MLN_CNY/CENTRAL_GOV_AID_BUDGET_MLN_CNY]
nhc[, NHC_PROPORTION_MEAN := mean(NHC_PROPORTION, na.rm = T)]
nhc[is.na(NHC_PROPORTION), NHC_PROPORTION := NHC_PROPORTION_MEAN]

# Generate DAH amount in million CNY value, and use the proportion to replace missing year data
nhc[, NHC_DAH_MLN_CNY := NHC_DAH_RAW_MLN_CNY]
nhc[is.na(NHC_DAH_MLN_CNY), NHC_DAH_MLN_CNY := CENTRAL_GOV_AID_BUDGET_MLN_CNY*NHC_PROPORTION]

# Generate log of DAH
nhc[, log_NHC_DAH := log(NHC_DAH_MLN_CNY)]

# Generate difference in log
nhc[, dlog_NHC_DAH := log_NHC_DAH - data.table::shift(log_NHC_DAH, 1L, type = "lag")]
#-----------------------# ####

cat('  Predict growth\n')
#----# Predict Growth #----# ####
# Run regression of difference in log and year to get growth rate
temp <- lm(dlog_NHC_DAH ~ YEAR, nhc)

# Predict growth rate for missing years
nhc$avgrt <- predict(temp, nhc)
rm(temp)

# Replace years with missing growth rate with predicted rate
nhc[is.na(dlog_NHC_DAH), dlog_NHC_DAH := avgrt]

# Transform growth rate for ease of conversion to health estimate
nhc[, dlog_NHC_DAH_calc := 1+ dlog_NHC_DAH]

# Calculate missing DAH when missing year of data is in the past
nhc[, NEWAVGH0 := NHC_DAH_MLN_CNY / data.table::shift(dlog_NHC_DAH_calc, 1L, type = "lag")]
nhc[, NEWAVGH01 := NEWAVGH0 / data.table::shift(dlog_NHC_DAH_calc, 1L, type = "lag")]
nhc[, NEWAVGH02 := NEWAVGH01 / data.table::shift(dlog_NHC_DAH_calc, 1L, type = "lag")]

# Fill in missing DAH
nhc[, E_NHC_DAH := NHC_DAH_MLN_CNY]
nhc[1, "E_NHC_DAH"] <- nhc[4, "NEWAVGH02"]
nhc[2, "E_NHC_DAH"] <- nhc[4, "NEWAVGH01"]
nhc[3, "E_NHC_DAH"] <- nhc[4, "NEWAVGH0"]
#--------------------------# ####

cat('  Add inkind\n')
#----# Add Inkind #----# ####
inkind <- fread(get_path('CHINA', 'int', 'CHINA_INKIND_ESTIMATE_1990_[report_year].csv'))
# Add in-kind
nhc <- merge(nhc, inkind, by="YEAR", all.x = TRUE)
nhc[, inkind := 0]

nhc_inkind <- copy(nhc)
nhc_inkind[, `:=`(E_NHC_DAH = E_NHC_DAH * inkind_ratio,
                  inkind = 1)]

nhc <- rbind(nhc, nhc_inkind)
#----------------------# ####

cat('  Save Stage 1c Data\n')
#----# Save Stage 1c Data #----# ####
save_dataset(nhc, paste0('1c_NHC'), 'CHINA', 'int')
#------------------------------# ####