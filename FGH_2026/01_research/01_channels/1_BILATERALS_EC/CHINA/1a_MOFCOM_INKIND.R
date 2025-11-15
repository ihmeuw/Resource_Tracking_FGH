#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Format CHINA MOFCOM Data and inkind ratio data (used by all china agencies)
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

# MOFCOM fraction of hospital projects from completed aid projects
# see HUB: https://hub.ihme.washington.edu/spaces/RT/pages/106614386/Bilaterals+-+China
hospital_pre_2013 <- 80/580
hospital_post_2013 <- 58/423
#----------------------------# ####


cat('\n\n')
cat(green(' ############################################\n'))
cat(green(' #### BEGIN CHINA MOFCOM DATA FORMATTING ####\n'))
cat(green(' ############################################\n\n'))


cat('  Read in MOFCOM Data\n')
#----# Read & Clean MOFCOM Data #----# ####
# Read in data
mofcom <- read_excel(get_path('CHINA', 'raw', "CHINA_MOFCOM_ESTIMATE_1990_[report_year].xlsx"))
setDT(mofcom)
setorder(mofcom, YEAR)

# Calculate fraction of disbursement/budget to predict report_yr disbursement
mofcom[, BUDGET_FRCT := MOFCOM_ODA_RAW_MLN_CNY / MOFCOM_ODA_BUDGET_MLN_CNY]

# Generate weighted fraction with three-year weighted average
mofcom[, WGT_AVG_FRCT := (((1/2) * data.table::shift(BUDGET_FRCT, 1L, type = "lag")) + 
                            ((1/3) * data.table::shift(BUDGET_FRCT, 2L, type="lag")) +
                            ((1/6) * data.table::shift(BUDGET_FRCT, 3L, type="lag")))]

# Replace report_yr budget number with predicted amount if report_yr budget is blank
mofcom[is.na(MOFCOM_ODA_RAW_MLN_CNY),
       MOFCOM_ODA_RAW_MLN_CNY := MOFCOM_ODA_BUDGET_MLN_CNY * WGT_AVG_FRCT]
#------------------------------------# ####

cat('  Merge with China Aid Data\n')
#----# Merge China Aid Data #----# ####
# Merge MOFCOM data with China Aid data
china_aid <- read_excel(get_path('CHINA', 'raw', "CHINA_AID_BUDGET_ESTIMATE_1990_[report_year].xlsx"))
mofcom <- merge(
    mofcom, china_aid, by="YEAR", all.x=TRUE
)

if (mofcom[, .N, by = YEAR][N > 1, .N] > 0) {
  stop("There are duplicate years created by the merge of MOFCOM and China Aid data. Please check the data.")
}


# Calculate the proportion of NHC over all central government aid budget
mofcom[, MOFCOM_PROPORTION := MOFCOM_ODA_RAW_MLN_CNY / CENTRAL_GOV_AID_BUDGET_MLN_CNY]
mofcom[, MOFCOM_PROPORTION_MEAN := mean(MOFCOM_PROPORTION, na.rm = T)]
mofcom[is.na(MOFCOM_PROPORTION), MOFCOM_PROPORTION := MOFCOM_PROPORTION_MEAN]

# Generate ODA amount in million CNY value, and use the proportion to replace missing year data
mofcom[, MOFCOM_ODA_MLN_CNY := MOFCOM_ODA_RAW_MLN_CNY]
mofcom[is.na(MOFCOM_ODA_MLN_CNY), MOFCOM_ODA_MLN_CNY := CENTRAL_GOV_AID_BUDGET_MLN_CNY * MOFCOM_PROPORTION]

# Generate DAH amount in million CNY value, and this 0.137931 was generated from excelsheet using elsewhere information
# Note from Method's annex: "We extracted from the White Paper on China’s Foreign Aid (2014) that 'out of all 580 complete aid projects between 2010 and 2012, 
#                           80 were hospital projects' (2). We used this proportion, 0.137931 as a proxy for the health aid percentage of all Ministry of Commerce aid."
mofcom[, MOFCOM_DAH_MLN_CNY := ifelse(YEAR < 2013, 
                                      MOFCOM_ODA_MLN_CNY * hospital_pre_2013, 
                                      MOFCOM_ODA_MLN_CNY * hospital_post_2013)]


# Generate log of DAH
mofcom[, log_MOFCOM_DAH := log(MOFCOM_DAH_MLN_CNY)]

# Generate difference in log
mofcom[, dlog_MOFCOM_DAH := log_MOFCOM_DAH - data.table::shift(log_MOFCOM_DAH, 1L, type = "lag")]
#--------------------------------# ####

cat('  Predict Growth Rate\n')
#----# Predict Growth Rate #----# ####
# Run regression of difference in log and year to get growth rate
temp <- lm(dlog_MOFCOM_DAH ~ YEAR, mofcom)

# Predict growth rate for missing years
mofcom$avgrt <- predict(temp, mofcom)
rm(temp)

# Replace years with missing growth rate with predicted rate
mofcom[is.na(dlog_MOFCOM_DAH), dlog_MOFCOM_DAH := avgrt]

# Transform growth rate for ease of conversion to health estimate
mofcom[, dlog_MOFCOM_DAH_calc := 1 + dlog_MOFCOM_DAH]

# Calculating missing DAH when missing year of data is in the past
mofcom[, NEWAVGH0 := MOFCOM_DAH_MLN_CNY / data.table::shift(dlog_MOFCOM_DAH_calc, 1L, type = "lag")]
mofcom[, NEWAVGH01 := NEWAVGH0 / data.table::shift(dlog_MOFCOM_DAH_calc, 1L, type = "lag")]
mofcom[, NEWAVGH02 := NEWAVGH01 / data.table::shift(dlog_MOFCOM_DAH_calc, 1L, type = "lag")]

# Fill in missing DAH
mofcom[, E_MOFCOM_DAH := MOFCOM_DAH_MLN_CNY]
mofcom[1, "E_MOFCOM_DAH"] <- mofcom[4, "NEWAVGH02"]
mofcom[2, "E_MOFCOM_DAH"] <- mofcom[4, "NEWAVGH01"]
mofcom[3, "E_MOFCOM_DAH"] <- mofcom[4, "NEWAVGH0"]

# Save pre-ebola data
mofcom_pre_ebola <- mofcom[, c("YEAR", "E_MOFCOM_DAH")]
#-------------------------------# ####

cat('  Add Ebola Data\n')
#----# Add Ebola Data #----# ####
# Add ebola
ebola_all <- fread(get_path("humanitarian_aid", "fin", "ebola_all_collapsed.csv"))
ebola_all <- ebola_all[source == "BIL_CHN" & channel != "WHO" & contributionstatus == "paid", ]
ebola_all[, E_MOFCOM_DAH_USD := amount / 1e6]
setnames(ebola_all, "year", "YEAR")
ebola_all <- ebola_all[, c("YEAR", "E_MOFCOM_DAH_USD")]
#--------------------------# ####

cat('  Add Inkind Data\n')
#----# Add Inkind Data #----# ####
# Add inkind
inkind <- read_excel(get_path('CHINA', 'raw', "CHINA_INKIND_ESTIMATE_1990_[report_year].xlsx"))
setDT(inkind)
inkind[, inkind_ratio := BASIC_EXPENDITURE / PROJECT_BUDGET]
if (max(inkind$YEAR) < report_year) {
    yrs <- seq(max(inkind$YEAR) + 1, report_year)
    inkind <- rbind(inkind, data.table(YEAR = yrs), fill = TRUE)
}

# Calculate report_yr inkind-ratio using three year average
setorder(inkind, YEAR)
inkind[, inkind_ratio_foreward := ((0.5 * data.table::shift(inkind_ratio, 1L, type = "lag")) +
                                     (0.3 * data.table::shift(inkind_ratio, 2L, type = "lag")) +
                                     (0.2 * data.table::shift(inkind_ratio, 3L, type = "lag")))]
inkind[is.na(inkind_ratio), inkind_ratio := inkind_ratio_foreward]

# Calculate the 2006 and backwards in-kind ratio using three year average
inkind[, inkind_ratio_backward := ((0.5 * data.table::shift(inkind_ratio, 1L, type = "lead")) +
                                     (0.3 * data.table::shift(inkind_ratio, 2L, type = "lead")) +
                                     (0.2 * data.table::shift(inkind_ratio, 3L, type = "lead")))]
inkind[YEAR > 2006, inkind_ratio_backward := NA]
inkind[, inkind_ratio_backward_mean := mean(inkind_ratio_backward, na.rm = T)]
inkind[is.na(inkind_ratio), inkind_ratio := inkind_ratio_backward_mean]
inkind <- inkind[, c("YEAR", "inkind_ratio")]
#---------------------------# ####

cat('  Save Inkind Data\n')
#----# Save Inkind Data #----# ####
# Save INT inkind data
save_dataset(inkind, "CHINA_INKIND_ESTIMATE_1990_[report_year]", 'CHINA', 'int')
#----------------------------# ####

cat('  Merge pre-ebola and Inkind\n')
#----# Merge ebola and inkind #----# ####
# Merge pre-ebola and inkind
ebola_inkind <- merge(mofcom_pre_ebola, inkind, by="YEAR", all = TRUE)
ebola_inkind[, inkind := 0]

# Calculate inkind
temp_ebola_inkind <- copy(ebola_inkind)
temp_ebola_inkind[, E_MOFCOM_DAH := E_MOFCOM_DAH * inkind_ratio]
temp_ebola_inkind[, inkind := 1]

# Merge inkind and non-inkind
ebola_inkind <- rbind(ebola_inkind, temp_ebola_inkind)
ebola_inkind <- rbind(ebola_inkind, ebola_all, fill = T)
#----------------------------------# ####

cat('  Save Stage 1a Data\n')
#----# Save Stage 1a data #----# ####
save_dataset(ebola_inkind, '1a_MOFCOM', 'CHINA', 'int')
#------------------------------# ####