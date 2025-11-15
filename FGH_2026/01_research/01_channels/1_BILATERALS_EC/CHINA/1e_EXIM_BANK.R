#----# Docstring #----# ####
# Project:  FGH 
# Purpose:  Format CHINA EXIM Bank Data
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

# Variable Prep
health_proj_frac <- 0.019312421 # from literature review 
#----------------------------# ####


cat('\n\n')
cat(green(' ###############################################\n'))
cat(green(' #### BEGIN CHINA EXIM BANK DATA FORMATTING ####\n'))
cat(green(' ###############################################\n\n'))


cat('  Read in EXIM Bank Data\n')
#----# Read EXIM Bank Data #----# ####
# Pull in raw data
exim <- read_excel(get_path('CHINA', 'raw', "CHINA_EXIM_BANK_ESTIMATE_1990_[report_year].xlsx"))
setDT(exim)

if (max(exim$YEAR) < dah_cfg$report_year) {
    max_yr <- max(exim$YEAR)
    diff <- dah_cfg$report_year - max_yr
    exim <- rbind(
        exim,
        data.table(YEAR = seq(max_yr + 1, dah_cfg$report_year)),
        fill = TRUE
    )
}

#-------------------------------# ####

cat('  Merge exchange rates\n')
#----# Merge with Exchange Rates #----# ####
exch_rates <- fread(get_path("meta", "rates", "OECD_XRATES_NattoUSD_1950_[report_year].csv"))
exch_rates <- exch_rates[LOCATION == "CHN" & TIME >= 1990, c("TIME", "Value")]
setnames(exch_rates, c("TIME", "Value"), c("YEAR", "XRATE"))

# Merge with exchange rate data
exim <- merge(exim, exch_rates, by = "YEAR", all.x = TRUE)
#-------------------------------------# ####

cat('  Generate concessional loan amounts\n')
#----# Generate concessional loan amounts #----# ####
# Generate concessional loan amount in million CNY value
exim[, CONCESSIONAL_LOAN_MLN_CNY := CONCESSIONAL_LOAN_100MLN_CNY  * 100]
exim[is.na(CONCESSIONAL_LOAN_100MLN_CNY), CONCESSIONAL_LOAN_MLN_CNY := CONCESSIONAL_LOAN_MLN_USD * XRATE]

# Generate health proportion of concessional spending, this 0.014773 was generated from excelsheet using Aiddata project information 
exim[, HEALTH_CONCESSIONAL_LOAN_MLN_CNY := CONCESSIONAL_LOAN_MLN_CNY * health_proj_frac]

# Generate difference in log variables
exim[is.na(EXPORT), EXPORT := ((1/2) * data.table::shift(EXPORT, 1L, type = "lag")) +
       ((1/3) * data.table::shift(EXPORT, 2L, type = "lag")) +
       ((1/6) * data.table::shift(EXPORT, 3L, type = "lag"))]

# Drop data before 1995, the EXIM concessional loan starts operating in 1995
exim <- exim[YEAR > 1994,]
#----------------------------------------------# ####

cat('  Ipolate missing years\n')
#----# Ipolate missing years #----# ####
# Ipolation of missing years 2002, 2003 and 2009
temp_exim <- exim[exim$YEAR %in% c(1996:2014)]
ipolate_data <- setDT(approx(x = temp_exim$YEAR, y = temp_exim$HEALTH_CONCESSIONAL_LOAN_MLN_CNY, 
                             n = (max(temp_exim$YEAR) - min(temp_exim$YEAR) + 1), method = "linear"))
colnames(ipolate_data) <- c("YEAR", "E_HEALTH_CONCESSIONAL_LOAN")
exim <- setDT(full_join(exim, ipolate_data, by = "YEAR"))
rm(temp_exim, ipolate_data)

# Generate log of health concessional loan
exim[, log_HEALTH_DAH := log(E_HEALTH_CONCESSIONAL_LOAN)]

# Generate difference in log
exim[, dlog_HEALTH_DAH := log_HEALTH_DAH - data.table::shift(log_HEALTH_DAH, 1L, type = "lag")]
#---------------------------------# ####

cat('  Predict growth rate\n')
setorder(exim, YEAR)
#----# Predict Growth Rate #----# ####
# Run regression of difference in log and year to get growth rate
temp <- lm(dlog_HEALTH_DAH ~ YEAR, exim)

# Predict growth rate for missing years
exim$avgrt <- predict(temp, exim)
rm(temp)

# Replace years with missing growth rate with predicted rate
exim[is.na(dlog_HEALTH_DAH), dlog_HEALTH_DAH := avgrt]

# Transform growth rate for ease of conversion to health estimate
exim[, dlog_HEALTH_DAH_calc := 1 + dlog_HEALTH_DAH]


# Calculating missing health concessional loans when missing year of data is in the past
exim[, health_loan := E_HEALTH_CONCESSIONAL_LOAN]

exim[, est := shift(
    health_loan / data.table::shift(dlog_HEALTH_DAH_calc),
    type = "lead"
)]
exim[is.na(health_loan), health_loan := est]


# Calculating missing health concessional loans when missing year of data is in the future
exim[, lag1 := shift(health_loan, n = 1, type = "lag")]
for (yr in seq(min(exim$YEAR), dah_cfg$report_year)) {
    # propagate value forward one year using the growth rate applied to the lag
    exim[YEAR == yr & is.na(health_loan),
         health_loan := lag1 * dlog_HEALTH_DAH_calc]
    # re-compute the lag for use in the next iteration
    exim[, lag1 := shift(health_loan, n = 1, type = "lag")]
}

if (exim[!is.na(E_HEALTH_CONCESSIONAL_LOAN) &
         E_HEALTH_CONCESSIONAL_LOAN != health_loan, .N] > 0) {
    stop("DAH forecast algorithm should not differ from any observed values.")
}

# Fill in missing health concessional loans
exim[, E_EXIM_DAH := health_loan]

#-------------------------------# ####

cat('  Add inkind\n')
#----# Add Inkind #----# ####
inkind <- fread(get_path('CHINA', 'int', 'CHINA_INKIND_ESTIMATE_1990_[report_year].csv'))

# Add in-kind
exim <- merge(exim, inkind, by = "YEAR", all.x = TRUE)
exim[, inkind := 0]

exim_inkind <- copy(exim)
exim_inkind[, `:=`(E_EXIM_DAH = E_EXIM_DAH * inkind_ratio,
                   inkind = 1)]

exim <- rbind(exim, exim_inkind)
#----------------------# ####

cat('  Save Stage 1e Data\n')
#----# Save Stage 1e Data #----# ####
save_dataset(exim, '1e_EXIM_BANK', 'CHINA', 'int')
#------------------------------# ####