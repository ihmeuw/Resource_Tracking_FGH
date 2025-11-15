#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Format CHINA MOE Data
#---------------------# ####

#----# Environment Prep #----# ####
# System prep
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
source(paste0(code_repo, "FUNCTIONS/helper_functions.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

#----------------------------# ####

cat('\n\n')
cat(green(' #########################################\n'))
cat(green(' #### BEGIN CHINA MOE DATA FORMATTING ####\n'))
cat(green(' #########################################\n\n'))


cat('  Read in MOE Data\n')
#----# Read MOE data #----# ####
# Pull in raw data
moe <- read_excel(get_path('CHINA', 'raw', "CHINA_MOE_ESTIMATE_1990_[report_year].xlsx"))
setDT(moe)
moe <- moe[, c("YEAR", "MOE_ALL_SCHOLAR", "MOE_MED_SCHOLAR", "MOE_ODA_RAW_MLN_CNY"),
           with = FALSE]
#-------------------------# ####

cat('  Generate cost data\n')
#----# Generate cost data #----# ####
# Generate cost for each year
setorder(moe, YEAR)

# use weighted average to estimate total MOE ODA for the report-year 
moe[, WGT_AVG_MOE_ODA := ((1/2) * data.table::shift(MOE_ODA_RAW_MLN_CNY, 1L, type = "lag")) +
      ((1/3) * data.table::shift(MOE_ODA_RAW_MLN_CNY, 2L, type = "lag")) +
      ((1/6) * data.table::shift(MOE_ODA_RAW_MLN_CNY, 3L, type = "lag"))]
moe[is.na(MOE_ODA_RAW_MLN_CNY), MOE_ODA_RAW_MLN_CNY := WGT_AVG_MOE_ODA]

#
# Impute MOE_ALL_SCHOLAR (count of foreign students on scholarships) and
#  calculate AVG_COST (the cost per student, based on the budget)
#

## only have observed student numbers pre-2019
moe[YEAR <= 2018, student_count := MOE_ALL_SCHOLAR]
moe[, cost_per_student := MOE_ODA_RAW_MLN_CNY / student_count]

## add gdp as covariate
gdp <- get_gdp()
gdp <- gdp[iso3 == "CHN",
           .(gdp_pc = mean(data_var)),  # summarize draws via average
           by = year]
moe <- merge(moe, gdp, by.x = "YEAR", by.y = "year", all.x = TRUE)


## estimate cost_per_student for future years based on gdp_pc
m <- mgcv::gam(cost_per_student ~ s(gdp_pc, k = 5),
               data = moe,
               method = "REML")

moe[, pred := predict(m, newdata = .SD)]

if (interactive()) {
    gdp[, `:=`(YEAR = year, pred = predict(m, newdata = .SD))]
    ggplot(moe[YEAR >= 2000], aes(x = YEAR)) +
        # compare observed cost_per_student to predicted
        geom_point(aes(y = cost_per_student, color = "observed")) +
        geom_line(aes(y = pred, color = "predicted")) +
        # see how the average cost will continue into the future based on
        #   gdp forecasts
        labs(title = "MOE Scholarships Cost per Foreign Student",
             color = "") +
        theme_bw()
    
    ggplot(moe, aes(x = YEAR)) +
        geom_point(aes(y = student_count, color = "observed")) +
        geom_line(aes(y = MOE_ODA_RAW_MLN_CNY / pred, color = "predicted")) +
        labs(title = "MOE Foreign Scholar Count",
             color = "") +
        theme_bw()
}


# Use predicted cost-per-student where observed is missing
moe[is.na(cost_per_student), cost_per_student := pred]
# Infer the student count for years without data using the observed total
#   scholarship funding and the complete cost-per-student data
moe[is.na(student_count), student_count := MOE_ODA_RAW_MLN_CNY / cost_per_student]


moe[, AVG_COST := cost_per_student]
moe[, MOE_ALL_SCHOLAR := student_count]

#------------------------------# ####

cat('  Generate medical student costs\n')
#----# Generate med student costs #----# ####
# Generating medical student cost based on it is 10% more expensive than other students
# Note from methods annex: "Given that medical and health sciences students typically cost more than other students in terms of tuition, 
# we adjusted for the difference in costs based on a 2015 Ministry of Finance notice (44). We estimated that medical and health sciences students 
# cost 10% more than other students based on additional data provided in this notice that listed the standard tuition and stipend fee for medical 
# students, science students and art students."
moe[, MED_AVG_COST := 1.1 * AVG_COST]

# Generate estimate of medical students on scholarship
moe[, MED_SCHOLAR_PROPORTION := MOE_MED_SCHOLAR / MOE_ALL_SCHOLAR]
moe[, MED_SCHOLAR_PROPORTION_MEAN := mean(MED_SCHOLAR_PROPORTION, na.rm = T)]
moe[is.na(MED_SCHOLAR_PROPORTION), MED_SCHOLAR_PROPORTION := MED_SCHOLAR_PROPORTION_MEAN]
moe[, MOE_MED_SCHOLAR_EST := MOE_ALL_SCHOLAR * MED_SCHOLAR_PROPORTION]

# Replace medical students on scholarship with estimate when missing
moe[is.na(MOE_MED_SCHOLAR), MOE_MED_SCHOLAR := MOE_MED_SCHOLAR_EST]

# Generate estimate of medical student scholarship money, and we are using the average cost of the specific year
moe[, MOE_DAH_MLN_CNY := MOE_MED_SCHOLAR * MED_AVG_COST]

# Generate log of MOE DAH
moe[, log_MOE_DAH := log(MOE_DAH_MLN_CNY)]

# Generate difference in log
moe[, dlog_MOE_DAH := log_MOE_DAH - data.table::shift(log_MOE_DAH, 1L, type = "lag")]
#--------------------------------------# ####

cat('  Predict Growth Rate\n')
#----# Predict Growth Rate to Impute 1990-1993 #----# ####
# Run regression of difference in log and year to get growth rate
temp <- lm(dlog_MOE_DAH ~ YEAR, moe)

# Predict growth rate for missing years
moe$avgrt <- predict(temp, moe)
rm(temp)

# Replace years with missing growth rate with predicted rate
moe[is.na(dlog_MOE_DAH), dlog_MOE_DAH := avgrt]

# Transform growth rate for ease of conversion to health estimate
moe[, dlog_MOE_DAH_calc := 1 + dlog_MOE_DAH]

# Calculating missing health aid when missing year of data is in the future
moe[, NEWAVGH := dlog_MOE_DAH_calc * data.table::shift(MOE_DAH_MLN_CNY, 1L, type = "lag")]

# Calculating missing health aid when missing year of data is in the past
moe[, NEWAVGH0 := MOE_DAH_MLN_CNY / data.table::shift(dlog_MOE_DAH_calc, 1L, type = "lag")]
moe[, NEWAVGH01 := NEWAVGH0 / data.table::shift(dlog_MOE_DAH_calc, 1L, type = "lag")]
moe[, NEWAVGH02 := NEWAVGH01 / data.table::shift(dlog_MOE_DAH_calc, 1L, type = "lag")]
moe[, NEWAVGH03 := NEWAVGH02 / data.table::shift(dlog_MOE_DAH_calc, 1L, type = "lag")]

# Fill in missing health aid
moe[, E_MOE_DAH := MOE_DAH_MLN_CNY]
moe[is.na(E_MOE_DAH), E_MOE_DAH := NEWAVGH]

moe[1, "E_MOE_DAH"] <- moe[5, "NEWAVGH03"]
moe[2, "E_MOE_DAH"] <- moe[5, "NEWAVGH02"]
moe[3, "E_MOE_DAH"] <- moe[5, "NEWAVGH01"]
moe[4, "E_MOE_DAH"] <- moe[5, "NEWAVGH0"]
#-------------------------------# ####

cat('  Add inkind\n')
#----# Add inkind #----# ####
inkind <- fread(get_path('CHINA', 'int', 'CHINA_INKIND_ESTIMATE_1990_[report_year].csv'))

# Add in-kind
moe <- merge(moe, inkind, by = "YEAR", all.x = TRUE)
moe[, inkind := 0]

moe_inkind <- copy(moe)
moe_inkind[, `:=`(E_MOE_DAH = E_MOE_DAH * inkind_ratio,
                  inkind = 1)]

moe <- rbind(moe, moe_inkind)
#----------------------# ####

cat('  Save Stage 1d Data\n')
#----# Save Stage 1d Data #----# ####
save_dataset(moe, '1d_MOE', 'CHINA', 'int')
#------------------------------# ####
