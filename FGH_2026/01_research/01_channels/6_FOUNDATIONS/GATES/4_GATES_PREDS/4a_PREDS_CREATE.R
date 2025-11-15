#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Generate BMGF Predictions
#---------------------#

#------------NOTES-------------#
# uses collapse which should be deprecated in favor of data.table lapply() syntax
#------------------------------#

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)
# Variable prep
defl <- get_path("meta", "defl")
#----------------------------#


cat('\n\n')
cat(green(' ###########################\n'))
cat(green(' #### BMGF PREDS CREATE ####\n'))
cat(green(' ###########################\n\n'))


# -----------------------------------------------------#

dt <- fread(get_path("bmgf", "fin", "BMGF_ADB_PDB_FGH_[report_year].csv"))
setorder(dt, YEAR)
dah <- dt[ELIM_CH == 0,
         .(dah = sum(DAH, na.rm = TRUE)),
         by = YEAR]

# load budget and calculate percent change in budget for next year
budget <- openxlsx::read.xlsx(get_path("bmgf", "raw", "BMGF_SPEND_COMMITMENT.xlsx"))
budget <- as.data.table(budget)[, .(YEAR = year, budget)]
budget[order(YEAR), lag1 := shift(budget, n = 1, type = 'lag')]
budget[, pct_chg := (budget - lag1) / lag1]
budget[order(-YEAR), apply_pct_chg := shift(pct_chg, n = 1, type = 'lag')]

# use percent change to make prediction
dah <- merge(dah, budget[, .(YEAR, apply_pct_chg)], by = "YEAR", all.x = TRUE)
dah[, pr_dah := dah * (1 + apply_pct_chg)]
new <- dah[YEAR == max(YEAR), ]
new[, `:=`(
  YEAR = max(YEAR) + 1,
  dah = pr_dah
)]
dah <- rbind(dah, new)
dah[, c("apply_pct_chg", "pr_dah") := NULL]

# NOTE:
# FGH 2024 TEMP PREDICTION METHOD
# Due to Gates rescaling that is going to happen in compiling, we want to base
# the predictions on the 2024 budget but not on the DAH envelope
health_spend <- fread(get_path("bmgf", "raw", "BMGF_HEALTH_SPEND.csv"))[, -"Source"]
budget <- openxlsx::read.xlsx(get_path("bmgf", "raw", "BMGF_SPEND_COMMITMENT.xlsx"))
budget <- as.data.table(budget)[, .(YEAR = year, budget)]
budget <- merge(budget, health_spend, by = "YEAR", all.x = TRUE)
frac <- budget[YEAR == 2023, HEALTH_SPENDING / budget]
budget[, dah := budget * frac]

dah <- rbind(
    dah[YEAR < report_year],
    budget[YEAR >= report_year, .(YEAR, dah)]
)




# deflate 
defl_yy <- substr(dah_cfg$report_year, 3, 4)
defl <- fread(get_path("meta", "defl", "imf_usgdp_deflators_[defl_mmyy].csv"),
               select = c("YEAR", paste0("GDP_deflator_", report_year)))

dah <- merge(dah, defl, by = "YEAR", all.x = TRUE)
dah[, paste0("OUTFLOW_final_", defl_yy) :=
        dah / get(paste0("GDP_deflator_", report_year))]

save_dataset(dah,
             "BMGF_PREDS_DAH_1990_[report_year].csv",
             channel = "BMGF",
             stage = "fin")

# -----------------------------------------------------#



# The below is left for reference, but if we switch permanently to the above
# method it can be deleted.
if (FALSE) {
cat('  Read in ADBPDB\n')
#----# Read in ADBPDB #----# ####
dt <- fread(get_path('BMGF', 'fin', 'BMGF_ADB_PDB_FGH_[report_year].csv'))
dt <- dt[YEAR < dah.roots$report_year]

dt[INKIND == 1, INKIND_OUTFLOW := DAH]
dt[INKIND == 1, DAH := 0]
dt[ELIM_CH == 1, OUTFLOW_DC := DAH]
dt <- collapse(dt, 'sum', 'YEAR', c('DAH', 'INKIND_OUTFLOW', 'OUTFLOW_DC'))
dt[, OUTFLOW_final := (DAH + INKIND_OUTFLOW) - OUTFLOW_DC]
setnames(dt, 'DAH', 'OUTFLOW')
#--------------------------# 

cat('  Import covariates data\n')
#----# Import covariates data #----# ####
covar <- setDT(fread(paste0(get_path('BMGF', 'raw'), 'bmgf_covariates_2000_', dah.roots$report_year, '.csv')))
moneycols <- c("Russell2000_Close", "BerkHath-A_Close", "SP500_Close")
covar[, (moneycols) := lapply(.SD, \(x) {
    as.numeric(gsub(",", "", x))
}), .SDcols = moneycols]

dt <- merge(dt, covar, 'YEAR', all=T)
rm(covar)
#----------------------------------# 

cat('  Import trust data\n')
#----# Import trust data #----# ####
trust <- setDT(fread(paste0(get_path('BMGF', 'raw'), 'bmgf_trust_totalassets_2002-', dah.roots$prev_report_year, '.csv')))
setnames(trust, c('TrustTotalAssets_thousands_of_curUSD', 'Year'), c('trust_curr', 'YEAR'))
trust[, trust_curr := str_replace_all(trust_curr, ',', '')]
trust[, trust_curr := as.numeric(trust_curr) * 1000]

dt <- merge(dt, trust, 'YEAR', all=T)
rm(trust)
#-----------------------------#

cat('  Import deflators\n')
#----# Import deflators #----# ####
rates <- fread(get_path("meta", "defl",
                        paste0("imf_usgdp_deflators_", dah.roots$defl_MMYY, ".csv")),
               select = c("YEAR",
                          "US_GDP_current",
                          paste0("GDP_deflator_", dah.roots$report_year)))

dt <- merge(dt, rates, 'YEAR', all.x=T)

for (col in c('US_GDP_current', 'OUTFLOW', 'OUTFLOW_final', 'trust_curr')) {
  dt[, eval(paste0(col, '_', dah.roots$abrv_year)) := get(col) / get(paste0('GDP_deflator_', dah.roots$report_year))]
}
setnames(dt, c(paste0('US_GDP_current_', dah.roots$abrv_year), paste0('trust_curr_', dah.roots$abrv_year)), 
         c(paste0('US_GDP_', dah.roots$abrv_year), paste0('trust_', dah.roots$abrv_year)))

rm(rates,col)
#----------------------------#

cat('  Estimate total envelope\n')
#----# Estimate total envelope #----# ####
dt[, ratio := get(paste0('OUTFLOW_', dah.roots$abrv_year)) / shift(get(paste0('trust_', dah.roots$abrv_year)), n=1, type='lag')]
dt[, pc := (get(paste0('OUTFLOW_', dah.roots$abrv_year)) - 
              shift(get(paste0('OUTFLOW_', dah.roots$abrv_year)), n=1, type='lag')) / shift(get(paste0('OUTFLOW_', dah.roots$abrv_year)), n=1, type='lag')]
dt[, pc_asset := (get(paste0('trust_', dah.roots$abrv_year)) - 
                    shift(get(paste0('trust_', dah.roots$abrv_year)), n=1, type='lag')) / shift(get(paste0('trust_', dah.roots$abrv_year)), n=1, type='lag')]

mod <- lm(data=dt, formula = get(paste0('OUTFLOW_', dah.roots$abrv_year)) ~ shift(get(paste0('US_GDP_', dah.roots$abrv_year)), n=2, type='lag') +
            shift(`BerkHath-A_Close`, n=2, type='lag') + shift(Russell2000_Close, n=2, type='lag') +  shift(get(paste0('trust_', dah.roots$abrv_year)), n=2, type='lag')
          + YEAR)

# Note, if you get an error here about `factor has new levels`, make sure you didn't enter numbers into one of the above
# CSVs as strings
preds <- predict(object=mod, newdata = dt)

dt[, eval(paste0('predicted_outflow_', dah.roots$abrv_year, 'model_d')) := preds]

dt[, ratio_model_d := get(paste0('predicted_outflow_', dah.roots$abrv_year, 'model_d')) / shift(get(paste0('trust_', dah.roots$abrv_year)), n=1, type='lag')]
dt[, pc_model_d := 
     (get(paste0('predicted_outflow_', dah.roots$abrv_year, 'model_d')) - shift(get(paste0('OUTFLOW_', dah.roots$abrv_year)), n=1, type='lag')) / 
     shift(get(paste0('OUTFLOW_', dah.roots$abrv_year)), n=1, type='lag')]
dt[YEAR != dah.roots$report_year, pc_model_d := NA]
rm(mod, preds)
#-----------------------------------#

cat('  Estimate DAH envelope\n')
#----# Estimate DAH envelope #----# ####
dt[, dif := get(paste0('OUTFLOW_', dah.roots$abrv_year)) - get(paste0('OUTFLOW_final_', dah.roots$abrv_year))]
dt[, temp := get(paste0('OUTFLOW_', dah.roots$abrv_year))]
dt[is.na(temp), temp := get(paste0('predicted_outflow_', dah.roots$abrv_year, 'model_d'))]

mod <- lm(data=dt[YEAR > 2003, ], formula = dif ~ temp + YEAR)
preds <- predict(mod, dt)
dt[, fit := preds]
rm(mod, preds)
#---------------------------------#

cat('  Replace report year values with predictions\n')
#----# Replace report year values with predictions #----# ####
dt[YEAR == dah.roots$report_year, eval(paste0('OUTFLOW_', dah.roots$abrv_year)) := get(paste0('predicted_outflow_', dah.roots$abrv_year, 'model_d'))]
dt[YEAR == dah.roots$report_year, eval(paste0('OUTFLOW_final_', dah.roots$abrv_year)) := get(paste0('OUTFLOW_', dah.roots$abrv_year)) - fit]
dt[, ratio := get(paste0('OUTFLOW_', dah.roots$abrv_year)) / shift(get(paste0('trust_', dah.roots$abrv_year)), n=1, type='lag')]
dt[, ratio_dinal := get(paste0('OUTFLOW_final_', dah.roots$abrv_year)) / shift(get(paste0('trust_', dah.roots$abrv_year)), n=1, type='lag')]
dt[, pc := (get(paste0('OUTFLOW_', dah.roots$abrv_year)) - shift(get(paste0('OUTFLOW_', dah.roots$abrv_year)), n=1, type='lag')) /
     shift(get(paste0('OUTFLOW_', dah.roots$abrv_year)), n=1, type='lag')]
dt[, pc_final := (get(paste0('OUTFLOW_final_', dah.roots$abrv_year)) - shift(get(paste0('OUTFLOW_final_', dah.roots$abrv_year)), n=1, type='lag')) / 
     shift(get(paste0('OUTFLOW_final_', dah.roots$abrv_year)), n=1, type='lag')]
#-------------------------------------------------------#



cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(dt, paste0('BMGF_PREDS_DAH_1990_', dah.roots$report_year), 'BMGF', 'fin')
#------------------------# 
}
