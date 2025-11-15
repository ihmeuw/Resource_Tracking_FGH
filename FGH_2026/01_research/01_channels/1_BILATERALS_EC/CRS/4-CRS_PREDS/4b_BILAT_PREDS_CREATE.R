#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Create CRS bilateral predictions
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(stringr, crayon, readstata13)

# Variable prep
last_obs_year <- get_dah_param('CRS', 'data_year')
data_lag <- dah_cfg$report_year - last_obs_year
defl <- get_path("meta", "defl")

AY <- dah_cfg$abrv_year
#----------------------------# ####


cat('\n\n')
cat(green(' ################################\n'))
cat(green(' #### CRS BILAT PREDS CREATE ####\n'))
cat(green(' ################################\n\n'))


cat('  Read in ODA_ALL data\n')
#----# Read in ODA_ALL data #----# ####
dt <- fread(get_path('BILAT_PREDICTIONS', 'fin', 'oda_all_[report_year].csv'))
setnames(dt, paste0('all_DAH_', AY), paste0('OUTFLOW_', AY))

# compute dah fraction as dah outflow out of ODA, or out of DAH in the rare (but
#   ideal) case that we have budgeted DAH
dt[, dah_frct := get(paste0('OUTFLOW_', AY)) / get(paste0('ODA_USD_', AY))]
dt[!is.na(get(paste0('DAH_USD_', AY))),
   dah_frct := get(paste0('OUTFLOW_', AY)) / get(paste0('DAH_USD_', AY))]
setorder(dt, ISO3, YEAR)


#--------------------------------# ####
cat('  Predict DAH\n')
#
# if we have reported DAH for missing years, use percent changes
#
dt[, budget := fifelse(
    is.na(get(paste0("DAH_USD_", AY))),
    get(paste0("ODA_USD_", AY)),
    get(paste0("DAH_USD_", AY))
)]
for (dlag in seq(1, data_lag)) {
    dt[order(YEAR), budget_lag1 := shift(budget, n=1, type='lag'), by = ISO3]
    dt[, dah_pctchg := (budget - budget_lag1) / budget_lag1,
       by = ISO3]
    dt[, outflow_lag1 := shift(get(paste0("OUTFLOW_", AY)), n=1, type='lag'),
       by = ISO3]
    dt[YEAR == last_obs_year + dlag,
       paste0("final_prediction_", AY) := outflow_lag1 * (1 + dah_pctchg)]
    
    # if data_lag > 1, then we need to repeat this process, using the final
    # prediction for the previous prediction year as an input for the next
    dt[YEAR == last_obs_year + dlag,
       paste0("OUTFLOW_", AY) := get(paste0("final_prediction_", AY))]
}

# reset the OUTFLOW_YY column if it was used cyclically
dt[YEAR > dah_cfg$report_year - data_lag, paste0("OUTFLOW_", AY) := NA]

# and set the pre-prediction years to be the observed outflow
dt[YEAR <= last_obs_year,
   paste0("final_prediction_", AY) := get(paste0("OUTFLOW_", AY))]



#----# Predict using 3-year weighted sum #----# ####
#
# If we don't have reported DAH, we use reported ODA, and use a 3 year weighted
#   average.
#
setorder(dt, ISO3, YEAR)
dt[YEAR > last_obs_year, dah_frct := as.numeric(NA)]

# compute a weighted average of the last 3 years to impute the given year
for (dlag in seq(1, data_lag)) {
    dt[, wgt_avg_frct :=
           ((1/2) * shift(dah_frct, n=1, type='lag')) +
           ((1/3) * shift(dah_frct, n=2, type='lag')) +
           ((1/6) * shift(dah_frct, n=3, type='lag')),
       by = ISO3]
    # if data_lag > 1, then we need to repeat this process, using the fraction
    # prediciton for the previous prediction year as an input for the next
    dt[YEAR == last_obs_year + dlag,
       dah_frct := wgt_avg_frct]
}

dt[is.na(get(paste0('DAH_USD_', AY))), 
   eval(paste0('wgt_avg_frct_', dah_cfg$report_year, '_out')) :=
       wgt_avg_frct *
       get(paste0('ODA_USD_', AY))]
dt[!is.na(get(paste0('DAH_USD_', AY))), 
   eval(paste0('wgt_avg_frct_', dah_cfg$report_year, '_out')) :=
       wgt_avg_frct *
       get(paste0('DAH_USD_', AY))]
#
# where we didn't have DAH budget % change, use weighted-avg ODA frac prediction
#
dt[is.na(get(paste0("final_prediction_", AY))),
   eval(paste0('final_prediction_', AY)) :=
       get(paste0('wgt_avg_frct_', dah_cfg$report_year, '_out'))]

#---------------------------------------------# ####

cat('  Predict EC member country DAH\n')
#----# Predict EC member country DAH #----# ####
setorder(dt, ISO3, YEAR)
if (data_lag == 2) {
  dt[, eval(paste0('temp_', dah_cfg$prev_report_year)) := (get(paste0('final_prediction_', AY)) - 
                                                              shift(get(paste0('OUTFLOW_', AY)), n=1, type='lag')) / 
                                                              shift(get(paste0('OUTFLOW_', AY)), n=1, type='lag')
     , by='ISO3']
  dt[YEAR != dah_cfg$prev_report_year, eval(paste0('temp_', dah_cfg$prev_report_year)) := as.numeric(NA)]
  dt[, eval(paste0('diff_', dah_cfg$prev_report_year)) := mean(get(paste0('temp_', dah_cfg$prev_report_year)), na.rm=T)
     , by='ISO3']
  dt[, eval(paste0('temp_', dah_cfg$report_year)) := (get(paste0('final_prediction_', AY)) - 
                                                          shift(get(paste0('final_prediction_', AY)), n=1, type='lag')) / 
                                                        shift(get(paste0('final_prediction_', AY)), n=1, type='lag')
     , by='ISO3']
  dt[YEAR != dah_cfg$report_year, eval(paste0('temp_', dah_cfg$report_year)) := as.numeric(NA)]
} else if (data_lag == 1) {
  dt[, eval(paste0('temp_', dah_cfg$report_year)) := (get(paste0('final_prediction_', AY)) - 
                                                          shift(get(paste0('final_prediction_', AY)), n=1, type='lag')) / 
                                                        shift(get(paste0('final_prediction_', AY)), n=1, type='lag')
     , by='ISO3']
}

dt[is.infinite(get(paste0('temp_', dah_cfg$report_year))),
   eval(paste0('temp_', dah_cfg$report_year)) := as.numeric(NA)]
dt[, eval(paste0('diff_', dah_cfg$report_year)) :=
       mean(get(paste0('temp_', dah_cfg$report_year)), na.rm=T),
   by='ISO3']
dt <- dt[, !c(names(dt)[names(dt) %like% 'temp_']), with=F]

# EC: mean percent difference in disbursements for EU countries
# Alternative for EC predictions, as budget numbers are not entirely believable
# mean_diff variables are used in EC Predictions .do file
if (data_lag == 2) {
  dt[, eval(paste0('mean_diff_', dah_cfg$prev_report_year)) :=
         mean(get(paste0('diff_', dah_cfg$prev_report_year)), na.rm=T)]
}
dt[, eval(paste0('mean_diff_', dah_cfg$report_year)) :=
       mean(get(paste0('diff_', dah_cfg$report_year)), na.rm=T)]
#-----------------------------------------# ####

cat('  Split out predicted source funding by channel\n')
#----# Split out predicted source funding by channel #----# ####
# Read in channel data
ngo <- fread(get_path("crs", "int", "adb_for_preds.csv"))
setnames(ngo, c('DAH', 'YEAR'), c('all_DAH', 'year'))

ngo <- ngo[, .(all_DAH = sum(all_DAH, na.rm = TRUE)),
           by = c('year', 'CHANNEL', 'ISO_CODE')]

ngo[CHANNEL == "INTERNATIONALNGOS", ngo := 1]
ngo[CHANNEL == "DONORCOUNTRYNGOS", ngo := 2]
ngo[CHANNEL == "EC"	, ngo := 3]
ngo[is.na(ngo), ngo := 0]

ngo <- ngo[, .(all_DAH = sum(all_DAH, na.rm = TRUE)),
           by = c('year', 'ngo', 'ISO_CODE')]

ngo <- ngo[year %in% 1990:last_obs_year, ]
setnames(ngo, 'ISO_CODE', 'isocode')
ngo[, DAH_source := sum(all_DAH, na.rm=T), by=c('isocode', 'year')]
ngo[, frct := all_DAH / DAH_source]

ngo <- collapse(ngo, 'sum', c('year', 'isocode', 'ngo'), c('frct', 'all_DAH', 'DAH_source'))
ngo <- ngo[, .(frct = sum(frct, na.rm = TRUE),
               all_DAH = sum(all_DAH, na.rm = TRUE),
               DAH_source = sum(DAH_source, na.rm = TRUE)),
           by = .(year, isocode, ngo)]
setnames(ngo, c('year', 'isocode'), c('YEAR', 'ISO3'))

ngo <- dcast.data.table(ngo,
                        formula = 'ISO3 + ngo ~ YEAR',
                        value.var = c('frct', 'all_DAH', 'DAH_source'))
ngo[, eval(paste0('frct_', dah_cfg$report_year)) := as.numeric(NA)]
if (data_lag == 2) {
  ngo[, eval(paste0('frct_', dah_cfg$prev_report_year)) := as.numeric(NA)]
}

ngo <- melt.data.table(ngo,
                       measure.vars = names(ngo)[names(ngo) %like% 'frct_' | names(ngo) %like% 'all_DAH_' | names(ngo) %like% 'DAH_source_'])
ngo$YEAR <- str_replace_all(ngo$variable, 'frct_', '') |>
    str_replace_all('all_DAH_', '') |>
    str_replace_all('DAH_source_', '') |>
    as.numeric()
ngo$variable <- as.character(ngo$variable)
ngo$variable <- substr(ngo$variable, 1, nchar(ngo$variable) - 5)
ngo <- dcast(ngo, formula = 'ISO3 + ngo + YEAR ~ variable', value.var = 'value')
setnames(ngo, 'all_DAH', 'adb_DAH')

# ---
# Merge 
dt <- merge(dt, ngo, by=c('YEAR', 'ISO3'), all.y=T)
rm(ngo)
dt[is.na(frct), frct := 0]
dt <- dt[order(ISO3, ngo, YEAR), ]
if (data_lag == 2) {
  dt[, frct2 := ((1/2) * shift(frct, n=1, type='lag')) + ((1/3) * shift(frct, n=2, type='lag')) + ((1/6) * shift(frct, n=3, type='lag')), by=c('ISO3', 'ngo')]
  dt[YEAR == dah_cfg$prev_report_year, frct := frct2]
  dt[, frct2 := NULL]
}

dt[, frct2 := ((1/2) * shift(frct, n=1, type='lag')) + ((1/3) * shift(frct, n=2, type='lag')) + ((1/6) * shift(frct, n=3, type='lag')), by=c('ISO3', 'ngo')]
dt[YEAR == dah_cfg$report_year, frct := frct2]
dt[, frct2 := NULL]

# ---
# Pull HFA totals
hfa <- fread(get_path("crs", "int", "adb_for_preds.csv"))

hfa <- hfa[, lapply(.SD, sum, na.rm = TRUE),
           by = c('YEAR', 'CHANNEL', 'ISO_CODE'),
           .SDcols = grep("_DAH", names(hfa), value = TRUE)]

hfa[CHANNEL == "INTERNATIONALNGOS", ngo := 1]
hfa[CHANNEL == "DONORCOUNTRYNGOS", ngo := 2]
hfa[CHANNEL == "EC"	, ngo := 3]
hfa[is.na(ngo), ngo := 0]

hfa <- hfa[, lapply(.SD, sum, na.rm = TRUE),
           by = c('YEAR', 'ngo', 'ISO_CODE'),
           .SDcols = grep("_DAH", names(hfa), value = TRUE)]

hfa <- hfa[YEAR >= 1990, ]
setnames(hfa, 'ISO_CODE', 'ISO3')

rates <- fread(get_path("meta", "defl", "imf_usgdp_deflators_[defl_mmyy].csv"))
rates <- rates[, c('YEAR', paste0('GDP_deflator_', dah_cfg$report_year)), with=FALSE]
hfa <- merge(hfa, rates, by='YEAR', all.x=T)
for (col in names(hfa)[names(hfa) %like% '_DAH']) {
  hfa[, eval(paste0(col, '_', AY)) := get(col) / get(paste0('GDP_deflator_', dah_cfg$report_year))]
}
hfa[, eval(paste0('GDP_deflator_', dah_cfg$report_year)) := NULL]
rm(rates)

# ---
# Merge
dt <- dt[, !c(names(dt)[names(dt) %like% '_DAH' & !(names(dt) %like% 'all_DAH' | names(dt) %like% 'adb_DAH')]),
         with=FALSE]
dt <- merge(dt, hfa, by=c('YEAR', 'ISO3', 'ngo'), all.x=T)
rm(hfa)


for (col in c(paste0('final_prediction_', AY), paste0('OUTFLOW_', AY))) {
   dt[, eval(col) := get(col) * frct]
}
#---------------------------------------------------------# ####
if (interactive()) {
    ex <- copy(dt)
    ex[, method := "DAH Fraction Weighted Average"]
    ex[ISO3 %in% ex[YEAR == dah_cfg$report_year & !is.na(dah_pctchg), unique(ISO3)],
       method := "DAH Fraction Percent Change"]
    ex[, envelope := get(paste0("OUTFLOW_", AY))]
    ex[is.na(envelope) | envelope == 0,
       envelope := get(paste0("final_prediction_", AY))]
    ex[, .(envelope = sum(envelope, na.rm = TRUE)),
       by = .(ISO3, YEAR, method)] |>
        ggplot(aes(x = YEAR, y = envelope, color = method)) +
        geom_line() +
        facet_wrap(~ISO3, scales = "free_y")
    ex[, .(envelope = sum(envelope, na.rm = TRUE)),
       by = .(YEAR)] |>
        ggplot(aes(x = YEAR, y = envelope)) +
        geom_line()
    rm(ex)
}

cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(dt,
             'Bilateral_predictions_[report_year]_[crs.update_mmyy]',
                    'BILAT_PREDICTIONS', 'fin')
#------------------------ ####