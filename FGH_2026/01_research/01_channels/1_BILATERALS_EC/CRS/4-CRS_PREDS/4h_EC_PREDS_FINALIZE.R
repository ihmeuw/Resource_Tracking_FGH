#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Finalize EC predictions
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(readstata13, crayon)

last_obs_year <- get_dah_param('CRS', 'data_year')
dah_yy <- paste0("DAH_", dah_cfg$abrv_year)

N_HFAS <- length(dah.roots$regular_pa_vars) - 1 #-1 for unalloc
CORRECT_EBOLA <- FALSE
PREDICT_COVID <- FALSE
#----------------------------# ####

cat('\n\n')
cat(green(' #####################################\n'))
cat(green(' #### CRS FINALIZE EC PREDICTIONS ####\n'))
cat(green(' #####################################\n\n'))


cat('  Read in post-TT_smooth data\n')
#----# Read in post-TT_smooth data #----# ####
dt <- as.data.table(read.dta13(get_path('CRS', 'int', 'crs_ec_post_tt_smooth.dta')))

# ---- HELPERS ----
# redistribute ebola funds to other PAs for the given years
redist_ebola <- function(dt, n_hfas, dist_years = c(2018, 2019)) {
    # For each year in dist_years, set ebz (ebola dah) to last year's and
    # distribute the difference between the original ebz and this new value
    # across all the other HFA(PA)s that received any funding.
    min_dist_yr <- min(dist_years)
    all_hfas <- gsub("pr_", "", grep("pr_", names(dt), value = TRUE))
    non_ebz_hfas <- all_hfas[!(all_hfas %like% "ebz")]
    ebz <- setdiff(all_hfas, non_ebz_hfas) 
    if (length(all_hfas) != n_hfas)
        stop("There are missing HFA (PA) columns in the input dataset.")
    if (length(non_ebz_hfas) != n_hfas - 1)
        stop("There was an error retireving names of non-Ebola HFA (PA) columns.")
     
    # for years to be redistributed, set this year's ebz dah equal to last year
    setorder(dt, "YEAR")
    dt[, eval(paste0('new_', ebz)) := shift(get(ebz), n=1, type='lag')]
    dt[! YEAR %in% dist_years, eval(paste0('new_', ebz)) := NA]
    dt[YEAR %in% dist_years,
       eval(paste0('final_', ebz)) := get(paste0('new_', ebz))]
    
    # calculate the difference between the original ebz and the new ebz (ie, last yrs)
    dt[, ebz_diff := get(ebz) - get(paste0('new_', ebz))]
 
    # determine which HFAs are non-zero (ie, they received some DAH) - these are
    # the only HFAs which will receive the re-distributed ebz funds
    for (col in non_ebz_hfas) {
      dt[, eval(paste0("zero_", col)) := fifelse(get(col) == 0 | is.na(get(col)),
                                                 1, 0)]
    }
    dt[, zerohfas := rowSums(.SD, na.rm = TRUE),
       .SDcols = grep("zero_", names(dt), value = TRUE)]
    dt[, nonzerohfas := length(non_ebz_hfas) - zerohfas]

    # distribute the ebz difference equally to all of the HFAs which are non-zero
    for (col in non_ebz_hfas) {
      dt[YEAR %in% dist_years & get(paste0("zero_", col)) != 1,
         eval(paste0("final_", col)) := get(col) + (ebz_diff / nonzerohfas)]
    }
 
    # set dah to the newly computed value (old + re-distributed ebz)
    for (col in all_hfas) {
      # USA + NLD_INTLNGO = exceptions where ebola is predicted to decrease in
      # 2018 and the resulting redistribution would take funding away from other
      # PAs causing them to go negative
      dt[YEAR %in% dist_years,
         eval(col) := get(paste0('final_', col))]
    }
    
    rm_cols <- c(
        grep("^new_", names(dt), value = TRUE),
        grep("^zero_", names(dt), value = TRUE)
    )
    return(dt[, -rm_cols, with = FALSE])
}

# ---- Input predictions for prediction year ----
hfas_def_other <- gsub("pr_", "", names(dt)[names(dt) %like% 'pr_'])
for (col in hfas_def_other) {
  dt[YEAR > last_obs_year, eval(col) := get(paste0('pr_', col))]
}
#---------------------------------------# ####

if (CORRECT_EBOLA) {
    dt <- redist_ebola(dt, n_hfas = N_HFAS, dist_years = 2018:2019)
    
    # verify that hfas still sum to total outflow
    dt <- rowtotal(dt, 'checksums', hfas_def_other)
    dt[, DAHdiff := get(paste0('OUTFLOW_', dah.roots$abrv_year)) - checksums]
    dt[, diff_flag := fifelse(
        (YEAR >= 2018) & (abs(DAHdiff / 1e6) > 0.1) & (nonzerohfas > 0),
        1, 0)]
    if (nrow(dt[diff_flag == 1, ]) > 0) {
      cat(red('    FATAL ERROR: SUM OF HFA TAGS != 1!!\n'))
      stop("HFAs must sum to total DAH", call. = FALSE)
    }
    
    # update TT smooth preds with redistributed values
    cat('  Update cols with predicted values\n')
    for (col in hfas_def_other) {
      dt[YEAR %in% 2018:2019, eval(paste0('pr_', col)) := get(paste0('final_', col))]
      dt[is.na(get(col)), eval(col) := 0]
    }
}

pr_cols <- grep("pr_", names(dt), value = TRUE)
dt[, (pr_cols) := NULL]

# ensure HFAs sum to total DAH
hfa_cols <- grep(dah_yy, names(dt), value = TRUE)
dt[, hfa_total := rowSums(.SD), .SDcols = hfa_cols]
dt[, (hfa_cols) := lapply(.SD,
                          \(x) get(paste0("OUTFLOW_", dah_cfg$abrv_year)) * x / hfa_total),
   .SDcols = hfa_cols]

dt[, tmp := rowSums(.SD), .SDcols = hfa_cols]
if (dt[abs(tmp - get(paste0("OUTFLOW_", dah_cfg$abrv_year))) > 0.1, .N]) {
    stop("HFAs must sum to total DAH")
}
dt[, tmp := NULL]

dt <- dt[YEAR <= report_year]

#---------------------------------------# ####
if (PREDICT_COVID) {
    #
    # FGH2024: adjust covid funds - we expect countries to be allocating away from COVID
    #
    # load data from covid pipeline, aggregate EC covid spending by year
    covid <- fread(get_path("crs", "int", "covid_iati_transactions.csv"))
    covid <- covid[pub_iso == "EC",
                   .(spend_const = sum(trans_value_usd22, na.rm = TRUE)),
                   by = .(YEAR = trans_year)]
    
    ## compute percent change from previous year
    setorder(covid, YEAR)
    covid[order(YEAR), lag1 := shift(spend_const, n = 1, type = 'lag')]
    covid[, covid_pct_diff := (spend_const - lag1) / lag1]
    
    ## merge onto dt to compute new covid spending
    dt <- merge(dt, covid[, .(YEAR, covid_pct_diff)],
                by = c("YEAR"), all.x = TRUE)
    
    if (dt[is.na(covid_pct_diff) & YEAR > last_obs_year, .N]) {
        stop("Missing covid percent change for a prediction year. Find data or impute.")
    }
    ## adjust covid spending:
    ##  - compute "new" covid spending by applying pct_diff to previous observed yrs,
    ##    iteratively so that if there are multiple predictions years, the adjusted
    ##    value for one prediction year feeds into the next year
    ##  - compute the amount of original predicted covid spending that is left over
    ##     - if the data suggests a country's covid spending actually increased,
    ##       then we will not adjust the predicted value.
    ##  - divide evenly amongst the other sectors
    dt[, covid_new := oid_covid_DAH_24]
    for (yr in seq(last_obs_year + 1, dah_cfg$report_year)) {
        dt[, covid_lag := shift(covid_new, n = 1, type = 'lag')]
        dt[YEAR == yr, covid_new := covid_lag * (1 + covid_pct_diff)]
    }
    dt[, covid_remainder := oid_covid_DAH_24 - covid_new]
    dt[is.na(oid_covid_DAH_24), covid_remainder := 0]
    
    ## don't adjust covid spending for countries where the data suggests covid
    ##   has increased since 2022, or for observed years (pre-2022)
    dt[covid_remainder < 0 | YEAR < last_obs_year, `:=`(
        covid_new = oid_covid_DAH_24,
        covid_remainder = 0
    )]
    dt[, oid_covid_DAH_24 := NULL]
    
    ## each sector gets an equal share of the remainder
    hfa_cols <- grep(dah_yy, names(dt), value = TRUE)
    dt[, remainder_allocation := covid_remainder / length(hfa_cols)]
    dt[, (hfa_cols) := lapply(.SD, \(x) x + remainder_allocation), .SDcols = hfa_cols]
    
    dt[, oid_covid_DAH_24 := covid_new]
    
    dt[, tmp := rowSums(.SD, na.rm = TRUE),
       .SDcols = grep("pr_", names(dt), value = TRUE)]
    if (dt[abs(tmp - OUTFLOW_24) > 0.1, .N]) {
        stop("COVID reallocation - the adjusted HFAs don't sum to total DAH.")
    }
    dt[, tmp := NULL]
}



cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(dt, paste0('EC_PREDS_BY_HFA_1990_', dah.roots$report_year, '_', get_dah_param('CRS', 'update_MMYY')), 'BILAT_EC', 'fin')
#------------------------# ####