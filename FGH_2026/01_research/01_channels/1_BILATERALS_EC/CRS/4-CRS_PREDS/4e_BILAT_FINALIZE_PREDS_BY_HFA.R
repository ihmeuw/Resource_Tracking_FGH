#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Finalize CRS predictions by HFA
#---------------------# ####
# NOTES:
#   Ebola correction was causing issues so it was refactored and some errors were
#   identified. Upon further investigation the team decided that the correction
#   was no longer necessary, so it can be ignored by keeping CORRECT_EBOLA as FALSE
#
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

CORRECT_EBOLA <- FALSE # run ebola correction? see below for details
PREDICT_COVID <- FALSE # run covid prediction step? see below
N_HFAS <- length(dah.roots$regular_pa_vars)
#----------------------------# ####


cat('\n\n')
cat(green(' ###################################\n'))
cat(green(' #### CRS FINALIZE PREDS BY HFA ####\n'))
cat(green(' ###################################\n\n'))


cat('  Read in post-TT_Smooth data\n')
#----# Read in post-TT_Smooth data #----# ####
dt <- data.table()
all <- list.files(paste0(get_path('CRS', 'int'), 'tt_smooth_datasets/'))
pre <- all[all %like% 'pre']
post <- all[all %like% 'post']

# If not all files output properly
if (length(pre) != length(post)) {
  cat(red('    FATAL ERROR!! NOT ALL JOBS FINISHED. CHECK OUTPUT LOGS & RERUN FAILED JOBS.'))
  stop()
} else {
  cat(yellow(paste0('    ', length(post), ' datasets to append\n      ')))
  for (file in post) {
    cat(paste0(match(file, post), ','))
    t <- setDT(read.dta13(paste0(get_path('CRS', 'int'), 'tt_smooth_datasets/', file)))
    dt <- rbind(dt, t)
    rm(t)
  }
  cat('\n')
}
rm(all, pre, post, file)

# ---- HELPERS ----

# redistribute smoothed ebola funds to other smoothed PAs for the given years
# - the "ebola correction" was instituted in 2020 since the ebola outbreak in
#   2018 caused a spike in funding in 2019 that caused TT Smooth to predict a
#   large quantity for 2020. This hasn't been a problem with the addition of
#   more recent years of data so it can be skipped, unless it becomes an issue
#   again
redist_ebola <- function(dt, n_hfas, dist_years = c(2018, 2019)) {
    # For each iso-year in dist_years, set ebz (ebola dah) to last year's and
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
    dt[, eval(paste0('new_', ebz)) := shift(get(ebz), n=1, type='lag'),
       by = ISO3]
    dt[! YEAR %in% dist_years, eval(paste0('new_', ebz)) := NA]
    dt[YEAR %in% dist_years,
       eval(paste0('redist_', ebz)) := get(paste0('new_', ebz))]
    
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
         eval(paste0("redist_", col)) := get(col) + (ebz_diff / nonzerohfas)]
    }
    
    # set dah to the newly computed value (old + re-distributed ebz)
    for (col in all_hfas) {
      # USA + NLD_INTLNGO = exceptions where ebola is predicted to decrease in
      # 2018 and the resulting redistribution would take funding away from other
      # PAs causing them to go negative
      dt[YEAR %in% dist_years & (! ISO3 %in% c("USA", "NLD_INTLNGO")),
         eval(col) := get(paste0('redist_', col))]
    }
    
    rm_cols <- c(
        grep("^new_", names(dt), value = TRUE),
        grep("^zero_", names(dt), value = TRUE)
    )
    return(dt[, -rm_cols, with = FALSE])
}


#---------------------------------------# ####
cat('  Calculate + reformat cols\n')
#----# Calculate + reformat cols #----# ####

# Update cols with predictions
to_calc <- gsub("pr_", "", names(dt)[names(dt) %like% 'pr_'])
for (col in to_calc) {
  dt[YEAR > last_obs_year, eval(col) := get(paste0('pr_', col))]
  dt[, eval(col) := get(col) / 1e6]
}

if (CORRECT_EBOLA) {
    dt <- redist_ebola(dt, n_hfas = N_HFAS, dist_years = 2018:2019)
    
    # Check that, post-re_distribution the sum of PAs == total DAH
    dt <- rowtotal(dt, 'checksums', to_calc)
    dt[, DAHdiff := (get(paste0('DAH_', dah.roots$abrv_year)) / 1e6) - checksums]
    dt[, diff_flag := fifelse(
        (YEAR >= 2018) & (abs(DAHdiff) > 0.1) & (nonzerohfas > 0),
        1, 0)]
    if (nrow(dt[diff_flag == 1, ]) > 0) {
      cat(red('    FATAL ERROR: SUM OF HFA TAGS != 1!!\n'))
      stop("HFAs must sum to total DAH", call. = FALSE)
    }
    
    for (col in to_calc) {
      dt[YEAR %in% 2018:2019,
         eval(paste0('pr_', col)) := get(paste0('redist_', col)) * 10^6]
    }
}


dt[ISO3 %like% '_INTLNGO', CHANNEL := 'INTERNATIONALNGOS']
dt[ISO3 %like% '_USANGO', CHANNEL := 'DONORCOUNTRYNGOS']
dt[!(ISO3 %like% '_INTLNGO') & !(ISO3 %like% '_USANGO'), CHANNEL := paste0('BIL_', ISO3)]

dt[, ISO3 := gsub('_INTLNGO', '', ISO3)]
dt[, ISO3 := gsub('_USANGO', '', ISO3)]

dt[, INCOME_SECTOR := 'PUBLIC']
setnames(dt, 'ISO3', 'ISO_CODE')

dt <- dt[, c('YEAR', 'ISO_CODE', 'INCOME_SECTOR', 'CHANNEL',
             paste0('DAH_', dah.roots$abrv_year),
             names(dt)[names(dt) %like% 'pr_']),
         with=FALSE]


# predictions need to be re-normalized since they don't sum exactly to total DAH
hfa_cols <- grep("pr_", names(dt), value = TRUE)
## compute the current sum of the HFAs, 
dt[, hfa_total := rowSums(.SD, na.rm = TRUE), .SDcols = hfa_cols]

## if there are no HFA predictions but there is total DAH, then we allocate to
##    unallocable
dt[hfa_total == 0 & get(dah_yy) > 0, unalloc := 1]
dt[unalloc == 1, paste0("pr_unalloc_DAH_", dah_cfg$abrv_year) := get(dah_yy)]
dt[unalloc == 1, hfa_total := get(dah_yy)]

## use the per-hfa fraction and apply to total predicted DAH
##  (we want to maintain the total envelope and the fractions)
dt[, (hfa_cols) := lapply(.SD, \(x) get(dah_yy) * x / hfa_total),
   .SDcols = hfa_cols]

dt[, tmp := rowSums(.SD, na.rm = TRUE),
   .SDcols = grep("pr_", names(dt), value = TRUE)]
if (dt[abs(tmp - get(dah_yy)) > 0.1, .N]) {
    stop("The predicted HFAs do not sum to total DAH!")
}
dt[, c("hfa_total", "unalloc", "tmp") := NULL]

#---------------------------------------# ####

if (PREDICT_COVID) {
    #
    # FGH2024: adjust covid funds - we expect countries to be allocating away from COVID
    # load data from covid pipeline, aggregate covid spending by country-year
    covid <- fread(get_path("crs", "int", "covid_iati_transactions.csv"))
    covid <- covid[,
                   .(spend_const = sum(trans_value_usd22, na.rm = TRUE)),
                   by = .(YEAR = trans_year, ISO_CODE = pub_iso)]
    covid <- covid[ISO_CODE != "EC"]
    
    ## compute percent change from previous year
    setorder(covid, ISO_CODE, YEAR)
    covid[order(YEAR),
          lag1 := shift(spend_const, n = 1, type = 'lag'),
         by = ISO_CODE]
    covid[, pct_diff := (spend_const - lag1) / lag1]
    
    ## merge onto dt to compute new covid spending
    covid <- covid[, .(YEAR, ISO_CODE, covid_pct_diff = pct_diff)]
    dt <- merge(dt, covid,
                by = c("YEAR", "ISO_CODE"), all.x = TRUE)
    
    ## impute with annual average across donors for countries with missing covid data
    dt[, mean_pct_diff := mean(covid_pct_diff, na.rm = TRUE), by = YEAR]
    dt[is.na(covid_pct_diff), covid_pct_diff := mean_pct_diff]
    
    ## adjust covid spending:
    ##  - compute "new" covid spending by applying pct_diff to previous observed yrs,
    ##    iteratively so that if there are multiple predictions years, the adjusted
    ##    value for one prediction year feeds into the next year
    ##  - compute the amount of original predicted covid spending that is left over
    ##     - if the data suggests a country's covid spending actually increased,
    ##       then we will not adjust the predicted value.
    ##  - divide evenly amongst the other sectors
    setorder(dt, ISO_CODE, CHANNEL, YEAR)
    dt[, covid_new := pr_oid_covid_DAH_24]
    for (yr in seq(last_obs_year + 1, dah_cfg$report_year)) {
        dt[, covid_lag := shift(covid_new, n = 1, type = 'lag'),
           by = .(ISO_CODE, CHANNEL)]
        dt[YEAR == yr, covid_new := covid_lag * (1 + covid_pct_diff)]
    }
    dt[, covid_remainder := pr_oid_covid_DAH_24 - covid_new]
    dt[is.na(pr_oid_covid_DAH_24), covid_remainder := 0]
    
    ## don't adjust covid spending for countries where the data suggests covid
    ##   has increased since 2022, or for observed years (pre-2022)
    dt[covid_remainder < 0 | YEAR < last_obs_year, `:=`(
        covid_new = pr_oid_covid_DAH_24,
        covid_remainder = 0
    )]
    dt[, pr_oid_covid_DAH_24 := NULL]
    
    ## each sector gets an equal share of the remainder
    hfa_cols <- grep("pr_", names(dt), value = TRUE)
    dt[, remainder_allocation := covid_remainder / length(hfa_cols)]
    dt[, (hfa_cols) := lapply(.SD, \(x) x + remainder_allocation), .SDcols = hfa_cols]
    
    dt[, pr_oid_covid_DAH_24 := covid_new]
    
    dt[, tmp := rowSums(.SD, na.rm = TRUE),
       .SDcols = grep("pr_", names(dt), value = TRUE)]
    if (dt[abs(tmp - DAH_24) > 0.1, .N]) {
        stop("COVID reallocation - the adjusted HFAs don't sum to total DAH.")
    }
    
    cov_col <- grep("pr_oid_covid_DAH_", names(dt), value = TRUE)
    dt[YEAR < 2020, (cov_col) := 0]
}
#-------------------------------------# ####

cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(dt,
             'Bilateral_predictions_[report_year]_BY_HFA_[crs.update_mmyy]',
             'BILAT_PREDICTIONS', 'fin')
#------------------------# ####
