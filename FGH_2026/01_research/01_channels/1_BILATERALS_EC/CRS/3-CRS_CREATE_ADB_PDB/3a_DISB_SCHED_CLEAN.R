#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Clean CRS disbursement schedules
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))
start.time <- Sys.time()
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon)
#----------------------------# ####


cat('\n\n')
cat(green(' #################################\n'))
cat(green(' #### CRS CLEAN DISB SCHEDULE ####\n'))
cat(green(' #################################\n\n'))


cat('  Read in BIL_ODA_2 dataset\n')
#----# Read in BIL_ODA_2 dataset #----# ####
## INCLUDE COVID PROJECTS FOR CALCULATING DISBURSEMENT RATIOS
dt <- fread(get_path('CRS', 'int',
                     'B_CRS_[crs.update_mmyy]_HEALTH_BIL_ODA_2_COVID.csv'))
#-------------------------------------# ####

cat('  Calculate total disbursements and implementation rates\n')
#----# Calculate total disbursements and implementation rates #----# ####
dt <- dt[order(donor_agency, recipient_code, crs_id, year), ]
dt[, dummy := 1]
dt[, proj_N := sum(dummy), by=c('donor_agency', 'recipient_code', 'crs_id')]
dt[, proj_n := 1:sum(dummy), by=c('donor_agency', 'recipient_code', 'crs_id')]
dt[, max_proj_yr := max(year, na.rm=T), by=c('donor_agency', 'recipient_code', 'crs_id')]
dt[, min_proj_yr := min(year, na.rm=T), by=c('donor_agency', 'recipient_code', 'crs_id')]
dt[, total_usd_com := sum(all_commcurr, na.rm=T), by=c('donor_agency', 'recipient_code', 'crs_id')]
dt[, total_usd_disb := sum(all_disbcurr, na.rm=T), by=c('donor_agency', 'recipient_code', 'crs_id')]
dt[all_disbcurr != 0 & !is.na(all_disbcurr), donor_min_disbyr := min(year, na.rm=T), by='donor_agency']
dt[, donor_min_dyear := mean(donor_min_disbyr, na.rm=T) - 1, by=c('donor_agency', 'recipient_code', 'crs_id')]
dt[, dummy := NULL]

# Limit sample to include only projects that provide valid insight into disbursement schedules
dt <- dt[eliminations == 0 & max_proj_yr < get_dah_param('CRS', 'data_year') & min_proj_yr >= donor_min_dyear, ]

# Overall implementation rates per donor
# Method 1: mean across each donor's projects
dt[total_usd_disb>0 & total_usd_com>0, imp_rate_1 := total_usd_disb / total_usd_com]
dt[total_usd_disb>0 & total_usd_com>0, donor_imp_rate_1 := mean(imp_rate_1, na.rm=T), by='donor_code']

# Method 2: median across each donor's projects
dt[, imp_rate_2 := total_usd_disb / total_usd_com]
dt[is.infinite(imp_rate_2), imp_rate_2 := as.numeric(NA)]
dt[, donor_imp_rate_2 := median(imp_rate_2, na.rm=T), by='donor_code']

# Method 3: ratio of all donor's disbursements as a share of all donor's commitments
dt[total_usd_disb>0 & total_usd_com>0, donor_total_com_if_3 := sum(total_usd_com, na.rm=T), by='donor_code']
dt[total_usd_disb>0 & total_usd_com>0, donor_total_disb_if_3 := sum(total_usd_disb, na.rm=T), by='donor_code']
dt[total_usd_disb>0 & total_usd_com>0, donor_imp_rate_3 := donor_total_disb_if_3/donor_total_com_if_3]

# Compare 3 methods
dt[, donor_imp_rate_1 := donor_imp_rate_1[!is.na(donor_imp_rate_1)[1L]], by='donor_code']
dt[, donor_imp_rate_2 := donor_imp_rate_2[!is.na(donor_imp_rate_2)[1L]], by='donor_code']
dt[, donor_imp_rate_3 := donor_imp_rate_3[!is.na(donor_imp_rate_3)[1L]], by='donor_code']

# Choose to use method 2 because leaves room for reporting error (because uses median) and fits with previous estimates
dt[, donor_imp_rate := donor_imp_rate_2]
#------------------------------------------------------------------# ####

cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(dt, 'B_CRS_[crs.update_mmyy]_HEATLH_BIL_ODA_3', 'CRS', 'int')
#------------------------# ####

cat('  Calculate disbursement schedules\n')
#----# Calculate disbursement schedules #----# ####
dt <- fread(get_path('CRS', 'int', 'B_CRS_[crs.update_mmyy]_HEATLH_BIL_ODA_3.csv'))
# 1 through 6 year disbursement schedules are calculated. Projects longer than six years have all disbursement 
# after six years added to the sixth year.)
lst <- list()
cat('    Establishing disbursement years (X:6): ')
for (i in 1:6) {
  cat(paste0(i, ', '))
  # Copy, subset + collapse
  t <- copy(dt[,c('all_disbcurr', 'donor_agency', 'recipient_code', 'crs_id', 'proj_n')])
  t[proj_n > i, proj_n := i]
  t <- collapse(t, 'sum', c('donor_agency', 'recipient_code', 'crs_id', 'proj_n'), 'all_disbcurr')
  setnames(t, 'all_disbcurr', paste0('all_disbcurr_', i, '_yr'))

  # Reshape wide, then long
  t <- dcast(t, formula = 'donor_agency + recipient_code + crs_id ~ proj_n', value.var = paste0('all_disbcurr_', i, '_yr'))
  setnames(t, names(t)[names(t) %ni% c('donor_agency', 'recipient_code', 'crs_id')],
           paste0('all_disbcurr_', i, '_yr', names(t)[names(t) %ni% c('donor_agency', 'recipient_code', 'crs_id')]))
  t <- melt(t, measure.vars = names(t)[names(t) %like% 'all_disbcurr'])
  t$variable <- as.numeric(gsub(paste0("all_disbcurr_", i, "_yr"), "", as.character(t$variable)))
  setnames(t, c('variable', 'value'), c('proj_n', paste0('all_disbcurr_', i, '_yr')))

  # Fill NAs
  t[is.na(get(paste0('all_disbcurr_', i, '_yr'))), eval(paste0('all_disbcurr_', i, '_yr')) := 0]
  # Add to appending list
  lst[[paste0('disb_', i)]] <- t
  rm(t)
}
cat('\n')

# Merge all the list datasets
cat('    Merging disbursement year datasets\n')
for (i in 1:6) {
  dt <- merge(dt, lst[[paste0('disb_', i)]], by=c('donor_agency', 'recipient_code', 'crs_id', 'proj_n'), all=T)
}
rm(lst)
# Fill NAs by donor_agency, recipient_code, + crs_id
dt[, donor_name := donor_name[!is.na(donor_name)[1L]], by=c('donor_agency', 'recipient_code', 'crs_id')]
dt[, donor_code := donor_code[!is.na(donor_code)[1L]], by=c('donor_agency', 'recipient_code', 'crs_id')]
dt[, isocode := isocode[!is.na(isocode)[1L]], by=c('donor_agency', 'recipient_code', 'crs_id')]
dt[, donor_imp_rate := donor_imp_rate[!is.na(donor_imp_rate)[1L]], by=c('donor_agency', 'recipient_code', 'crs_id')]
dt[, donor_imp_rate_1 := donor_imp_rate[!is.na(donor_imp_rate)[1L]], by=c('donor_agency', 'recipient_code', 'crs_id')]
dt[, donor_imp_rate_2 := donor_imp_rate_2[!is.na(donor_imp_rate_2)[1L]], by=c('donor_agency', 'recipient_code', 'crs_id')]
dt[, donor_imp_rate_3 := donor_imp_rate_3[!is.na(donor_imp_rate_3)[1L]], by=c('donor_agency', 'recipient_code', 'crs_id')]

lst <- list()
cat("    Calculating ratio of disb to com't (X/6): ")
for (i in 1:6) {
  cat(paste0(i, ', '))
  t <- copy(dt)
  t[!is.na(get(paste0('all_disbcurr_', i, '_yr'))), 
    eval(paste0('total_usd_disb_', i, '_yr')) := sum(get(paste0('all_disbcurr_', i, '_yr')), na.rm=T), 
    by=c('donor_agency', 'recipient_code', 'crs_id')]
  
  # Method 3: ratio of the sum of the disbursements and sum of the commitments aggregated by donor and project year
  t[!is.na(get(paste0('all_disbcurr_', i, '_yr'))), 
    eval(paste0('donor_total_dis_if_3_', i)) := sum(get(paste0('all_disbcurr_', i, '_yr')), na.rm=T), 
    by=c('donor_code', 'proj_n')]
  t[!is.na(get(paste0('all_disbcurr_', i, '_yr'))), 
    eval(paste0('donor_total_disb_iff_3_', i)) := sum(get(paste0('all_disbcurr_', i, '_yr')), na.rm=T),
    by='donor_code']
  t[!is.na(get(paste0('all_disbcurr_', i, '_yr'))), 
    eval(paste0('donor_dis_rate_3_', i)) := get(paste0('donor_total_dis_if_3_', i)) / get(paste0('donor_total_disb_iff_3_', i))]
  
  t <- t[proj_n <= i, c('isocode', 'proj_n', 'donor_imp_rate', paste0('donor_dis_rate_3_', i)), with=F]
  t <- unique(t)
  
  setnames(t, c('donor_imp_rate', paste0('donor_dis_rate_3_', i)),
           c(paste0('MED_IMPRATE_', i, 'YR_OV'), paste0('MED_IMPRATE_', i, 'YR_YR_')))
  
  t <- dcast(t, formula='... ~ proj_n', value.var = paste0('MED_IMPRATE_', i, 'YR_YR_'))
  
  setnames(t, names(t)[names(t) != 'isocode' & names(t) != paste0('MED_IMPRATE_', i, 'YR_OV')],
           paste0('MED_IMPRATE_', i, 'YR_YR_', names(t)[names(t) != 'isocode' & names(t) != paste0('MED_IMPRATE_', i, 'YR_OV')]))
  
  lst[[paste0('med_', i)]] <- t
  rm(t)
}
cat('\n')

data <- lst[['med_1']]
cat('    Merging ratioed datasets\n')
for (i in 2:6) {
  data <- merge(data, lst[[paste0('med_', i)]], by='isocode', all=T)
}


cat('    Updating USA ratios (X/6): ')
for (i in 1:6) {
  cat(paste0(i, ', '))
  
  data[isocode == "USA", eval(paste0('MED_IMPRATE_', i, 'YR_YR_1')) := 0.6]
  # Replace some cols if they exist
  if (paste0('MED_IMPRATE_', i, 'YR_YR_2') %in% names(data)) {
    data[isocode == "USA", eval(paste0('MED_IMPRATE_', i, 'YR_YR_2')) := 0.25*(1- get(paste0('MED_IMPRATE_', i, 'YR_YR_1')))]
  }
  if (paste0('MED_IMPRATE_', i, 'YR_YR_3') %in% names(data)) {
    data[isocode == "USA", eval(paste0('MED_IMPRATE_', i, 'YR_YR_3')) :=  0.25*(1- (get(paste0('MED_IMPRATE_', i, 'YR_YR_1')) + get(paste0('MED_IMPRATE_', i, 'YR_YR_2'))))]
  }
  if (paste0('MED_IMPRATE_', i, 'YR_YR_4') %in% names(data)) {
    data[isocode == "USA", eval(paste0('MED_IMPRATE_', i, 'YR_YR_4')) := 0.25*(1- (get(paste0('MED_IMPRATE_', i, 'YR_YR_1')) + 
                                                                                     get(paste0('MED_IMPRATE_', i, 'YR_YR_2')) + 
                                                                                     get(paste0('MED_IMPRATE_', i, 'YR_YR_3'))))]
  }
  if (paste0('MED_IMPRATE_', i, 'YR_YR_5') %in% names(data)) {
    data[isocode == "USA", eval(paste0('MED_IMPRATE_', i, 'YR_YR_5')) := 0.25*(1- (get(paste0('MED_IMPRATE_', i, 'YR_YR_1')) + 
                                                                                     get(paste0('MED_IMPRATE_', i, 'YR_YR_2')) + 
                                                                                     get(paste0('MED_IMPRATE_', i, 'YR_YR_3')) +
                                                                                     get(paste0('MED_IMPRATE_', i, 'YR_YR_4'))))]
  }
  if (paste0('MED_IMPRATE_', i, 'YR_YR_', i) %in% names(data)) {
    if (length(names(data)[names(data) %like% paste0('MED_IMPRATE_', i)]) == 2) { # i == 1
      data[isocode == "USA", eval(paste0('MED_IMPRATE_', i, 'YR_YR_', i)) := 0.25*(1- (get(paste0('MED_IMPRATE_', i, 'YR_YR_1'))))]
    } 
    else if (length(names(data)[names(data) %like% paste0('MED_IMPRATE_', i)]) == 3) { # i == 2
      data[isocode == "USA", eval(paste0('MED_IMPRATE_', i, 'YR_YR_', i)) := 0.25*(1- (get(paste0('MED_IMPRATE_', i, 'YR_YR_1')) + 
                                                                                         get(paste0('MED_IMPRATE_', i, 'YR_YR_2'))))]
    } 
    else if (length(names(data)[names(data) %like% paste0('MED_IMPRATE_', i)]) == 4) { # i == 3
      data[isocode == "USA", eval(paste0('MED_IMPRATE_', i, 'YR_YR_', i)) := 0.25*(1- (get(paste0('MED_IMPRATE_', i, 'YR_YR_1')) + 
                                                                                         get(paste0('MED_IMPRATE_', i, 'YR_YR_2')) + 
                                                                                         get(paste0('MED_IMPRATE_', i, 'YR_YR_3'))))]
    } 
    else if (length(names(data)[names(data) %like% paste0('MED_IMPRATE_', i)]) == 5) { # i = 4
      data[isocode == "USA", eval(paste0('MED_IMPRATE_', i, 'YR_YR_', i)) := 0.25*(1- (get(paste0('MED_IMPRATE_', i, 'YR_YR_1')) + 
                                                                                         get(paste0('MED_IMPRATE_', i, 'YR_YR_2')) + 
                                                                                         get(paste0('MED_IMPRATE_', i, 'YR_YR_3')) +
                                                                                         get(paste0('MED_IMPRATE_', i, 'YR_YR_4'))))]
    } 
    else if (length(names(data)[names(data) %like% paste0('MED_IMPRATE_', i)]) == 6) { # i = 5
      data[isocode == "USA", eval(paste0('MED_IMPRATE_', i, 'YR_YR_', i)) := 0.25*(1- (get(paste0('MED_IMPRATE_', i, 'YR_YR_1')) + 
                                                                                         get(paste0('MED_IMPRATE_', i, 'YR_YR_2')) + 
                                                                                         get(paste0('MED_IMPRATE_', i, 'YR_YR_3')) +
                                                                                         get(paste0('MED_IMPRATE_', i, 'YR_YR_4')) + 
                                                                                         get(paste0('MED_IMPRATE_', i, 'YR_YR_5'))))]
    }
    else if (length(names(data)[names(data) %like% paste0('MED_IMPRATE_', i)]) == 7) { # i = 6
      data[isocode == "USA", eval(paste0('MED_IMPRATE_', i, 'YR_YR_', i)) := 0.25*(1- (get(paste0('MED_IMPRATE_', i, 'YR_YR_1')) + 
                                                                                         get(paste0('MED_IMPRATE_', i, 'YR_YR_2')) + 
                                                                                         get(paste0('MED_IMPRATE_', i, 'YR_YR_3')) +
                                                                                         get(paste0('MED_IMPRATE_', i, 'YR_YR_4')) + 
                                                                                         get(paste0('MED_IMPRATE_', i, 'YR_YR_5'))))]
    }
  }
  if (paste0('MED_IMPRATE_', i, 'YR_OV') %in% names(data)) {
    data[isocode == "USA", eval(paste0('MED_IMPRATE_', i, 'YR_OV')) := 1]
  }
}
cat('\n')

# Rescale disbursement schedules using implementation rates (Disbursement rates original assumed all DAH was disbursed.  
# Implementation rate shows that for some countries disbursement does not match commitments.)
data <- rowtotal(data, 'TOT_MED_IMPRATE_1YR', c('MED_IMPRATE_1YR_YR_1'))
data <- rowtotal(data, 'TOT_MED_IMPRATE_2YR', c('MED_IMPRATE_2YR_YR_1', 'MED_IMPRATE_2YR_YR_2'))
data <- rowtotal(data, 'TOT_MED_IMPRATE_3YR', c('MED_IMPRATE_3YR_YR_1', 'MED_IMPRATE_3YR_YR_2', 'MED_IMPRATE_3YR_YR_3'))
data <- rowtotal(data, 'TOT_MED_IMPRATE_4YR', c('MED_IMPRATE_4YR_YR_1', 'MED_IMPRATE_4YR_YR_2', 'MED_IMPRATE_4YR_YR_3', 'MED_IMPRATE_4YR_YR_4'))
data <- rowtotal(data, 'TOT_MED_IMPRATE_5YR', c('MED_IMPRATE_5YR_YR_1', 'MED_IMPRATE_5YR_YR_2', 'MED_IMPRATE_5YR_YR_3', 'MED_IMPRATE_5YR_YR_4', 
                                              'MED_IMPRATE_5YR_YR_5' ))
data <- rowtotal(data, 'TOT_MED_IMPRATE_6YR', c('MED_IMPRATE_6YR_YR_1', 'MED_IMPRATE_6YR_YR_2', 'MED_IMPRATE_6YR_YR_3', 'MED_IMPRATE_6YR_YR_4', 
                                              'MED_IMPRATE_6YR_YR_5', 'MED_IMPRATE_6YR_YR_6'))

for (i in 1:6) {
  data[, eval(paste0('rescale', i)) := get(paste0('MED_IMPRATE_', i, 'YR_OV')) / get(paste0('TOT_MED_IMPRATE_', i, 'YR'))]
}

for (i in 1:6) {
  data[, eval(paste0('MED_IMPRATE_', i, 'YR_YR_1')) := get(paste0('MED_IMPRATE_', i, 'YR_YR_1')) * get(paste0('rescale', i))]
  # Replace some cols only if they exist
  if (paste0('MED_IMPRATE_', i, 'YR_YR_2') %in% names(data)) {
    data[, eval(paste0('MED_IMPRATE_', i, 'YR_YR_2')) := get(paste0('MED_IMPRATE_', i, 'YR_YR_2')) * get(paste0('rescale', i))]
  }
  if (paste0('MED_IMPRATE_', i, 'YR_YR_3') %in% names(data)) {
    data[, eval(paste0('MED_IMPRATE_', i, 'YR_YR_3')) := get(paste0('MED_IMPRATE_', i, 'YR_YR_3')) * get(paste0('rescale', i))]
  }
  if (paste0('MED_IMPRATE_', i, 'YR_YR_4') %in% names(data)) {
    data[, eval(paste0('MED_IMPRATE_', i, 'YR_YR_4')) := get(paste0('MED_IMPRATE_', i, 'YR_YR_4')) * get(paste0('rescale', i))]
  }
  if (paste0('MED_IMPRATE_', i, 'YR_YR_5') %in% names(data)) {
    data[, eval(paste0('MED_IMPRATE_', i, 'YR_YR_5')) := get(paste0('MED_IMPRATE_', i, 'YR_YR_5')) * get(paste0('rescale', i))]
  }
  if (paste0('MED_IMPRATE_', i, 'YR_YR_6') %in% names(data)) {
    data[, eval(paste0('MED_IMPRATE_', i, 'YR_YR_6')) := get(paste0('MED_IMPRATE_', i, 'YR_YR_6')) * get(paste0('rescale', i))]
  }
}

data <- data[!is.na(isocode) & isocode != '', ]
#--------------------------------------------# ####

cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(data, paste0('B_CRS_[crs.update_mmyy]_DISB_SCHEDULES_STD'), 'CRS', 'int')
#------------------------# ####
