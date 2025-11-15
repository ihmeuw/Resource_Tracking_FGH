#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Adjust CRS DAC commitments
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))
start.time <- Sys.time()
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr)
#----------------------------# ####


cat('\n\n')
cat(green(' ####################################\n'))
cat(green(' #### CRS ADJUST DAC COMMITMENTS ####\n'))
cat(green(' ####################################\n\n'))


cat('  Read in BIL_ODA_2 dataset\n')
#----# Read in BIL_ODA_2 dataset #----# ####
dt <- fread(get_path('CRS', 'int', 'B_CRS_[crs.update_mmyy]_HEALTH_BIL_ODA_2_COVID.csv'))
#-------------------------------------# ####
#----# Calculate avg project length #----# ####
avg_proj_length <- function(dt_) {
    cat('  Calculate avg project length\n')
    # Max proj length
    dt <- dt_[order(donor_agency, recipient_code, crs_id, year), ]
    dt[, dummy := 1]
    dt[, proj_n := 1:sum(dummy), by=c('donor_agency', 'recipient_code', 'crs_id')]
    dt[, max_proj_yr := max(year, na.rm=T), by=c('donor_agency', 'recipient_code', 'crs_id')]
    dt[, max_proj_yr_date := paste0('1/1/', as.character(max_proj_yr))]
    dt[, max_proj_date := as.Date(max_proj_yr_date, format='%m/%d/%Y')]
    
    # Min proj length
    dt[, min_proj_yr := min(year, na.rm=T), by=c('donor_agency', 'recipient_code', 'crs_id')]
    dt[, min_proj_yr_date := paste0('1/1/', as.character(min_proj_yr))]
    dt[, min_proj_date := as.Date(min_proj_yr_date, format='%m/%d/%Y')]
    
    # Replace with observed value if present
    dt[, close_date := completion_date]
    dt[, close_date := str_replace_all(close_date, '-', '/')]
    dt[, close_day := as.Date(close_date, format='%Y/%m/%d')]
    
    dt[, open_date := expected_start_date]
    dt[, open_date := str_replace_all(open_date, '-', '/')]
    dt[, open_day := as.Date(open_date, format='%Y/%m/%d')]
    
    dt[close_day > max_proj_date & !is.na(close_day), max_proj_date := close_day]
    dt[open_day > min_proj_date & !is.na(open_day), min_proj_date := open_day]
    
    # Calculate proj length
    dt[, proj_length := max_proj_date - min_proj_date]
    dt[proj_length <0, proj_length := 0]
    dt[proj_n!=1, proj_length := NA]
    
    # Calculate avg project length
    dt <- collapse(dt, 'median', c('isocode', 'year'), 'proj_length')
    dt[, proj_length := round(proj_length / 365, 0)]
    dt[proj_length > 6 | is.na(proj_length), proj_length := 6]
    return(dt)
}
channel_avg_proj_length <- avg_proj_length(dt)
#----------------------------------------# ####

cat('  Calculate total commitments from unstarted projects\n')
#----# Calculate total commitments from unstarted projects #----# ####
tot_future_comm <- function(dt_) {
    dt <- copy(dt_)
    dt[, open_date := expected_start_date]
    dt[, open_date := str_replace_all(open_date, '-', '/')]
    dt[, open_day := as.Date(open_date, format='%Y/%m/%d')]
    dt[, open_year := year(open_day)]
    dt <- dt[open_year > get_dah_param('CRS', 'data_year') & !is.na(open_year), ]
    dt <- collapse(dt, 'sum', c('isocode', 'year'), c('all_commcurr', 'all_commcons'))
    return(dt)
}
future_commitments <- tot_future_comm(dt)
#---------------------------------------------------------------# ####

cat("  Use avg. length + unstarted com'ts to adjust DAC commitments\n")
#----# Use avg. length + unstarted com'ts to adjust DAC commitments #----# ####
dt <- fread(get_path('DAC', 'fin',
                     'B_DAC_[crs.update_mmyy]_COMM_COV_BYDONOR_BYYEAR_1990-[crs.data_year].csv')
            )[year <= get_dah_param('CRS', 'data_year'), ]
dt[, m_m := 1]
future_commitments[, u_m := 2]
# Merge
dt <- merge(dt, future_commitments, by=c('isocode', 'year'), all=T)
dt[, merge := rowSums(dt[, c('u_m', 'm_m')], na.rm=T)]

dt[merge == 3, health_dac_all_commcons := health_dac_all_commcons - all_commcons]
dt[merge == 3, health_dac_all_commcurr := health_dac_all_commcurr - all_commcurr]
dt <- dt[, !c('all_commcons', 'all_commcurr', 'merge', 'u_m', 'm_m')]
dt[, `:=`(health_dac_all_commcons = health_dac_all_commcons * 1e6,
          health_dac_all_commcurr = health_dac_all_commcurr * 1e6)]
dt <- dt[, c('year', 'isocode', names(dt)[names(dt) %like% 'health_dac_all']), with=F]

# Add disb schedules
disb_std <- fread(get_path('CRS', 'int', 'B_CRS_[crs.update_mmyy]_DISB_SCHEDULES_STD.csv'))
dt <- merge(dt, disb_std, by='isocode', all=T)
rm(disb_std)

# Merge avg_proj_length
dt <- merge(dt, channel_avg_proj_length, by=c('isocode', 'year'), all.x=T)
dt[proj_length == 0 | is.na(proj_length), proj_length := 1]
rm(channel_avg_proj_length)
#------------------------------------------------------------------------# ####

cat('  Multiply DAC commitments by donor disbursement schedules\n')
#----# Multiply DAC commitments by donor disbursement schedules #----# ####
data <- data.table()
for (i in 1:6) {
  t <- copy(dt[proj_length == i, ])
  for (unit in c('curr', 'cons')) {
    for (p in 1:i) {
      j <- p - 1
      t[, eval(paste0('disbyr', p, '_', unit)) := get(paste0('health_dac_all_comm', unit)) * get(paste0('MED_IMPRATE_', i, 'YR_YR_', p))]
    }
  }
  data <- rbind(data, t, fill=T)
  rm(t, i, unit, p, j)
}

# Redistribute disbursement shedules for a given year to the correct years the disbursements will occur in
for (col in c('disbyr2_curr', 'disbyr2_cons', 'disbyr3_curr', 'disbyr3_cons', 'disbyr4_curr', 
              'disbyr4_cons', 'disbyr5_curr', 'disbyr5_cons', 'disbyr6_curr', 'disbyr6_cons')) {
  data[is.na(get(col)), eval(col) := 0]
  rm(col)
}


lst <- list()
for (n in 2:6) {
  t <- copy(data[proj_length == n, ])
  # duplicate every observation (n-1) times
  t <- do.call('rbind', replicate((n-1), t, simplify=F))

  t[, dummy := 1]
  t[, n := 1:sum(dummy) + 1, by=c('isocode', 'year')]
  t[, dummy := NULL]
  for (p in n:2) {
    t[n == p, eval(paste0('fill', p, '_curr')) := get(paste0('disbyr', p, '_curr'))]
    t[n == p, eval(paste0('fill', p, '_cons')) := get(paste0('disbyr', p, '_cons'))]
  }
  t[, year := year + n - 1]
  t <- t[year <= get_dah_param('CRS', 'data_year'),
         c('year', 'isocode', names(t)[names(t) %like% 'fill']), with=F]
  lst[[paste0('dac_', n, '_redist')]] <- t
  rm(t, n, p)
}

redist_all <- lst[['dac_2_redist']]
for (i in 3:6) {
  redist_all <- rbind(redist_all, lst[[paste0('dac_', i, '_redist')]], fill=T)
  rm(i)
}
rm(lst)
for (col in names(redist_all)[names(redist_all) %like% 'fill']) {
  redist_all[is.na(get(col)), eval(col) := 0]
  rm(col)
}
redist_all <- collapse(redist_all, 'sum', c('year', 'isocode'),
                       names(redist_all)[names(redist_all) %like% 'fill'])

# Merge together
redist_all[, u_m := 2]
data[, m_m := 1]
data <- merge(data, redist_all, by=c('year', 'isocode'), all=T)
data[, merge := rowSums(data[, c('u_m', 'm_m')], na.rm=T)]
for (i in 2:6) {
  data[, eval(paste0('disbyr', i, '_curr')) := 0]
  data[, eval(paste0('disbyr', i, '_cons')) := 0]
  
  data[merge == 3, eval(paste0('disbyr', i, '_curr')) := get(paste0('fill', i, '_curr'))]
  data[merge == 3, eval(paste0('disbyr', i, '_cons')) := get(paste0('fill', i, '_cons'))]
}
data <- data[, !c('u_m', 'm_m', 'merge')]
for (unit in c('curr', 'cons')) {
  data <- rowtotal(data, paste0('disbest_', unit, '_dac'),
                   c(paste0('disbyr1_', unit),
                     paste0('disbyr2_', unit),
                     paste0('disbyr3_', unit),
                     paste0('disbyr4_', unit)))
}
#--------------------------------------------------------------------# ####

cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(data, 'dac_adjusted_comm', 'CRS', 'int')
#------------------------# ####
