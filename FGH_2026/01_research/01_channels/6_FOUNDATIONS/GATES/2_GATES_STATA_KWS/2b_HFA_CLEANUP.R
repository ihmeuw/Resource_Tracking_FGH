#----# Docstring #----# ####
# Project:  FGH
# Purpose:  BMGF HFA Cleanup
#---------------------#

#----------NTOES-----------#
# This does use ingr but the wb_historical, can this be incorporated into
# the donor_standardization function?
# In general code is fine as is and doesn't need updating. Adding this section to 
# comply with documentaiton standards and signify code has been inspected
#--------------------------#

#----# Environment Prep #----# ####
rm(list=ls())

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

# Variable prep
inc <- get_path("meta", "locs")
#----------------------------# 


cat('\n\n')
cat(green(' ##########################\n'))
cat(green(' #### BMGF HFA CLEANUP ####\n'))
cat(green(' ##########################\n\n'))


cat('  Read in post-kws data\n')
#----# Read in post-kws data #----# ####
dt <- setDT(read.dta13(paste0(get_path('BMGF', 'int'), 'bmgf_post_kws.dta')))
#---------------------------------#

cat('  Calculate disbursement fractions\n')
#----# Calculate disbursement fractions #----# ####
to_calc <- names(dt)[names(dt) %like% 'final_' & !(names(dt) %like% '_total_')] %>% str_replace_all('final_', '') %>% str_replace_all('_frct', '')
for (col in to_calc) {
  dt[, eval(paste0(col, '_DAH')) := get(paste0('final_', col, '_frct')) * DISBURSEMENT]
}
#--------------------------------------------# 

cat('  Tag negative disbursements for re-calculation\n')
#----# Tag negative disbursements for re-calculation #----# ####
dt[DISBURSEMENT < 0 & !is.na(DISBURSEMENT), neg := 1]

dt[, neg_proj := sum(neg, na.rm=T), by=c('RECIPIENT_AGENCY', 'RECIPIENT_COUNTRY')]
dt <- dt[order(RECIPIENT_AGENCY, RECIPIENT_COUNTRY, YEAR), ]
dt[, dummy := 1]
dt[, proj_n := 1:sum(dummy), by=c('RECIPIENT_AGENCY', 'RECIPIENT_COUNTRY')]
dt[, proj_N := sum(dummy), by=c('RECIPIENT_AGENCY', 'RECIPIENT_COUNTRY')]

setnames(dt, 'DISBURSEMENT', 'all_DAH')
#---------------------------------------------------------#

cat('  Negative disbursement re-calculation...\n')
#----# Negative disbursement re-calculation #----# ####
neg_fix <- copy(dt)[neg_proj >= 1, ]
# Setup base column to calculate
for (col in to_calc) {
  neg_fix[, eval(paste0(col, '_fix_0')) := get(paste0(col, '_DAH'))]
  neg_fix[is.na(get(paste0(col, '_fix_0'))), eval(paste0(col, '_fix_0')) := 0]
}

# For each HFA
for (col in to_calc) {
  cat(paste0('    ', col, '\n      '))
  for (i in 1:max(neg_fix$proj_N)) {
    cat(i)
    j <- i - 1
    
    # Generate new disbursment variable
    neg_fix[, eval(paste0(col, '_fix_', i)) := get(paste0(col, '_fix_', j))]
    neg_fix[is.na(get(paste0(col, '_fix_', i))), eval(paste0(col, '_fix_', i)) := 0]
    
    # Isolate negative values
    neg_fix[(get(paste0(col, '_fix_', i)) < 0) & (proj_n == (proj_N - (i-1))), 
            eval(paste0(col, '_neg_disb_', i)) := get(paste0(col, '_fix_', i))]
    neg_fix[, eval(paste0(col, '_neg_proj_', i)) := sum(get(paste0(col, '_neg_disb_', i)), na.rm=T), by=c('RECIPIENT_AGENCY', 'RECIPIENT_COUNTRY')]
    
    # Subtract negative value from previous disbursement
    neg_fix[(get(paste0(col, '_neg_proj_', i)) != 0) & (proj_n == proj_N - i), 
            eval(paste0(col, '_fix_', i)) := get(paste0(col, '_fix_', i)) + get(paste0(col, '_neg_proj_', i))]
    
    # Generate dummy variable to indicate value change
    neg_fix[(get(paste0(col, '_neg_proj_', i)) != 0) & (proj_n == proj_N - i) & (proj_N >= (i + 1)), 
            eval(paste0(col, '_change', i)) := 1]
    neg_fix[, eval(paste0(col, '_pchange', i)) := sum(get(paste0(col, '_change', i)), na.rm=T), by=c('RECIPIENT_AGENCY', 'RECIPIENT_COUNTRY')]
    
    # Replace negative value as 0
    neg_fix[(get(paste0(col, '_neg_proj_', i)) != 0) & (proj_n == proj_N - j) & (get(paste0(col, '_pchange', i)) == 1), 
            eval(paste0(col, '_fix_', i)) := 0]
  }
  cat('\n')
  
  setnames(neg_fix, paste0(col, '_fix_', max(neg_fix$proj_N)), paste0(col, '_noneg'))
  to_drop <- names(neg_fix)[names(neg_fix) %like% paste0(col, '_fix') | 
                              names(neg_fix) %like% paste0(col, '_neg_disb_') |
                              names(neg_fix) %like% paste0(col, '_neg_proj_') |
                              names(neg_fix) %like% paste0(col, '_change') |
                              names(neg_fix) %like% paste0(col, '_pchange')]
  neg_fix[, eval(to_drop) := NULL]
}

for (col in to_calc) {
  neg_fix[, eval(paste0(col, '_DAH')) := get(paste0(col, '_noneg'))]
}

to_drop <- names(neg_fix)[names(neg_fix) %like% '_noneg']
neg_fix[, eval(to_drop) := NULL]
#------------------------------------------------#

cat('  Drop negatives from dataset + append re-calculated\n')
#----# Drop negatives from dataset + append re-calculated #----# ####
dt <- dt[neg_proj < 1, ]
data <- rbind(dt, neg_fix)
setnames(data, 'all_DAH', 'DISBURSEMENT')
rm(dt, neg_fix)
#--------------------------------------------------------------# 

cat('  Merge on income groups\n')
#----# Merge on income groups #----# ####
ingr <- fread(get_path("meta", "locs", "wb_historical_incgrps.csv"),
              select = c('YEAR', 'ISO3_RC', 'INC_GROUP'))
data <- merge(data, ingr, by=c('YEAR', 'ISO3_RC'), all.x=T)

data[ISO3_RC %in% c('N/A', 'QZA') | INC_GROUP == 'H', ISO3_RC := 'WLD']
setnames(data, 'ISO3_RC', 'iso3')
data[, ISO3_RC := str_trim(iso3, side='both')]
data[, iso3 := NULL]
#----------------------------------#

cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(data, paste0('BMGF_INTPDB_1999_', dah.roots$report_year, '_update'), 'BMGF', 'fin')
#------------------------#
