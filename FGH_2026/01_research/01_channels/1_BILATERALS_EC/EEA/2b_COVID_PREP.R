#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Process EEA COVID data
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls())

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}
source(paste0(code_repo, 'FUNCTIONS/utils.R'))

# Variable prep
codes <- paste0(dah.roots$j, 'FILEPATH')
#----------------------------# ####


cat('\n\n')
cat(green(' ##################################\n'))
cat(green(' #### EEA CLEAN COVID PROJECTS ####\n'))
cat(green(' ##################################\n\n'))


cat('  Read in data\n')
#----# Read in data #----# ####
dt <- setDT(fread(paste0(get_path('EEA', 'raw'), 'covid_raw.csv')))
#------------------------# ####

cat('  Run COVID KWS\n')
#----# Run COVID KWS #----# ####
dt <- covid_kws(dataset = dt, keyword_search_colnames = 'purpose', keep_clean = F, 
                keep_counts = F, languages = 'English')
covid_stats_report(dataset = dt, amount_colname = 'amount', save_plot = T, output_path = get_path('EEA', 'output'))
#-------------------------# ####

cat('  Calculate amounts by HFA\n')
#----# Calculate amounts by HFA #----# ####
dt[, `:=`(COVID_total = NULL, COVID_total_prop = NULL)]

hfas <- gsub('_prop', '', names(dt)[names(dt) %like% '_prop'])
for (hfa in hfas) {
  dt[, eval(paste0(hfa, '_amt')) := amount * get(paste0(hfa, '_prop'))]
  dt[, eval(paste0(hfa, '_prop')) := NULL]
}

# Check sum still holds
dt <- rowtotal(dt, 'amt_test', names(dt)[names(dt) %like% '_amt'])
dt[round(amount, 2) == round(amt_test, 2), check := 1]
dt[, `:=`(amt_test = NULL, check = NULL)]

# Rename 
setnames(dt, c('amount', 'YEAR'), c('total_amt', 'year'))
#------------------------------------# ####

cat('  Save out COVID dataset\n')
#----# Save out COVID dataset #----# ####
dt[, `:=`(channel = 'EEA', money_type = 'repurposed')]

dt <- dt[, c('year', 'channel', paste0(hfas, '_amt'), 'total_amt', 'money_type'), with=F]
save_dataset(dt, 'COVID_prepped', 'EEA', 'fin')
#----------------------------------# ####