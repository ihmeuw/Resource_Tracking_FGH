#----# Docstring #----# ####
#
# Project:  FGH
# Purpose:  Reads contributions to PAHO for Covid from 2020 to 2021
# and runs key word search to match PAHO expended categories to FGH health 
# functions.
#---------------------# 

#----# Environment Prep #----# ####

rm(list=ls())
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, dplyr)
#----------------------------#

cat('  Read in COVID data\n')
#----# Read in COVID data #----# ####
dt <- setDT(fread(paste0(get_path('PAHO', 'int'), '1.COVID_clean_donors.csv'), encoding = 'Latin-1'))


#----# Estimating total DAH estimates by year #----# ####

# amount that PAHO report was donated for COVID
dah_2020 <- sum(dt$disbursement_2020)

# amount that PAHO report was donated for COVID
dah_2021 <- sum(dt$disbursement_2021)

dah_2022 <- sum(dt$disbursement_2022)

#----# Estimating how funds were distributed #----#

# Loading 2020 expense categories
# data from page 4 of covid-19 paho response summary
# description is the description provided by PAHO
# share is the amt for each pillar/total

dis_20 <- setDT(fread(paste0(dirname(get_path('PAHO', 'raw')), "/",
                             'PAHO_expense_categories_2020_COVID_121321.csv'), 
                             encoding = 'Latin-1')) 

# estimating amount that was distributed by function
dis_20[, expensed := share * dah_2020]
dis_20[, year := 2020]

# Loading 2021 expense categories
# data from page 5 of covid-19 paho response summary
# description is the description provided by PAHO
# share is the amt for each pillar/total
dis_21 <- setDT(fread(paste0(dirname(get_path('PAHO', 'raw')), "/", 
                             'PAHO_expense_categories_2021_COVID_111021.csv'), encoding = 'Latin-1'))

# estimating amount that was distributed by function
dis_21[, expensed := share * dah_2021]
dis_21[, year := 2021]

# Loading 2022 expense categories
dis_22 <- fread(get_path("paho", "raw", c("covid", "PAHO_expense_categories_2022_COVID.csv")))
dis_22 <- dis_22[!is.na(share)]
dis_22[, expensed := share * dah_2022]
dis_22[, year := 2022]



# Bringing 2020 and 2021 into single dataset
paho <- rbind(dis_20, dis_21, dis_22)

#----# Adding and cleaning needed columns #----#
paho[, iso3_rc := "QZA"]
paho[, recipient_country := "Unallocable"]
paho[, channel := "PAHO"]
paho[, upper_expense_category := toupper(expense_category)]


cat('  Configure and launch keyword search\n')
#----# Configure and launch keyword search #----# ####
paho <- covid_kws(dataset = paho, keyword_search_colnames = c('upper_expense_category'), 
                  keep_clean = F, keep_counts = T, languages = c('english'))

covid_stats_report(dataset = paho, amount_colname = 'expensed', 
                   recipient_iso_colname = 'iso3_rc', save_plot = T, 
                   output_path = get_path('PAHO', 'output'))

cat('  Applying fractions to funding\n')
#----# Multiply fractions created by key word search to money dispensed by PAHO #----# ####

formatting <- function(chan, channel_name) {
  hfas <- gsub('_prop', '', names(chan)[names(chan) %like% '_prop'])
  for (hfa in hfas) {
    chan[, eval(paste0(hfa, '_amt')) := expensed * get(paste0(hfa, '_prop'))]
    chan[, eval(paste0(hfa, '_prop')) := NULL]
  }
  
  # Check sum still holds
  chan <- rowtotal(chan, 'AMT_TEST', names(chan)[names(chan) %like% '_amt'])
  chan[round(expensed, 2) == round(AMT_TEST, 2), CHECK := 1]
  chan[, `:=`(AMT_TEST = NULL, CHECK = NULL)]
}

formatting(paho, "PAHO")
names(paho)
#-------------------------------------------------# 

cat('  Final dataset clean up before saving file \n')
#----# Final dataset clean up before saving file #----# ####

# creating vectors with columns that we want to keep
ctc <- c('year', 'iso3_rc', 'recipient_country', 'channel') 
hfas <- gsub('_amt', '', names(paho)[names(paho) %like% '_amt'])


paho_f <- paho[, c(ctc, paste0(hfas, '_amt')), with = F]

# updating column name
setnames(paho_f, old = 'COVID_total_amt', 'disbursement')

#-----------------------------------------------# 

#---# Saving dataset #----# ####
save_dataset(paho_f, 
             paste0('2.COVID_health_functions'), 
             'PAHO', 'int') 
