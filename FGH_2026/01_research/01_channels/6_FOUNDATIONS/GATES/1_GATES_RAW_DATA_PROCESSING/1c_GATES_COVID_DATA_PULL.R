#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Clean web scraped COVID data
#---------------------#
####----#Environment prep#----####
rm(list = ls())

library(data.table)
library(readxl)
library(dplyr)

if (Sys.info()["sysname"] == "Linux") {
   j <- "FILEPATH" 
   h <- paste0("/homes/", Sys.getenv("USER"), "/")
   k <- "FILEPATH"
} else { 
   j <- "J:/"
   h <- "H:/"
   k <- "K:/"
}
code_repo <- 'FILEPATH'


report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

cat('Read in file, reformat')
####------# read in file, reformat #-----#####
#skip 1 for false header
dt <- setDT(fread(paste0(get_path('BMGF','raw'),'bmgf_all_grants.csv'),
                  skip = 1, stringsAsFactors = F))
names(dt) <- gsub(" ", ".", names(dt))

#add "-01" to dates to make them YYYY-MM-DD formatted
dt <- dt[,DATE.COMMITTED := paste0(DATE.COMMITTED, "-01")]
#filter only 2020 & 2021 commitments (future scalable for a decade)
dt <- dt[grepl('202',DATE.COMMITTED)]
names(dt) <- tolower(names(dt))

#format the column names as expected in 1d_COVID_PROJECTS_CLEAN.R
#From the 1d file setnames(dt, c('date', 'term', 'topic', 'grantee_location', 'grantee', 'amount'), 
setnames(dt, c('amount.committed','date.committed','duration.(months)','grantee.country'),
                        c('amount','date','term','grantee_country'))
names(dt) <- gsub('\\.','_',names(dt))

cat(' Subset and save dataset')
####------# subset and save dataset #----####
dt <- dt[date < dah.roots$report_year + 1,
         .(purpose,division,date,term,topic,grantee_country,grantee,amount,grantee_website)]
save_dataset(dt, filename = paste0('bmgf_covid_all_grants_data'), 'BMGF', 'raw')
