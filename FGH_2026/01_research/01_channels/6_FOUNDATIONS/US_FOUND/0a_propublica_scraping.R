#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Scrape 1:1 data from efiles
#---------------------#

#----# Environment Prep #----# ####
rm(list=ls())

if (!exists("code_repo"))  {
  code_repo <- "FILEPATH"
}
library(irs990efile)
library(dplyr)
source("FILEPATH")
report_year <- dah.roots$report_year
prev_year <- dah.roots$prev_report_year
##
#----# Get Data #----# ####
# High priority orgs to scrape based on previous data--
# Code will look a bit different next year.
int_prev <- fread("FILEPATH")
gm_info <- unique(int_prev[, .(gm_name, gm_ein, YEAR)])
gm_info <- gm_info[!is.na(gm_ein)]
ein_list <- gm_info[order(YEAR, gm_ein)]
ein_list <- ein_list[, by = gm_ein, .(gm_name = last(gm_name))]
save_dataset(ein_list, "foundations_for_scraping.csv", "US_FOUNDS", "raw")


# import index--check https://990data.givingtuesday.org/access-via-aws-account-2/
indexupdate <- "2024-12-23"
index <- fread("FILEPATH")

wantfound <- fread("FILEPATH")
index_cy <- index[TaxYear == 2022]
index_cy <- index_cy[FormType %ni% c("990T", "990EZ") & OrgType != "4947a1PF"]
n_occur <- data.frame(table(index_cy$EIN))
singlereturns <- index_cy[index_cy$EIN %in% n_occur$Var1[n_occur$Freq == 1],]
multreturns <- index_cy[index_cy$EIN %in% n_occur$Var1[n_occur$Freq != 1],]
setorder(multreturns, EIN, -SubmittedOn)
multreturns <- multreturns[, .SD[1], by = EIN]
# returns for every 990 and 990pf org
index_cy <- rbind(multreturns, singlereturns)
# filters for our targets--maintains every org in wantfounds
foundations_index <- index_cy[abs(TotalExpensesCY) > 10000]
foundations_index <- foundations_index[FormType == "990PF" | grepl("Trust|Foundation|Fund|Philanthrop|Endow", OrganizationName, ignore.case = TRUE)]
rm(index, multreturns, singlereturns, n_occur)
save_dataset(foundations_index, "scraped_foundations_index.csv", "US_FOUNDS", "raw")


# Save out to be scraped in Python
notfound <- wantfound[gm_ein %in% check_overlap[is.na(BuildTs)]$gm_ein]
found990s <- wantfound[gm_ein %in% check_overlap[FormType == "990"]$gm_ein]
propublica_scrape <- bind_rows(notfound, found990s)
save_dataset(propublica_scrape, "propublica_scraping_target.csv", "US_FOUNDS", "raw")

# Save out to be scraped from datalake in Python
pf990s <- foundations_index[FormType == "990PF"]
save_dataset(pf990s, "990_pf_index.csv", "US_FOUNDS", "raw")