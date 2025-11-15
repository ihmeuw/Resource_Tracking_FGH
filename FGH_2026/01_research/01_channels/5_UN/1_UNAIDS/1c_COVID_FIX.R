########################################################################################
## UNAIDS Recipient CFix using CRS data
## Description: Read in COVID_prepped data and ADB_PDB. Merge & Collapse COVID total
## onto ADB_PDB and save same filename but _COVID_fix appeneded 
## to be used in compile to avoid double counts
########################################################################################
rm(list = ls())

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}


report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)
asn_date <- format(Sys.time(), "%Y%m%d")

# Variable prep
codes <- paste0(dah.roots$j, 'FILEPATH')

#------------# read in data #------------#
adbpath <- get_path('UNAIDS', 'fin', paste0("UNAIDS_ADB_PDB_FGH", dah.roots$report_year, "_includesDC.dta"))
adb_pdb <- setDT(readstata13::read.dta13(adbpath))
covid <- setDT(fread(paste0(get_path('UNAIDS','fin', report_year = 2023),'COVID_prepped.csv')))

#--------# subset and clean data #----------#
covid <- covid[, sum(TOTAL_AMT, na.rm=T), by=.(YEAR,DONOR_NAME, INCOME_SECTOR,INCOME_TYPE, ISO_CODE)]
covid <- covid[YEAR >= 2020]

#clean donor names for better matches later
covid[, DONOR_NAME := string_to_std_ascii(DONOR_NAME)]
adb_pdb[, DONOR_NAME := string_to_std_ascii(DONOR_NAME)]

dt <- merge(adb_pdb,covid, by=c("YEAR","DONOR_NAME", "INCOME_SECTOR","INCOME_TYPE", 'ISO_CODE'), all.x=T)

dt[,oid_covid := V1]

#----# Save out COVID dataset #----# ####
cat('  Save out COVID dataset\n')
save_dataset(dt, paste0("UNAIDS_ADB_PDB_FGH_", dah.roots$report_year, "_includesDC_COVID_fix"), 'UNAIDS', 'fin')
