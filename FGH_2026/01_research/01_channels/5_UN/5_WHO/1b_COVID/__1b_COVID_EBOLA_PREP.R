#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Intake + format COVID data to merge into ebola
#---------------------# ####

#*********************************************************************#
####-----                       NOTES                         -----####
# Unmatch Donor Names:
#     Look at the 'Unmatch donor names' section to add any new donor names
#     that are not already in the code.
#
# NOTE ON DONOR AGGREGATE:
#     we aggregate by donor since we only know from the raw WHO data donor
#     contributions. We then go head and use the donor information.

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)
# Variable prep
codes <- get_path("meta", "locs")
#----------------------------# ####

cat(green('\n\n ###############################\n #### 2a - COVID EBOLA PREP ####\n ###############################\n \n\n'))

#----# Read in data #----# ####
cat('  Read in data\n')

dt_incexp <- setDT(readstata13::read.dta13(paste0(get_path('WHO', 'fin'), '/WHO_INC_EXP_1990_',dah.roots$prev_report_year,'.dta')))
covid <- setDT(fread(paste0(get_path('WHO', 'int'), 'WHO_COVID_CLEAN_oct.csv')))
#------------------------------#

#----# Clean data - keep only covid years #----# ####
cat('  Clean data - keep only covid years \n')

# keep disbursement/commitment
covid[, amount := ifelse(is.na(disbursement),commitment, disbursement)]
# aggregate to keep donors
covid <- covid[,sum(amount, na.rm = T), by=c('year','donor')]
# WHO only has data for up to prior year
covid <- covid[year < dah.roots$report_year]
dt_incexp <- dt_incexp[YEAR>=2020 & CHANNEL=="WHO"]
setnames(covid, old = c('year','donor'), new = c('YEAR','DONOR_NAME'))

# Clean donor names to match better in both datasets
covid[, upper_DONOR_NAME := string_to_std_ascii(DONOR_NAME)]
setnames(dt_incexp,
         c("DONOR_NAME_upper", "DONOR_NAME"),
         c("upper_DONOR_NAME", "original_DONOR_NAME"))
dt_incexp$DONOR_NAME <- ifelse(
  dt_incexp$original_DONOR_NAME == str_to_upper(dt_incexp$original_DONOR_NAME),
  str_to_title(dt_incexp$original_DONOR_NAME),
  dt_incexp$original_DONOR_NAME)
dt_incexp[, upper_DONOR_NAME := string_to_std_ascii(DONOR_NAME, pad_char = "")]
dt_incexp[upper_DONOR_NAME %like% "UNITED KINGDOM OF GREAT BRITAIN AND NORTHEN IRELAND", upper_DONOR_NAME := "UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND"]
# aggregate by donor name to match covid donors
# *READ NOTE FOR WHY WE KEEP DONOR ONLY
dt_incexp <- dt_incexp[,.(OUTFLOW = sum(OUTFLOW,na.rm = T)), by=c('YEAR','upper_DONOR_NAME','DONOR_NAME','ISO_CODE','INCOME_SECTOR','INCOME_TYPE')]
#------------------------------#

#----# Unmatch donor names #----# ####
cat('  Unmatch donor names \n')

# merge INC_EXP and Covid dataset
dt_temp <-merge(dt_incexp,covid, by=c('YEAR','upper_DONOR_NAME'), all = T )

dt_unmatch <- dt_temp %>% .[!is.na(DONOR_NAME.y) & is.na(DONOR_NAME.x)]
dt_unmatch[upper_DONOR_NAME %like% "NETHERLANDS KINGDOM OF THE", upper_DONOR_NAME := "NETHERLANDS"]
dt_unmatch[upper_DONOR_NAME %like% "BIG HEART FOUNDATION TBHF", upper_DONOR_NAME := "THE BIG HEART FOUNDATION TBHF"]
dt_unmatch[upper_DONOR_NAME %like% "UNITED KINGDOM", upper_DONOR_NAME := "UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND"]

#------------------------------#

#----# Merge donor names #----# ####
cat('  Merge donor names \n')

# Keep only matched donor names in to the INC_EXP dataset to then merge the Unmatch
dt_temp <-merge(dt_incexp,covid[,.(YEAR,upper_DONOR_NAME,V1)], by=c('YEAR','upper_DONOR_NAME'), all.x = T )
dt <-merge(dt_temp,dt_unmatch[,.(YEAR,upper_DONOR_NAME,V1)], by=c('YEAR','upper_DONOR_NAME'), all = T )

rm(dt_temp, dt_unmatch, covid, dt_incexp)
#------------------------------#

#----# Fix Income Sector Type #----# ####
cat('  Fix Income Sector Type \n')

# fixing the INCOME_SECTOR INCOME_TYPE for those donors that do not have a match in the main dataset
dt[upper_DONOR_NAME %like% "PROGRAM FOR APPROPRIATE TECHNOLOGY IN HEALTH PATH", `:=` (INCOME_SECTOR = 'PRIVATE', INCOME_TYPE = 'NGO', ISO_CODE='USA')]
dt[upper_DONOR_NAME %like% "MISCELLANEOUS", `:=` (INCOME_SECTOR = 'UNALL', INCOME_TYPE = 'UNALL', ISO_CODE='NA')]
dt[upper_DONOR_NAME %like% "UN MULTI PARTNER TRUST FUND OFFICE MPTF", `:=` (INCOME_SECTOR = 'OTHER', INCOME_TYPE = 'UN', ISO_CODE='NA')]
dt[upper_DONOR_NAME %like% "AFRICA RE FOUNDATION", `:=` (INCOME_SECTOR = 'INK', INCOME_TYPE = 'CORP', ISO_CODE='NA')]
dt[upper_DONOR_NAME %like% "CHINA MEDICAL BOARD CMB", `:=` (INCOME_SECTOR = 'PUBLIC', INCOME_TYPE = 'CENTRAL', ISO_CODE='CHN')]
dt[upper_DONOR_NAME %like% "EAST AFRICAN COMMUNITY", `:=` (INCOME_SECTOR = 'PUBLIC', INCOME_TYPE = 'OTHER', ISO_CODE='NA')]
dt[upper_DONOR_NAME %like% "SOUTHERN AFRICAN DEVELOPMENT COMMUNITY SECRETARIAT SADC", `:=` (INCOME_SECTOR = 'PUBLIC', INCOME_TYPE = 'OTHER', ISO_CODE='NA')]


# check missing INCOME_SECTOR INCOME_TYPE
# where donor name is NA and donor
if (length(dt[is.na(DONOR_NAME) & is.na(INCOME_SECTOR)]$DONOR_NAME) > 0) {
  dt %>% .[is.na(DONOR_NAME)] %>% View()
  cat(paste0(red("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n")))
  cat(paste0(red("   There are unmatched values, need to update INCOME SECTOR/TYPE   \n\n")))
  cat(paste0(red("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n")))
  stop(" Check that all donors have income sector/type. Add to those that are missing")
  
} else { #continue the script
  cat(green("All donors have income sector and type... Proceed\n"))  
}

#------------------------------#

#----# Finalize dataset #----# ####
cat('  Finalize dataset \n')

# set aggregate V1 values to be oid_covid_DAH values
dt[, `:=` (V1.x = ifelse(is.na(V1.x),0,V1.x),
           V1.y = ifelse(is.na(V1.y),0,V1.y))]
dt[, oid_covid_DAH := V1.x + V1.y]

# aggregate dataset to then save and merge into ebola
dt <- dt %>% .[, .(OUTFLOW = sum(OUTFLOW,na.rm = T),
                   oid_covid_DAH = sum(oid_covid_DAH, na.rm = T)), by=c('YEAR','upper_DONOR_NAME','DONOR_NAME','ISO_CODE','INCOME_SECTOR','INCOME_TYPE')]
dt <- dt[oid_covid_DAH > 0,.(YEAR,DONOR_NAME, upper_DONOR_NAME,ISO_CODE,INCOME_SECTOR,INCOME_TYPE,oid_covid_DAH)]
dt[is.na(DONOR_NAME), DONOR_NAME := upper_DONOR_NAME]
dt[, upper_DONOR_NAME := NULL]
dt <- dt[!(DONOR_NAME == 'WB' & ISO_CODE=="NA")]
# add columns for merge with ebola
dt[,`:=`(CHANNEL = 'WHO')]
# add donor country
isos <- setDT(fread(paste0(codes, 'fgh_location_set.csv')))[level == 3, c('location_name', 'ihme_loc_id')]
dt <- merge(dt, isos, by.x = 'ISO_CODE', by.y = 'ihme_loc_id', all.x = T)
setnames(dt,old = 'location_name', new = 'DONOR_COUNTRY')
dt[DONOR_COUNTRY=='United States of America',DONOR_COUNTRY:='United States']
dt[DONOR_COUNTRY=='Republic of Korea',DONOR_COUNTRY:='South Korea']
dt[DONOR_COUNTRY=='Iran (Islamic Republic of)',DONOR_COUNTRY:='Iran']
dt[DONOR_COUNTRY=='Russian Federation',DONOR_COUNTRY:='Russia']
dt[DONOR_COUNTRY=='Eswatini',DONOR_COUNTRY:='Swaziland']
dt[DONOR_COUNTRY %like% 'Ivoire',DONOR_COUNTRY:="Cote d'Ivoire"]
dt[DONOR_COUNTRY=='Czechia',DONOR_COUNTRY:='Czech Republic']
dt[DONOR_COUNTRY=='Gambia',DONOR_COUNTRY:='The Gambia']

dtprev <- setDT(fread(paste0("FILEPATH",
                             'COVID_DONOR_AGGREATE_EBOLA_MERGE.csv')))
dt <- rbind(dt, dtprev)

#----# Save out COVID dataset #----# ####
cat('  Save out COVID dataset\n')
save_dataset(dt, 'COVID_DONOR_AGGREGATE_EBOLA_MERGE_2022', 'WHO', 'fin')
