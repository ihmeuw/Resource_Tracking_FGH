#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Prepare Gates Foundation DISB_FINAL dataset
#---------------------# 

#----# Environment Prep #----# ####
library(dplyr)
rm(list=ls())

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

# Variable prep
ngo <- paste0(dah.roots$j, 'FILEPATH')
#----------------------------#


cat('\n\n')
cat(green(' ##############################\n'))
cat(green(' #### BMGF DISB_FINAL PREP ####\n'))
cat(green(' ##############################\n\n'))


cat('  Read in 1999-2011 disbursement data\n')
#----# Read in 1999-2011 disbursement data #----# ####
disb <- setDT(readstata13::read.dta13(paste0(dah.roots$j, 'FILEPATH/BMGF_GRANTS_DISBURSEMENTS_1999-2011.dta'),
                                      encoding = 'latin1'))
disb <- disb[year < 2009, ]
#-----------------------------------------------#

cat('  Read in historical NGOs data\n')
#----# Read in historical NGOs data #----# ####
ngos <- setDT(readstata13::read.dta13(paste0(ngo, 'NGO_VOLAG_DATA_1990_TO_2013.dta'),
                                      encoding = 'latin1'))[, c('year', 'id')]
ngos[, dummy := 1]
ngos[, n := 1:sum(dummy), by=c('id', 'year')]
ngos <- ngos[n == 1, !c('dummy', 'n')]

# Merge
disb <- merge(disb, ngos, by=c('id', 'year'), all.x=T)
setnames(disb, 'recipient_agency', 'agency')
rm(ngos)
#----------------------------------------#

cat('  Read in historical INTL NGOs data\n')
#----# Read in historical INTL NGOs data #----# ####
intl_ngos <- setDT(readstata13::read.dta13(paste0(ngo, 'NGO_VOLAG_INGO_DATA_1998_2013.dta')))[, c('id', 'agency', 'year')]
intl_ngos[, dummy := 1]
intl_ngos[, n := 1:sum(dummy), by=c('id', 'agency', 'year')]
intl_ngos <- intl_ngos[n == 1, !c('n', 'dummy')]
setnames(intl_ngos, 'id', 'ingo_id')

# Merge
disb <- merge(disb, intl_ngos, by=c('agency', 'year'), all.x=T)
disb[is.na(ingo_id.x) & !is.na(ingo_id.y), ingo_id.x := ingo_id.y]
disb[, ingo_id.y := NULL]
setnames(disb, 'ingo_id.x', 'ingo_id')
setnames(disb, 'agency', 'recipient_agency')
rm(intl_ngos)
#---------------------------------------------#

cat('  Append data with current grant disbursement dataset\n')
#----# Append data with current grant disbursement dataset #----# ####
curr <- fread(get_path('BMGF', 'int',
                       'BMGF_GRANTS_DISBURSEMENTS_2009_[report_year]_update.csv'),
              encoding = 'Latin-1')

dt <- rbind(disb, curr, fill=T)
dt[, usaid_ngo := NULL]
dt[, usaid_ngo := 0]
dt[!is.na(id) | !is.na(ingo_id), usaid_ngo := 1]
setnafill(dt, fill = 0, cols = "is_ngo")
rm(disb, curr)
#---------------------------------------------------------------# 

cat('  Loads of misc formatting\n')
#----# Loads of misc formatting #----# ####
dt[, `:=`(oldid = NULL, recipient_agency_country = NULL, recipient_sector = NULL, sector = 'HEALTH')]
setnames(dt, c('year','recipient_agency','sector','purpose','topic','recipient_legal_sector','recipient_legal_type','country','countryiso'), 
         c('YEAR','RECIPIENT_AGENCY','SECTOR','long_description','short_description','RECIPIENT_AGENCY_SECTOR','RECIPIENT_AGENCY_TYPE','RECIPIENT_COUNTRY','ISO3_RC'))
dt[!is.na(agency) & agency != "", RECIPIENT_AGENCY := agency]

dt[, RECIPIENT_COUNTRY := str_replace_all(RECIPIENT_COUNTRY, ', ,', ',')]
dt[RECIPIENT_COUNTRY == 'Africa, QMA', `:=`(ISO3_RC = 'QMA', RECIPIENT_COUNTRY = 'Africa')]
dt[ISO3_RC == '', ISO3_RC := 'N/A']
dt[, RECIPIENT_COUNTRY := str_replace_all(RECIPIENT_COUNTRY, 'Developing countries, unspecified', 'Unspecified')]
dt[, RECIPIENT_COUNTRY := str_replace_all(RECIPIENT_COUNTRY, ', regional', '')]
dt[toupper(RECIPIENT_COUNTRY) == "PALESTINIAN TERRITORY, OCCUPIED",
   RECIPIENT_COUNTRY := "PALESTINIAN TERRITORY OCCUPIED"]

if (max(dt[, str_count(RECIPIENT_COUNTRY, ",")]) + 1 != 5) {
    stop("Need to split recipients into more than 5 rows")
}

# Split ISO3_RC
dt[, ISO3_RC_1 := tstrsplit(ISO3_RC, ',', keep=1)]
dt[, ISO3_RC_2 := tstrsplit(ISO3_RC, ',', keep=2)]
dt[, ISO3_RC_3 := tstrsplit(ISO3_RC, ',', keep=3)]
dt[, ISO3_RC_4 := tstrsplit(ISO3_RC, ',', keep=4)]
dt[, ISO3_RC_5 := tstrsplit(ISO3_RC, ',', keep=5)]
# Split RECIPIENT_COUNTRY
dt[, RECIPIENT_COUNTRY_1 := tstrsplit(RECIPIENT_COUNTRY, ',', keep=1)]
dt[, RECIPIENT_COUNTRY_2 := tstrsplit(RECIPIENT_COUNTRY, ',', keep=2)]
dt[, RECIPIENT_COUNTRY_3 := tstrsplit(RECIPIENT_COUNTRY, ',', keep=3)]
dt[, RECIPIENT_COUNTRY_4 := tstrsplit(RECIPIENT_COUNTRY, ',', keep=4)]
dt[, RECIPIENT_COUNTRY_5 := tstrsplit(RECIPIENT_COUNTRY, ',', keep=5)]
# Tag number of NA vals
dt[, denominator := 5]
for (i in 2:5) {
  dt[is.na(get(paste0('RECIPIENT_COUNTRY_', i))), denominator := denominator - 1]
}
setnames(dt, 'outflow', 'total_outflow')
dt[, outflow := total_outflow / denominator]
dt[, proj_id := 1:nrow(dt)]

# Reshape long - split twice & merge
test <- melt(dt[, !c('RECIPIENT_COUNTRY_1', 'RECIPIENT_COUNTRY_2', 'RECIPIENT_COUNTRY_3', 'RECIPIENT_COUNTRY_4', 'RECIPIENT_COUNTRY_5')], 
             measure.vars = c('ISO3_RC_1', 'ISO3_RC_2', 'ISO3_RC_3','ISO3_RC_4', 'ISO3_RC_5'))
test$variable <- gsub('ISO3_RC_', '', test$variable) %>% as.numeric()
setnames(test, c('variable', 'value'), c('FOCUS_REGIONS_GRANT', 'ISO3_RC_'))

t2 <- melt(dt[, !c('ISO3_RC_1', 'ISO3_RC_2', 'ISO3_RC_3','ISO3_RC_4', 'ISO3_RC_5')], 
           measure.vars = c('RECIPIENT_COUNTRY_1', 'RECIPIENT_COUNTRY_2', 'RECIPIENT_COUNTRY_3', 'RECIPIENT_COUNTRY_4', 'RECIPIENT_COUNTRY_5'))
t2$variable <- gsub('RECIPIENT_COUNTRY_', '', t2$variable) %>% as.numeric()
setnames(t2, c('variable', 'value'), c('FOCUS_REGIONS_GRANT', 'RECIPIENT_COUNTRY_'))

full <- merge(test, t2, by.x=names(test)[names(test) != 'ISO3_RC_'], by.y=names(t2)[names(t2) != 'RECIPIENT_COUNTRY_'])
project_data <- full[ISO3_RC_ != ' ' & !is.na(ISO3_RC_), !c('proj_id', 'RECIPIENT_COUNTRY', 'ISO3_RC')]
setnames(project_data, c('ISO3_RC_', 'RECIPIENT_COUNTRY_'), c('ISO3_RC', 'RECIPIENT_COUNTRY'))
rm(dt, test, t2, full, i)

# Fix some ISOs
project_data[RECIPIENT_COUNTRY == "LATIN AMERICA", ISO3_RC := "QNE"]
project_data[RECIPIENT_COUNTRY == "SOUTH PACIFIC", ISO3_RC := "QTA"]
project_data <- project_data[ISO3_RC != 'DUE', ]
project_data[RECIPIENT_COUNTRY == "Bolivia", ISO3_RC := "BOL"]
project_data[RECIPIENT_COUNTRY == "Cote d'Ivoire", ISO3_RC := "CIV"]
project_data[RECIPIENT_COUNTRY == "Laos", ISO3_RC := "LAO"]
project_data[RECIPIENT_COUNTRY == "Central African Rep.", ISO3_RC := "CAF"]
project_data[RECIPIENT_COUNTRY == "Vietnam", ISO3_RC := "VNM"]
project_data[RECIPIENT_COUNTRY == "Venezuela", ISO3_RC := "VEN"]
project_data[RECIPIENT_COUNTRY == "Tanzania", ISO3_RC := "TZA"]
project_data[RECIPIENT_COUNTRY == "Syria", ISO3_RC := "SYR"]
project_data[RECIPIENT_COUNTRY == "Swaziland", ISO3_RC := "SWZ"]
project_data[RECIPIENT_COUNTRY == "Iran", ISO3_RC := "IRN"]
project_data[RECIPIENT_COUNTRY == "Congo", ISO3_RC := "COG"]
project_data[RECIPIENT_COUNTRY == "Korea", ISO3_RC := "KOR"]
project_data[RECIPIENT_COUNTRY == "Kyrgyz Republic", ISO3_RC := "KGZ"]
project_data[RECIPIENT_COUNTRY == "Sao Tome & Principe", ISO3_RC := "STP"]
project_data[RECIPIENT_COUNTRY == "Cape Verde", ISO3_RC := "CPV"]

project_data[, RECIPIENT_AGENCY := replace_r_hexadecimal(RECIPIENT_AGENCY)]
project_data[RECIPIENT_AGENCY %in% c('GAVI Fund', 'Vaccine Fund', "Global Fund for Children's Vaccines", 'The GAVI Fund', 'GAVI Alliance'), 
     RECIPIENT_AGENCY := "GAVI"]
project_data[RECIPIENT_AGENCY == "GAVI", usaid_ngo := 0]
project_data[, RECIPIENT_AGENCY_UPPER := string_to_std_ascii(RECIPIENT_AGENCY, pad_char = "")]
#------------------------------------# 

cat('  Tag ELIM_CH\n')
#----# Tag ELIM_CH #----# ####
dt <- copy(project_data)
dt[usaid_ngo == 1, ELIM_CH := 1]
dt[is_ngo == 1, ELIM_CH := 1]
dt[RECIPIENT_AGENCY_UPPER %like% 'WORLD HEALTH ORGANIZATION' | RECIPIENT_AGENCY_UPPER %like% 'WORLD HEALTH ORGANISATION' | RECIPIENT_AGENCY_UPPER %like% 'WHO', 
   ELIM_CH := 1]
dt[RECIPIENT_AGENCY_UPPER %like% 'UNICEF' | RECIPIENT_AGENCY_UPPER %like% 'UNITED NATIONS CHILDREN', 
   ELIM_CH := 1]
dt[RECIPIENT_AGENCY %like% 'UNITED NATIONS POPULATION FUND' | RECIPIENT_AGENCY_UPPER %like% 'UNFPA', 
   ELIM_CH := 1]
dt[RECIPIENT_AGENCY_UPPER %like% 'GLOBAL FUND TO FIGHT AIDS' | RECIPIENT_AGENCY_UPPER %like% 'UNITED NATIONS PROGRAMME ON HIV' | RECIPIENT_AGENCY_UPPER %like% 'FUND FOR THE GLOBAL FUND', 
   ELIM_CH := 1]
dt[RECIPIENT_AGENCY_UPPER %like% 'GLOBAL ALLIANCE FOR VACCINES' | RECIPIENT_AGENCY_UPPER %like% 'GAVI', 
   ELIM_CH := 1]
dt[RECIPIENT_AGENCY_UPPER %like% 'PAN AMERICAN HEALTH ORG', ELIM_CH := 1]
dt[RECIPIENT_AGENCY_UPPER %like% 'INTER AMERICAN DEVELOPMENT BANK', ELIM_CH := 1]
dt[RECIPIENT_AGENCY_UPPER %like% 'ASIAN DEVELOPMENT BANK', ELIM_CH := 1]
dt[RECIPIENT_AGENCY_UPPER %like% 'AFRICAN DEVELOPMENT BANK', ELIM_CH := 1]
dt[RECIPIENT_AGENCY_UPPER %like% 'BANK FOR RECONSTRUCTION AND DEVELOPMENT', ELIM_CH := 1]
dt[RECIPIENT_AGENCY_UPPER %like% 'COALITION FOR EPIDEMIC PREPAREDNESS INNOVATIONS', `:=`(
    CHANNEL = "CEPI", ELIM_CH = 1
)]
dt[RECIPIENT_AGENCY_UPPER %like% 'UNITED NATIONS POPULATION FUND', `:=`(
    CHANNEL = "UNFPA", ELIM_CH = 1
)]
rm(project_data)
#-----------------------# 

cat('  More misc formatting\n')
#----# More misc formatting #----# ####
dt[RECIPIENT_AGENCY == "GAVI" & YEAR == 1999, YEAR := 2000]
dt[, `:=`(INCOME_SECTOR = "PRIVATE", INCOME_TYPE = "FOUND", DONOR_NAME = "BMGF",
          DONOR_COUNTRY = "United States", ISO_CODE = "USA", SOURCE = "IRS 990s",
          CHANNEL = "BMGF")]
setnames(dt, 'outflow', 'OUTFLOW')
dt[RECIPIENT_AGENCY_UPPER %like% 'WORLD HEALTH ORGANIZATION' | RECIPIENT_AGENCY_UPPER %like% 'WORLD HEALTH ORGANISATION' | RECIPIENT_AGENCY_UPPER %like% 'WHO', 
   CHANNEL := 'WHO']
dt[RECIPIENT_AGENCY_UPPER %like% 'UNICEF' | RECIPIENT_AGENCY_UPPER %like% 'UNITED NATIONS CHILDREN', 
   CHANNEL := 'UNICEF']
dt[RECIPIENT_AGENCY %like% 'UNITED NATIONS POPULATION FUND' | RECIPIENT_AGENCY_UPPER %like% 'UNFPA', 
   CHANNEL := 'UNFPA']
dt[RECIPIENT_AGENCY_UPPER %like% 'GLOBAL FUND TO FIGHT AIDS' | RECIPIENT_AGENCY_UPPER %like% 'FUND FOR THE GLOBAL FUND', 
   CHANNEL := 'GFATM']
dt[RECIPIENT_AGENCY_UPPER %like% 'GLOBAL ALLIANCE FOR VACCINES' | RECIPIENT_AGENCY_UPPER %like% 'GAVI', 
   CHANNEL := 'GAVI']
dt[RECIPIENT_AGENCY_UPPER %like% 'UNITED NATIONS PROGRAMME ON HIV', 
   CHANNEL := 'UNAIDS']
dt[RECIPIENT_AGENCY_UPPER %like% 'PAN AMERICAN HEALTH ORG', CHANNEL := "PAHO"]
dt[RECIPIENT_AGENCY_UPPER %like% 'INTER AMERICAN DEVELOPMENT BANK', CHANNEL := 'IDB']
dt[RECIPIENT_AGENCY_UPPER %like% 'ASIAN DEVELOPMENT BANK', CHANNEL := 'AsDB']
dt[RECIPIENT_AGENCY_UPPER %like% 'AFRICAN DEVELOPMENT BANK', CHANNEL := 'AfDB']
dt[RECIPIENT_AGENCY_UPPER %like% 'BANK FOR RECONSTRUCTION AND DEVELOPMENT', CHANNEL := 'WB-IBRD']
dt[usaid_ngo==1 | is_ngo==1, CHANNEL := 'NGO']

#--------------------------------#

cat('  Read in historical INKIND data\n')
#----# Read in historical INKIND data #----# ####
inkind <- setDT(read_excel(paste0(get_path('BMGF', 'raw'), 'BMGF_INKIND_RATIO_1999_', dah.roots$report_year - 1, '.xlsx'), sheet = 'stata'))[, !c('INKIND_ORG')] # changed from previous report year to accomodate new data provided 5/3. Since we wanted to update estimates but do not have inkind for 2020
maxyr <- max(dt$YEAR, na.rm = TRUE)
if (nrow(inkind[YEAR == maxyr]) == 0) {
    stop(paste0("in-kind ratio is missing for the latest year (",
                maxyr, "), make sure the input is updated."))
}
rm(maxyr)

# Merge
dt <- merge(dt, inkind, by='YEAR', all=T)

# Calculate inkind & append
inkind_exp <- copy(dt)
inkind_exp[, OUTFLOW := OUTFLOW * INKIND_RATIO]
inkind_exp[, INKIND := 1]
inkind_exp[INKIND == 1, ELIM_CH := 0]
dt <- rbind(dt, inkind_exp, fill=T)
dt[is.na(INKIND), INKIND := 0]

setnames(dt, 'keywords', 'covid_flag')
dt[is.na(covid_flag), covid_flag := ""]
#------------------------------------------#

cat(' Find median commitment to disbursement ratios\n')
####--------# Find median commitment to disbursement ratios #------####
comm_disb <- copy(dt[covid_flag != "COVID-19"])
#need to flag multi year projects
#expected_starting date expected_completion_date
comm_disb[, expected_starting_date := as.Date(expected_starting_date,
                                              format= "%Y-%m-%d")]
comm_disb[, expected_completion_date := as.Date(expected_completion_date,
                                                format = "%Y-%m-%d")]
comm_disb[, start_yr := data.table::year(expected_starting_date)]
comm_disb[, end_yr := data.table::year(expected_completion_date)]
comm_disb[, yr_diff := end_yr - start_yr]
#Find by year commitment to disbursement ratio
comm_disb <- comm_disb[yr_diff == 0 | is.na(yr_diff),
                       .(commitment = sum(commitment, na.rm = TRUE),
                         OUTFLOW = sum(OUTFLOW, na.rm = TRUE)),
                       by = .(YEAR)]
comm_disb[, commitment := commitment*1000]
comm_disb[OUTFLOW != 0, comm_disb_ratio := commitment/OUTFLOW]
comm_disb[commitment != 0, disb_comm_ratio := OUTFLOW/commitment]


save_dataset(comm_disb, 'BMGF_comm_disb_ratio_by_year', 'BMGF','int')

#------------------------------------------# 

# Save COVID dataset and remove COVID projects from regular projects dataset
dt_covid <- dt[covid_flag %like% "COVID",
    c('YEAR', 'usaid_ngo', 'covid_flag', 'ISO3_RC', 'RECIPIENT_COUNTRY', 'OUTFLOW',
      'ELIM_CH', 'INCOME_SECTOR', 'INCOME_TYPE', 'DONOR_NAME', 'DONOR_COUNTRY',
      'ISO_CODE', 'SOURCE', 'CHANNEL', 'INKIND',
      names(dt)[names(dt) %like% 'RECIPIENT_AGENCY'],
      names(dt)[names(dt) %like% '_description'],
      "project_title"
      ), with=F]
save_dataset(dt_covid, paste0('BMGF_DISB_FINAL_1999_', dah.roots$report_year, '_covid'), 'BMGF', 'int') # remove COVID projects


cat('  Save dataset\n')
#----# Save final disbursement dataset #----# ####
dt <- dt[! covid_flag %like% "COVID-19"]
dt <- dt[, c('YEAR', 'usaid_ngo', 'covid_flag', 'ISO3_RC', 'RECIPIENT_COUNTRY', 'OUTFLOW', 'ELIM_CH', 'INCOME_SECTOR', 'INCOME_TYPE', 'DONOR_NAME', 'DONOR_COUNTRY', 'ISO_CODE', 
             'SOURCE', 'CHANNEL', 'INKIND', names(dt)[names(dt) %like% 'RECIPIENT_AGENCY'], names(dt)[names(dt) %like% '_description']), with=F]
dt[, covid_flag := NULL]
check <- dt %>%
  group_by(YEAR) %>%
  filter(ELIM_CH == 0) %>%
  summarize(OUTFLOW = sum(OUTFLOW, na.rm = T)) %>%
  mutate(fgh = "new")
plot(check$YEAR, check$OUTFLOW, type = "l")
save_dataset(dt, paste0('BMGF_DISB_FINAL_1999_', dah.roots$report_year, '_update'), 'BMGF', 'fin') # changed to report year later
old <- fread(get_path("BMGF", "fin", "BMGF_DISB_FINAL_1999_2023_update.csv", report_year = 2023))
check2 <- old %>%
  group_by(YEAR) %>%
  filter(ELIM_CH == 0) %>%
  summarize(OUTFLOW = sum(OUTFLOW, na.rm = T)) %>%
  mutate(fgh = "old")

plot(check2$YEAR, check2$OUTFLOW, type = "l", col = "blue", xlab = "X", ylab = "Values")
lines(check$YEAR, check$OUTFLOW, col = "red")
#------------------------#

cat('  Do a little more renaming & save Health_ADO Config\n')
#----# Do a little more renaming & save Health_ADO Config #----# ####
setnames(dt, 'OUTFLOW', 'DISBURSEMENT')
dt[, `:=`(DATA_SOURCE = "BMGF IRS 990s", DATA_LEVEL = "Project", FUNDING_AGENCY = "BMGF",
          FUNDING_AGENCY_SECTOR = "CSO", FUNDING_AGENCY_TYPE = "FOUND", FUNDING_COUNTRY = "United States",
          ISO3_FC = "USA", FUNDING_TYPE = "GRANT")]
save_dataset(dt, 'bmgf_pre_kws', 'BMGF', 'int', format = 'dta')

#--------------------------------------------------------------#