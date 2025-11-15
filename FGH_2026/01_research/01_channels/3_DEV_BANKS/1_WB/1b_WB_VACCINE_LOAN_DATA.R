###############################################################################################
# Title: VACCINE_LOAN_DATA
# Purpose : getting the VACCINE loan data for COVID from WB projects 
###############################################################################################

#*********************************************************************#
#***********                    NOTES:                    ************#
#           The data was downloaded from:
# https://www.worldbank.org/en/who-we-are/news/coronavirus-covid19/world-bank-support-for-country-access-to-covid-19-vaccines
#           copied "Project details" table from "Project Financing" page, just copy all table and paste on excel
#           The "short_description" is the "Development Objective" or "Abstract" for each "Project Financing" page
#           The "long_description" is the "Press Release" if available for each country
#           Data was manually copied and then saved to xlsx file 
#*********************************************************************#
#----------------------------# ####

# System prep
rm(list=ls(all.names = TRUE))
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, dplyr)
source(paste0(dah.roots$k, "FILEPATH/get_location_metadata.R"))
source(paste0(code_repo, 'FUNCTIONS/helper_functions.R'))
pacman::p_load(ggpubr)
#----------------------------# ####

cat('\n\n')
cat(green(' #############################\n ####  VACCINE_LOAN_DATA  ####\n #############################\n\n'))


cat('  Read in RAW data\n')
#----# Read in COVID data #----# ####

dt_raw <- setDT(read_excel(paste0(get_path('WB', 'raw', report_year = 2021),
                                  'Update_COVID/WB_COVID_VACCINE_project_download_2022_01_13_update.xlsx'),
                           sheet = 1,col_names = F))

cat('  Cleaning Data\n')
#----# Cleaning Data #----# ####

# set the column names to be the values with column ...1==1
# values were copied and pasted from website table so they are stacked and have indicator 1 for rows which have labels
# and have indicator 2 for rows with the actual data values
col_names <- dt_raw[...1==1,]$...2 

# getting the data, removing rows with labels
dt <- dt_raw[...1==2,!c("...1")]
# pivoting the table so that now each project is on a row 
dt <- setDT(list(t(dt)))
# setting the names from the labels
names(dt) <- col_names
names(dt) <- tolower(gsub(" ","_", names(dt)))
# cleaning dates and commitment amount to millions
dt[, `:=` (total_project_cost = as.numeric(readr::parse_number(total_project_cost))*1e6,
           commitment_amount = as.numeric(readr::parse_number(commitment_amount))*1e6,
           idacommamt = as.numeric(readr::parse_number(idacommamt))*1e6,
           ibrdcommamt = as.numeric(readr::parse_number(ibrdcommamt))*1e6,
           approval_date = openxlsx::convertToDate(approval_date),
           last_update_date = openxlsx::convertToDate(last_update_date),
           closing_date = openxlsx::convertToDate(closing_date))]

# fixing missing approval dates
dt[project_id=="P177769" & country =="Burundi", approval_date:=as.Date("2021-12-22")]
dt[project_id=="P178255" & country =="Cameroon", approval_date:=as.Date("2021-12-23")]
dt[project_id=="P176778" & country =="Lebanon", approval_date:=as.Date("2021-01-20")]
dt[project_id=="P178279" & country =="Madagascar", approval_date:=as.Date("2021-06-24")]
dt[project_id=="P178068" & country =="Mozambique", approval_date:=as.Date("2021-12-14")]
dt[project_id=="P178181" & country =="Peru", approval_date:=as.Date("2021-12-20")]
dt[project_id=="P177884" & country =="Philippines", approval_date:=as.Date("2021-12-21")]
dt[project_id=="P177780" & country =="Tajikistan", approval_date:=as.Date("2021-12-21")]
dt[project_id=="P177956" & country =="Togo", approval_date:=as.Date("2021-12-21")]
dt[project_id=="P177273" & country =="Uganda", approval_date:=as.Date("2021-12-16")]

# Looked at the website and the graphics they had to check if the values of countries with commitment 0 had actual values
# some were given the value of the total_project_cost 
# DRC and Guyana were manually searched in the graphic
dt[commitment_amount==0, commitment_amount := total_project_cost]
dt[,`:=` (commitment_amount = as.numeric(commitment_amount))]
dt[project_id=="P176215" & country =="Congo, Democratic Republic of", commitment_amount:=200000000]
dt[project_id=="P176546" & country =="Guyana", commitment_amount := 5000000]

# summing up commitment for all years
dt[,eval(names(dt)[names(dt) %like% '_dah']) := lapply(.SD, FUN = function(x) as.numeric(x)),.SDcols = names(dt)[names(dt) %like% '_dah']]
dt[, vax_amount := rowSums(.SD, na.rm=T), .SDcols = names(dt)[names(dt) %like% '_dah']]
dt[, `:=` (vax_amount = vax_amount*1e6)]
# remove project P176778 since there is no data available for the commitment to the redcross
dt <- dt[ project_id != 'P176778']
#-------------------# ####

cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(dt, '1b_wb_vaccine_loan', 'WB', 'int')
#------------------------# ####

