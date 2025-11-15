###############################################################################################
# Title: VACCINE_HFA_ASSIGN
# Purpose : splitting the funding for Vaccine projects by HFA from component description
###############################################################################################

#*********************************************************************#
#***********                    NOTES:                    ************#
# 02/25/22:
#           The data was downloaded from:
# https://www.worldbank.org/en/who-we-are/news/coronavirus-covid19/world-bank-support-for-country-access-to-covid-19-vaccines
#           copied "Project details" table from "Project Financing" page, just copy all table and paste on excel
#           The "short_description" is the "Development Objective" or "Abstract" for each "Project Financing" page
#           The "long_description" is the "Press Release" if available for each country
#           Data was manually copied and then saved to xlsx file 
#           * Unsure of the commitment value for Lebanon project_id = P176778
#*********************************************************************#
#----------------------------# ####

# System prep
rm(list=ls(all.names = TRUE))
start.time <- Sys.time()
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

# Variable prep
codes <- get_path("meta", "locs")
defl <- get_path("meta", "defl")
#----------------------------# ####

cat('\n\n')
cat(green(' #############################\n ####  VACCINE_HFA_Assign ####\n #############################\n\n'))


cat('  Read in data\n')
#----# Read in data #----# ####
vax <- fread(paste0(get_path('WB', 'int'), 'VAX_pre_kws.csv'))

cat('  Deflate\n')
#----# Deflate #----# ####
rates <- setDT(readstata13::read.dta13(paste0(defl, 'imf_usgdp_deflators_', dah.roots$defl_MMYY, '.dta')))[, c('YEAR', paste0('GDP_deflator_', dah.roots$report_year)), with=F]
vax[, YEAR := year(approval_date)]
vax[YEAR < 2020, YEAR := year(last_update_date)] # fix for lebanon 
vax <- merge(vax, rates, by='YEAR', all.x=T)
rm(rates)

# ## adjust commitments by days
vax[is.na(closing_date) | closing_date =='', closing_date := as.Date(paste0(dah.roots$report_year, "-12-31"))]
vax[, project_days_total := as.Date(closing_date, format='%Y-%m-%d') - as.Date(approval_date, format = '%Y-%m-%d')]
vax[, project_days_elapsed := as.Date('01/01/2022', format = '%m/%d/%Y') - as.Date(approval_date, format = '%Y-%m-%d')]
vax[, `:=` (vax_amount = vax_amount * .137267,
            idacommamt = idacommamt * .137267,
            ibrdcommamt = ibrdcommamt * .137267)]
# fix disbursements from both ibrd and ida
ibrd <- vax[ibrdcommamt > 0 & idacommamt > 0]
vax <- vax[!(ibrdcommamt > 0 & idacommamt > 0)]
ibrd[, `:=` (ibrdcommamt = as.numeric(gsub(',', '', ibrdcommamt)),
             idacommamt = as.numeric(gsub(',', '', idacommamt)))]

# split into two separate projects and calculate separate disbursement contributions
ida <- copy(ibrd)

ibrd <- ibrd[, `:=` (agreementtype = 'IBRD',
                     vax_amount = vax_amount * (ibrdcommamt / (ibrdcommamt + idacommamt)),
                     idacommamt = 0)]
ida <- ida[, `:=` (agreementtype = 'IDA',
                   vax_amount = vax_amount * (idacommamt / (ibrdcommamt + idacommamt)),
                   ibrdcommamt = 0)]


ibrd[, vax_amount := vax_amount / get(eval(paste0('GDP_deflator_20', dah.roots$abrv_year)))]
ida[, vax_amount := vax_amount / get(eval(paste0('GDP_deflator_20', dah.roots$abrv_year)))]
vax[, vax_amount := vax_amount / get(eval(paste0('GDP_deflator_20', dah.roots$abrv_year)))]
vax[, agreementtype := ifelse(idacommamt>0, "IDA","IBRD")]



# add back to regular dataset
vax <- rbind(vax, ibrd, ida)

rm(ibrd, ida)

cat('  ISO CODES\n')
#----# ISO CODES #----# ####
# fixing names to match main covid dataset
# Read in ISOs
isos <- setDT(readstata13::read.dta13(paste0(codes, 'countrycodes_official.dta')))[, c('country_lc', 'countryname_ihme', 'iso3')]
setnames(vax, old = c('project_id','country','vax_amount'), new = c('projid','country_lc','DAH'))
vax[country_lc == 'Macedonia, former Yugoslav Republic of', country_lc := 'Macedonia, the Former Yugoslav Republic of']
vax <- merge(vax, isos, by = 'country_lc', all.x = T)
setnames(vax, 'country_lc', 'countryname')
vax[countryname == "Eswatini", iso3 := 'SWZ']
vax[countryname == "South Sudan", iso3 := "SSD"]
vax[countryname == 'North Macedonia', iso3 := 'MKD']


rm(isos)

cat('  Split VAX Component datasets\n')
#----# Split VAX Component datasets #----# ####
# to keep the relative values for each component, we will split the dataset by the components and run KWS on each
comp1 <- copy(vax)
comp2 <- copy(vax)

comp1 <- covid_kws(comp1, keyword_search_colnames = c('component_1'),
                            keep_clean = T,
                            languages = 'english')
comp2 <- covid_kws(comp2, keyword_search_colnames = c('component_2'),
                   keep_clean = T,
                   languages = 'english')
comp1[, upper_component_1:=NULL]
comp2[, upper_component_2:=NULL]

comp1[, health_percentage := 1] # all the vaccine projects are 100% health related
comp2[, health_percentage := 1] # all the vaccine projects are 100% health related

## get covid program areas
to_use <- colnames(comp1)[colnames(comp1) %like% '_prop']
to_use <- gsub('_prop', '', to_use)
to_use_covid <- c("clc","rcce","sri","nl","ipc","cm",
                   "scl","hss","r&d","ett","other" )
## there should be no covid values
for (col in to_use_covid) {

  comp1[, eval(paste0(col, '_prop')) := 0]
  comp2[, eval(paste0(col, '_prop')) := 0]

}
## normalize fractions
for (col in to_use) {
  
  comp1[, eval(paste0(col, '_fraction')) := get(paste0(col, '_prop')) * health_percentage]
  comp2[, eval(paste0(col, '_fraction')) := get(paste0(col, '_prop')) * health_percentage]
  
}

## distribute disbursements
for (col in to_use) {
  
  comp1[, eval(paste0(col, '_DAH')) := get(paste0(col, '_fraction')) * component_1_dah * 1e6]
  comp2[, eval(paste0(col, '_DAH')) := get(paste0(col, '_fraction')) * component_2_dah * 1e6]
  
}
## fixing fractions for merge
for (col in to_use) {
  comp1[, eval(paste0(col, '_prop')) := get(paste0(col, '_prop')) * (component_1_dah/(component_1_dah+component_2_dah))]
  comp2[, eval(paste0(col, '_prop')) := get(paste0(col, '_prop')) * (component_2_dah/(component_1_dah + component_2_dah))]
  comp1[, eval(paste0(col, '_fraction')) := get(paste0(col, '_fraction')) * (component_1_dah/(component_1_dah + component_2_dah))]
  comp2[, eval(paste0(col, '_fraction')) := get(paste0(col, '_fraction')) * (component_2_dah/(component_1_dah + component_2_dah))]
  
}
# columns to collapse on
collapse_on <- names(vax)
temp <- rbind(comp1, comp2)

vax1 <- temp[,lapply(.SD, sum, na.rm=T), by=collapse_on, .SDcols=c(36:137)]

cat('  Fix column names for COVID datasets merge\n')
#----# Fix column names for COVID datasets merge #----# ####
# columns to keep to later append to main covid
names(vax1)
vax1[,`:=` (proj_name = paste0(YEAR, " COVID-VACCINE: ", borrower),
            projname = paste0(YEAR, " COVID-VACCINE: ", toupper(borrower)),
            abstract = paste0(component_1, component_2),
            lendinginstr = ifelse(money_type == "new money", 'Investment Project Financing','Development Policy Lending'),
            project_name = short_description,
            countryname = countryname_ihme,
            dateapproval = as.Date(approval_date,'%Y-%m-%d'),
            daterevclosng = as.Date(closing_date,'%Y-%m-%d'))]
vax1[,`:=` (dateapproval = format(dateapproval, '%m/%d/%Y'),
            daterevclosng = format(daterevclosng, '%m/%d/%Y'))]
vax1[, eval(paste0('DAH_',dah.roots$abrv_year)) := DAH]
vax1[, disbursement := DAH]
## grant loan
vax1[ida_credit == 0 & ida_grant == 0, grant_loan := 'loan']  #these are ibrd loans
vax1[ida_credit > 0 & ida_grant == 0, grant_loan := 'loan']
vax1[ida_credit == 0 & ida_grant > 0, grant_loan := 'grant']

## half IDA projects when credit and grant (this is where the halving earlier comes into play for ida projects with grant & loan money)
vax1[ida_credit > 0 & ida_grant > 0, grant_loan := 'grant']
setnames(vax1, old = c('short_description','iso3','component_1_dah', 'component_2_dah'), 
         new = c('description','ISO3_RC','component_1_DAH', 'component_2_DAH'))
# add income label
incgrps <- setDT(readstata13::read.dta13(paste0(codes, 'wb_historical_incgrps.dta')))[, c('ISO3_RC', 'YEAR', 'INC_GROUP')]
vax1 <- merge(vax1, incgrps, by = c('ISO3_RC', 'YEAR'), all.x = T)


dt <- vax1[,.SD, .SDcols = c('projid', 'proj_name', 'YEAR', 'ISO3_RC', 'projname', 'lendinginstr', 'status', 'project_name','commitment_amount','ibrdcommamt', 'idacommamt', 
                             'borrower', 'countryname', 'dateapproval', 'daterevclosng', 'DAH','INC_GROUP',
                             paste0('GDP_deflator_', dah.roots$report_year),
                             paste0('DAH_', dah.roots$abrv_year), 'abstract', 'disbursement', 
                             'component_1', 'component_2', 'component_1_DAH', 'component_2_DAH','ida_credit', 'ida_grant', 'description', 'agreementtype',
                             'money_type', 'grant_loan',names(vax1)[35:136])]

cat('  Save vaccine dataset\n')
#----# Save vaccine dataset #----# ####
save_dataset(dt, '2b_VACCINE_HFA_ASSIGN', 'WB', 'int')
#-------------------# ####
