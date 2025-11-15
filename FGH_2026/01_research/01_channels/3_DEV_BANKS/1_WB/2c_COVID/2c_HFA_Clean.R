
#*********************************************************************#
#***********                    NOTES:                    ************#
#     Append vaccine dataset
#           We were given the Commitment values for the vaccine delivery
#           for the WB projects in their vaccine efforts.
#*********************************************************************#


#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))
start.time <- Sys.time()
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)
#----------------------------# ####

data <- fread(paste0(get_path('WB', 'int'), 'COVID_post_kws.csv'))

## calculate health sector percentage
data[, `:=`(sector_name = 'health',
            health_percentage = 0)]

for (i in 1:5) {
  data[, eval(paste0('sector', i, '1')) := tstrsplit(get(paste0('sector', i)), '!', keep=1)]
  data[, eval(paste0('sector', i, '3')) := tstrsplit(get(paste0('sector', i)), '!', keep=3)]
  data[, eval(paste0('sector', i, '3')) := as.numeric(get(paste0('sector', i, '3')))]
  data[get(paste0('sector', i, '1')) %like% 'Health' | get(paste0('sector', i, '1')) %like% 'health', health_percentage := health_percentage + get(paste0('sector', i, '3'))]
  data[!(get(paste0('sector', i, '1')) %like% 'Health' | get(paste0('sector', i, '1')) %like% 'health'), eval(paste0('sector', i, '3')) := 0]
  data[, eval(paste0('sector', i, '1')) := NULL]
  data[, eval(paste0('sector', i, '3')) := NULL]
  data[, eval(paste0('sector', i)) := NULL]
}

# Calculate total percent
data[, health_percentage := health_percentage / 100]
data[ida_grant > 0 & ida_credit > 0, health_percentage := health_percentage * 0.5] #this will half all the values so we can duplicate for half loan and half grant

## get covid program areas
to_use <- colnames(data)[colnames(data) %like% '_prop']
to_use <- gsub('_prop', '', to_use)

## normalize fractions
for (col in to_use) {
  
  data[, eval(paste0(col, '_fraction')) := get(paste0(col, '_prop'))]
  
}

## distribute disbursements
for (col in to_use) {
  
  data[, eval(paste0(col, '_DAH')) := get(paste0(col, '_fraction')) * get(paste0('DAH_',dah.roots$abrv_year))]
  
}

## update DAH_20 and commitments
data[, c('DAH', paste0('DAH_',dah.roots$abrv_year), 'idacommamt', 'ibrdcommamt', 'disbursement') := .SD * health_percentage,
     .SDcols = c('DAH',paste0('DAH_',dah.roots$abrv_year), 'idacommamt', 'ibrdcommamt', 'disbursement')]

## new/repurposed money
data[, money_type := ifelse(lendinginstr == 'Development Policy Lending', 'repurposed money', 'new money')]

## grant loan
data[ida_credit == 0 & ida_grant == 0, grant_loan := 'loan']  #these are ibrd loans
data[ida_credit > 0 & ida_grant == 0, grant_loan := 'loan']
data[ida_credit == 0 & ida_grant > 0, grant_loan := 'grant']

## half IDA projects when credit and grant (this is where the halving earlier comes into play for ida projects with grant & loan money)
data[ida_credit > 0 & ida_grant > 0, grant_loan := 'grant']
temp <- data[ida_credit > 0 & ida_grant > 0]
temp[, grant_loan := 'loan']
data <- rbind(data, temp)

cat('  Append vaccine dataset\n')
#----# Append vaccine dataset #----# ####
vax <- fread(paste0(get_path('WB', 'int'), '1a_vax.csv'))
vax2b <- fread(paste0(get_path('WB', 'int'), '2b_VACCINE_HFA_ASSIGN.csv'))

vax2b[, description_all := paste(project_name, abstract, component_1, component_2, description, sep = ". ")]

dt <- merge(vax, vax2b[, .(projid, idacommamt, ibrdcommamt, money_type, grant_loan, description_all)], by='projid', all.x = T)
dt <- merge(dt, unique(data[, .(projid, idacommamt, ibrdcommamt, money_type, grant_loan)], by='projid'), by='projid', all.x = T)

dt[,`:=`(idacommamt = ifelse(is.na(idacommamt.x),idacommamt.y, idacommamt.x),
         ibrdcommamt = ifelse(is.na(ibrdcommamt.x),ibrdcommamt.y, ibrdcommamt.x),
         money_type = ifelse(is.na(money_type.x),money_type.y, money_type.x),
         grant_loan = ifelse(is.na(grant_loan.x),grant_loan.y, grant_loan.x))]

dt[, eval(names(dt)[names(dt)%like%"\\."]) := NULL]

dt[is.na(money_type), money_type := ifelse(mpa_status %like% "^AF", "new money", 'repurposed money')]
dt[is.na(idaprop) & idacommamt > 0 & ibrdcommamt > 0, `:=`(idaprop = .5, ibrdprop=.5)]
dt[is.na(idaprop) & idacommamt > 0 & ibrdcommamt == 0, `:=`(idaprop = 1, ibrdprop=0)]
dt[is.na(idaprop) & idacommamt == 0 & ibrdcommamt > 0, `:=`(idaprop = 0, ibrdprop=1)]

desc <- fread(get_path('WB', 'raw', 'WB_project_download_2022_01_25.csv',
                       report_year = 2023))
setnames(desc, c('id','projectstatusdisplay'), c('projid','status'))

dt1 <- merge(dt, desc[, .(projid, countryname, curr_total_commitment, curr_ibrd_commitment, curr_ida_commitment, grantamt)], by='projid', all.x = T)
dt1[, grant_loan := ifelse(is.na(grant_loan) & grantamt > 0, "grant", "loan")]
dt1[is.na(idaprop) & curr_ida_commitment > 0 & curr_ibrd_commitment > 0, `:=`(idaprop = .5, ibrdprop=.5)]
dt1[is.na(idaprop) & curr_ida_commitment > 0 & curr_ibrd_commitment == 0, `:=`(idaprop = 1, ibrdprop=0)]
dt1[is.na(idaprop) & curr_ida_commitment == 0 & curr_ibrd_commitment > 0, `:=`(idaprop = 0, ibrdprop=1)]
dt1[is.na(idaprop) & curr_ida_commitment == 0 & curr_ibrd_commitment == 0, `:=`(idaprop = 1, ibrdprop=0)]
dt1[idaprop > 0  & ibrdprop > 0, `:=`(idaprop = .5, ibrdprop=.5)]
dt1[idaprop > 0  & ibrdprop > 0, eval(c('commitment_vax','disbursement_vax','vax_other_1','vax_other_2','vax_other','not_DAH', 
                                        'vax_mobil', 'vax_comm','vax_sc','vax_coord', 'vax_safety','vax_m&e')) := lapply(.SD, function(x) x * .5),
    .SDcols = c('commitment_vax','disbursement_vax', 'vax_other_1','vax_other_2','vax_other', 'not_DAH', 'vax_mobil', 
                'vax_comm','vax_sc','vax_coord', 'vax_safety','vax_m&e')]

ida <- dt1[idaprop > 0  & ibrdprop > 0,]
ibrd <- dt1[idaprop > 0  & ibrdprop > 0,]
dt1 <- dt1[!(idaprop > 0  & ibrdprop > 0),]

dt1[, agreementtype := ifelse(curr_ida_commitment>0 & agreementtype == "", "IDA", "IBRD")]
ida[, agreementtype := "IDA"] 
ibrd[, agreementtype := "IBRD"]

dt1 <- rbind(dt1, ida)
dt1 <- rbind(dt1, ibrd)

dt1[, eval(c('commitment_vax','disbursement_vax','vax_other_1','vax_other_2','vax_other','not_DAH', 
             'vax_mobil', 'vax_comm','vax_sc','vax_coord', 'vax_safety','vax_m&e')) := nafill(.SD, fill = 0), 
    .SDcols=c('commitment_vax','disbursement_vax', 'vax_other_1','vax_other_2','vax_other', 'not_DAH', 'vax_mobil', 
              'vax_comm','vax_sc','vax_coord', 'vax_safety','vax_m&e')]
dt1[, other := rowSums(.SD, na.rm = T), .SDcols=c('vax_other_1', 'vax_other_2', 'vax_other') ]
dt1[, `:=` (vax_other_1=NULL, vax_other_2=NULL, vax_other=other,
            `vax_r&d`=0, vax_hygiene=0, vax_waste=0, vax_hr=0, vax_ta=0, vax_delivery=0)]
vax_cols <- names(dt1)[names(dt1) %like% 'vax_']

names(dt1)[names(dt1) %ni% names(data)]

codes <- get_path("meta", "locs")
isos <- setDT(readstata13::read.dta13(paste0(codes, 'countrycodes_official.dta')))[, c('country_lc', 'countryname_ihme', 'iso3')]

dt1 <- merge(dt1, isos, by='country_lc',all.x = T)

dt1[country_lc=='CAR', `:=` (countryname_ihme = 'Central African Republic', iso3='CAF')]
dt1[country_lc %like% 'Cabo Verd' & is.na(countryname_ihme), `:=` (countryname_ihme = 'Cape Verde', iso3 = 'CPV')]
dt1[country_lc %like% 'Eswat' & is.na(countryname_ihme), `:=` (countryname_ihme = 'Swaziland', iso3 = 'SWZ')]
dt1[country_lc %like% 'Ethio' & is.na(countryname_ihme), `:=` (countryname_ihme = 'Ethiopia', iso3 = 'ETH')]
dt1[country_lc %like% 'Guatem' & is.na(countryname_ihme), `:=` (countryname_ihme = 'Guatemala', iso3 = 'GTM')]
dt1[country_lc %like% 'Iran' & is.na(countryname_ihme), `:=` (countryname_ihme = 'Iran', iso3 = 'IRN')]
dt1[country_lc %like% 'Maurita' & is.na(countryname_ihme), `:=` (countryname_ihme = 'Mauritania', iso3 = 'MRT')]
dt1[country_lc %like% 'North Maced' & is.na(countryname_ihme), `:=` (countryname_ihme = 'North Macedonia', iso3 = 'MKD')]
dt1[country_lc %like% 'Philip' & is.na(countryname_ihme), `:=` (countryname_ihme = 'Philippines', iso3 = 'PHL')]
dt1[country_lc %like% 'Sao Tom' & is.na(countryname_ihme), `:=` (countryname_ihme = 'Sao Tome and Principe', iso3 = 'STP')]
dt1[country_lc %like% 'Sierra Leo' & is.na(countryname_ihme), `:=` (countryname_ihme = 'Sierra Leone', iso3 = 'SLE')]
dt1[country_lc %like% 'Sri Lan' & is.na(countryname_ihme), `:=` (countryname_ihme = 'Sri Lanka', iso3 = 'LKA')]
dt1[country_lc %like% 'Suda' & is.na(countryname_ihme), `:=` (countryname_ihme = 'Sudan', iso3 = 'SDN')]
dt1[country_lc %like% 'Togo' & is.na(countryname_ihme), `:=` (countryname_ihme = 'Togo', iso3 = 'TGO')]
dt1[country_lc %like% 'Democratic Republi' & is.na(countryname_ihme), `:=` (countryname_ihme = 'Democratic Republic of the Congo', iso3 = 'COD')]
dt1[country_lc %like% 'ikistan' & is.na(countryname_ihme), `:=` (countryname_ihme = 'Tajikistan', iso3 = 'TJK')]
dt1[country_lc %like% 'ongo, Democ' & is.na(countryname_ihme), `:=` (countryname_ihme = 'Democratic Republic of the Congo', iso3 = 'COD')]

setnames(dt1, c('countryname_ihme','iso3'),c('countryname','ISO3_RC'))

# disbursement here is only that amount associated to vax in DAH that was committed from the total commitment
# we now have some disbursement values, for places where disbursement was not available, we applied median of the ones that did have
dt1[, VACC_disb := rowSums(.SD, na.rm = T), .SDcols=vax_cols]
dt1[, disbursement := disbursement_vax]
# converting disbursement to 1 where it is 0 just to make division work, will return to 0 later
dt1[VACC_disb == 0,`:=` (VACC_disb = 1, flag = 1)]
dt1[, eval(paste0(vax_cols,"_prop")) := lapply(.SD, function(x) x / VACC_disb), .SDcols=vax_cols]
dt1[, eval(paste0(vax_cols,"_fraction")) := lapply(.SD, function(x) x / VACC_disb), .SDcols=vax_cols]
dt1[, eval(paste0(vax_cols,"_DAH")) := lapply(.SD, function(x) x * disbursement), .SDcols=paste0(vax_cols,"_prop")]
dt1[, eval(paste0(vax_cols)) := lapply(.SD, function(x) round(x * 100)), .SDcols=paste0(vax_cols,"_prop")]
dt1[, COVID_total := rowSums(.SD, na.rm = T), .SDcols=vax_cols]
dt1[, COVID_total_prop := rowSums(.SD, na.rm = T), .SDcols=paste0(vax_cols,"_prop")]
dt1[, COVID_total_fraction := rowSums(.SD, na.rm = T), .SDcols=paste0(vax_cols,"_fraction")]
# have to return disbursement back to 0
dt1[flag == 1, `:=` (VACC_disb = 0)]
dt1[, COVID_total_DAH := disbursement]
dt1[, DAH := disbursement]
dt1[, DAH_21 := disbursement]
dt1[, flag := NULL]

covid_cols <- c("clc","rcce","sri","nl","ipc","cm","scl","hss","r&d","ett","other")
dt1[, eval(covid_cols) := NaN]
dt1[, eval(covid_cols) := lapply(.SD, function(x) as.numeric(x)), .SDcols=covid_cols]
dt1[, eval(covid_cols) := nafill(.SD, fill = 0), .SDcols=covid_cols]
dt1[, eval(paste0(covid_cols,'_prop')) := lapply(.SD, function(x) x * 0), .SDcols=covid_cols]
dt1[, eval(paste0(covid_cols,'_fraction')) := lapply(.SD, function(x) x * 0), .SDcols=covid_cols]
dt1[, eval(paste0(covid_cols,'_DAH')) := lapply(.SD, function(x) x * 0), .SDcols=covid_cols]

# checking that there are no vaccine values in General Covid data
print(data[, VACC := rowSums(.SD, na.rm=T), .SDcols=names(data)[names(data) %like% "^vax_" & names(data) %like% "_DAH"]] %>% 
       .[, lapply(.SD, sum, na.rm=T), by='YEAR', .SDcols='VACC'])

# fixing negative values
# need to get first all projec id that have negative values and check that we can subtract to previous year
# in 2021 only 1 project had negative values P122629
neg_ids <- unique(data[get(paste0("DAH_", dah.roots$abrv_year)) < 0]$projid)

negs <- copy(data)
data <- data[!(projid %in% neg_ids)]
negs <- negs[projid %in% neg_ids]

negs[, eval(names(data)[names(data) %like% "DAH"]) := lapply(.SD, function(x) x + shift(x, n = 1, type = 'lead')), .SDcols = names(data)[names(data) %like% "DAH"]]
negs <- negs[!is.na(get(paste0('DAH_', dah.roots$abrv_year)))]
data <- rbind(negs, data)
data <- rbind(data, dt1[YEAR <= dah.roots$report_year], fill=T)

chart_path <- get_path("wb", "output")

cat('  Save covid prepped dataset\n')

#----# Save dataset #----# ####
save_dataset(data, 'COVID_cleaned', 'WB', 'int')
#-------------------# ####

## checking runtime for slurm optimazation purpose
end.time <- Sys.time()
time.taken <- end.time - start.time

cat('\n\n')
cat(green(' #####################################\n',paste0('####          END OF 2c          ####\n')))
time.taken
cat(green('#####################################\n\n'))

  
