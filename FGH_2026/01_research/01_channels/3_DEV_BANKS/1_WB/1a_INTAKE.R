#----# Docstring #----# ####
# Project:  FGH
# Purpose:  WB Intake & cleaning data
#---------------------# 
#----------------------------#

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}


report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, dplyr)

# Variable prep
codes <- get_path("meta", "locs")
defl <- get_path("meta", "defl")
#----------------------------#


cat('\n\n')
cat(green(' ###########################\n'))
cat(green(' #### WB INTAKE & CLEAN ####\n'))
cat(green(' ###########################\n\n'))


cat('  Read in disbursement data\n')
#----# Read in disb data #----# ####

# Use data processed in FGH 2023, which used the last received correspondent data
disb <- fread(get_path("wb", "raw", "wb_corr_data_processed_2023.csv",
                       report_year = 2023))
disb[`project descr` != "", `Proj Name` := paste0(`Proj Name`, ' ', `project descr`)]
disb[, `2022` := NULL] ## incomplete data for 2022 when this file was received
disb[, `2023` := NULL] ## and we no longer want the 2023 estimates from this file
colnames(disb) <- tolower(colnames(disb))
setnames(disb, c('proj id', 'proj name', 'date, approval', 'date, rev closng', 'proj stat', 'lending instr', 'agreement type'),
         c('projid', 'projname', 'dateapproval', 'daterevclosng', 'projstat', 'lendinginstr', 'agreementtype'))


yr_cols <- grep("\\d+", colnames(disb), value = TRUE)
setnafill(disb, fill = 0, cols = yr_cols)

disb <- disb[country != 'Result' & region != 'Overall Result']
#-----------------------------# ####

cat('  Redistribute Africa multi-country projects\n')
#----# Redistribute Africa multi-country projects #----# ####
afr_multi <- copy(disb)
afr_projid <- unique(disb[country == 'Africa']$projid)
afr_multi <- afr_multi[projid %in% afr_projid]
afr_multi <- afr_multi[order(projid, country)]
# 
# sum same projects/countries with different region codes (AFR, AFE, AFW)
afr_multi[, region := NULL]
to_remain <- colnames(afr_multi)[! colnames(afr_multi) %in% yr_cols]

afr_multi <- afr_multi %>%
  group_by_at(.vars = to_remain) %>%
  summarize_at(.vars = yr_cols,
               .funs = sum)
afr_multi <- setDT(afr_multi)

for (col in yr_cols) {
  afr_multi[country == 'Africa', eval(paste0('africa1_', col)) := sum(get(col), na.rm=T), by=c('projid', 'country')]
  afr_multi[, eval(paste0('africa_', col)) := sum(get(paste0('africa1_', col)), na.rm=T), by='projid']
  afr_multi[, eval(paste0('africa1_', col)) := NULL]
}
afr_multi <- afr_multi[country != 'Africa']
afr_multi[, dummy := 1]
afr_multi[, num := sum(dummy), by='projid']
afr_multi[, dummy := NULL]
for (col in yr_cols) {
  afr_multi[, eval(paste0('t_', col)) := sum(get(col), na.rm=T), by='projid']
  afr_multi[, eval(paste0('f_', col)) := (get(col) / get(paste0('t_', col))) * get(paste0('africa_', col))]
  afr_multi[get(paste0('africa_', col)) != 0 & is.na(get(paste0('f_', col))), eval(paste0('f_', col)) := get(paste0('africa_', col)) / num]
  afr_multi[, eval(paste0('ff_', col)) := get(col) + get(paste0('f_', col))]
  afr_multi[, eval(col) := get(paste0('ff_', col))]
  afr_multi[, eval(paste0('f_', col)) := NULL]
  afr_multi[, eval(paste0('ff_', col)) := NULL]
  afr_multi[, eval(paste0('africa_', col)) := NULL]
  afr_multi[, eval(paste0('t_', col)) := NULL]
}
afr_multi[, num := NULL]
afr_multi[, region := 'AFR']
# Drop projects from disb & append
disb <- disb[! projid %in% afr_projid]
disb <- rbind(disb, afr_multi)
rm(afr_multi, to_remain, afr_projid)
#------------------------------------------------------# ####

cat('  Read in project description data & merge\n')
#----# Read in project description data & merge #----# ####
desc <- fread(get_path('WB', 'raw', 'WB_project_download_2022_01_25.csv',
                       report_year = 2023))
setnames(desc, c('id','projectstatusdisplay'), c('projid','status'))
desc <- setDT(distinct(desc, projid, .keep_all = T))
desc[, m_m := 1]
disb[, u_m := 2]
desc <- merge(desc, disb, by = 'projid', all = T)
desc[, merge := rowSums(desc[, c('u_m', 'm_m')], na.rm=T)]
desc <- desc[merge %in% c(2, 3), !c('merge', 'u_m', 'm_m')]
# Random cleanup from where Stata merge is funky
desc[, lendinginstr.y := NULL]
setnames(desc, 'lendinginstr.x', 'lendinginstr')
desc[is.na(lendinginstr), lendinginstr := '#'] # Recoding this one row where the Stata merge would have filled the data
rm(disb)
#----------------------------------------------------# ####

cat('  Add WB Themes\n')
#----# Add WB Themes #----# ####
# in 2021 the Project download file didnt provide the theme breakdown so had to webscrape it from urls
themes <- fread(get_path('WB', 'raw', 'WB_project_themes_2022_01_25.csv',
                         report_year = 2023))
setnames(themes, old = 'proj_url', new = 'url')
# removing percent sign from values
themes[, theme_percentage := as.numeric(stringr::str_replace(theme_percentage,"%",""))]
# WB themes as of 01/25/2022, might need update
wbthms <- c('HIV/AIDS', 'Malaria', 'Tuberculosis', 'Neglected tropical diseases', 'Non-communicable diseases'
            , 'Pandemic Response','Health System Strengthening', 'Health Service Delivery'
            , 'Health Finance', 'Private Sector Delivery in Health', 'Reproductive and Maternal Health'
            , 'Adolescent Health', 'Child Health', 'Nutrition')

# some projects have over 100%, so take this value as a sum and then create weighted/actual value
themes[, `:=` (tot_pcnt = sum(theme_percentage, na.rm = T)), by = c('projid','url')]
themes[, pcnt_actual := ifelse(tot_pcnt>0, theme_percentage/tot_pcnt*100,theme_percentage)]
# some projects have 0 total but still have themes to them, count to assign equal values to themes
themes[, HFA_count := .N , by = c('projid','url')]
themes[, pcnt_actual := ifelse(pcnt_actual>0, pcnt_actual, 1/HFA_count*100)]
# we only care about Health related themes in list wbthms, other themes merged
themes[!(theme %in% wbthms), theme := "Other HFA"]
themes <- themes[,lapply(.SD, sum, na.rm=T), by=c("projid","url", "theme"), .SDcols=c("pcnt_actual")]
# creating a count to create "theme#" column name after pivot data to match previous year's data
themes[ , `:=`( COUNT = .N , IDX = seq_len(.N) ) , by = c('projid','url')]
themes[,`:=` (pcnt_actual = round(pcnt_actual),
              IDX = paste0("theme",IDX) ,COUNT = NULL)]
themes[, values := paste0(theme,"!$!",pcnt_actual,"!$!",pcnt_actual)]
themes[,`:=` (theme=NULL, pcnt_actual=NULL)]
# converting data from long to wide
themes <- dcast(themes, projid + url ~ IDX, value.var = 'values')
# merge into desc
desc[,`:=` (theme1=NULL, theme2=NULL)]
desc <- merge(desc, themes, by = c('projid','url'), all.x = T)
rm(themes)
#----------------------------------------------------# ####

cat('  Add ISO Codes\n')
#----# Add ISO Codes #----# ####
desc[, countryname := NULL]
setnames(desc, 'country', 'country_lc')
desc[country_lc == 'Venezuela, Republica Bolivariana de', country_lc := 'Venezuela']
desc[country_lc == 'Macedonia, former Yugoslav Republic of', country_lc := 'Macedonia, the Former Yugoslav Republic of']
desc[country_lc == 'Cabo Verde', country_lc := 'Cape Verde']
desc[, m_m := 1]
# Read in ISOs
isos <- fread(paste0(codes, 'countrycodes_official.csv'))[, c('country_lc', 'countryname_ihme', 'iso3')]
isos[, u_m := 2]
# Merge
desc <- merge(desc, isos, by = 'country_lc', all = T)
desc[, merge := rowSums(desc[, c('u_m', 'm_m')], na.rm = T)]
setnames(desc, 'country_lc', 'countryname')
desc[countryname == "Eswatini", iso3 := 'SWZ']
desc[countryname == "South Sudan", iso3 := "SSD"]
desc <- desc[merge %in% c(1,3), !c('merge', 'u_m', 'm_m')]
rm(isos)
save_dataset(desc, '1a_desc_disb', 'WB', 'int')mes.
# themes were gathered from webscraper
# Using the values from last year for the available projects
cat('  Add sectors\n')
#----# Add sectors #----# ####
desc_old <- setDT(fread(paste0('/share/resource_tracking/DAH/CHANNELS/3_DEV_BANKS/1_WB/DATA/INT/FGH_2020/1a_desc_disb.csv')))
# keep only the identifier columns to merge the sectors into current data
# there are multiple values for the same year and projectid so just get unique 
desc_old <- desc_old[, .SD, .SDcols=c("projid","regionname","url",
                                      names(desc_old)[names(desc_old) %like% paste0('sector')])]
# remove current sector columns as they dont have % numbers
desc <- desc[, !c('sector1','sector2','sector3')]
desc_old <- unique(desc_old, by=c("projid","regionname","url"))
desc <- merge(desc, desc_old, by=c("projid","regionname","url"), all.x = T)
rm(desc_old)

#----------------------------------------------------# ####

cat('  Misc. cleaning\n')
#----# Misc. cleaning #----# ####
data <- copy(desc)
data <- data[, c('projid', 'projname', 'lendinginstr', 'status', 'project_name', 'curr_ibrd_commitment','curr_ida_commitment','borrower', 'impagency',
                 'countryname', 'iso3', 'dateapproval', 'daterevclosng', 'agreementtype',
                 yr_cols, names(data)[names(data) %like% 'theme'], # 2021 ibrdcommamt > curr_ibrd
                 names(data)[names(data) %like% 'sector']), with=F]
setnames(data, c('curr_ibrd_commitment','curr_ida_commitment'), c('ibrdcommamt','idacommamt'))
data <- data[, !c(names(data)[names(data) %like% 'mj'], names(data)[names(data) %like% 'major']), with = F]

# Reshape long
data <- melt.data.table(data,
                        measure.vars = c(yr_cols),
                        variable.name = 'YEAR',
                        variable.factor = F,
                        value.name = 'DAH')
setnames(data, 'iso3', 'ISO3_RC')
data[, YEAR := as.numeric(YEAR)]

# FGH 2017 - WB recoded project health disbursements from 2001 onward, however, DAH prior to 2001 need to be adjusted downward
# the analysis performed in WB_Data_Explortation_FGH17v3.do show that between 2001 and 2005, the recoding adjusted the projects downward by 0.22%
# thus we manually adjust all project health disbursements prior to 2001 downward 0.22084%
data[YEAR < 2001, DAH := DAH * (1-0.0022084)]

data[countryname == 'Kosovo', ISO3_RC := 'XKX']
data[countryname == 'North Macedonia', ISO3_RC := 'MKD']
rm(desc)
#--------------------------# ####

cat('  Drop high income countries & emergency loans\n')
#----# Drop high income countries & emergency loans #----# ####
incgrps <- fread(paste0(codes, 'wb_historical_incgrps.csv'))[, c('ISO3_RC', 'YEAR', 'INC_GROUP')]
if (max(incgrps$YEAR) < dah.roots$report_year) {
    warning('INC_GROUP data is not up to date, replicating for report year')
    incgrps <- rbind(
        incgrps,
        incgrps[YEAR == dah.roots$report_year - 1][, YEAR := dah.roots$report_year]
    )
}

incgrps[, u_m := 2]
data[, m_m := 1]
# Merge datasets
data <- merge(data, incgrps, by=c('ISO3_RC', 'YEAR'), all = T)
data[, merge := rowSums(data[, c('u_m', 'm_m')], na.rm = T)]
data <- data[merge %in% c(1,3), !c('merge', 'u_m', 'm_m')]
# Recode
data[countryname == 'Kosovo', INC_GROUP := 'UM']
# Drop high income
data[is.na(INC_GROUP), INC_GROUP := '']
data <- data[INC_GROUP != 'H']

# Drop emergency loans
data <- data[lendinginstr != 'Emergency Recovery Loan']

data <- data[agreementtype %in% c('IDA', 'IBRD')]
rm(incgrps)
#--------------------------------------------------------# ####

# / start covid creation
#
cat('  Isolate covid data\n')
#-------------------# ####

# run covid intercept to flag covid projects
# first fix the encoding for project_name for toupper to work
data[, upper_project_name := string_to_std_ascii(project_name, pad_char = "")]
data <- covid_intercept(data,
                        year_colname = 'YEAR',
                        keyword_search_colnames = "upper_project_name")
data[, `:=` (project_name = upper_project_name,
             project_name_srch = upper_project_name_srch)]
data[, `:=` (upper_project_name = NULL,
             upper_project_name_srch = NULL)]
## manually flag covid projects that aren't being flagged
data[projid %in% c('P174118', 'P173796') & YEAR == 2020,
     project_name_srch := 1]
data[project_name_srch==0 & YEAR>2019 & 
            (grepl("COVID ", project_name)| grepl("CORONAVIRUS ", project_name)|
               grepl("SARS ", project_name)|grepl("NCOV", project_name)), project_name_srch := 1]
save_dataset(data, '1a_pre_covid', 'WB', 'int')


cat('  Fixing Vaccine Double Counting\n')
#----# Fixing Vaccine Double Counting #----# ####

# read in vaccine data 
wb <- fread(paste0(get_path('WB', 'raw', report_year = 2021),
                   'Update_COVID/WB MPA Funding Allocations-as of 03242022 _draft.csv'))
# remove extra empty columns
wb <- wb[, !c('V7', 'V9')]
# setting names and updating values to actual millions USD
colnames(wb) <- c('number_id', 'mpa_status', 'country_lc', 'projid', 'commitment_vax', 'vax_other_1','vax_other_2','vax_other', 
                  'not_DAH', 'vax_mobil', 'vax_comm','vax_sc','vax_coord', 'vax_safety','vax_m&e')
wb[, eval(c('commitment_vax','vax_other_1','vax_other_2','vax_other', 
            'not_DAH', 'vax_mobil', 'vax_comm','vax_sc','vax_coord', 'vax_safety','vax_m&e')) := lapply(.SD, function(x) x * 1e6),
   .SDcols = c('commitment_vax', 'vax_other_1','vax_other_2','vax_other', 
               'not_DAH', 'vax_mobil', 'vax_comm','vax_sc','vax_coord', 'vax_safety','vax_m&e')]

# read in update
vu <- fread(paste0(get_path('WB', 'raw', report_year = 2021),
                   'Update_COVID/WB_Weekly_Report_June_22_2022_VAX.csv'))
# integrate update to WB projects, changing value for commitments if they were different from current values
vu <- vu[,.(`Project ID`,Country, `IDA / IBRD`, `Approval Date`, Signing, Effectiveness, `Vaccine commitments`, `Disb ($mi)`)]
colnames(vu) <- c('projid', 'country_lc',"ida_ibrd", 'approve_date','sign_date', 'start_date', 'commitment_vax_u', 'disbursement_vax')
# remove rows for totals
vu <- vu[!(country_lc %like% 'Total')]
# make commitment columns approrpiate type and values
vu[, c('commitment_vax_u', 'disbursement_vax') := lapply(.SD, function(x) as.numeric(gsub("\\$", "", x))), .SDcols = c('commitment_vax_u', 'disbursement_vax')]
vu[, `:=` (commitment_vax_u = commitment_vax_u * 1e6, disbursement_vax = disbursement_vax * 1e6)]
dist_comm_ratio <- median(vu$disbursement_vax/vu$commitment_vax_u, na.rm = T)
vu[start_date == "" & disbursement_vax == 0,
   start_date := approve_date]
# temporary merge to fix commitment where they are not the same
temp <- merge(wb, vu[,.(projid, country_lc, commitment_vax_u, disbursement_vax, start_date)],
              by=c('projid', 'country_lc'), all.x = T)

temp[, YEAR := year(as.Date(start_date, format = '%m/%d/%Y'))]
## determine year for projects with missing start date
## get min year during COVID that a project had a disbursement - NAs expected
## where a project doesn't meet these criteria
yr <- data[DAH > 0,
           .(yr = as.integer(min(YEAR[which(YEAR > 2019)]), na.rm = TRUE)),
           by = projid]
temp <- merge(temp, yr, by='projid', all.x = T)
temp[is.na(YEAR), YEAR := yr]
## if still missing year...
temp[is.na(YEAR), YEAR := 2022] # since hadn't started yet in early 2022

temp[!is.na(commitment_vax_u) & commitment_vax != commitment_vax_u, `:=` (commitment_vax = commitment_vax_u)]
temp[is.na(disbursement_vax), `:=` (disbursement_vax = commitment_vax * dist_comm_ratio)]
temp[, commitment_vax_u := NULL]
wb <- copy(temp)
rm(temp)
#
# merging and updating the commitment values and ida and ibrd
#
dt <- merge(data[YEAR <= 2022], wb, by=c('projid', 'YEAR'), all= TRUE)
dt[,`:=` (commitment_test = idacommamt + ibrdcommamt,
          idaprop = idacommamt /(idacommamt + ibrdcommamt),
          ibrdprop = ibrdcommamt / (idacommamt + ibrdcommamt))]
dt[, test := commitment_test-commitment_vax]
dt[test < 0, test:=0]
dt[!is.na(test), `:=` (idacommamt = test*idaprop, ibrdcommamt = test * ibrdprop) ]
# setting the vax and data datasets to continue code
vax <- dt[,.SD, .SDcols=c('number_id', 'mpa_status', 'country_lc', 'projid', 'commitment_vax','disbursement_vax', 'vax_other_1','vax_other_2','vax_other', 
                            'not_DAH', 'vax_mobil', 'vax_comm','vax_sc','vax_coord', 'vax_safety','vax_m&e','idaprop','ibrdprop','agreementtype',
                            'YEAR', 'project_name_srch')]

vax <- unique(vax[!is.na(mpa_status)], by=c('projid', 'YEAR'))

data <- dt[, .SD, .SDcols=colnames(data)]
data <- data[!is.na(project_name_srch)]


covid_data <- copy(data)
covid_data <- covid_data[project_name_srch == 1]
covid_data[, project_name_srch := NULL]

# manually input North Macedonia iso code
covid_data[countryname == 'North Macedonia', ISO3_RC := 'MKD']

# remove covid data from general pipeline
data <- data[project_name_srch == 0 & projid %ni% unique(covid_data$projid)]
data[, project_name_srch := NULL]


cat('  Save covid pre-KWS dataset\n')
#----# Save dataset #----# ####
save_dataset(covid_data, 'COVID_pre_kws', 'WB', 'int')
save_dataset(vax, '1a_vax', 'WB', 'int')
#-------------------# ####

#
# / end covid creation

cat('Disaggregate funding for projects with IDA & IBRD commitments')
#----# Disaggregate IDA & IBRD #----# ####

d = copy(data)

# fix disbursements from both ibrd and ida
data[, `:=` (ibrdcommamt = as.numeric(gsub(',', '', ibrdcommamt)),
             idacommamt = as.numeric(gsub(',', '', idacommamt)))]
setnafill(data, fill = 0, cols = c('ibrdcommamt', 'idacommamt'))
ibrd <- data[ibrdcommamt > 0 & idacommamt > 0]
data <- data[!(ibrdcommamt > 0 & idacommamt > 0)]

# split into two separate projects and calculate separate disbursement contributions
ida <- copy(ibrd)

ibrd <- ibrd[, `:=` (agreementtype = 'IBRD',
                     DAH = DAH * (ibrdcommamt / (ibrdcommamt + idacommamt)),
                     idacommamt = 0)]
ida <- ida[, `:=` (agreementtype = 'IDA',
                   DAH = DAH * (idacommamt / (ibrdcommamt + idacommamt)),
                   ibrdcommamt = 0)]


# add back to regular dataset
data <- rbind(data, ibrd, ida)
data[,`:=`(ibrdcommamt = as.numeric(ibrdcommamt),
           idacommamt = as.numeric(idacommamt),
           DAH = as.numeric(DAH))]
rm(ibrd, ida)

#-------------------# ####

cat('  Save pre-KWS dataset\n')
#----# Save dataset #----# ####
save_dataset(data, 'pre_kws', 'WB', 'int', format = "dta")
#------------------------# ####