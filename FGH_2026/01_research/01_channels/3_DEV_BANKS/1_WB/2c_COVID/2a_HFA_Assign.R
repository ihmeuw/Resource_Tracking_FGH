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


defl <- fread(get_path("meta", "defl", "imf_usgdp_deflators_[defl_mmyy].csv"))

# read in covid project disb
covid <- fread(paste0(get_path('WB', 'int'), 'COVID_pre_kws.csv'))

covid <- merge(
    covid,
    defl[, c("YEAR", paste0("GDP_deflator_", report_year)), with = FALSE],
    by = "YEAR",
    all.x = TRUE
)
covid[, paste0("DAH_", dah.roots$abrv_year) := DAH / get(paste0("GDP_deflator_", report_year))]

# read in and merge covid project descriptions
covid_descriptions <- fread(paste0(get_path('WB', 'raw', report_year = 2021), 'Update_COVID/WB_covid_project_information_update.csv'), header = T, encoding = 'Latin-1')
covid_descriptions[is.na(covid_descriptions)] <- ''
covid <- merge(covid, covid_descriptions, by = 'projid', all.x = T)

# read in and merge commitment, grant, and loan info
covid_info <- fread(paste0(get_path('WB', 'raw', report_year = 2021), 'Update_COVID/wb_covid_commitments_update.csv'), header = T)
# converting each year 'commitment' column in to the disbuserment
# 2021 onwards commitment values actually are disbursement for each year
covid_info <- covid_info[,.(projid, proj_name, ida_credit, ida_grant, vax_projid, Vax_commitment, commitment_2020, commitment_2021)]
covid_info <- melt(covid_info, id.vars = c('projid', 'proj_name', 'ida_credit', 'ida_grant', 'vax_projid', 'Vax_commitment'), variable.name = 'YEAR', value.name = 'disbursement')
covid_info[, YEAR := tstrsplit(YEAR, "_", fixed=TRUE)[2]]
covid_info[, YEAR := as.numeric(YEAR)]
covid <- merge(covid, covid_info, by = c('projid','proj_name','YEAR'), all.x = T)
covid[, eval(c(names(covid)[names(covid) %like% 'commitment_'], 'Vax_commitment')) := lapply(.SD, FUN = function(x) x * 1e6), 
      .SDcols = c(names(covid)[names(covid) %like% 'commitment_'], 'Vax_commitment')]

# fix disbursements from both ibrd and ida
ibrd <- covid[ibrdcommamt > 0 & idacommamt > 0]
covid <- covid[!(ibrdcommamt > 0 & idacommamt > 0)]
ibrd[, `:=` (ibrdcommamt = as.numeric(gsub(',', '', ibrdcommamt)),
             idacommamt = as.numeric(gsub(',', '', idacommamt)))]

# split into two separate projects and calculate separate disbursement contributions
ida <- copy(ibrd)

ibrd <- ibrd[, `:=` (agreementtype = 'IBRD',
                     DAH = DAH * (ibrdcommamt / (ibrdcommamt + idacommamt)),
                     disbursement = disbursement * (ibrdcommamt / (ibrdcommamt + idacommamt)),
                     idacommamt = 0)]
ida <- ida[, `:=` (agreementtype = 'IDA',
                   DAH = DAH * (idacommamt / (ibrdcommamt + idacommamt)),
                   disbursement = disbursement * (idacommamt / (ibrdcommamt + idacommamt)),
                   ibrdcommamt = 0)]

ibrd[, eval(paste0('DAH_', dah.roots$abrv_year)) := DAH / get(eval(paste0('GDP_deflator_20', dah.roots$abrv_year)))]
ida[, eval(paste0('DAH_', dah.roots$abrv_year)) := DAH / get(eval(paste0('GDP_deflator_20', dah.roots$abrv_year)))]

# add back to regular dataset
covid <- rbind(covid, ibrd, ida)

rm(ibrd, ida)

#----# Add and remove double counted vaccine projects  #----# ####
cat('  Add and remove double counted vaccine projects\n')
# if projects have a vaccine project attached to it it will be added as such otherwise new project
vax <- fread(paste0(get_path('WB', 'int'), '1b_wb_vaccine_loan.csv'))
vax_dcp <- unique(vax$project_id[(vax$project_id %in% covid$projid)])
# getting repurposed values, projects that existed in 2020 but are now vaccine
rep_dcp <- unique(covid[(projid %in% vax_dcp) & YEAR==2020 & DAH > 0,]$projid)
# remove vax projects and label them as repurpose 
covid <- covid[!(projid %in% vax_dcp),]
vax[, money_type := ifelse(project_id %in% rep_dcp, 'repurposed money', 'new money')]

## melt by component descriptions
component_descriptions <- melt.data.table(covid,
                                          measure.vars = c(colnames(covid)[colnames(covid) %like% '_description']),
                                          variable.name = 'component_number',
                                          value.name = 'description')
component_descriptions <- component_descriptions[description != '']
component_descriptions[, component_number := gsub('_description', '', component_number)]

## melt by component values
component_values <- melt.data.table(covid,
                                   measure.vars = c(colnames(covid)[colnames(covid) %like% '_DAH']),
                                   variable.name = 'component_number',
                                   value.name = 'value')
component_values <- component_values[value != '']
component_values[, component_number := gsub('_DAH', '', component_number)]

## combine components and values
component_data <- merge(component_descriptions, component_values[, .(projid, component_number, value, agreementtype,YEAR)],
                        by = c('projid', 'component_number', 'agreementtype','YEAR'))

## get sum of components
component_data[, total_value := sum(as.numeric(value)),
               by = .(projid, agreementtype, YEAR)]


## combine projects and subprojects
component_data[, abstract := description]

covid <- covid[!(projid %in% unique(component_data$projid))]
covid <- rbind(covid, component_data, fill = T)

## make commitments numeric
covid[, ibrdcommamt := as.numeric(gsub(',', '', ibrdcommamt))]
covid[, idacommamt := as.numeric(gsub(',', '', idacommamt))]

## update DAH disbursements and commitments
covid[!(is.na(total_value)), c('DAH', paste0("DAH_", dah.roots$abrv_year), 'ibrdcommamt', 'idacommamt', 'disbursement') := .SD * (as.numeric(value)/total_value),
               .SDcols = c('DAH', paste0("DAH_", dah.roots$abrv_year), 'ibrdcommamt', 'idacommamt', 'disbursement')]

# update abstract to pdo if not available
covid[abstract == '' | is.na(abstract), abstract := pdo]
covid[abstract == '' | is.na(abstract), abstract := projname]

# removing any projects with Vaccine keywords in thier description to no double count
vax_tag_keywords <- dah.roots$vax_intercept_keywords
covid[, upper_abstract := string_to_std_ascii(abstract)]
covid <- covid[!(grepl(paste(vax_tag_keywords,collapse="|"), upper_abstract)),]
covid[, upper_abstract:=NULL]
## launch kws
covid <- covid_kws(covid, keyword_search_colnames = c('abstract'),
                   keep_clean = T,
                   languages = 'english')


cat('  Save covid post-KWS dataset\n')
#----# Save dataset #----# ####
save_dataset(covid, 'COVID_post_kws', 'WB', 'int')
save_dataset(vax, 'VAX_pre_kws', 'WB', 'int')
#-------------------# ####
