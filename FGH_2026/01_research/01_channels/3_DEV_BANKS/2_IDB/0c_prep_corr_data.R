#
# intercept covid funding and apply covid proportion so projects can be handled
# by 1_CREATE_IDB.do
#


code_repo <- 'FILEPATH'



report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))







corr_data <- fread(get_path("idb", "raw", "IDB_2015_[report_year]_disbursements.csv"))




# apply covid proportion
covid <- corr_data[covid_prop > 0]
covid[, Disbursement := Disbursement * covid_prop]
covid[, is_covid := TRUE]



corr_data[, Disbursement := Disbursement * (1 - covid_prop)]
corr_data[, is_covid := FALSE]
corr_data[, covid_prop := 0]


corr_data <- rbind(corr_data, covid)
setorder(corr_data, YEAR, proj_id, is_covid)



# save
fwrite(corr_data,
       get_path("idb", "raw", "IDB_2015_[report_year]_disbursements_covidadjust.csv"))

