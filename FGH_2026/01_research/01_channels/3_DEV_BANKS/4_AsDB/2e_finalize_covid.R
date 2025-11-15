#
# Project: FGH
# Channel: AsDB
#
# covid script is based on correspondent data which we did not get for years
# after 2021, so combine it with data from online database (processed in script 2b)
#
code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
source(paste0(code_repo, "/FUNCTIONS/helper_functions.R"))
pacman::p_load(readstata13)



old <- fread(get_path("asdb", "fin", "COVID_prepped_2020_2021.csv"))
old <- old[YEAR <= 2021]


new <- fread(get_path("asdb", "int", "covid_extract.csv"))

new <- new[year >= 2022]
new[, total_amt := DAH * 1e6]
new[, grant_loan := ifelse(Modality %like% "Grant", "grant", "loan")]

new <- new[, .(
    YEAR = year,
    iso3_rc = DMCs,
    grant_loan = grant_loan,
    project_name = ProjectName,
    total_amt
)]



old <- old[, .(total_amt = sum(total_amt, na.rm = TRUE)),
           by = .(YEAR, iso3_rc, grant_loan)]

old[, project_name := "COVID-Prepped Disbursement"]

out <- rbind(old, new, fill = TRUE)


save_dataset(out, "COVID_prepped",
             "asdb", "fin")

