
code_repo <- 'FILEPATH'


report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))




#
# Load and prep WHOs IATI transaction data
#

iati <- fread(get_path("who", "int", c("iati_clean", "who.csv")))

# drop transactions with missing or zero value
iati <- iati[!is.na(trans_value) & trans_value != 0]
# keep only outgoing transactions (drop incoming)
iati <- iati[outgoing_flag == TRUE]

if (! all(unique(iati$trans_currency) == "USD")) {
    stop("There are transactions reported in a currency other than USD. ",
         "Standardize currencies before aggregating transaction value.")
}


dat <- unique(iati[, .(iati_identifier,
                       title_narr,
                       description_long_narr)])

dat[, `:=`(
    title_narr = string_to_std_ascii(title_narr),
    description_long_narr = string_to_std_ascii(description_long_narr)
)]


# save for keyword searching
save_dataset(dat, "iati_pre_kws", "who", "int", format = "dta")


# launch keyword search
create_Health_config(data_path = get_path("who", "int", "iati_"), ## stata will look for "`path'_pre_kws.dta"
                     channel_name = "who_iati",
                     varlist = c("title_narr",
                                 "description_long_narr"),
                     language = "english",
                     function_to_run = 1)

launch_Health_ADO(channel_name = "who_iati")




