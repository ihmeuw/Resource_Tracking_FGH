
code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))



fa_disb <- fread(get_path("compiling", "int", "foreign_assistance_disbursements.csv"))
fa_disb <- fa_disb[
    year >= 2020 &
        international_sector_name %in% c(
            "Health, General",
            "Basic Health",
            "HIV/AIDS",
            "Maternal and Child Health, Family Planning",
            "Non-communicable diseases (NCDs)"
        ),
    .(disb = as.numeric(sum(disb))),
    by = .(
        year,
        international_sector_name,
        international_purpose_name,
        country_code,
        country_name,
        activity_project_number,
        activity_name,
        activity_description,
        multilat,
        aid_type_group_name
    )
]
setorder(fa_disb, year)



# prep for KWS
## note - clean strings in R since preferred to method used by KWS program
fa_disb[, `:=`(
    name_clean = string_to_std_ascii(activity_name, pad_char = ""),
    desc_clean = string_to_std_ascii(activity_description, pad_char = ""),
    sector_clean = string_to_std_ascii(international_sector_name, pad_char = ""),
    purpose_clean = string_to_std_ascii(international_purpose_name, pad_char = "")
)]

# save
## note - file must end in 'pre_kws.dta'
readstata13::save.dta13(
    fa_disb,
    get_path("compiling", "int", "fa_pre_kws.dta")
)

# create config file to be read by Health ADO (ie, KWS program)
## note - Health ADO finds the config based on the channel_name provided here
create_Health_config(
    data_path = get_path("compiling", "int", "fa_"),
    channel_name = "fa_usa",
    varlist = c(
        "name_clean", "desc_clean", "sector_clean", "purpose_clean"
    ),
    language = "english",
    function_to_run = 1
)


# launch Health ADO
## note - adjust queue or other cluster resource parameters as needed
launch_Health_ADO(
    channel_name = "fa_usa",
    queue = "long.q"
)
