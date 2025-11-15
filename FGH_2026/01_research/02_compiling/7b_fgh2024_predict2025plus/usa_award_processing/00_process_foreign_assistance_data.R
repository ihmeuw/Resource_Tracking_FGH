#
# Calculate funding cuts to USAID and State Department foreign assistance
# based on impact of terminated awards
code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))


data_dir <- get_path(
    "bilat_predictions",
    "raw",
    "USA",
    report_year = 2025
)



#
# Load and process award statuses =============================================
#
awards_fp <- file.path(data_dir, "USAID and State Department Grants and Contracts.xlsx")

# USAID grants and contracts
usaid <- openxlsx::read.xlsx(awards_fp, sheet = "USAID Contracts Grants")
setDT(usaid)
usaid <- usaid[, .(
    award_id_piid,
    project_id,
    status = Status.Aug.1.2025,
    usaspending_total_obligated_amount,
    usaspending_total_outlayed_amount,
    doge_obligated,
    doge_total_estimated,
    usaspending_recipient_country_name,
    usaspending_recipient_name,
    usaspending_prime_award_base_transaction_description,
    usaspending_program_activities_funding_this_award,
    usaspending_period_of_performance_start_date,
    type_award
)]
usaid[, id_clean := gsub("[^[:alnum:]]", "", award_id_piid)]
## ensure no duplicate ids
stopifnot(usaid[, .N, by = id_clean][N > 1, .N] == 0)


# DOS Contracts
dos_contracts <- openxlsx::read.xlsx(awards_fp, sheet = "DOS Contracts")
setDT(dos_contracts)
dos_contracts <- dos_contracts[, .(
    award_id_piid,
    type,
    status = Status.Aug.1.2025,
    total_obligated_amount,
    total_outlayed_amount,
    tmp_id = .I
)]
## split multi-id rows
dos_contracts <- dos_contracts[, .(award_id_piid = unlist(tstrsplit(award_id_piid, "\\/"))),
                               by = .(tmp_id, status)]

dos_contracts[, id_clean := gsub("Req:", "", award_id_piid, fixed = TRUE)]
dos_contracts[, id_clean := gsub("- NTP", "", id_clean, fixed = TRUE)]
dos_contracts[, id_clean := gsub("[^[:alnum:]]", "", id_clean)]
## this is a duplicate award, but one entry is marked as active and the other
## as terminated, so assume active
dos_contracts <- dos_contracts[!(id_clean == "193CSO24Y0001" & status == "Active")]
## multiple projects, all active, so just drop for now
dos_contracts <- dos_contracts[id_clean != "19AQ"]
## ensure no duplicate ids
stopifnot(dos_contracts[, .N, by = id_clean][N > 1, .N] == 0)


# DOS Grants
dos_grants <- openxlsx::read.xlsx(awards_fp, sheet = "DOS Grants")
setDT(dos_grants)
dos_grants <- dos_grants[, .(
    award_id_piid,
    type,
    status = Status.Aug.1.2025,
    total_obligated_amount,
    total_outlayed_amount,
    cost_doge
)]
dos_grants[, id_clean := gsub("[^[:alnum:]]", "", award_id_piid)]
## drop duplicate rows
dos_grants[, n := 1:.N, by = .(id_clean, status)]
dos_grants <- dos_grants[n == 1, -"n"]
## ensure no duplicate ids
stopifnot(dos_grants[, .N, by = id_clean][N > 1, .N] == 0)




#
# Load & process ForeignAssistance.gov data ===================================
#
fa <- fread(file.path(data_dir, "us_foreign_aid_complete.csv"))
names(fa) <- tolower(gsub(" ", "_", names(fa)))

#/TMP
fa <- fa[managing_agency_acronym %in% c("USAID", "STATE")]

fa[activity_end_date == "NULL", activity_end_date := NA_character_]
fa[, activity_end_date := as.Date(activity_end_date)]
fa[, constant_dollar_amount := as.numeric(constant_dollar_amount)]
fa[fiscal_year == "1976tq", fiscal_year := "1976"]
fa[, fiscal_year := as.numeric(fiscal_year)]
fa[, multilat := fifelse(
    implementing_partner_category_name %in% c("Multilateral", "Public and Private Partnerships"),
    TRUE,
    FALSE
)]


## Aggregate info by project number ====
## 

## merge aggregate disbursements onto aggregate obligations for all projects,
## wherever possible
fa_by_projectnum <- merge(
    fa[transaction_type_name == "Obligations" & multilat == FALSE,
       .(
           total_obligation = sum(constant_dollar_amount, na.rm = TRUE),
           max_fy_of_oblig = max(fiscal_year[constant_dollar_amount > 0]),
           fy23_obligation =
               sum(constant_dollar_amount[fiscal_year == 2023], na.rm = TRUE),
           fy24_obligation =
               sum(constant_dollar_amount[fiscal_year == 2024], na.rm = TRUE),
           fy25_obligation =
               sum(constant_dollar_amount[fiscal_year == 2025], na.rm = TRUE)
        ),
       by = .(activity_project_number, managing_agency_acronym)
    ],
    fa[transaction_type_name == "Disbursements" & multilat == FALSE,
       .(
           total_disbursement = sum(constant_dollar_amount, na.rm = TRUE),
           max_fy_of_disb = max(fiscal_year[constant_dollar_amount > 0]),
           fy24_disbursement =
               sum(constant_dollar_amount[fiscal_year == 2024], na.rm = TRUE),
           fy25_disbursement =
               sum(constant_dollar_amount[fiscal_year == 2025], na.rm = TRUE)
        ),
       by = .(activity_project_number, managing_agency_acronym)
    ],
    by = c("activity_project_number", "managing_agency_acronym"),
    all.x = TRUE
)
fa_by_projectnum[is.infinite(max_fy_of_oblig), max_fy_of_oblig := NA]
fa_by_projectnum[is.infinite(max_fy_of_disb), max_fy_of_disb := NA]
setnafill(fa_by_projectnum, fill = 0, cols = c(
    "total_obligation", "fy24_obligation", "fy25_obligation",
    "total_disbursement", "fy24_disbursement", "fy25_disbursement"
))


## Merge on award status info to projects
fa_by_projectnum[, id_clean := gsub("[^[:alnum:]]", "", activity_project_number)]
fa_by_projectnum <- merge(
    fa_by_projectnum,
    usaid[, .(id_clean, usaid_status = status)],
    by = "id_clean",
    all.x = TRUE
)
fa_by_projectnum <- merge(
    fa_by_projectnum,
    dos_grants[, .(id_clean, dos_grants_status = status)],
    by = "id_clean",
    all.x = TRUE
)
fa_by_projectnum <- merge(
    fa_by_projectnum,
    dos_contracts[, .(id_clean, dos_contracts_status = status)],
    by = "id_clean",
    all.x = TRUE
)
## finalize current status of the project
fa_by_projectnum[, has_status := fifelse(
    !is.na(usaid_status) | !is.na(dos_grants_status) | !is.na(dos_contracts_status),
    TRUE,
    FALSE
)]
fa_by_projectnum[, latest_status := fcase(
    !is.na(usaid_status), usaid_status,
    !is.na(dos_grants_status), dos_grants_status,
    !is.na(dos_contracts_status), dos_contracts_status,
    default = "Unknown"
)]

## Classify projects as active if they have outstanding obligations and have been
## active since at least FY24
fa_by_projectnum[, outstand := total_obligation - total_disbursement]
fa_by_projectnum[, max_fy := pmax(max_fy_of_oblig, max_fy_of_disb, na.rm = TRUE)]

fa_by_projectnum[, was_active := max_fy >= 2024 & outstand > 0]
fa_by_projectnum[, terminated := latest_status %ilike% "terminated"]


## Get country fractions for each project ====
##
fa_countries <- fa[
    transaction_type_name == "Obligations" &
        multilat == FALSE &
        fiscal_year >= 2024,
    .(amount = sum(constant_dollar_amount)),
    by = .(activity_project_number, managing_agency_acronym, country_code)
]
fa_countries <- fa_countries[amount > 0]
fa_countries[, prop_oblig_country := as.numeric(amount) / as.numeric(sum(amount)),
             by = .(activity_project_number, managing_agency_acronym)]
fa_countries[, amount := NULL]


## Get sector fractions for each project-country ====
##
fa_sectors <- fa[
    transaction_type_name == "Obligations" &
        multilat == FALSE &
        fiscal_year >= 2024,
    .(amount = sum(constant_dollar_amount)),
    by = .(activity_project_number,
           managing_agency_acronym,
           country_code,
           international_sector_name,
           international_purpose_name)
]
fa_sectors <- fa_sectors[amount > 0]
fa_sectors[, prop_oblig_sector := as.numeric(amount) / as.numeric(sum(amount)),
            by = .(activity_project_number,
                   managing_agency_acronym,
                   country_code)]
fa_sectors[, amount := NULL]


## Merge project financials with country and sector fractions ====
##

fa_by_projectnum[, recent_obligations := fy24_obligation + fy25_obligation]

## merge on country fractions
fa_summ <- merge(
    fa_by_projectnum,
    fa_countries,
    by = c("activity_project_number", "managing_agency_acronym"),
    all.x = TRUE
)
stopifnot(
    ## ensure country fractions sum to 1
    ### (will be NA for projects with no positive 2024-25 obligations)
    fa_summ[!is.na(prop_oblig_country),
            sum(prop_oblig_country),
            by = .(activity_project_number, managing_agency_acronym)
           ][abs(V1 - 1) > 1e-6, .N] == 0
)

## merge on sector fractions
fa_summ <- merge(
    fa_summ,
    fa_sectors,
    by = c("activity_project_number",
           "managing_agency_acronym",
           "country_code"),
    all.x = TRUE
)
stopifnot(
    ## ensure sector fractions sum to 1
    ## (will be NA for projects with no positive 2024-25 obligations)
    fa_summ[!is.na(prop_oblig_sector),
            sum(prop_oblig_sector),
            by = .(activity_project_number,
                   managing_agency_acronym,
                   country_code)
           ][abs(V1 - 1) > 1e-6, .N] == 0
)

stopifnot(
    ## should be no projects which had recent obligations but no country fraction
    fa_summ[is.na(prop_oblig_country) & recent_obligations > 0, .N] == 0
)

## Assign obligation amount to each flow by disaggregating total recent
##   obligations into project-country-sector flows
fa_summ[, assigned_obligation :=
            recent_obligations * prop_oblig_country * prop_oblig_sector]
fa_summ[recent_obligations <= 0, assigned_obligation := 0]



## Save ====
## merge on country names for convenience
fa_summ <- merge(
    fa_summ,
    unique(fa[, .(country_code, country_name)]),
    by = "country_code",
    all.x = TRUE
)


fwrite(
    fa_summ,
    get_path("compiling", "int", "foreign_assistance_summary.csv")
)


## Summarize disbursements by country and sector ====
fa[, year := as.integer(substr(transaction_date, 6, 9))]
fa_disb <- fa[
    transaction_type_name == "Disbursements",
    .(
        disb = sum(constant_dollar_amount)
    ),
    by = .(
        year,
        activity_project_number,
        multilat,
        aid_type_group_name,
        country_code,
        country_name,
        us_category_name,
        us_sector_name,
        international_category_name,
        international_sector_name,
        international_purpose_name,
        activity_name,
        activity_description
    )
]
fa_disb <- fa_disb[!is.na(year)]

fwrite(
    fa_disb,
    get_path("compiling", "int", "foreign_assistance_disbursements.csv")
)


# Summarize cuts by country and sector ========================================
if (interactive()) {
ex <- fa_summ[was_active == TRUE &
                  international_sector_name %in% c(
                      "Non-communicable diseases (NCDs)",
                      "Maternal and Child Health, Family Planning",
                      "Basic Health",
                      "Health, General",
                      "HIV/AIDS"
                  ),
              .(amt = as.numeric(sum(assigned_obligation))),
              by = .(international_purpose_name, country_code, terminated)]
ex <- dcast(ex,
            international_purpose_name + country_code ~
                ifelse(terminated == TRUE, "term", "act"),
            value.var = "amt",
            fill = 0)
ex[, total := act + term]
ex[, pct_chg := act/total - 1]

ex <- merge(
    ex,
    unique(fa[, .(country_code, country_name)]),
    by = "country_code",
    all.x = TRUE
)

total_country <- ex[, .(total_recent_oblig = sum(total), terminated_recent_oblig = sum(term)),
                    by = .(country_code, country_name)]
total_country[, pct_chg := - terminated_recent_oblig/total_recent_oblig]
setorder(total_country, pct_chg)

purp_country <- ex[, .(
    country_code,
    country_name,
    international_purpose_name,
    total_recent_oblig = total,
    terminated_recent_oblig = term,
    pct_chg = pct_chg
)]
setorder(purp_country, country_name, pct_chg)


openxlsx::write.xlsx(
    list(
        "all_health_sectors" = total_country,
        "by_purpose" = purp_country
    ),
    "~/Estimates of US aid cuts by country and purpose.xlsx"
)

}