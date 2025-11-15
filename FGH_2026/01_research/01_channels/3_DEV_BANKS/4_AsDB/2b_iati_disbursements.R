#
# Project: FGH
# Channel: AsDB
#
# Use IATI data to get disbursements for recent years.
# 
# We haven't received DAH disbursement data from AsDB since 2021, so we use their
# reported disbursements in IATI for years 2022+
#
# 
code_repo <- 'FILEPATH'

report_year <- 2024

# source shared functions
source(file.path(code_repo, paste0("FGH_", report_year), "utils.R"))
pacman::p_load(readstata13)





#
# CORRESPONDENT DATA
#

# read in the processed disbursement data from the correspondent
# (processed by beginning of do script 2)
corr <- fread(get_path("asdb", "int", "asdb_correspondent_disbs.csv"))




#
# IATI DATA
#
iati <- fread(get_path("asdb", "int", "asdb_iati_clean.csv"))

stopifnot(all(unique(iati$trans_currency) == "USD"))
disb <- iati[trans_type == "Disbursement" & trans_year > 2013, .(
    iati_identifier,
    approval_date = activity_date_start_planned,
    close_date = activity_date_end_planned,
    start_year = year(activity_date_start_actual),
    year = trans_year,
    country = recip,
    project_name = title_narr,
    description = description_general_narr,
    sector,
    project_type_or_modality = finance_type,
    disb = trans_value
)]


disb[, project_number := gsub("XM-DAC-46004-", "", iati_identifier)]
disb[, project_number := vapply(strsplit(project_number, "-"), \(x) {
    paste(x[1], x[2], sep = "-")
}, character(1))]


## aggregate disbursements by project year
disb <- disb[, .(disb = sum(disb, na.rm = TRUE)),
             by = .(iati_identifier, project_number,
                    approval_date, close_date, start_year, year, country,
                    project_name, description, sector, project_type_or_modality)]




#
# Identify health projects
#
dah_purpose_names <- c(
    "Health policy and administrative management",
    "Health statistics and data",
    "Medical education/training",
    "Medical research",
    "Medical services",
    "Basic health care",
    "Basic nutrition",
    "Infectious disease control",
    "Health education",
    "Malaria control",
    "Tuberculosis control",
    "COVID-19 control",
    "Health personnel development",
    "Population policy and administrative management",
    "Population statistics and data",
    "Reproductive health care",
    "Family planning",
    "STD control including HIV/AIDS",
    "Personnel development for population and reproductive health"
)
disb[sector %like% paste(dah_purpose_names, collapse = "|"),
     is_dah := TRUE]


#
# Resolve negative disbursements
#
disb[order(year), proj_n := 1:.N, by = iati_identifier]
disb[, proj_N := .N, by = iati_identifier]
disb[, orig_total := sum(disb, na.rm = TRUE), by = iati_identifier]
## drop projects where the total disbursement amount is negative (nothing has
##  actually been disbursed, just paid/returned in advance)
disb <- disb[orig_total > 0]

max_N <- max(disb$proj_N)
for (i in seq(1, max_N)) {
    # get the value of the next disbursement
    disb[order(year),
         next_disb := shift(disb, type = "lead"),
         by = .(iati_identifier)]
    
    # update the annual total by adding the next disbursement (<0) to the current
    disb[proj_n == proj_N - i & next_disb < 0,
         disb := disb + next_disb]
    
    # set the next disbursement to 0 if it's been removed from the current
    disb[proj_n == proj_N - i + 1 & disb < 0 &          ## if next project disbursement is negative,
                proj_N - i != 0,                        ## and this is not the earliest appearance of this project,
         disb := 0]                                     ## set this next disb to 0, since it's been subtracted from the current disb
}


# now, there are only negative disbursements if backing out negatives made
#   it to the first year of the project. So remove these from later years
disb[order(-year), proj_n := 1:.N, by = iati_identifier]
disb[, proj_N := .N, by = iati_identifier]
disb[, orig_total := sum(disb, na.rm = TRUE), by = iati_identifier]
disb <- disb[orig_total > 0]
max_N <- max(disb$proj_N)
for (i in seq(1, max_N)) {
    # get the value of the next disbursement
    disb[order(-year),
         next_disb := shift(disb, type = "lead"),
         by = .(iati_identifier)]
    
    # update the annual total by adding the next disbursement (<0) to the current
    disb[proj_n == proj_N - i & next_disb < 0,
         disb := disb + next_disb]
    
    # set the next disbursement to 0 if it's been removed from the current
    disb[proj_n == proj_N - i + 1 & disb < 0 &          ## if next project disbursement is negative,
                proj_N - i != 0,                        ## and this is not the earliest appearance of this project,
         disb := 0]                                     ## set this next disb to 0, since it's been subtracted from the current disb
}



#
# Clean recipient country
#

locs <- fread(get_path("meta", "locs", "fgh_location_set.csv"))
disb <- merge(
    disb,
    unique(locs[, .(country = location_name, ihme_loc_id)]),
    by = "country",
    all.x = TRUE
)
disb[, ihme_loc_id := fcase(
    country == "Asia, regional", "S6",
    country == "Cook Islands (the)", "COK",
    country == "Lao People's Democratic Republic (the)", "LAO",
    country == "Marshall Islands (the)", "MHL",
    country == "Philippines (the)", "PHL",
    rep_len(TRUE, .N), ihme_loc_id
)]

stopifnot( disb[is.na(ihme_loc_id), .N] == 0 )



#
# Add COVID flag
#

disb[, srchstr := paste0(
    string_to_std_ascii(project_name), ";",
    string_to_std_ascii(description)
)]
disb[, covid := grepl("COVID", srchstr)]

## overtagging in later pandemic years
disb[year %in% 2023:2024 & !grepl("COVID", project_name),
     covid := FALSE]

## By 2024, COVID-tagged projects are being repurposed for other issues, or are
##   only mention COVID as a motivating factor for other health issues
disb[year >= 2024, covid := FALSE]




#
# Save IATI disbursements
#
save_dataset(disb,
             "asdb_iati_disbursements",
             channel = "asdb",
             stage = "int")




#
# Convert to DAH and combine with correspondent data
#
fin <- disb[is_dah == TRUE,
            .(DMCs = ihme_loc_id,
              YEAR_OF_START = start_year,
              projid = project_number,
              ProjectName = project_name,
              ProjectDescription = description,
              Modality = project_type_or_modality,
              DateofApproval = approval_date,
              CurrentClosingDate = close_date,
              DAH = disb / 1e6,
              year = year,
              covid
              )]
setorder(fin, year)



#
# Separate covid and save covid
#

covid <- fin[covid == TRUE]
fin <- fin[covid == FALSE, -"covid"]

save_dataset(covid,
             "covid_extract",
             channel = "asdb",
             stage = "int")



if (interactive()) {
    pred <- merge(
        fin[year <= dah.roots$report_year + 2,
            .(pred = sum(DAH, na.rm = TRUE)), by = year],
        covid[year >= 2020 & year <= dah.roots$report_year + 2,
              .(covid = sum(DAH, na.rm = TRUE)), by = year],
        by = "year",
        all.x = TRUE
    )
    setnafill(pred, fill = 0, cols = "covid")
    pred[, tot := pred + covid]
    merge(
        pred,
        corr[, .(act = sum(DAH, na.rm = TRUE)), by = year],
        by = "year",
        all.x = TRUE
    ) |> ggplot(aes(x = year)) +
        geom_line(aes(y = pred, color = "Predicted")) +
        geom_line(aes(y = act, color = "Observed")) +
        scale_x_continuous(breaks = seq(min(fin$year), max(fin$year), 1)) +
        theme_minimal() +
        labs(
            title = "Observed vs. Predicted Disbursements",
            subtitle = "Observed data received from AsDB correspondents.",
            x = "", y = "Disbursements (USD, millions)",
            color = ""
        ) +
        theme(
            axis.text.x = element_text(angle = 45, hjust = 1),
            legend.position = "bottom"
        )
}


setorder(fin, year)
fin <- fin[
    year > max(corr$year) & year <= dah_cfg$report_year
]


#
# MERGE SOURCES
#

corr[, c("DateofApproval", "CurrentClosingDate") := lapply(.SD, as.character),
      .SDcols = c("DateofApproval", "CurrentClosingDate")]
fin[, c("DateofApproval", "CurrentClosingDate") := lapply(.SD, \(x) as.character(as.Date(x))),
     .SDcols = c("DateofApproval", "CurrentClosingDate")]

out <- rbind(
    corr,
    fin,
    fill = TRUE
)





save_dataset(out,
             "AsDB_Disbursements_from_correspondence_FGH[report_year]",
             channel = "asdb",
             stage = "fin")
