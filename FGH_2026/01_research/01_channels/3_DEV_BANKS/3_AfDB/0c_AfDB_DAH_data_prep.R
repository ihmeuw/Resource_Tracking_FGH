#----# Docstring #----# ####
## AfDB COVID and DAH  data split
## Description: Splits raw data into DAH and COVID datasets
#------------------------------------------------------------------------#

cat('Environment Prep \n')
#----# Environment Prep #----# ####
rm(list = ls(all.names = TRUE))

library(data.table)
library(readxl)
library(dplyr)
library(readstata13)

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
asn_date <- "20250228"

#------------------------------------------------------------------------------#

cat("Read in data \n")
#----# Load in data received from AfDB  #----# ####

raw_dt <- data.table(read_excel(
    get_path('AFDB',
             "raw",
             "AfDB Bank Group Disbursement statistics for the university of washington as at end Dec [report_year].xlsx"),
    sheet = 'Sheet1'))

raw_dt_orig <- copy(raw_dt)


cat("Tag COVID projects\n")
#----# Split COVID and non-COVID projects  #----# ####
raw_dt[, Projecttitle_clean := string_to_std_ascii(Projecttitle)]
raw_dt[, is_covid := FALSE]
raw_dt[Projecttitle_clean %like% 'COVID|CORONAVIRUS|SARS-COV-2|NCOV|CIVD-19',
       is_covid := TRUE]




#
# load COVID-specific data from COVID years 
#
# During the pandemic, the correspondent provided "health proportions" for COVID
# related projects that are not under the health sub-sector.
# Here we convert these disbursements to "health" disbursements, since the main
# AfDB stata script will start by filtering down to sub-sector == "Health".
# So we load in the health proportions, merge them onto the COVID projects,
# apply the proportions to the disbursement values, and make sure any pre-COVID
# disbursements are set to 0. Then we can safely re-label the sub-sector as "Health".

cov_dt <- fread(
    file.path(dirname(get_path("AFDB", "raw")), "COVID_2021_20220214updated.csv"),
    encoding = 'Latin-1'
)
cov_dt <- unique(cov_dt[, .(`Project ID`, health_prop)])

raw_dt <- merge(raw_dt, cov_dt, by = 'Project ID', all.x = TRUE)

raw_dt[`Sub Sector` == "Health", health_prop := 1]
raw_dt[is.na(health_prop), health_prop := 0]

## some covid projects not in health sub-sector, but could be
raw_dt[Projecttitle == "PAAPS II - COVID-19",
       health_prop := 1]


disb_cols <- paste0("Disbursements for ", seq(2002, dah_cfg$report_year))
raw_dt[is_covid == TRUE,
       (disb_cols) := lapply(.SD, function(x) as.numeric(x) * health_prop),
       .SDcols = disb_cols]

pre_covid_disb_cols <- paste0("Disbursements for ", seq(2002, 2019))
raw_dt[is_covid == TRUE,
       (pre_covid_disb_cols) := lapply(.SD, \(x) NA_real_),
       .SDcols = pre_covid_disb_cols]

## make sure we haven't tagged any projects as COVID that aren't health and
## aren't part of the COVID-specific data
stopifnot( raw_dt[is_covid == TRUE, min(health_prop)] > 0 )

raw_dt[is_covid == TRUE,
       `Sub Sector` := "Health"]




#
# Add IATI descriptions for health keyword search
#
iati <- fread(get_path("afdb", "int", "afdb_iati_clean.csv"))

descr_cols <- c(
    "title_narr",
    "description_long_narr",
    "description_objectives_narr",
    "description_target_group_narr"
)
iati_proj_descr <- unique(iati[, c(
    "iati_identifier", descr_cols
), with = FALSE])

iati_proj_descr[, project_id := gsub("46002-", "", iati_identifier)]
iati_proj_descr[, iati_identifier := NULL]

## clean strings before sending to stata
iati_proj_descr[, (descr_cols) := lapply(.SD, string_to_std_ascii), .SDcols = descr_cols]
iati_proj_descr[, srchstr := paste(
    title_narr,
    description_long_narr,
    description_objectives_narr,
    description_target_group_narr,
    sep = " SEP "
)]

## merge on descriptions by project_id, where possible
raw_dt <- merge(
    raw_dt,
    iati_proj_descr[, .(project_id, srchstr)],
    by.x = "Project ID",
    by.y = "project_id",
    all.x = TRUE
)

## also include the project title provided in the correspondent data
raw_dt[, srchstr := paste0(
    Projecttitle_clean, " SEP ", srchstr
)]


cat("Saving dataset \n")
raw_dt[, fgh_id := .I]
fwrite(raw_dt, paste0(get_path('AFDB', 'raw'), 'M_AfDB_PD_', asn_date, 'updated.csv'))




if (interactive()) {
    # explore raw data
    dt <- copy(raw_dt)
    names(dt) <- tolower(gsub(" ", "_", names(dt)))
    disb_cols <- grep("^disbursements_for", names(dt), value = TRUE)
    dt <- melt(dt,
               measure.vars = disb_cols,
               variable.name = "year")
    dt[, year := as.integer(gsub("disbursements_for_", "", year))]
    dt <- dt[!is.na(value)]
    
    dt[, .(tot = sum(value)), by = year] |>
        ggplot(aes(x = year, y = tot/1e9)) +
        geom_line(alpha = 0.5) +
        geom_point() +
        geom_text(aes(label = round(tot/1e9, 1)), 
                  vjust = -0.5, size = 4) +
        theme_bw() +
        labs(title = "AfDB - Raw data, total disbursements",
             x = "", y = "UAC Billions")
    
    dt[sub_sector == "Health" & is_covid == F, .(tot = sum(value)), by = year] |>
        ggplot(aes(x = year, y = tot/1e6)) +
        geom_line(alpha = 0.5) +
        geom_point() +
        geom_text(aes(label = round(tot/1e6, 1)), 
                  vjust = -0.5, size = 4) +
        theme_bw() +
        labs(title = "AfDB - Raw data, health sub-sector disbursements",
             x = "", y = "UAC Millions")
    
}