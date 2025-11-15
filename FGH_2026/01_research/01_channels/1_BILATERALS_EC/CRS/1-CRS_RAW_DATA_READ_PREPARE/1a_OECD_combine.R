#### #----#                    Docstring                    #----# ####
# Project:  FGH
# Purpose:  Read in .txt raw CRS files and combine. Outputs csvs of other channels that
#           have some data available in the CRS
#---------------------------------------------------------------------#

#----# Environment Prep #----# ####
## System prep
rm(list=ls(all.names = TRUE))

library(arrow)
library(duckdb)
library(dplyr)
library(DBI)

code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "FGH_", report_year, "/utils.R"))
pacman::p_load(readstata13,
               crayon)

cat("\n\n")
cat(green(" ################################\n"))
cat(green(" #### CRS: PREPARE OECD DATA ####\n"))
cat(green(" ################################\n\n"))

# load full CRS file once 

# iterate over each year-file and row bind into one database
raw_data_yr <- get_dah_param("crs", "data_year")
yearlist <- c(seq(raw_data_yr, 2006),
              "2004-05", "2002-03", "2000-01", "1995-99", "1973-94")

crs_raw <- list()
for (yr in yearlist) {
    nm <- paste("CRS", yr, "data")
    cat("  Loading", nm, "\n")
    crs_raw[[yr]] <- fread(
        file = get_path("crs", "raw", c("related_files", nm, paste0(nm, ".txt"))),
        encoding = "UTF-8"
    )
}
crs_raw <- rbindlist(crs_raw)

# add unique ID to each row
crs_raw[, fgh_id := .I]

# convert names to snake_case
names(crs_raw) <- janitor::make_clean_names(names(crs_raw))

# convert date columns to character
date_cols <- grep("date", names(crs_raw), value = TRUE)
crs_raw[, (date_cols) := lapply(.SD, as.character), .SDcols = date_cols]

# add currency info column - ensure this remains accurate in new downloads
# (i.e., the monetary columns are reported in millions of dollars)
crs_raw[, monetary_units := "millions"]

# re-save full file in apache parquet format
year_range <- paste(c(min(crs_raw$year), raw_data_yr), collapse = "_")
crs_parq <- get_path("crs", "raw", paste0("CRS_", year_range, ".parquet"))
cat("** Saving:", crs_parq, "\n")
arrow::write_parquet(crs_raw, crs_parq)


# connect to the parquet file via duckdb for efficiency
con <- duckdb::dbConnect(duckdb::duckdb())
crs_tbl <- dplyr::tbl(con, paste0("read_parquet('", crs_parq, "')"))


# output an ID to date-column mapping
## because stata likes to convert our dates to integers
## (we will use this to get back the original dates once we're done with stata)
tmp <- crs_tbl |>
    select(fgh_id, expected_start_date, completion_date,
           commitment_date, repaydate1, repaydate2) |>
    collect() |>
    as.data.table()
arrow::write_parquet(tmp, get_path("crs", "raw", "crs_date_map.parquet"))
rm(tmp)


cat("  Output non-bilateral data\n")
save_non_bilat <- function(donorcodes, filename, db_tbl) {
    tmp <- db_tbl |>
        filter(donor_code %in% donorcodes) |>
        collect()

    donors <- tmp |> distinct(donor_name) |> pull()
    cat("* ", filename, "\n")
    cat("*** Donors: ", paste(donors, collapse = ", "), "\n")
    cat("*** Codes: ", paste(donorcodes, collapse = ", "), "\n")

    save_dataset(tmp, filename, "crs", "fin", "common_channels")
}


donorcode_map <- list(
    # file name = channel_code
    "M_CRS_GAVI_2" = 1311,
    "M_CRS_GFATM_2" = 1312,
    "M_CRS_AsDB_2" = c(915, 916),
    "M_CRS_AfDB_2" = c(913, 914),
    "M_CRS_IDB_2" = c(909, 912),
    "M_CRS_IsDB_2" = 976,
    "M_CRS_WB_2" = c(901, 903, 905),
    "M_CRS_UNAIDS_2" = 971,
    "M_CRS_UNFPA_2" = 974,
    "M_CRS_UNICEF_2" = 963,
    "M_CRS_WHO_2" = 928
)
for (nm in names(donorcode_map)) {
    cat("\n")
    save_non_bilat(donorcode_map[[nm]], nm, crs_tbl)
}


dbDisconnect(con)

cat("  CRS Stage 1a Done\n")
