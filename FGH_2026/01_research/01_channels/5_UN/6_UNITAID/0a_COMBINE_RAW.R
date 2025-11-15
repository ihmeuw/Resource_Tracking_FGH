#### #----#                        Docstring                         #----# ####
#' Project:         FGH
#'    
#' Purpose:         Match UNITAID 2021 and 2022 data
#------------------------------------------------------------------------------#
#####-----------------------------# notes #---------------------------------####
#
# this file is used to fill in any missing values across 'disease area', 'program
# area', 'Receiver org', and 'transaction receiver org'.
# to match the previous year and current year's data, we use the report year's
# project disbursements data and the previous year's filled in raw data. 
#
####-------------------------# environment #------------------------------####
# OPTIONS
code_repo <- 'FILEPATH'

REPORT_YEAR <- 2024
RUN_TESTS <- TRUE

source(paste0(code_repo, "/FGH_", REPORT_YEAR, "/utils.R"))
pacman::p_load(stringr, openxlsx, data.table)

# CONSTS
RAW <- get_path("unitaid", "raw")
PREV_REPORT_YEAR <- get_dah_param("prev_report_year")

UPDATE_DATE <- get_dah_param("UNITAID", "update_MMYY")
PREV_UPDATE_DATE <- get_dah_param("UNITAID", "prev_update_MMYY")


# FILE PATHS
EXPEND_FP <- get_path("unitaid", "raw",
                       paste0("project_expenditure_", UPDATE_DATE, ".csv"))

DISB_FP <- get_path("unitaid", "raw",
                    paste0("project_disbursements_", UPDATE_DATE, ".csv"))

OUTPUT_NAME <- paste0("project_expenditure_", UPDATE_DATE, "_hfa_update.csv")

PREV_OUTPUT_FP <- get_path("unitaid", "raw",
                           paste0("project_expenditure_", PREV_UPDATE_DATE, "_hfa_update.csv"),
                           report_year = PREV_REPORT_YEAR)



####-------------------------# helpers #------------------------------####
clean_names <- function(col_names) {
    .gsub <- \(pat, repl, x) gsub(pat, repl, x, fixed = TRUE)
    .clean_name <- function(nm) {
        nm <- tolower(nm)
        nm <- .gsub(" ", "_", .gsub("(", "", .gsub(")", "", nm)))
        return(nm)
    }
    return(unname(sapply(col_names, .clean_name)))
}


clean_numeric <- function(num_col) {
    .clean_num <- function(num) {
        # rm all chars that are not digits or decimals
        num <- stringr::str_replace_all(num, "[^0-9.]", "")
    }
    return(as.numeric(sapply(num_col, .clean_num)))
}

.g <- \(...) cat(crayon::green(...), "\n")

####-------------------------# data loaders #------------------------------####
get_prev_output <- function() {
    # load
    out <- data.table::fread(
        PREV_OUTPUT_FP,
        select = list(
            character = c("activity_identifier",
                          "transaction_date",
                          "disease_area",
                          "program_area",
                          "receiver_orgs"),
            numeric = c("transaction_amount")
        ),
        sep = ","
    )
    names(out) <- clean_names(names(out))
    
    # transform
    out[, transaction_date := as.Date(transaction_date)]
    return(out)
}

.convert_date <- function(datevec) {
    data.table::fifelse(nchar(datevec) == 4, paste0(datevec, "-01-01"), datevec)
}

get_expenditure <- function() {
    # load
    new <- data.table::fread(
        EXPEND_FP,
        select = list(
            character = c("Activity Identifier", "Transaction Internal Reference",
                          "Transaction Type", "Transaction Date",
                          "Transaction Value Date", "disease area",
                          "program area", "Receiver org(s)",
                          "Transaction Amount")
        ),
        sep = ","
    )
    # transform 
    names(new) <- clean_names(names(new))

    ## parse transaction amount
    new <- new[transaction_amount != "" & !is.na(transaction_amount), ]
    new[, transaction_amount := clean_numeric(transaction_amount)]

    ## fix dates
    ## older values in the spreadsheet are full dates, newer are just years
    date_cols <- c("transaction_date", "transaction_value_date")
    new[, (date_cols) := lapply(.SD, .convert_date), .SDcols = date_cols]
    ## - convert to datetime
    ##   if this fails, date conversion was unsuccessful
    new[, transaction_date := as.Date(transaction_date)]
    new[, transaction_value_date := as.Date(transaction_value_date)]
    return(new)
}

get_disbursement <- function() {
    # load
    new <- data.table::fread(
        DISB_FP,
        select = list(
            character = c("Activity Identifier",
                          "Transaction Type", "Transaction Date",
                          "Transaction Value Date", "disease area",
                          "program area", "Receiver org(s)"),
            numeric = c("Transaction Amount")
        ),
        sep = ","
    )
    # transform
    names(new) <- clean_names(names(new))
    new <- new[transaction_date != ""]
    new <- new[transaction_amount != "" & !is.na(transaction_amount), ]
    date_cols <- c("transaction_date", "transaction_value_date")
    ## convert "YYYY" dates to Jan 1
    new[, (date_cols) := lapply(.SD, \(x) as.Date(.convert_date(x))),
                                .SDcols = date_cols]
    return(new)
}

####-------------------------# main #------------------------------####

merge_unitaid <- function() {
    .g("PROCESS RAW DATA")
    # load input data
    .g("  Loading and processing previous year data.")
    prev <- get_prev_output()
    prev <- prev[, .(activity_identifier, transaction_date,
                     transaction_amount,
                     disease_area, program_area, receiver_orgs)]
    # rm duplicate IDs since we just need one's information to fill in missing
    prev <- prev[, lapply(.SD, \(x) x[!is.na(x)][1]),
                 by = c("activity_identifier", "transaction_date", "transaction_amount"),
                 .SDcols = c("disease_area", "program_area", "receiver_orgs")]

    .g("  Loading and processing UNITAID expenditure data.")
    curr <- get_expenditure()

    .g("  Loading and processing UNITAID disbursement data.")
    disb <- get_disbursement()
    if (! "transaction_internal_reference" %in% names(disb))
        disb[, transaction_internal_reference := NA_character_]
    
    
    curr[, year := year(transaction_date)]
    if (max(curr$year) != REPORT_YEAR) {
        curr <- rbind(curr, disb)
    }
    curr[, year := NULL]

    # Left-join old to new expenditure data, so we can use old to fill in
    # gaps in new
    .g("  Merging old with new.")
    unitaid <- merge(curr, prev,
                     by = c("activity_identifier",
                            "transaction_date",
                            "transaction_amount"),
                     suffixes = c("", "_old"),
                     all.x = TRUE
                     )
 
    # fill in new missing data with old values
    .g("  Imputing new missing with old data")
    unitaid[disease_area == "", disease_area := disease_area_old]
    unitaid[program_area == "", program_area := program_area_old]    
    unitaid[receiver_orgs == "", receiver_orgs := receiver_orgs_old]
    ## drop old cols
    unitaid[, grep("_old", names(unitaid)) := NULL]
 
 
    # save
    .g("SAVE")
    unitaid[, c("transaction_type", "transaction_value_date") := NULL]
    save_dataset(unitaid, OUTPUT_NAME,
                 channel = "unitaid", stage = "raw")

    .g("  Final dataset, first few rows:")
    print(head(unitaid))
}


#
# main
#
merge_unitaid()
