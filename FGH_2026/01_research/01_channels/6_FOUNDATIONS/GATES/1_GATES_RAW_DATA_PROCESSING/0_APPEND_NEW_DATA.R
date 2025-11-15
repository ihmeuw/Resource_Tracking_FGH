#----# Docstring #----#
# Project:  FGH
# Purpose:  Append New Gates Foundation Grant Disbursements to Historical
#---------------------#
code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(
    lubridate,
    readxl,
    data.table
)

REPORT_YEAR <- dah.roots$report_year
PREVIOUS_RY <- dah.roots$prev_report_year


clean_old <- function(old) {
    colnames(old) <- gsub(" ", "_", tolower(colnames(old)))
    return(old)
}

clean_new <- function(new_) {
    #
    # format dates (want them all to be ISO standard YYYY-MM-DD)
    #
    date_cols <- c("committment_date",
                   "expected_starting_date",
                   "expected_completion_date")
    # guess date format and use first match
    fmts <-  c("ymd", "mdy", "dmy", "ydm")
    new_[, date_fmt := ""]
    for (dc in date_cols) {
        # convert date if not in proper format
        tryCatch({
            new_[, eval(dc) := as.character(lubridate::date(get(dc)))]
        }, error = function(e) {
            message(paste0("*** Could not convert '", dc, "' to date, guessing format."))
            new_[, paste0(dc, "_orig") := get(dc)]
            new_[, date_fmt := unlist(lapply(
                get(dc),
                \(x) lubridate::guess_formats(x, orders = fmts)[1]
                ))]
            new_[, eval(dc) := as.Date(get(dc), format = date_fmt)]
        })
        new_[, eval(dc) := as.character(get(dc))]
    }
    new_[, date_fmt := NULL]
    return(new_)
}


# MAIN ========================================================================

#
# PREP OLD DATA ====
#
old <- fread(get_path(
    "BMGF", "raw",
    paste0("oecd_reports_appended_", PREVIOUS_RY, ".csv"),
    report_year = PREVIOUS_RY
))
old <- clean_old(old)


#
# PREP NEW DATA ====
#
new <- fread(get_path(
    "BMGF", "raw",
    paste0("BMGF_new_OECD_report_", PREVIOUS_RY, ".csv")
))
colnames(new) <- gsub(" ", "_", tolower(colnames(new)))

# implement necessary cleaning to make "new" compatible with "oecd"
if (! "extending_agency" %in% names(new))
    new[, extending_agency := "Bill & Melinda Gates Foundation"]
new[, sdg_focus := NULL]

setnames(new,
         c("yearreporting", "extending_agency", "channel_of_delivery_name",
           "short_description/project_title", "amounts_extended",
           "amounts_received", "purpose_oecd_crs_code", "description"),
         c("year", "donor_name", "recipient_agency", "project_title", "outflow",
           "repayment", "purpose_code", "purpose"))

if (length(base::setdiff(names(new), names(old))) != 0) {
    stop("new data has columns not in old data. please resolve before next step")
}

new <- clean_new(new)


#
# MERGE ====
#
stopifnot(max(old$year) < min(new$year))
merged <- rbind(old, new, fill = TRUE) # fill bc clean_new may add cols

save_dataset(merged,
             paste0("oecd_reports_appended_", REPORT_YEAR),
             channel = "BMGF", stage = "raw")
