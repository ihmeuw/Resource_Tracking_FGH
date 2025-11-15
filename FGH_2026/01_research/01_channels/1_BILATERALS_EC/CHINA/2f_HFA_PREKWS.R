#
# Project: FGH
#
# Load and process the raw AidData database which we will use to assign HFAs.
# Clean data and send to keyword search.
#
code_repo <-'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))


get_n_days <- function(year) {
    if (length(year) == 0) return(0)
    ly <- lubridate::leap_year(year)
    o <- rep_len(NA_real_, length(year))
    o[ly] <- 366
    o[!ly] <- 365
    return(o)
}


# generate a list of days per year between a start day and end day
# start_day: a date, the beginning of the period
# end_day: a date, the end of the period
# incl_start: a logical, whether to include the start day in the output
# incl_end: a logical, whether to include the end day in the output
seq_days_per_year <- function(start_day,
                              end_day,
                              incl_start = TRUE,
                              incl_end = TRUE) {
    start_day <- as.Date(start_day)
    end_day <- as.Date(end_day)
    styr <- data.table::year(start_day)
    endyr <- data.table::year(end_day)
    myrs <- seq(styr, endyr)
    myrs <- myrs[! myrs %in% c(styr, endyr)]
    # first year days
    .sd <- if (incl_start) paste0(styr + 1, "-01-01") else paste0(styr, "-12-31")
    fy_days <- as.Date(.sd) - start_day
    fy_days <- as.list(setNames(as.integer(fy_days), styr))
    # last year days
    .ed <- if (incl_end) paste0(endyr - 1, "-12-31") else paste0(endyr, "-01-01")
    ly_days <- end_day - as.Date(.ed)
    ly_days <- as.list(setNames(as.integer(ly_days), endyr))
    # middle years days
    my_days <- setNames(lapply(myrs, get_n_days), myrs)
    if (styr == endyr) {
        return(fy_days)
    }
    if (length(my_days) == 0) {
        return(c(fy_days, ly_days))
    }
    return(c(fy_days, my_days, ly_days))
}

year_days <- function(start_day,
                      end_day,
                      incl_start = TRUE,
                      incl_end = TRUE) {
    yrs <- seq_days_per_year(start_day, end_day, incl_start, incl_end)
    return(list(names(yrs), unlist(yrs)))
}


aiddata <- openxlsx::read.xlsx(
    get_path("china", "raw",
             c("AidDatas_Global_Chinese_Development_Finance_Dataset_Version_3_0",
               "AidDatasGlobalChineseDevelopmentFinanceDataset_v3.0.xlsx")),
    sheet = "GCDF_3.0",
    sep.names = "_"
)
setDT(aiddata)
names(aiddata) <- tolower(names(aiddata))

aiddata <- aiddata[recommended_for_aggregates == "Yes"]

health <- aiddata[sector_name == "HEALTH", .(
    aiddata_record_id,
    commitment_year,
    start_year = implementation_start_year,
    end_year = completion_year,
    commitment_date = `commitment_date_(mm/dd/yyyy)`,
    start_date = `actual_implementation_start_date_(mm/dd/yyyy)`,
    planned_start_date = `planned_implementation_start_date_(mm/dd/yyyy)`,
    completion_date = `actual_completion_date_(mm/dd/yyyy)`,
    planned_completion_date = `planned_completion_date_(mm/dd/yyyy)`,
    funding_agencies,
    funding_agencies_type,
    recipient = `recipient_iso-3`,
    flow_type,
    flow_class,
    status,
    title,
    description,
    sector_name,
    covid,
    amount_usd = `amount_(nominal_usd)`
)]

health <- health[!is.na(amount_usd)]
health[, covid := fifelse(is.na(covid), FALSE, TRUE)]
health[, original_row_id := .I]

# fix excel dates
date_cols <- grep("_date", names(health), value = TRUE)
health[, (date_cols) := lapply(.SD, openxlsx::convertToDate),
       .SDcols = date_cols]


# define year of disbursement
health[commitment_year == end_year | start_year == end_year,
       year := end_year]

health[is.na(year) &
           is.na(planned_start_date) & is.na(start_date) &
           is.na(completion_date) & is.na(planned_completion_date),
       year := fcase(
           !is.na(end_year), end_year,
           !is.na(start_year), start_year,
           rep_len(TRUE, .N), commitment_year # default if no other info
       )]

## break up multi-year flows
tmp <- health[is.na(year), -"year"]
tmp[, date1 := start_date]
tmp[is.na(date1), date1 := planned_start_date]
tmp[is.na(date1), date1 := commitment_date]  ## if no start date, assume commitment date

tmp[, date2 := completion_date]
tmp[is.na(date2), date2 := planned_completion_date]
tmp[is.na(date2), date2 := date1]  ## assume start date - so all in one year

tmp[, year1 := fifelse(is.na(start_year), year(date1), start_year)]
tmp[, year2 := fifelse(is.na(end_year), year(date2), end_year)]
tmp[, nyears := year2 - year1 + 1]

tmp[, id := .I]

expand <- tmp[, .(year = names(seq_days_per_year(date1, date2)),
                  n_days = as.numeric(seq_days_per_year(date1, date2))),
              by = id]
expand[, total_days := sum(n_days), by = id]
expand[, year_frac  := n_days / total_days]

expand <- merge(expand, tmp, by = "id", all.x = TRUE)
setnames(expand, "amount_usd", "total_project_amount")

# distribute total project amount across years
expand[, amount_usd := total_project_amount * year_frac]

stopifnot( # ensure total project amount is preserved
    expand[, abs(sum(amount_usd) - unique(total_project_amount)) > 0.1,
           by = id][, sum(V1)] == 0
)

expand[, c("date1", "date2", "year1", "year2", "nyears", "n_days",
           "id", "total_days", "year_frac") := NULL]


one_year <- health[!is.na(year)]
one_year[, total_project_amount := amount_usd]
health <- rbind(
    one_year,
    expand
)

health[, grep("date", names(health), value = TRUE) := NULL]



# launch kws

## clean search columns in R before sending to stata
search_cols <- c("title", "description")
health[, paste0(search_cols, "_clean") := lapply(.SD, string_to_std_ascii),
       .SDcols = search_cols]


# save as csv, and dta for KWS
save_dataset(health, "aiddata_china_health_pre_kws",
             channel = "china",
             stage = "int",
             format = "csv")

save_dataset(health, "aiddata_china_health_pre_kws",
             channel = "china",
             stage = "int",
             format = "dta")

create_Health_config(
    data_path = get_path("china", "int", "aiddata_china_health_"),
    channel_name = "china",
    varlist = paste0(search_cols, "_clean"),
    language = "english",
    function_to_run = 1 # all stages of KWS
)

launch_Health_ADO(
    channel_name = "china",
    job_mem = "3",
    runtime =  "0:05:00",
    queue = "all.q",
    errors_path = file.path(dah_cfg$h, "china_kws.err"),
    output_path = file.path(dah_cfg$h, "china_kws.out")
)


