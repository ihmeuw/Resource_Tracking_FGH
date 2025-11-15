
code_repo <- 'FILEPATH'



report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))



pre_kws <- data.table(readstata13::read.dta13(
    get_path("wb", "int", "iati_pre_kws.dta")
))
post_kws <- data.table(readstata13::read.dta13(
    get_path("wb", "int", "iati_post_kws.dta")
))


frct_cols <- grep("^final_.+_frct$", names(post_kws), value = TRUE)
frct_cols <- frct_cols[frct_cols != "final_total_frct"]
post_kws <- post_kws[, c(names(pre_kws), frct_cols), with = FALSE]

## ensure fractions sum exactly to 1
post_kws[, tmp := rowSums(.SD), .SDcols = frct_cols]
post_kws[, (frct_cols) := lapply(.SD, function(x) x / tmp),
         .SDcols = frct_cols]
post_kws[, tmp := NULL]



dah_cols <- gsub("_frct", "_DAH", frct_cols)
dah_cols <- gsub("final_", "", dah_cols)
post_kws[, (dah_cols) := lapply(.SD, function(x) x * DAH),
         .SDcols = frct_cols]


#
# tag for COVID
#
covid_rgx <- "COVID|CORONAVIRUS|CORONA VIRUS"

post_kws[, covid := FALSE]
post_kws[grepl(covid_rgx, proj_name), covid := TRUE]
post_kws[grepl(covid_rgx, proj_desc), covid := TRUE]
## if COVID is picked up in earlier/later disbursements, it is typically because
## COVID was appended as a project theme (if pre-2020) or because COVID is used
## as a motivating factor for the project (if post-2023). We don't want to count
## this as a COVID project - we want to keep other PA assignments
post_kws[YEAR < 2020 | YEAR > 2023, covid := FALSE]


post_kws[covid == TRUE, (dah_cols) := 0]
post_kws[covid == TRUE, oid_covid_DAH := DAH]


# test
dah_cols <- c(dah_cols, "oid_covid_DAH")
post_kws[, tmp := rowSums(.SD), .SDcols = dah_cols]
stopifnot( post_kws[abs(DAH - tmp) > 1e-6, .N] == 0 )
post_kws[, tmp := NULL]

post_kws[, (frct_cols) := NULL]






save_dataset(
    post_kws,
    "iati_post_kws_cleaned.csv",
    channel = "wb",
    stage = "int"
)

