#
# Project: FGH
#
# Finalize the post-keyword search AidData database so that it can be used to
# assign HFA fractions for each of the China donor agencies.
#
code_repo <- 'FILEPATH'


report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))


#
# load and process post-KWS data
#
post_kws <- readstata13::read.dta13(
    get_path("china", "int", "aiddata_china_health_post_kws.dta")
)
setDT(post_kws)

pre_kws <- fread(get_path("china", "int", "aiddata_china_health_pre_kws.csv"))

## drop unwanted columns added by KWS
post_kws <- post_kws[, c(
    names(pre_kws),
    grep("final_.+_frct$", names(post_kws), value = TRUE)
), with = FALSE]
post_kws[, final_total_frct := NULL]

setnames(post_kws, "amount_usd", "DAH")

## merge on original strings from pre_kws since readstata13 mangles them
str_cols <- c("funding_agencies", "title", "description")
post_kws[, (str_cols) := NULL]
post_kws <- merge(post_kws,
                  unique(pre_kws[, c("aiddata_record_id", str_cols), with = FALSE]),
                  by = "aiddata_record_id",
                  all.x = TRUE)



#
# apply HFA fraction columns to project amount 
#
frct_cols <- grep("final_.+_frct$", names(post_kws), value = TRUE)
dah_cols <- paste0(
    gsub("final_|_frct", "", frct_cols), "_DAH"
)

post_kws[, (dah_cols) := lapply(.SD, \(x) DAH * x),
         .SDcols = frct_cols]
post_kws[, (frct_cols) := NULL]

# test that the sum of the components is equal to the total (some error expected
#  due to floating point arithmetic)
post_kws[, tmp := rowSums(.SD), .SDcols = dah_cols]
if (post_kws[abs(DAH - tmp) > 5, .N] > 0) {
    stop("DAH components do not sum to total")
}

# knowing that any remaining error is very minor, adjust the HFAs to ensure the
#   observed project total is retained exactly
post_kws[, (dah_cols) := lapply(.SD, \(x) (x / tmp) * DAH ),
         .SDcols = dah_cols]
post_kws[, tmp := rowSums(.SD), .SDcols = dah_cols]
if (post_kws[abs(DAH - tmp) > 0.1, .N] > 0) {
    stop("DAH components do not sum to total")
}
post_kws[, tmp := NULL]



#
# adjust covid tagged activities to be entirely for oid_covid_DAH
#
post_kws[covid == TRUE,
         (dah_cols) := lapply(.SD, \(x) 0),
         .SDcols = dah_cols]
post_kws[covid == TRUE,
         oid_covid_DAH := DAH]
setnafill(post_kws, fill = 0, cols = "oid_covid_DAH")



#
# finalize
#
post_kws[, year := as.numeric(year)]
post_kws[, agency := ""]
post_kws[funding_agencies %like% "China Ministry of Commerce",
         agency := paste0(agency, "|mofcom")]

post_kws[funding_agencies %like% "China International Development Cooperation Agency",
         agency := paste0(agency, "|cidca")]

post_kws[funding_agencies %like% "National Health Commission of China",
         agency := paste0(agency, "|nhc")]

post_kws[funding_agencies %like% "Export-Import Bank of China",
         agency := paste0(agency, "|eximbank")]

post_kws[agency == "", agency := "other"]
post_kws[, agency := gsub("^\\|", "", agency)]

## cidca was created from mofcom in 2018, so if the project is from mofcom and
##   cidca, let's assume cidca as the agency since it is the "more granular" agency,
##   in some sense.
post_kws[agency == "mofcom|cidca", agency := "cidca"]


# aggregate
##  note, not filtering 'flow_class' to "ODA-like", so we are including "OOF-like"
##  and "Vague (Official Finance)" projects, with the assumption that these still
##  help inform HFA fractions
dah_cols <- grep("DAH", names(post_kws), value = TRUE)
agency_dah <- post_kws[, lapply(.SD, sum),
                       .SDcols = dah_cols,
                       by = .(year, agency, covid)]

save_dataset(agency_dah, "aiddata_agency_hfa",
             channel = "china",
             stage = "int",
             format = "csv")

