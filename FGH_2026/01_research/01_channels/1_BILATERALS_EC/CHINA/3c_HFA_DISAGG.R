#
# Project: FGH
#
# Apply HFA fractions from AidData to our tracked total envelopes.
#
code_repo <- 'FILEPATH'


report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))

#
# Load and prep HFA fractions from AidData
#
hfas <- fread(get_path("china", "int", "aiddata_agency_hfa.csv"))
hfas <- hfas[covid == 0, -"covid"]  ## covid is seperate from the rest of data
hfas[, `:=`(
    unalloc_DAH = 0.,
    oid_covid_DAH = NULL
)]

hfas[, DONOR_NAME := fcase(
    agency == "mofcom", "CHN_MOFCOM",
    agency == "cidca", "CHN_CIDCA",
    agency == "nhc", "CHN_NHC",
    agency == "eximbank", "CHN_EXIM",
    rep_len(TRUE, .N), agency
)]
setnames(hfas, "year", "YEAR")

## convert amounts to fractions
dah_cols <- grep("_DAH", names(hfas), value = TRUE)
hfas[, (dah_cols) := lapply(.SD, function(x) x / DAH), .SDcols = dah_cols]
hfas[, DAH := NULL]
hfas <- melt(hfas[, -"agency"],
             id.vars = c("YEAR", "DONOR_NAME"),
             variable.name = "HFA",
             value.name = "frct_new")

## drop agencies that we don't want to use AidData for
## NOTE:
##   We do not use AidData for NHC or MOE because they represent medical teams
##   and scholarships and other "small" DAH that would not usually be indexed in
##   news agencies and thus won't show up in AidData. This is evident looking at
##   how little data they have in AidData.
##   We would like to include CIDCA, but they have patchy data in AidData so for
##   now it is safer to exclude them here.
hfas <- hfas[DONOR_NAME %in% c("CHN_MOFCOM", "CHN_EXIM")]


#
# Load and prep ADB PDB (without COVID funding)
#
adb <- fread(get_path("china", "int", "BIL_CHINA_ADBPDB_1990_[report_year]_preCOVID.csv"))

## ensure all HFAs are present in ADB PDB
for (hfa_pa in unique(hfas$HFA)) {
    if (! hfa_pa %in% names(adb)) {
        adb[, (hfa_pa) := 0.]
    }
}

## convert amounts to fractions
dah_cols <- grep("_DAH", names(adb), value = TRUE)
adb[, (dah_cols) := lapply(.SD, \(x) x / DAH), .SDcols = dah_cols]
adb <- melt(adb,
            measure.vars = grep("_DAH", names(adb), value = TRUE),
            variable.name = "HFA",
            value.name = "frct")
group_cols <- setdiff(names(adb), c("HFA", "frct"))

stopifnot( length(unique(adb$HFA)) == length(unique(hfas$HFA)) )


#
# Merge fractions onto ADB PDB and redistribute DAH
#
adb <- merge(adb, hfas,
             by = c("YEAR", "DONOR_NAME", "HFA"),
             all.x = TRUE)


# for donor-years where we have HFA data, apply fractions to total amounts
adb[!is.na(frct_new), amount_new := DAH * frct_new]
# for donor-years where we don't have HFA data, keep the original amounts
adb[is.na(frct_new), amount_new := DAH * frct]

# check that the new amounts sum to the original DAH
adb[, tmp := sum(amount_new), by = group_cols]
if (adb[abs(tmp - DAH) > 0.1, .N] > 0)
    stop("New HFA disaggregation doesn't add up to original DAH")

adb[, c("frct", "frct_new", "tmp") := NULL]


#
# Cast wide and save
#
adb <- dcast(adb, ... ~ HFA, value.var = "amount_new")

save_dataset(adb, "BIL_CHINA_ADBPDB_1990_[report_year]_HFA",
             channel = "china",
             stage = "int")

