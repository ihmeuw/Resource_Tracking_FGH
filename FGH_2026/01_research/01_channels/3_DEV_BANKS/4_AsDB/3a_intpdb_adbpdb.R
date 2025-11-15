#
# Project: FGH
# Channel: AsDB
#
# Load in post kws results and create INTPDB, INC_DISB, and ADBPDB
#

code_repo <- 'FILEPATH'


report_year <- 2024
replenishment_file <- "AsDB_donor_replenishment_220303.xlsx"

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))

locs <- fread(get_path("meta", "locs", "fgh_location_set.csv"))


#
# load post-kws results
#
dt <- as.data.table(readstata13::read.dta13(
    get_path("asdb", "int", "asdb_post_kws.dta")
))
dt[, DAH := DAH * 1e6]
dt[, YEAR := as.integer(YEAR)]
dt[, final_total_frct := NULL]
dt <- dt[!is.na(DAH)]


frct_cols <- grep("final_.+_frct", names(dt), value = TRUE)
dah_cols <- gsub("final_", "", gsub("_frct", "_DAH", frct_cols))

dt[, tmp := rowSums(.SD), .SDcols = frct_cols]
stopifnot(dt[abs(tmp - 1) > 1e-6, .N] == 0)
dt[, (frct_cols) := lapply(.SD, \(f) f/tmp), .SDcols = frct_cols] ## remove slight inconsistencies
dt[, tmp := NULL]

dt[, (dah_cols) := lapply(.SD, function(x) x * DAH),
   .SDcols = frct_cols]


dt <- dt[, c(
    "PROJECT_ID", "YEAR", "PROJECT_NAME", "PROJECT_DESCRIPTION",
    "RECIPIENT_COUNTRY", "ISO3_RC", "FUNDING_COUNTRY", "ISO3_FC",
    "FUNDING_AGENCY", "FUNDING_AGENCY_SECTOR", "FUNDING_AGENCY_TYPE",
    "DATA_LEVEL", "DATA_SOURCE", "RECIPIENT_AGENCY_SECTOR",
    "RECIPIENT_AGENCY_TYPE", "SECTOR", "PURPOSE", "FUNDING_TYPE",
    "DAH", dah_cols
), with = FALSE]

dt <- dt[!is.na(DAH) & YEAR >= 1990]


dt[, tmp := rowSums(.SD), .SDcols = dah_cols]
stopifnot(dt[abs(tmp - DAH) > 1e-6, .N] == 0)
dt[, tmp := NULL]


#
# add covid
#
covid <- fread(get_path("asdb", "fin", "COVID_prepped.csv"))
covid <- covid[, .(DAH = sum(total_amt, na.rm = TRUE)),
               by = .(ISO3_RC = iso3_rc, YEAR, FUNDING_TYPE = grant_loan)]
covid[, oid_covid_DAH := DAH]
covid[, PROJECT_NAME := "COVID-Prepped Disbursement"]

dt <- rbind(dt, covid, fill = TRUE)

dah_cols <- grep("_DAH", names(dt), value = TRUE)
setnafill(dt, fill = 0, cols = dah_cols)




#
# add in-kind
#
inkind <- as.data.table(openxlsx::read.xlsx(
    get_path("asdb", "raw", "AsDB_INKIND_RATIO_1990_[report_year].xlsx")
))
inkind <- inkind[, .(YEAR = year, inkind_ratio)]
setorder(inkind, YEAR)

inkind <- inkind[YEAR < dah_cfg$report_year]
inkind <- rbind(inkind, data.table(YEAR = dah_cfg$report_year, inkind_ratio = NA))

# predict report-year inkind ratio using 3 year weighted average
inkind[, `:=`(
    lag1 = shift(inkind_ratio, 1),
    lag2 = shift(inkind_ratio, 2),
    lag3 = shift(inkind_ratio, 3)
)]
inkind[, inkind_pred := (1/2) * lag1 + (1/3) * lag2 + (1/6) * lag3]
inkind[YEAR == dah_cfg$report_year, inkind_ratio := inkind_pred]
inkind <- inkind[, .(YEAR, inkind_ratio)]

dt <- merge(dt, inkind, by = "YEAR", all.x = TRUE)

ink <- copy(dt)
ink[, DAH := DAH * inkind_ratio]
ink[, (dah_cols) := lapply(.SD, function(x) x * inkind_ratio),
     .SDcols = dah_cols]

dt[, INKIND := 0]
ink[, INKIND := 1]
dt <- rbind(dt, ink)
dt[, CHANNEL := "AsDB"]


#
# save INTPDB
#
save_dataset(dt, "AsDB_INTPDB_FGH[report_year]",
             channel = "asdb",
             stage = "fin")




#
# identify income sources using replenishment data
#
replen <- as.data.table(openxlsx::read.xlsx(get_path(
    "asdb", "raw", replenishment_file
)))

## distribute replenishments for rounds 7-13
r0 <- replen[between(round, 7, 13)]
r0[, YEAR := fcase(
    round == 7, 1997,
    round == 8, 2001,
    round == 9, 2005,
    round == 10, 2009,
    round == 11, 2013,
    round == 12, 2017,
    round == 13, 2021
)]
r0[, annual_commitment := commitment / 4]
r0 <- r0[, .(yi = seq(0, 3)), by = .(country, YEAR, annual_commitment)]
r0[, YEAR := YEAR + yi]
r0 <- r0[, .(YEAR, country, annual_commitment)]


## distribute replenishments for round 6
r6 <- replen[round == 6]
r6[, `:=`(YEAR = 1992, annual_commitment = commitment / 5)]
r6 <- r6[, .(yi = seq(0, 4)), by = .(country, YEAR, annual_commitment)]
r6[, YEAR := YEAR + yi]
r6 <- r6[, .(YEAR, country, annual_commitment)]

replen <- rbind(r0, r6)


# calculate total revenue
revenue <- as.data.table(openxlsx::read.xlsx(
    get_path("asdb", "raw", "AsDB_INKIND_RATIO_1990_[report_year].xlsx")
))
revenue[, TOTAL_REVENUE := OCR_total_revenue + ADF_total_revenue + TASF_total_revenue]
revenue <- revenue[year < dah_cfg$report_year, .(YEAR = year, TOTAL_REVENUE)]
setorder(revenue, YEAR)
revenue <- rbind(revenue,
                 data.table(YEAR = dah_cfg$report_year, TOTAL_REVENUE = NA))
revenue[, `:=`(lag1 = shift(TOTAL_REVENUE, 1),
              lag2 = shift(TOTAL_REVENUE, 2),
              lag3 = shift(TOTAL_REVENUE, 3))]
revenue[, pred_tot_rev := (1/2) * lag1 + (1/3) * lag2 + (1/6) * lag3]
revenue[YEAR == dah_cfg$report_year, TOTAL_REVENUE := pred_tot_rev]
revenue <- revenue[, .(YEAR, TOTAL_REVENUE)]


# determine how much of total revenue is not covered by replenishment commitments,
#   and assign to "other source" and "debt"
replen_agg <- replen[, .(annual_commitment = sum(annual_commitment)),
                     by = .(YEAR)]
revenue <- merge(revenue, replen_agg, by = "YEAR", all.x = TRUE)
revenue[, other_source := TOTAL_REVENUE - annual_commitment]
other_source <- revenue[, .(YEAR, annual_commitment = other_source,
                            country = "other source")]
# now disaggregate "other source" into DEBT vs. OTHER 
#
debt <- fread(get_path("asdb", "raw", "AsDB_DEBT.csv"))
other_source <- merge(other_source, debt[, .(YEAR = year, debt_fraction)],
                      by = "YEAR", all.x = TRUE)
stopifnot(other_source[is.na(debt_fraction), .N] == 0)

debt_source <- copy(other_source)
debt_source[, country := "debt"]

other_source[, annual_commitment := annual_commitment * (1 - debt_fraction)]
other_source[, debt_fraction := NULL]
debt_source[, annual_commitment := annual_commitment * debt_fraction]
debt_source[, debt_fraction := NULL]


income <- rbind(replen, other_source, debt_source)
setnames(income, "annual_commitment", "INCOME_ALL")
income <- income[YEAR <= dah_cfg$report_year]

# total income by year
income[, INCOME_TOTAL_YR := sum(INCOME_ALL), by = YEAR]
# share of total income attributable to row
income[, INCOME_ALL_SHARE := INCOME_ALL / INCOME_TOTAL_YR]
income[is.na(INCOME_ALL_SHARE), INCOME_ALL_SHARE := 1] ## 1990 and 1991


save_dataset(income, "AsDB_INCOME_SHARES_FGH[report_year]",
             channel = "asdb",
             stage = "fin")



#
# merge income with disbursements
#
dt <- fread(get_path("asdb", "fin", "AsDB_INTPDB_FGH[report_year].csv"))
disb_tot <- dt[, .(DISB_TOTAL = sum(DAH, na.rm = TRUE)),
               by = .(YEAR)]
disb_tot[, YEAR := as.integer(YEAR)]

inc_disb <- merge(income, disb_tot, by = "YEAR", all.x = TRUE)
# imputed disbursements by income source and year
inc_disb[, DISBURSEMENT := DISB_TOTAL * INCOME_ALL_SHARE]
inc_disb[, OUTFLOW := DISBURSEMENT]

## generate "inkind source"
inc_disb <- merge(inc_disb, inkind, by = "YEAR", all.x = TRUE)
inc_disb[, INKIND := 0]

inc_ink <- copy(inc_disb)
inc_ink[, DISBURSEMENT := DISBURSEMENT * inkind_ratio]
inc_ink[, `:=`(
    OUTFLOW = DISBURSEMENT,
    INKIND = 1,
    INCOME_ALL = 0
)]

inc_disb <- rbind(inc_disb, inc_ink)



#
# clean up
#
setnames(inc_disb, "country", "DONOR_NAME")
inc_disb[DONOR_NAME == "Hongkong", DONOR_NAME := "China"]
inc_disb[DONOR_NAME == "Taipei", DONOR_NAME := "Taiwan"]
inc_disb[DONOR_NAME == "Korea, Republic of", DONOR_NAME := "Republic of Korea"]
inc_disb[DONOR_NAME == "Turkey", DONOR_NAME := "Türkiye"]
inc_disb[DONOR_NAME == "United States", DONOR_NAME := "United States of America"]

inc_disb <- merge(inc_disb,
                  locs[, .(DONOR_NAME = location_name, iso3 = ihme_loc_id)],
                  by = "DONOR_NAME", all.x = TRUE)

stopifnot( inc_disb[! DONOR_NAME %in% c("other source", "debt") & is.na(iso3), .N] == 0 )


inc_disb[, `:=`(
    CHANNEL = "AsDB",
    INCOME_SECTOR = "PUBLIC",
    INCOME_TYPe = "CENTRAL",
    DONOR_COUNTRY = DONOR_NAME,
    SOURCE = "AsDB Online Projects Database",
    GHI = "AsDB"
)]
inc_disb[DONOR_NAME == "other source",
         `:=`(
             INCOME_SECTOR = "OTHER", INCOME_TYPE = "UNALL"
         )]
inc_disb[DONOR_NAME == "debt",
         `:=`(
             INCOME_SECTOR = "DEBT", INCOME_TYPE = "OTHER"
         )]

inc_disb <- inc_disb[, .(
    YEAR, INCOME_ALL, DONOR_NAME, DONOR_COUNTRY, INCOME_SECTOR, INCOME_TYPE,
    INCOME_TOTAL_YR, INCOME_ALL_SHARE, DISB_TOTAL, DISBURSEMENT, OUTFLOW,
    inkind_ratio, INKIND, ISO_CODE = iso3, CHANNEL, SOURCE
)]
names(inc_disb) <- toupper(names(inc_disb))

save_dataset(inc_disb, "AsDB_INC_DISB_FINAL_FGH[report_year]",
             channel = "asdb",
             stage = "int")




#
# MERGE ADB/PDB
#
intpdb <- fread(get_path("asdb", "fin", "AsDB_INTPDB_FGH[report_year].csv"))
intpdb <- intpdb[INKIND != 1]
intpdb[, YEAR := as.integer(YEAR)]

stopifnot( intpdb[is.na(ISO3_RC), .N] == 0 )

intagg <- intpdb[, lapply(.SD, sum, na.rm = TRUE),
                 .SDcols = grep("DAH", names(intpdb), value = TRUE),
                 by = .(YEAR, ISO3_RC)]



adbpdb <- merge(inc_disb, intagg,
                by = c("YEAR"),
                ## must allow cartesian since each donor-year maps to multiple
                ## recipient countries
                allow.cartesian = TRUE,
                all.x = TRUE)

# apply income shares to DAH columns
all_dah_cols <- grep("DAH", names(adbpdb), value = TRUE)
adbpdb[INKIND == 0,
       (all_dah_cols) := lapply(.SD, function(x) x * INCOME_ALL_SHARE),
      .SDcols = all_dah_cols]


adbpdb[INKIND == 1,
       (all_dah_cols) := lapply(.SD, function(x) x * INKIND_RATIO * INCOME_ALL_SHARE),
      .SDcols = all_dah_cols]

adbpdb <- adbpdb[!is.na(DAH), c(
    "YEAR", "DONOR_NAME", "DONOR_COUNTRY", "ISO_CODE",
    "INCOME_SECTOR", "INCOME_TYPE", 
    "CHANNEL",
    "ISO3_RC",
    "INKIND",
    all_dah_cols
), with = FALSE]


save_dataset(adbpdb, "AsDB_ADB_PDB_FGH[report_year]",
             channel = "asdb",
             stage = "fin")

