#
# Clean up the ADB PDB further so that it is ready for standard analyses and
# forecasting.
#
rm(list = ls(all.names = TRUE))
code_repo <- 'FILEPATH'


REPORT_YEAR <- 2024

source(paste0(code_repo, "/FGH_", REPORT_YEAR, "/utils.R"))
source("FILEPATH/get_location_metadata.R")
pacman::p_load(patchwork, kableExtra)


OUTPUT_DIR <- get_path("compiling", "output", "results")
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)


EI_CURR_YEAR <- dah_cfg$ei_base_year
dah_yr <- paste0("DAH_", substr(EI_CURR_YEAR, 3, 4))


loc_dt <- fread(get_path("meta", "locs", "fgh_location_set.csv"))
gbd_locs_dt <- loc_dt[dah_location == FALSE & level == 3, ]
ig_dt <- fread(get_path("meta", "locs", "wb_historical_incgrps.csv"))
names(ig_dt) <- tolower(names(ig_dt))

 

#
# load main DAH database - in EI currency year
#
# new - report year
new <- fread(
    "FILEPATH/EI_DAH_ADB_PDB_1990_2024.csv"
)
names(new) <- gsub(dah_yr, "DAH", names(new))
## drop double-counted transfers
new <- new[ELIM_CH == 0 & ELIM_DONOR == 0]

#
# Reassign income sectors based on donor names
#
new[, `:=`(
    source = INCOME_SECTOR,
    source_group = NA_character_
)]

## Gates
new[(DONOR_NAME == "BMGF" | CHANNEL == "BMGF"), source := "BMGF"]
new[source == "BMGF", source := "GATES"]

main_pub_locs <- c(names(dah_cfg$crs$donors), "CHN")

new <- merge(
    new,
    gbd_locs_dt[, .(ISO_CODE = ihme_loc_id, flag = 1)],
    by = "ISO_CODE",
    all.x = TRUE
)
setnafill(new, fill = 0, cols = "flag")
new[INCOME_SECTOR == "PUBLIC" & flag == 1, source := ISO_CODE]
new[INCOME_SECTOR == "PUBLIC" & flag == 0, source := "OTHERPUB_NONGBD"]
## mark which public donors are not main donors
new[
    INCOME_SECTOR == "PUBLIC",
    source_group := fifelse(
        source %in% main_pub_locs,
        "MAINPUB",
        "OTHERPUB"
    )
]


# Private sources
new[INCOME_SECTOR == "INK", source := "PRIVINK"]
new[source %like% "PRIV", source_group := "PRIVATE"]

# Other sources
new[is.na(source_group), source_group := source]




#
# Relabel channel
#
new[CHANNEL == "BMGF", CHANNEL := "GATES"]
new[CHANNEL == "NGO" | CHANNEL == "INTL_NGO", CHANNEL := "NGO"]


#
# Reassign recipients
#
new[INKIND == 1, ISO3_RC := "INK"]

new[ISO3_RC %in% c("QZA", "INK", "WLD"),
    ISO3_RC := paste0("UNALLOCABLE_DAH_", ISO3_RC)]




#
# Aggregate the data
#
agg <- new[, lapply(.SD, sum, na.rm = TRUE),
           .SDcols = grep("DAH", names(new), value = TRUE),
           by = .(YEAR, source_group, source, CHANNEL, ISO3_RC)]
names(agg) <- tolower(names(agg))

setnafill(agg,
          fill = 0,
          cols = grep("dah", names(agg), value = TRUE))



#
# Relabel HFAs and PAs 
#
pa_cols <- paste0(dah_cfg$regular_pa_vars, "_dah")
setnames(agg,
         pa_cols,
         paste0("pa_", pa_cols))


hfa_cols <- paste0(dah_cfg$regular_hfa_vars, "_dah")
### note, "unalloc" and "other" have no sub-program areas, so they are
### technically both HFAs and PAs
agg[, `:=`(
    unalloc_dah = pa_unalloc_dah,
    other_dah = pa_other_dah
)]
setnames(agg,
         hfa_cols,
         paste0("hfa_", hfa_cols))

#
# Add source and channel names
#
agg <- merge(
    agg,
    loc_dt[, .(source = ihme_loc_id, source_name = location_name)],
    by = "source",
    all.x = TRUE
)
agg[, source_name := fcase(
    source == "DEBT", "Debt Repayments",
    source == "GATES", "Gates Foundation",
    source == "OTHER", "Other Sources",
    source == "OTHERPUB_NONGBD", "Other Governments (non-GBD)",
    source == "PRIVINK", "Corporate Donations",
    source == "PRIVATE", "Private Philanthropy",
    source == "UN", "Other UN Agencies",
    source == "WHO", "WHO",
    source == "UNALL", "Unallocated",
    rep_len(TRUE, .N), source_name
)]

stopifnot(agg[is.na(source_name), .N] == 0)


agg[channel %like% "^BIL_",
    channel_name := paste(source_name, "- Bilateral")]

agg[, channel_name := fcase(
    channel == "WB_IDA", "World Bank - IDA",
    channel == "WB_IBRD", "World Bank - IBRD",
    channel == "UNFPA", "UNFPA",
    channel == "UNICEF", "UNICEF",
    channel == "WHO", "WHO",
    channel == "AsDB", "Asian Development Bank",
    channel == "NGO", "NGOs",
    channel == "INTL_NGO", "International NGOs",
    channel == "UNAIDS", "UNAIDS",
    channel == "GAVI", "GAVI",
    channel == "GFATM", "Global Fund",
    channel == "CEPI", "CEPI",
    channel == "UNITAID", "UNITAID",
    channel == "IDB", "Inter-American Development Bank",
    channel == "EC", "European Commission",
    channel == "AfDB", "African Development Bank",
    channel == "GATES", "Gates Foundation",
    channel == "PAHO", "PAHO",
    channel == "EEA", "European Economic Area Grants",
    channel == "US_FOUND", "US Foundations",
    rep_len(TRUE, .N), channel_name
)]

stopifnot(agg[is.na(channel_name), .N] == 0)



#
# Add recipient information
#
agg <- merge(
    agg,
    loc_dt[, .(iso3_rc = ihme_loc_id,
               rc_name = location_name,
               rc_region = region_name,
               rc_super_region = super_region_name)],
    by = "iso3_rc",
    all.x = TRUE
)

agg[, rc_name := fcase(
    iso3_rc == "BES", "Caribbean Netherlands",
    iso3_rc == "GLP", "Guadeloupe",
    iso3_rc == "GUF", "French Guiana",
    iso3_rc == "MTQ", "Martinique",
    iso3_rc == "XKX", "Kosovo",
    iso3_rc == "CUW", "Curaçao",
    iso3_rc == "SXM", "Sint Maarten",
    rep_len(TRUE, .N), rc_name
)]

agg[, rc_region := fcase(
    iso3_rc == "BES", "Caribbean",
    iso3_rc == "GLP", "Caribbean",
    iso3_rc == "GUF", "Caribbean",
    iso3_rc == "MTQ", "Caribbean",
    iso3_rc == "CUW", "Caribbean",
    iso3_rc == "SXM", "Caribbean",
    iso3_rc == "XKX", "Central Europe",
    rep_len(TRUE, .N), rc_region
)]
agg[, rc_super_region := fcase(
    iso3_rc == "BES", "Latin America and Caribbean",
    iso3_rc == "GLP", "Latin America and Caribbean",
    iso3_rc == "GUF", "Latin America and Caribbean",
    iso3_rc == "MTQ", "Latin America and Caribbean",
    iso3_rc == "CUW", "Latin America and Caribbean",
    iso3_rc == "SXM", "Latin America and Caribbean",
    iso3_rc == "XKX", "Central Europe, Eastern Europe, and Central Asia",
    rep_len(TRUE, .N), rc_super_region
)]


agg <- merge(
    agg,
    ig_dt[, .(year, iso3_rc, rc_inc_group = inc_group)],
    by = c("year", "iso3_rc"),
    all.x = TRUE
)
agg[iso3_rc %in% c("MSR", "SHN", "WLF", "AIA", "CXR", "BES", "GLP", "GUF", "MTQ", "XKX"),
    rc_inc_group := "Unallocable"]

agg[, rc_name := fcase(
    iso3_rc == "UNALLOCABLE_DAH_QZA", "Unallocable DAH",
    iso3_rc == "UNALLOCABLE_DAH_INK", "In-Kind DAH",
    iso3_rc == "UNALLOCABLE_DAH_WLD", "World DAH",
    rep_len(TRUE, .N), rc_name
)]
agg[iso3_rc %like% "UNALLOCABLE_DAH", `:=`(
    rc_region = "Unallocable",
    rc_super_region = "Unallocable",
    rc_inc_group = "Unallocable"
)]


stopifnot(agg[is.na(rc_name), .N] == 0)
stopifnot(agg[is.na(rc_region), .N] == 0)
stopifnot(agg[is.na(rc_super_region), .N] == 0)
stopifnot(agg[is.na(rc_inc_group), .N] == 0)



#
# Finalize
#

# set column order
pa_cols <- grep("pa_", names(agg), value = TRUE)
hfa_cols <- grep("hfa_", names(agg), value = TRUE)
agg[, currency := paste(EI_CURR_YEAR, "USD")]


setcolorder(
    agg,
    c(
        "year",
        "source_group", "source", "source_name",
        "channel", "channel_name",
        "iso3_rc", "rc_name", "rc_region", "rc_super_region", "rc_inc_group",
        "currency",
        "dah",
        pa_cols,
        hfa_cols
    )
)


fwrite(
    agg,
    get_path("compiling", "int", "retro_fgh_data.csv")
)

message("* Done")
