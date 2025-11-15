#
# Finalize the standarized FGH data by combining the retro data with the 2025
# predictions
#
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

ig_dt <- fread(get_path("meta", "locs", "wb_historical_incgrps.csv"))
names(ig_dt) <- tolower(names(ig_dt))


 
#
# load main fgh database
#
ret <- fread(get_path("compiling", "int", "retro_fgh_data.csv"))



#
# load 2025 predictions (2023 USD)
#
pr <- fread(get_path("compiling", "fin", "dah_2025plus_preds.csv"))
pr <- pr[
    year > max(ret$year) & scenario == "ref",
    -"scenario"
]
sc_end_years <- unique(pr[, .(source, channel, source_channel_end_year = end_year)])
pr[, end_year := NULL]
    
## no hfa/pa disaggregation or recipient disaggreation
pr[, `:=`(
    hfa_unalloc_dah = dah,
    pa_unalloc_dah = dah,
    iso3_rc = "UNALLOCABLE_DAH_QZA"
)]

## merge on features from main data
pr <- merge(
    pr, unique(ret[, .(source, source_name)]),
    by = "source",
    all.x = TRUE
)
pr <- merge(
    pr, unique(ret[, .(channel, channel_name)]),
    by = "channel",
    all.x = TRUE
)
pr[, `:=`(
    rc_name = "Unallocable DAH",
    rc_region = "Unallocable",
    rc_super_region = "Unallocable",
    rc_inc_group = "Unallocable"
)]


agg <- rbind(ret, pr, fill = TRUE)


# set column order
hfa_cols <- sort(grep("hfa_", names(agg), value = TRUE))
pa_cols <- sort(grep("pa_", names(agg), value = TRUE))

agg <- agg[, lapply(.SD, sum, na.rm = TRUE),
           by = .(
               year,
               source_group, source, source_name,
               channel, channel_name,
               iso3_rc, rc_name, rc_region, rc_super_region, rc_inc_group,
               currency
           ),
           .SDcols = c("dah", hfa_cols, pa_cols)]


#
# Finalize
#


agg[, scenario := "ref"]

# Add end years for retro/prelim data

## calculate and merge on last observed recipient year for each channel
last_rc_obs_years <- agg[! iso3_rc %like% "UNALL",
                         .(channel_last_rc_year = max(year)),
                         by = channel]
## for some channels, we don't actually have real recipient information so we
## want to keep recip unallocable - make last recip year just a recent year with
## unalloc
last_rc_obs_years[channel %in% c("US_FOUND", "EEA", "BIL_CHN"),
                  channel_last_rc_year := REPORT_YEAR]

agg <- merge(
    agg, sc_end_years,
    by = c("source", "channel"),
    all.x = TRUE
)
agg <- merge(
    agg, last_rc_obs_years,
    by = "channel",
    all.x = TRUE
)
## these channels have never had recipient info
agg[channel %in% c("CEPI", "UNITAID"), channel_last_rc_year := REPORT_YEAR]
stopifnot(
    agg[is.na(source_channel_end_year), .N] == 0,
    agg[is.na(channel_last_rc_year), .N] == 0,
    agg[channel_last_rc_year > source_channel_end_year, .N] == 0
)

agg[, channel_last_healthfocus_year := 2024]


# Save

## retrospective with prelim
setorder(agg, year)
setcolorder(
    agg, c(
        "year",
        "source_group", "source", "source_name",
        "channel", "channel_name",
        "iso3_rc", "rc_name", "rc_region", "rc_super_region", "rc_inc_group",
        "currency",
        "dah",
        hfa_cols,
        pa_cols
    )
)

save_dataset(
    agg,
    "fgh_retro_and_prelim.csv",
    channel = "compiling",
    stage = "fin",
    folder = "ei_dah_handoff"
)

## retrospective only
agg_ret <- agg[year <= max(ret$year)]


save_dataset(
    agg_ret,
    "fgh_figure_data",
    channel = "compiling",
    stage = "fin"
)


## for back compatability - stop after FGH2024
fwrite(
    agg_ret,
    "FILEPATH/fgh_figure_data.csv"
)

save_dataset(
    agg_ret,
    "fgh_figure_data",
    channel = "compiling",
    stage = "int"
)


message("* Done")
