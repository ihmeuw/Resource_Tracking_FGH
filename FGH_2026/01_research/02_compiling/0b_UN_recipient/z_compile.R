code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))


last_recip_yr <- report_year - 1


# load recipient fractions for each channel
channels <- c(
    "paho",
    "unaids",
    "unfpa",
    "unicef",
    "who"
)

dt <- data.table()
for (chan in channels) {
    dt <- rbind(
        dt,
        fread(get_path("compiling", "int", paste0(chan, "_recip_fracs.csv")))
    )
}
dt <- dt[between(year, 2000, 2023)]


dt[iso3 %in% c("XKX", "KSV"), iso3 := "XKX"]


# test data
if (dt[is.na(iso3) | iso3 == "", .N] > 0)
    stop("All rows must have a valid ISO3 code.")

nyear <- length(2000:last_recip_yr)
if (dt[frac > 0, uniqueN(year), by = channel][V1 != nyear, .N] > 0)
    stop(
        "All channels must have recipient fractions for the full time period."
    )

if (dt[frac < 0, .N] > 0)
    stop("All recipient fractions must be non-negative.")

if (dt[, sum(frac), by = .(channel, year)][abs(V1 - 1) > 1e-6, .N] > 0)
    stop(
        "Recipient fractions for all channel-years must sum to 1"
    )



# save
save_dataset(
    dt, "un_recip.csv",
    channel = "compiling",
    stage = "int"
)
