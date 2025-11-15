#----# Docstring #----# ####
# Project:  FGH 
# Purpose:  PAHO Unallocable reallocation
#---------------------------# 
library(tidyverse)

#----# Environment Prep #----# ####rm(list=ls())
code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
IN_DIR <- "FILEPATH"
adb <- fread(get_path("compiling", "fin", "DAH_ADB_PDB_1990_2024.csv"))
dt <- adb[ELIM_CH == 0 & ELIM_DONOR == 0]


# Cleaning functions
# Get dataset splitting total into country totals
split_countryfrac <- function(dt, years) {
    countryfrac <- dt[YEAR %in% years]
    countryfrac <- countryfrac[ISO3_RC != "QZA"]
    countryfrac <- countryfrac[, .(dah = sum(DAH_24, na.rm = TRUE)), 
                               by = .(ISO3_RC, RECIPIENT_COUNTRY)]
    countryfrac[, RECIPIENT_COUNTRY := toupper(RECIPIENT_COUNTRY)]
}

cf <- split_countryfrac(dt, c(2000:2023))
#
# Post-process PAHO extracted recipient data ================================ 
#
paho_raw <- fread(file.path(IN_DIR, "PAHO", "extractions_cleaned.csv"))
paho <- paho_raw[location != "Global"]
paho <- paho[iso3 %in% unique(cf$ISO3_RC)]
paho <- paho[, .(
    RECIPIENT_COUNTRY = country_clean,
    TOTAL = total,
    ISO3_RC = iso3,
    YEAR = year)]
## drop high-income recips
ig <- fread(get_path("meta", "locs", "wb_historical_incgrps.csv"))
paho <- merge(paho,
              ig[, .(YEAR, ISO3_RC, INC_GROUP)],
              by = c("YEAR", "ISO3_RC"),
              all.x = TRUE)
paho[is.na(INC_GROUP), INC_GROUP := ""]
paho <- paho[INC_GROUP != "H"]



paho <- paho[, .(TOTAL = sum(TOTAL, na.rm = TRUE)), 
             by = .(ISO3_RC, RECIPIENT_COUNTRY, YEAR)]
countryfracs <- paho[, frac := (TOTAL / sum(TOTAL, na.rm = TRUE)), by = YEAR]

# avg 2006-2011 data for 2000-2005 estimates
pre06fracs <- countryfracs[YEAR <= 2011, .(frac = sum(frac, na.rm = TRUE)), by = ISO3_RC]
pre06fracs <- pre06fracs[, frac := frac/(sum(frac))]
pre06fracs <- pre06fracs[, .(YEAR = 2000:2005),
                         by = names(pre06fracs)]
# avg 2011-2018 data for 2012-2017 estimates
fracs1217 <- countryfracs[YEAR %in% c(2011,2018), .(frac = sum(frac, na.rm = TRUE)), by = ISO3_RC]
fracs1217 <- fracs1217[, frac := frac/(sum(frac))]
fracs1217 <- fracs1217[, .(YEAR = 2012:2017),
                       by = names(fracs1217)]
countryfracs <- countryfracs[,.(ISO3_RC, frac, YEAR)]

# Rescale datasets based on adbpdb changes
rescaleyrs <- function(adb, fracstorescale, refyrs, imputedyrs) {
    ctry_ref <- split_countryfrac(adb, refyrs)
    ctry_ref <- ctry_ref[ISO3_RC != "WLD"]
    ctry_ref[, dah_share_ref := dah / sum(dah, na.rm = TRUE)]
    ctry_ref[, dah := NULL]
    ctry_fracs <- data.table()
    for (yr in imputedyrs) {
        new_fracs <- split_countryfrac(adb, c(yr))
        new_fracs <- new_fracs[ISO3_RC != "WLD"]
        new_fracs[, dah_share := dah / sum(dah, na.rm = TRUE)]
        new_fracs[, year := yr]
        ctry_fracs <- bind_rows(ctry_fracs, new_fracs)
        ctry_fracs[, dah := NULL]
        ctry_fracs[, RECIPIENT_COUNTRY := NULL]
    }
    ctry_scales <- merge(ctry_fracs, ctry_ref, by = c("ISO3_RC"), all.x = T)
    ctry_scales <- ctry_scales[, change := dah_share/dah_share_ref]
    ctry_scales <- ctry_scales[, .(ISO3_RC, year, change)]
    setnames(ctry_scales, "year", "YEAR")
    
    scaled <- merge(fracstorescale, ctry_scales, by = c("YEAR", "ISO3_RC"), all.x = T)
    scaled <- scaled[, adj_fraction := frac * change]
    scaled[, adj_fraction := adj_fraction * 
               (sum(frac, na.rm = TRUE) / sum(adj_fraction, na.rm = TRUE)), 
           by = YEAR]
    scaled[, `:=`(
        frac = adj_fraction,
        adj_fraction = NULL
    )]
    scaled <- scaled[!is.na(frac), .(YEAR, ISO3_RC, frac)]
}

scaled06 <- rescaleyrs(dt, pre06fracs, c(2006:2011), c(2000:2005))
scaled1217 <- rescaleyrs(dt, fracs1217, c(2011, 2018), c(2012:2017))

fin <- rbind(scaled1217, scaled06, countryfracs)
fin <- fin[, .(
    year = YEAR,
    iso3 = ISO3_RC,
    channel = "PAHO",
    frac
)]

fwrite(
    fin,
    get_path("compiling", "int", "paho_recip_fracs.csv")
)
