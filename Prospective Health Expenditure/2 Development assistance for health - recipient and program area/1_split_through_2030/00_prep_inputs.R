#
# Purpose: Prepare input data for DAH forecasts
#   a) DAH by Source-Channel-HFA: Constant fraction approach
#   b) DAH by Source-Recipient: Raking
#   c) DAH by Channel-HFA-Recipient through 2050: Raking
#

library(data.table)
library(arrow)
library(ggplot2)
library(viridis)
library(stringr)

scenario <- "reference"
project <- "ID"
dah_d_folder <- "ID"

if(scenario == "reference") {
  WORK_DIR <- 'FILEPATH'
} else {
  WORK_DIR <- paste0("FILEPATH", scenario, 'FILEPATH')
}

## Generate data directory
dir.create(WORK_DIR, showWarnings = FALSE)
dir.create(paste0(WORK_DIR, "FILEPATH"), showWarnings = FALSE)

## Set DAH retrospective parameters
RETRO_END <- list(
  SOURCE = 2025,
  CHANNEL = 2025,
  HFA = 2024,
  RECIP = 2023
)

## Retrieve location metadata
source("FILEPATH")
locs <- get_location_metadata(location_set_id = 35, release_id = 16)[level == 3]
lox <- unique(locs$ihme_loc_id)

#
# Prospective DAH-D and DAH-R ================================================
#

# DAH-D (DAH by source/donor)

if(scenario == "reference") {
  dah_s <- fread("FILEPATH")[iso3 != "AGGREGATE"]
  setnames(dah_s, c("iso3"), c("source"))
  dah_s_agg <- fread(paste0("FILEPATH"))[iso3 == "AGGREGATE"]
} else if(scenario == "better") {
  dah_s <- fread('FILEPATH')[iso3 != 'AGGREGATE']
  setnames(dah_s, c("iso3", 'mean'), c("source", 'dah_donor_ref'))
  dah_s_agg <- fread(paste0('FILEPATH'))[iso3 == 'AGGREGATE']
  setnames(dah_s_agg, c('iso3', 'mean'), c('source', 'dah_donor_ref'))
} else if(scenario == "worse") {
  dah_s <- fread('FILEPATH')[iso3 != "AGGREGATE"]
  setnames(dah_s, c("iso3", 'dah_donated_worse'), c("source", 'dah_donor_ref'))
  dah_s_agg <- fread('FILEPATH')[iso3 == "AGGREGATE"]
  setnames(dah_s_agg, c("iso3", 'dah_donated_worse'), c("source", 'dah_donor_ref'))
}

# save out aggregate estimates
fwrite(dah_s_agg, paste0(WORK_DIR, "FILEPATH"))


#
# Retrospective DAH ==========================================================
#

ret_dah <- fread("FILEPATH")

ret_dah[source == "PRIVINK", source := "PRIVATE"]
non_GBD_locs <- setdiff(unique(ret_dah$iso3_rc), lox)
ret_dah[iso3_rc %in% non_GBD_locs & ! iso3_rc %like% "UNALLOCABLE",
        iso3_rc := "UNALLOCABLE_DAH_QZA"]

## Re-aggregate by health focus area and program area
ret_dah_hfa <- ret_dah[, lapply(.SD, sum, na.rm = TRUE),
                       .SDcols = grep("^hfa_", names(ret_dah), value = TRUE),
                       by = .(year, source, channel, recip = iso3_rc, currency, 
                              channel_last_rc_year, channel_last_healthfocus_year, source_channel_end_year)]

ret_dah_pa <- ret_dah[, lapply(.SD, sum, na.rm = TRUE),
                      .SDcols = grep("^pa_", names(ret_dah), value = TRUE),
                      by = .(year, source, channel, recip = iso3_rc, currency, 
                             channel_last_rc_year, channel_last_healthfocus_year, source_channel_end_year)]

## Reshape long by HFA or PA
ret_dah_hfa <- melt.data.table(ret_dah_hfa,
                               id.vars = c("year", "source", "channel", "recip", "currency", 
                                           "channel_last_rc_year", "channel_last_healthfocus_year", "source_channel_end_year"),
                               variable.name = "hfa",
                               variable.factor = FALSE,
                               value.name = "dah")
ret_dah_hfa[, hfa := gsub("_dah", "", hfa)]


ret_dah_pa <- melt.data.table(ret_dah_pa,
                              id.vars = c("year", "source", "channel", "recip", "currency", 
                                          "channel_last_rc_year", "channel_last_healthfocus_year", "source_channel_end_year"),
                              variable.name = "pa",
                              variable.factor = FALSE,
                              value.name = "dah")
ret_dah_pa[, hfa := gsub("_dah", "", pa)]

#
# Build derivatives ===========================================================
#

#
# Calculate DAH by Source-Channel-HFA (S-C-H)
#

# 1. DAH by Source -> DAH by Source-Channel
#

## Calculate channel fractions for each source
c_frac_by_s <- ret_dah_hfa[, .(dah = sum(dah)), by = .(year, source, channel, source_channel_end_year)]
c_frac_by_s[, s_yr_tot := sum(dah), by = .(year, source)]
c_frac_by_s <- c_frac_by_s[s_yr_tot > 0]
c_frac_by_s[, chan_frac := dah / s_yr_tot]

### Hold average last 3 years source-to-channel fraction constant and rescale to 1
tmp_values <- copy(c_frac_by_s)[year == source_channel_end_year, - "year"][, -"chan_frac"]

## Country-specific fixes
tmp <- c_frac_by_s[year >= eval(source_channel_end_year - 2) & year <= source_channel_end_year]
tmp <- rbind(tmp, c_frac_by_s[source %in% c("ASM", "PSE") & year == 2019][, year := source_channel_end_year])
tmp <- rbind(tmp, c_frac_by_s[source %in% c("GUM") & year == 2015][, year := source_channel_end_year])

tmp <- dcast.data.table(tmp, source + channel + source_channel_end_year ~ year, value.var = 'chan_frac')
tmp <- melt.data.table(tmp, id.vars = c('source', 'channel', 'source_channel_end_year'), value.name = 'chan_frac', variable.factor = F)
tmp[is.na(chan_frac), chan_frac := 0]
tmp <- tmp[variable <= source_channel_end_year]

## Calculate mean fractions
tmp <- tmp[, .(chan_frac = mean(chan_frac)), by = c("source", "channel", "source_channel_end_year")]
tmp <- tmp[, .(year = seq(min(tmp$source_channel_end_year), 2100)), by = names(tmp)]
tmp <- tmp[year > source_channel_end_year]
tmp <- merge(tmp, tmp_values, by = c("source", "channel", "source_channel_end_year"), all = T)
tmp[, sum_frac := sum(chan_frac), by = c("year", "source")]
tmp[year>source_channel_end_year, chan_frac := chan_frac / sum_frac, by = c("year", "source")]
tmp[, sum_frac := NULL]
tmp[is.na(dah), dah := 0][is.na(s_yr_tot), s_yr_tot := 0]

## Append
c_frac_by_s <- rbind(
  c_frac_by_s[year <= source_channel_end_year],
  tmp[year > source_channel_end_year]
)


# Add in channel fractions for countries without retrospective data
# Average fractions from other donors across non-bilateral channels
viable_channels <- sort(unique(c_frac_by_s$channel))
viable_channels <- viable_channels[!(viable_channels %like% "BIL_")]
viable_channels <- viable_channels[!(viable_channels %like% "DB")]
viable_channels <- viable_channels[!viable_channels %in% c("EC", "EEA", "INTL_NGO", "NGO")]

average_fractions <- copy(c_frac_by_s)[year >= 2025 & channel %in% viable_channels]
average_fractions <- average_fractions[, .(chan_frac = mean(chan_frac)), by = c("year", "channel")]
average_fractions[, source_channel_end_year := 2024]
average_fractions[, sum_frac := sum(chan_frac), by = c("year")]
average_fractions[year>source_channel_end_year, chan_frac := chan_frac / sum_frac, by = c("year")]
average_fractions[, sum_frac := NULL]
for(country in c("GRL", "BMU", "VIR", "MNP")) {
  c_frac_by_s_extracountries <- average_fractions[, source := country]
  c_frac_by_s <- rbind(c_frac_by_s, c_frac_by_s_extracountries, fill = T)
}

## apply channel fraction to DAH-S to get DAH-SC
dah_sc <- merge(
  dah_s[, .(year, source, dah_donor_ref)],
  c_frac_by_s[, .(year, source, channel, chan_frac)],
  by = c("year", "source"),
  all.x = TRUE,
  allow.cartesian = TRUE
)

dah_sc <- dah_sc[year < 2024 & is.na(chan_frac), chan_frac := 0]
dah_sc[, dah_channel_ref := dah_donor_ref * chan_frac]
dah_sc[, chan_frac := NULL]


# USA-specific updates: constant channel fraction from the last year of generated estimates
usa_2026_channels <- copy(dah_sc[source == "USA" & year == 2026])
usa_2026_channels[, channel_frac := dah_channel_ref / sum(dah_channel_ref)]
usa_2026_channels <- usa_2026_channels[, .(source, channel, channel_frac)]
dah_sc <- merge(dah_sc, usa_2026_channels, by = c("source", "channel"), all.x = T)
dah_sc[source == "USA" & year > 2026, dah_channel_ref := channel_frac * dah_donor_ref]
dah_sc[, channel_frac := NULL]


if (scenario == 'reference') {
  # Adjust Gavi source-to-channel estimates using 2026-2030 replenishment budget data
  gavi <- fread("FILEPATH")
  dah_sc <- merge(dah_sc, gavi, by = c("year", "source", "channel"), all.x = T)
  sources_to_adjust <- unique(dah_sc[!is.na(flow_to_gavi), source])
  
  dah_sc <- merge(
    dah_sc,
    unique(ret_dah[, .(source, channel, source_channel_end_year)]),
    by = c("source", "channel"),
    all.x = TRUE
  )
  
  ## Re-scale forecasts
  dah_sc[source %in% sources_to_adjust & year %in% c(2026:2030), new_gavi := mean(flow_to_gavi, na.rm = T), by = c("year", "source")]
  dah_sc[source %in% sources_to_adjust & year %in% c(2026:2030) & channel == "GAVI", 
         dah_channel_ref := flow_to_gavi]
  dah_sc[source %in% sources_to_adjust & year %in% c(2026:2030) & channel != "GAVI", 
         non_gavi_channel_totals := sum(dah_channel_ref, na.rm = T), by = c("year", "source")]
  dah_sc[source %in% sources_to_adjust & year %in% c(2026:2030) & channel != "GAVI", 
         new_non_gavi_channel_totals := dah_donor_ref - new_gavi]
  dah_sc[source %in% sources_to_adjust & year %in% c(2026:2030) & channel != "GAVI", 
         raking_factor := new_non_gavi_channel_totals / non_gavi_channel_totals]
  dah_sc[source %in% sources_to_adjust & year %in% c(2026:2030) & channel != "GAVI", 
         dah_channel_ref := dah_channel_ref * raking_factor]
  dah_sc[source %in% sources_to_adjust & year %in% c(2026:2030), 
         new_source_total := sum(dah_channel_ref), by = c("year", "source")]
  dah_sc[, c("new_source_total", "raking_factor", "new_non_gavi_channel_totals", "non_gavi_channel_totals", "new_gavi", "currency", "flow_to_gavi") := NULL]
}

# 2. DAH by Source-Channel -> DAH by Source-Channel-HFA
#
# Calculate HFA fractions by source-channel to get DAH by Source-Channel-HFA
h_frac_by_sc <- ret_dah_hfa[, .(dah = sum(dah)), by = .(year, source, channel, hfa, channel_last_healthfocus_year)]
h_frac_by_sc[, sc_yr_tot := sum(dah), by = .(year, source, channel)]
h_frac_by_sc <- h_frac_by_sc[sc_yr_tot > 0] ##don't need hfa-src-chan if no flow
h_frac_by_sc[, hfa_frac := dah / sc_yr_tot]
h_frac_by_sc[, max_observed_year := max(year), by = c("source", "channel")]
tmp_values <- copy(h_frac_by_sc)
tmp_values <- tmp_values[year == max_observed_year]
tmp_values <- tmp_values[, c("year", "hfa_frac") := NULL]

## Country-specific fixes
tmp <- h_frac_by_sc[year >= eval(channel_last_healthfocus_year - 2) & year <= channel_last_healthfocus_year]
tmp <- rbind(tmp, h_frac_by_sc[source %in% c("ASM", "PSE") & year == 2019])
tmp <- rbind(tmp, h_frac_by_sc[source %in% c("GUM") & year == 2015])
tmp <- rbind(tmp, h_frac_by_sc[source %in% c("UNALL") & channel == "GFATM" & year == 2013])

tmp[, channel_last_healthfocus_year_covid := channel_last_healthfocus_year]
tmp[source %in% c("SAU", "KWT") & channel == "GAVI" & hfa == "hfa_oid", channel_last_healthfocus_year_covid := year]
tmp[source %in% c("OTHER", "OTHERPUB_NONGBD") & channel == "NGO" & hfa == "hfa_oid", channel_last_healthfocus_year_covid := max_observed_year]
tmp[source %in% c("ISL") & channel == "CEPI" & hfa == "hfa_oid", channel_last_healthfocus_year_covid := year]
tmp <- tmp[!(hfa == "hfa_oid" & year != '2024' & channel_last_healthfocus_year_covid >= 2024)]
tmp <- tmp[!(hfa == "hfa_oid" & year != '2023' & channel_last_healthfocus_year_covid == 2023)]
tmp <- tmp[, .(hfa_frac = mean(hfa_frac)), by = c("source", "channel", "hfa")]

tmp <- tmp[, .(year = seq(RETRO_END$HFA, 2100)), by = names(tmp)]
tmp <- merge(tmp, tmp_values, by = c("source", "channel", "hfa"), all = T)
tmp <- tmp[, channel_last_healthfocus_year := mean(channel_last_healthfocus_year, na.rm = T), by = c("source")]
tmp <- tmp[!(year == 2024 & channel_last_healthfocus_year >= 2024)]
tmp[, sum_frac := sum(hfa_frac), by = c("year", "source", "channel")]
tmp[, hfa_frac := hfa_frac / sum_frac, by = c("year", "source")]
tmp[, sum_frac := NULL]

# GRL, BMU, VIR, and MNP updates
# Make fractions for missing countries based on historical mean
average_fractions <- copy(tmp)[year >= 2025 & channel %in% viable_channels]
average_fractions <- average_fractions[, .(hfa_frac = mean(hfa_frac, na.rm = T)), by = c("year", "channel", "hfa")]
average_fractions[, channel_last_healthfocus_year := 2024]
average_fractions[, sum_frac := sum(hfa_frac), by = c("year", "channel")]
average_fractions[year>channel_last_healthfocus_year, hfa_frac := hfa_frac / sum_frac, by = c("year", "channel")]
average_fractions[, sum_frac := NULL]

for(country in c("GRL", "BMU", "VIR", "MNP")) {
  hfa_frac_by_sc_extracountries <- average_fractions[, source := country]
  tmp <- rbind(tmp, hfa_frac_by_sc_extracountries, fill = T)
}

## Append
h_frac_by_sc <- rbind(
  h_frac_by_sc[year <= channel_last_healthfocus_year],
  tmp[year > channel_last_healthfocus_year]
)

## Apply channel fraction to DAH-SC to get DAH-SCH
dah_sch <- merge(
  dah_sc[, .(year, source, channel, dah_channel_ref)],
  h_frac_by_sc[, .(year, source, channel, hfa, hfa_frac)],
  by = c("year", "source", "channel"),
  all.x = TRUE,
  allow.cartesian = TRUE
)

## Apply HFA fraction to channel totals
dah_sch[, dah_ref := dah_channel_ref * hfa_frac]
dah_sch[, c("hfa_frac", "dah_channel_ref") := NULL]
dah_sch <- dah_sch[!(dah_ref == 0)]


# 2. DAH by Source-Channel -> DAH by Source-Channel-PA
#
# Calculate PA fractions by source-channel to get DAH by Source-Channel-PA
p_frac_by_sc <- ret_dah_pa[, .(dah = sum(dah)), by = .(year, source, channel, pa, channel_last_healthfocus_year)]
p_frac_by_sc[, sc_yr_tot := sum(dah), by = .(year, source, channel)]
p_frac_by_sc <- p_frac_by_sc[sc_yr_tot > 0]
p_frac_by_sc[, pa_frac := dah / sc_yr_tot]
p_frac_by_sc[, max_observed_year := max(year), by = c("source", "channel")]
tmp_values <- copy(p_frac_by_sc)
tmp_values <- tmp_values[year == max_observed_year]
tmp_values <- tmp_values[, c("year", "pa_frac") := NULL]

## Country-specific fixes
tmp <- p_frac_by_sc[year >= eval(channel_last_healthfocus_year - 2) & year <= channel_last_healthfocus_year]
tmp <- rbind(tmp, p_frac_by_sc[source %in% c("ASM", "PSE") & year == 2019])
tmp <- rbind(tmp, p_frac_by_sc[source %in% c("GUM") & year == 2015])
tmp <- rbind(tmp, p_frac_by_sc[source %in% c("UNALL") & channel == "GFATM" & year == 2013])

tmp[, channel_last_healthfocus_year_covid := channel_last_healthfocus_year]
tmp[source %in% c("SAU", "KWT") & channel == "GAVI" & pa %like% "oid_", channel_last_healthfocus_year_covid := 2023]
tmp[source %in% c("OTHER", "OTHERPUB_NONGBD") & channel == "NGO" & pa %like% "oid_", channel_last_healthfocus_year_covid := max_observed_year]
tmp[source %in% c("ISL") & channel == "CEPI" & pa %like% "oid_", channel_last_healthfocus_year_covid := year]
tmp <- tmp[!(pa %like% c("oid_") & year != 2024  & channel_last_healthfocus_year_covid >= 2024)]
tmp <- tmp[!(pa %like% c("oid") & year != 2023  & channel_last_healthfocus_year_covid == 2023)]
tmp <- tmp[, .(pa_frac = mean(pa_frac)), by = c("source", "channel", "pa")]

tmp <- tmp[, .(year = seq(RETRO_END$HFA, 2100)), by = names(tmp)]
tmp <- merge(tmp, tmp_values, by = c("source", "channel", "pa"), all = T)
tmp <- tmp[, channel_last_healthfocus_year := mean(channel_last_healthfocus_year, na.rm = T), by = c("source")]
tmp <- tmp[!(year == 2024 & channel_last_healthfocus_year >= 2024)]
tmp[, sum_frac := sum(pa_frac), by = c("year", "source", "channel")]
tmp[, pa_frac := pa_frac / sum_frac, by = c("year", "source")]
tmp[, sum_frac := NULL]


# Make fractions for missing countries based on historical mean
average_fractions <- copy(tmp)[year >= 2025 & channel %in% viable_channels]
average_fractions <- average_fractions[, .(pa_frac = mean(pa_frac, na.rm = T)), by = c("year", "channel", "pa")]
average_fractions[, channel_last_healthfocus_year := 2024]
average_fractions[, sum_frac := sum(pa_frac), by = c("year", "channel")]
average_fractions[year>channel_last_healthfocus_year, pa_frac := pa_frac / sum_frac, by = c("year", "channel")]
average_fractions[, sum_frac := NULL]
for(country in c("GRL", "BMU", "VIR", "MNP")) {
  pa_frac_by_sc_extracountries <- average_fractions[, source := country]
  tmp <- rbind(tmp, pa_frac_by_sc_extracountries, fill = T)
}

## Append
p_frac_by_sc <- rbind(
  p_frac_by_sc[year <= channel_last_healthfocus_year],
  tmp[year > channel_last_healthfocus_year]
)

## Apply channel fraction to DAH-SC to get DAH-SCP
dah_scp <- merge(
  dah_sc[, .(year, source, channel, dah_channel_ref)],
  p_frac_by_sc[, .(year, source, channel, pa, pa_frac)],
  by = c("year", "source", "channel"),
  all.x = TRUE,
  allow.cartesian = TRUE
)

## Apply HFA fraction to channel totals
dah_scp[, dah_ref := dah_channel_ref * pa_frac]
dah_scp[, c("pa_frac", "dah_channel_ref") := NULL]
dah_scp <- dah_scp[!(dah_ref == 0)]


#
# Calculate DAH by Channel-HFA-Recipient (C-H-R)
#

# 1. DAH by Source-Channel-PA -> DAH by Channel-PA
#
## Use SCH estimates and aggregate across source
dah_scp[, usa_source := fifelse(source == "USA", TRUE, FALSE)]
dah_cp <- dah_scp[,
                  .(dah_ref = sum(dah_ref)), 
                  by = .(year, usa_source, channel, pa)]


# 2. DAH by Channel-PA -> DAH by Channel-PA-Recipient
#
ret_dah3 <- copy(ret_dah_pa)
ret_dah3[, channel_rc_average_start_year := channel_last_rc_year - 5]
ret_dah3[source %in% c("ASM", "GUM"), channel_rc_average_start_year := 2015]
r_frac_by_cp <- ret_dah3[year >= channel_rc_average_start_year & year <= channel_last_rc_year &
                             dah > 0,
                         .(dah = sum(dah)),
                         by = .(channel, pa, recip)]
r_frac_by_cp[, ch_yr_tot := sum(dah), by = .(channel, pa)]
r_frac_by_cp <- r_frac_by_cp[ch_yr_tot > 0]
r_frac_by_cp[, recip_frac := dah / ch_yr_tot]
r_frac_by_cp[, recip_total_dah := sum(dah), by = .(recip)]

## Subset to only future years
dah_cpr <- merge(
  dah_cp[year > RETRO_END$RECIP, .(year, usa_source, channel, pa, dah_ref)],
  r_frac_by_cp[, .(channel, pa, recip, recip_frac)],
  by = c("channel", "pa"),
  all.x = TRUE,
  allow.cartesian = TRUE
)

dah_cpr[is.na(recip_frac), `:=`(
  recip = "UNALLOCABLE_DAH_QZA",
  recip_frac = 1
)]
dah_cpr[, dah_ref := dah_ref * recip_frac]
setnafill(dah_cpr, fill = 0, cols = "dah_ref")

dah_cpr[, c("recip_frac") := NULL]
dah_cpr <- dah_cpr[!(dah_ref == 0)]


# Modify US bilateral fractions based on FA.gov data =========================
#
dah_cpr[, bilusa := fifelse(
    usa_source == TRUE & channel %in% c("BIL_USA", "NGO"),
    TRUE,
    FALSE
)]

# Use 2024 disbursement data to calculate US bilateral fractions for 2024
#
fa_disb <- fread('FILEPATH')
fa_disb <- fa_disb[year == 2024, -"year"]
setnames(fa_disb, c("country_code", "hfa_pa"), c("recip", "pa"))
fa_disb[recip == "WLD", recip := "UNALLOCABLE_DAH_WLD"]
fa_disb[recip == "CS-KM", recip := "UNALLOCABLE_DAH_QZA"] ## Kosovo
fa_disb <- fa_disb[recip %in% dah_cpr[, unique(recip)]]
fa_disb[, pa := paste0("pa_", pa, "_dah")]
fa_disb <- fa_disb[, .(amt = sum(amt)), by = .(pa, recip)]
fa_disb[, recip_frac := amt / sum(amt), by = pa]
fa_disb <- fa_disb[, .(channel = c("BIL_USA", "NGO")),
                   by = names(fa_disb)]

## Get old 2024 US bilateral estimates
bilusa24_old <- dah_cpr[
    bilusa == TRUE & year == 2024 & recip != "UNALLOCABLE_DAH_INK",
    .(dah = sum(dah_ref)),
    by = .(channel, pa)
]

## Merge on channel-pa totals to each channel-pa-recip fraction
fa_disb <- merge(
    fa_disb,
    bilusa24_old,
    by = c("channel", "pa"),
    all.x = TRUE
)

fa_disb <- fa_disb[!is.na(dah)]
fa_disb[, dah := dah * recip_frac]

## Append on in-kind amount for each channel-pa
inkind <- dah_cpr[
    bilusa == TRUE & year == 2024 & recip == "UNALLOCABLE_DAH_INK",
    .(dah = sum(dah_ref)),
    by = .(channel, pa, recip)
]

bilusa24 <- rbind(
    fa_disb[, .(channel, pa, recip, dah)],
    inkind
)

# Apply US bilateral 2025 cuts derived from FA.gov data
#
bilusa24[, hfa := tstrsplit(pa, "_", keep = 2)]
bilusa24[hfa == "swap", hfa := "swap_hss_total"]
bilusa24[, hfa := paste0("hfa_", hfa, "_dah")]

if (scenario == 'reference') {
  
  ## load award amounts calculated from from ForeignAssistance.gov
  fa_term <- fread('FILEPATH')
  fa_term[hfa_pa == "hfa_swap_hss", hfa_pa := "hfa_swap_hss_total"]
  fa_term[, hfa_pa := paste0(hfa_pa, "_dah")]
  fa_term <- dcast(fa_term,
                   hfa_pa + country_code ~
                     ifelse(terminated == TRUE, "term", "act"),
                   value.var = "amt",
                   fill = 0)
  fa_term[, total := act + term]
  fa_term[, change := act/total - 1]
  fa_term[country_code == "WLD", country_code := "UNALLOCABLE_DAH_WLD"]
  setnames(fa_term, "country_code", "recip")
  
  ## Merge on percent changes by country and hfa/pa
  bilusa25 <- copy(bilusa24)
  setnames(bilusa25, "dah", "dah_2024")
  bilusa25 <- merge(
    bilusa25,
    fa_term[, .(hfa = hfa_pa, recip, hfa_loc_chg = change)],
    by = c("hfa", "recip"),
    all.x = TRUE
  )
  bilusa25 <- merge(
    bilusa25,
    fa_term[, .(pa = hfa_pa, recip, pa_loc_chg = change)],
    by = c("pa", "recip"),
    all.x = TRUE
  )
  bilusa25[, change := fifelse(!is.na(pa_loc_chg), pa_loc_chg, hfa_loc_chg)]
  
  ## Impute with 0 where no change is calculated
  bilusa25[is.na(change), change := 0]
  
  ## Calculate counterfactual DAH for 2025 based on 2024 amounts and changes
  bilusa25[, dah_2025 := dah_2024 * (1 + change)]
  
  ## Use counterfactual DAH as new distribution of pa-locs for 2025+
  bilusa25[, pa_recip_frac := dah_2025 / sum(dah_2025), by = .(channel)]
  bilusa_post_cut_shares <- bilusa25[, .(channel, pa, recip, pa_recip_frac)]
  
  bilusa_post_cut <- dah_cpr[
    bilusa == TRUE & year >= 2025,
    .(dah_ref = sum(dah_ref)),
    by = .(year, channel)
  ]
  bilusa_post_cut <- merge(
    bilusa_post_cut,
    bilusa_post_cut_shares,
    by = "channel",
    all.x = TRUE,
    allow.cartesian = TRUE
  )
  bilusa_post_cut[, dah_ref := dah_ref * pa_recip_frac]
  
}


# Re-combine updated USA bilateral flows with rest of DAH data
#

## replace 2024 with new bilusa24
bilusa24 <- bilusa24[, .(
    year = 2024, channel, pa, recip, dah_ref = dah, usa_source = TRUE, bilusa = TRUE
)]

if (scenario == 'reference') {
  
  ## replace 2025+ with bilusa_post_cut
  bilusa_post_cut <- bilusa_post_cut[, .(
    year, channel, pa, recip, dah_ref, usa_source = TRUE, bilusa = TRUE
  )]
  
  dah_cpr_fin <- rbind(
    dah_cpr[!(bilusa == TRUE & year >= 2024)],
    bilusa24,
    bilusa_post_cut
  )
  
} else {
  
  dah_cpr_fin <- rbind(
    dah_cpr[!(bilusa == TRUE & year == 2024)],
    bilusa24)
  
}


dah_cpr <- copy(dah_cpr_fin)
dah_cpr[, bilusa := NULL]


# Generate dah_chr by aggregating from program areas to HFAs
dah_chr <- copy(dah_cpr)
dah_chr[, hfa := str_extract(gsub("pa_", "", pa), "[^_]+")]
dah_chr[hfa == 'swap', hfa := 'swap_hss_total']
dah_chr[, hfa := paste0('hfa_', hfa)]
dah_chr <- dah_chr[, .(dah_ref = sum(dah_ref)),
                   by = .(channel, year, hfa, recip, usa_source)]


#
# Calculate DAH by Source-Recipient
#
ret_dah2 <- copy(ret_dah_pa)
ret_dah2[, channel_rc_average_start_year := channel_last_rc_year - 5]

## Country-specific fixes
ret_dah2[source %in% c("ASM", "GUM"), channel_rc_average_start_year := 2015]
ret_dah2[source %in% c("PSE"), channel_rc_average_start_year := 2019]
ret_dah2[source %in% c("DMA", "GRD", "PRI", "SUR", "TON", "VEN"), channel_rc_average_start_year := 2022]
r_frac_by_s <- ret_dah2[year >= channel_rc_average_start_year & year <= channel_last_rc_year,
                        .(dah = sum(dah)),
                        by = .(source, recip)]
r_frac_by_s[, s_yr_tot := sum(dah), by = .(source)]
r_frac_by_s <- r_frac_by_s[s_yr_tot > 0]
r_frac_by_s[, recip_frac := dah / s_yr_tot]
r_frac_by_s[, recip_total_dah := sum(dah), by = .(recip)]


# Add in recipient fractions for donors without retrospective data
# Use average fractions from other donors 
average_fractions <- copy(r_frac_by_s)
average_fractions <- average_fractions[, .(recip_frac = mean(recip_frac)), by = c("recip")]
average_fractions[, sum_frac := sum(recip_frac)]
average_fractions[, recip_frac := recip_frac / sum_frac]
average_fractions[, sum_frac := NULL]
for(country in c("GRL", "BMU", "VIR", "MNP")) {
  r_frac_by_s_extracountries <- average_fractions[, source := country]
  r_frac_by_s <- rbind(r_frac_by_s, r_frac_by_s_extracountries, fill = T)
}


## Merge and calculate forecast
dah_sr <- merge(
  dah_s[year > RETRO_END$RECIP, .(year, source, dah_donor_ref)],
  r_frac_by_s[, .(source, recip, recip_frac)],
  by = c("source"),
  all.x = TRUE,
  allow.cartesian = TRUE
)
dah_sr[, dah_ref := dah_donor_ref * recip_frac]

dah_sr[, c("recip_frac", "dah_donor_ref") := NULL]
dah_sr <- dah_sr[!(dah_ref == 0)]

## Replace USA-as-source's fraction-based estimates with the estimates from channel-HFA-recipient
usa_sr <- dah_cpr[
    usa_source == TRUE,
    .(
        source = "USA",
        dah_ref = sum(dah_ref)
    ),
    by = .(year, recip)
]
dah_sr <- rbind(
    dah_sr[source != "USA"],
    usa_sr
)


#
# Finalize & Save ===========================================================
#

message("* Saving ret_dah, dah_r, dah_sch, dah_chr, dah_scp, dah_cpr, dah_sr...")

dah_cpr <- dah_cpr[
    ,
    .(dah_ref = sum(dah_ref)),
    by = .(year, channel, pa, recip)
]
dah_chr <- dah_chr[
    ,
    .(dah_ref = sum(dah_ref)),
    by = .(year, channel, hfa, recip)
]
dah_sr <- dah_sr[
    ,
    .(dah_ref = sum(dah_ref)),
    by = .(year, source, recip)
]


# FINAL ESTIMATES:

# 1. Processed retrospective data
arrow::write_feather(
  ret_dah_hfa,
  file.path(WORK_DIR, "FILEPATH")
)

arrow::write_feather(
  ret_dah_pa,
  file.path(WORK_DIR, "FILEPATH")
)


# 3. DAH by Source-Channel-HFA
arrow::write_feather(
  dah_sch,
  file.path(WORK_DIR, "FILEPATH")
)

# 3. DAH by Source-Channel-PA
arrow::write_feather(
  dah_scp,
  file.path(WORK_DIR, "FILEPATH")
)

#
# Data frames for raking
#

# 4. DAH by Channel-HFA-Recipient
dah_chr[, src := paste(channel, hfa, sep = "-")]
arrow::write_feather(
  dah_chr,
  file.path(WORK_DIR, "FILEPATH")
)

# 4. DAH by Channel-PA-Recipient
dah_cpr[, src := paste(channel, pa, sep = "-")]
arrow::write_feather(
  dah_cpr,
  file.path(WORK_DIR, "FILEPATH")
)


# 5. DAH by Source-Recipient
dah_sr[, src := source]
arrow::write_feather(
  dah_sr,
  file.path(WORK_DIR, "FILEPATH")
)

