#
# Prepare input data for DAH forecasts
#   a) DAH by Source-Channel-HFA: constant fraction approach
#   b) DAH by Source-Recipient: raking
#   c) DAH by Channel-HFA-Recipient through 2050: raking
library(data.table)
library(arrow)
library(ggplot2)
library(viridis)

scenario <- 'reference'
project <- "ID"
dah_d_folder <- "ID"
if(scenario == "reference") {
  WORK_DIR <- paste0("FILEPATH")
} else {
  WORK_DIR <- paste0("FILEPATH", scenario, "FILEPATH")
}
dir.create(WORK_DIR, showWarnings = FALSE)
dir.create(paste0(WORK_DIR, "FILEPATH"), showWarnings = FALSE)

RETRO_END <- list(
  SOURCE = 2025,
  CHANNEL = 2025,
  HFA = 2024,
  RECIP = 2023
)

## Retrieve location metadata
source("FILEPATH")
locs <- get_location_metadata(location_set_id = ID, release_id = ID)[level == 3]
lox <- unique(locs$ihme_loc_id)

#
# Prospective DAH-D and DAH-R ================================================
#

# DAH-D (DAH by source/donor)
if(scenario == "reference") {
  dah_s <- fread(paste0("FILEPATH"))[iso3 != "AGGREGATE"]
  setnames(dah_s, c("iso3"), c("source"))
} else if(scenario == "better") {
  dah_s <- fread(paste0('FILEPATH'))[iso3 != 'AGGREGATE']
  setnames(dah_s, c("iso3", 'mean'), c("source", 'dah_donor_ref'))
} else if(scenario == "worse") {
  dah_s <- fread(paste0("FILEPATH"))[iso3 != "AGGREGATE"]
  setnames(dah_s, c("iso3", 'dah_donated_worse'), c("source", 'dah_donor_ref'))
}

# DAH-R (DAH by recipient country, including OTH)
if(scenario == "reference") {
  dah_sr_modeled <- fread(paste0("FILEPATH"))
  dah_sr_modeled_nons <- fread(paste0("FILEPATH"))
} else if(scenario == "better") {
  dah_sr_modeled <- fread(paste0("FILEPATH"))
  dah_sr_modeled_nons <- fread(paste0("FILEPATH"))
} else if(scenario == "worse") {
  dah_sr_modeled <- fread(paste0("FILEPATH"))
  dah_sr_modeled_nons <- fread(paste0("FILEPATH"))
}

dah_sr_modeled <- dah_sr_modeled[, .(year, donor, recip = iso3, dah = mean)]
dah_sr_modeled_nons <- dah_sr_modeled_nons[year <= 2030]
dah_sr_modeled_nons <- dah_sr_modeled_nons[, .(year, donor, recip = iso3, dah = dah_recip)]
dah_sr_modeled <- rbind(dah_sr_modeled, dah_sr_modeled_nons)

## standardize recipient names
dah_sr_modeled[!(recip %in% c(lox, "OTH", "UNALLOCABLE_DAH_WLD", "UNALLOCABLE_DAH_INK", "QZA", "UNALLOCABLE_DAH_QZA")), recip := "OTH"]
dah_sr_modeled[recip %like% c("UNALLOCABLE"), recip := "UNALLOCABLE_DAH_QZA"]
dah_sr_modeled <- dah_sr_modeled[, .(dah = sum(dah)),
               by = .(year, recip, donor)]
dah_r <- dah_sr_modeled[, .(dah = sum(dah)), by = c("year", "recip")]

# Make a version of DAH-R that is the total only across RESIDUAL sources for raking source-to-recipient
if(scenario == "reference") {
  dah_d <- fread(paste0("FILEPATH"))
} else if(scenario == "better") {
  dah_d <- fread(paste0('FILEPATH'))
  setnames(dah_d, 'mean', 'dah_donor_ref')
} else if(scenario== "worse") {
  dah_d <- fread(paste0("FILEPATH"))
  setnames(dah_d, 'dah_donor_ref_ACTUAL', 'dah_donor_ref')
}

dah_d <- dah_d[year == 2025]
dah_d <- dah_d[!iso3 %in% c("AGGREGATE", "AGG", "PRIVATE", "DEBT", "OTHER", "UNALL", "CHN", "OTHERPUB_NONGBD")]
top_10_sources <- dah_d[order(-dah_donor_ref)][1:10, iso3]
all_sources <- c(top_10_sources, "RESID")

dah_r_resid <- copy(dah_sr_modeled[!donor %in% top_10_sources])[, .(dah = sum(dah)), by = c("year", "recip")]

#
# Retrospective DAH ==========================================================
#

ret_dah <- fread("FILEPATH")

ret_dah[source == "PRIVINK", source := "PRIVATE"]
non_GBD_locs <- setdiff(unique(ret_dah$iso3_rc), lox)
ret_dah[iso3_rc %in% non_GBD_locs,
        iso3_rc := "UNALLOCABLE_DAH_QZA"]

## re-aggregate
ret_dah_hfa <- ret_dah[, lapply(.SD, sum, na.rm = TRUE),
                       .SDcols = grep("^hfa_", names(ret_dah), value = TRUE),
                       by = .(year, source, channel, recip = iso3_rc, currency, 
                              channel_last_rc_year, channel_last_healthfocus_year, source_channel_end_year)]

ret_dah_pa <- ret_dah[, lapply(.SD, sum, na.rm = TRUE),
                      .SDcols = grep("^pa_", names(ret_dah), value = TRUE),
                      by = .(year, source, channel, recip = iso3_rc, currency, 
                             channel_last_rc_year, channel_last_healthfocus_year, source_channel_end_year)]

## Reshape long by health focus area and program area
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

dah_sc <- arrow::read_feather(file.path(WORK_DIR, "FILEPATH"))
dah_sc <- dah_sc[,
                 .(dah_ref = sum(dah_ref)),
                 by = .(year, source, channel)]
#
# Calculate DAH by Channel-Recipient (C-R)
#

# 1. DAH by Source-Channel-HFA -> DAH by Channel-HFA
#
## Use SC estimates and aggregate across source
dah_c <- dah_sc[, .(dah_ref = sum(dah_ref)), by = .(year, channel)]


# 2. DAH by Channel -> DAH by Channel-Recipient
#
ret_dah2 <- copy(ret_dah_pa)
ret_dah2[, channel_rc_average_start_year := channel_last_rc_year - 5]
ret_dah2[source %in% c("DMA", "GRD", "PRI", "SUR", "TON", "VEN"), channel_rc_average_start_year := 2022]
ret_dah2[source %in% c("ASM", "PSE"), channel_rc_average_start_year := 2019]
ret_dah2[source %in% c("GUM"), channel_rc_average_start_year := 2015]


r_frac_by_ch <- ret_dah2[year >= channel_rc_average_start_year & year <= channel_last_rc_year,
                         .(dah = sum(dah)),
                         by = .(channel, recip)]
r_frac_by_ch[, ch_yr_tot := sum(dah), by = .(channel)]
r_frac_by_ch <- r_frac_by_ch[ch_yr_tot > 0]
r_frac_by_ch[, recip_frac := dah / ch_yr_tot]
r_frac_by_ch[, recip_total_dah := sum(dah), by = .(recip)]

## Subset to prospective years
dah_cr <- merge(
  dah_c[year > RETRO_END$RECIP, .(year, channel, dah_ref)],
  r_frac_by_ch[, .(channel, recip, recip_frac)],
  by = c("channel"),
  all.x = TRUE,
  allow.cartesian = TRUE
)

dah_cr[is.na(recip_frac), `:=`(
  recip = "UNALLOCABLE_DAH_QZA",
  recip_frac = 1
)]
dah_cr[, dah_ref := dah_ref * recip_frac]
setnafill(dah_cr, fill = 0, cols = "dah_ref")

dah_cr[, c("recip_frac") := NULL]
dah_cr <- dah_cr[!(dah_ref == 0)]


#
# Calculate DAH by Source-Recipient
ret_dah2 <- copy(ret_dah_pa)
ret_dah2[, channel_rc_average_start_year := channel_last_rc_year - 5]
ret_dah2[source %in% c("DMA", "GRD", "PRI", "SUR", "TON", "VEN"), channel_rc_average_start_year := 2022]
ret_dah2[source %in% c("ASM", "PSE"), channel_rc_average_start_year := 2019]
ret_dah2[source %in% c("GUM"), channel_rc_average_start_year := 2015]

r_frac_by_s <- ret_dah2[year >= channel_rc_average_start_year & year <= channel_last_rc_year,
                        .(dah = sum(dah)),
                        by = .(source, recip)]
r_frac_by_s[, s_yr_tot := sum(dah), by = .(source)]
r_frac_by_s <- r_frac_by_s[s_yr_tot > 0]
r_frac_by_s[, recip_frac := dah / s_yr_tot]

# Make fractions for missing countries based on historical mean
average_fractions <- copy(r_frac_by_s)
average_fractions <- average_fractions[, .(recip_frac = mean(recip_frac)), by = c("recip")]
average_fractions[, sum_frac := sum(recip_frac)]
average_fractions[, recip_frac := recip_frac / sum_frac]
average_fractions[, sum_frac := NULL]
for(country in c("GRL", "BMU", "VIR", "MNP")) {
  hfa_frac_by_sc_extracountries <- average_fractions[, source := country]
  r_frac_by_s <- rbind(r_frac_by_s, hfa_frac_by_sc_extracountries, fill = T)
}

r_frac_by_s[, recip_total_dah := sum(dah, na.rm=T), by = .(recip)]


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

# drop source-to-recipient estimates from top 10 sources and replace with AFModel forecasts
dah_sr_top10 <- copy(dah_sr_modeled)[donor %in% top_10_sources & year >= min(dah_sr$year)]
dah_sr_top10 <- dah_sr_top10[,.(source = donor, year, recip, dah_ref = dah)]
dah_sr <- dah_sr[!source %in% top_10_sources]
dah_sr <- rbind(dah_sr, dah_sr_top10)

dah_sr_residonly <- copy(dah_sr)[!source %in% top_10_sources]

#
# Finalize & Save ===========================================================
#

message("* Saving ret_dah, dah_r, dah_sch, dah_chr, dah_scp, dah_cpr, dah_sr...")



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

# 2. DAH-R represents one of the margins we need to rake to
dah_r <- dah_r[!(dah < 1e-6 & year > 2023)]
dah_r <- dah_r[, .(year, recip, dah_ref = dah)]
arrow::write_feather(
  dah_r,
  file.path(WORK_DIR, "FILEPATH")
)

dah_r_residonly <- dah_r_resid[!(dah < 1e-6 & year > 2023)]
dah_r_residonly <- dah_r_residonly[, .(year, recip, dah_ref = dah)]
arrow::write_feather(
  dah_r_residonly,
  file.path(WORK_DIR, "data", "FILEPATH")
)

# 5. DAH by Source-Recipient:
#
dah_sr <- dah_sr[!(dah_ref < 1e-6 & year > 2023)]
dah_sr[, src := source]
arrow::write_feather(
  dah_sr,
  file.path(WORK_DIR, "FILEPATH")
)

dah_sr_residonly <- dah_sr_residonly[!(dah_ref < 1e-6 & year > 2023)]
dah_sr_residonly[, src := source]
arrow::write_feather(
  dah_sr_residonly,
  file.path(WORK_DIR, "FILEPATH")
)

# 4. DAH by Channel-Recipient:

dah_cr[, src := paste(channel)]
arrow::write_feather(
  dah_cr,
  file.path(WORK_DIR, "FILEPATH")
)
