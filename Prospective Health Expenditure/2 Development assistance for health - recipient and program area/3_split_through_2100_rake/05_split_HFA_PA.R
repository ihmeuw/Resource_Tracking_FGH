# Purpose: Create output files by channel-recip-HFA and channel-recip-PA

library(data.table)
library(Cairo)
library(ggplot2)

scenario <- 'reference'
project <- "ID"
if(scenario == "reference") {
  WORK_DIR <- paste0("FILEPATH")
} else {
  WORK_DIR <- paste0("FILEPATH", scenario, "FILEPATH")
}

# Get channels to recipients through 2100
final_dah_cr <- fread(paste0(WORK_DIR, "FILEPATH"))
final_dah_cr[recip %like% "UNALLOCABLE", recip := "UNALLOCABLE_DAH_QZA"]
final_dah_cr[recip %like% "UNALLOCABLE", location_name := "Unallocable DAH"]
final_dah_cr <- final_dah_cr[, .(dah_ref = sum(dah_ref)), 
                             by = c("recip", "year", "channel", "currency", "channel_name", "recip_name", "location_id")]

# Get HFA and PA fractions in 2030
dah_hfa_frac <- fread(paste0(WORK_DIR, "FILEPATH"))
dah_hfa_frac <- dah_hfa_frac[, .(channel, recip, hfa, hfa_name, dah_ref, year)]
dah_hfa_frac[recip %like% "UNALLOCABLE", recip := "UNALLOCABLE_DAH_QZA"]
dah_hfa_frac <- dah_hfa_frac[, .(dah_ref = sum(dah_ref)), 
                             by = c("channel", "recip", "hfa", "hfa_name", "year")]
dah_hfa_frac[, tot_ref := sum(dah_ref), by = c("channel", "recip", "year")]
dah_hfa_frac[, hfa_fraction_ref := dah_ref / tot_ref]
dah_hfa_frac[, c("dah_ref", "tot_ref") := NULL]
dah_hfa_frac[, max_channel_recip_yr := max(year), by = c("channel", "recip")]
dah_hfa_frac[, max_channel_recip_yr := mean(max_channel_recip_yr, na.rm = T), by = c("channel", "recip")]
dah_hfa_frac[, min_keep_year := min(max_channel_recip_yr, 2030), by = c("channel", "recip")]
dah_hfa_frac_2030 <- copy(dah_hfa_frac)[year == min_keep_year]
for(yr in 2031:2100) {
  dah_hfa_frac_yr <- copy(dah_hfa_frac_2030)
  dah_hfa_frac_yr[, year := yr]
  dah_hfa_frac <- rbind(dah_hfa_frac, dah_hfa_frac_yr)
}

dah_pa_frac <- fread(paste0(WORK_DIR, "FILEPATH"))
dah_pa_frac <- dah_pa_frac[, .(channel, recip, pa, pa_name, dah_ref, year)]
dah_pa_frac[recip %like% "UNALLOCABLE", recip := "UNALLOCABLE_DAH_QZA"]
dah_pa_frac <- dah_pa_frac[, .(dah_ref = sum(dah_ref)), 
                             by = c("channel", "recip", "pa", "pa_name", "year")]
dah_pa_frac[, tot_ref := sum(dah_ref), by = c("channel", "recip", "year")]
dah_pa_frac[, pa_fraction_ref := dah_ref / tot_ref]
dah_pa_frac[, c("dah_ref", "tot_ref") := NULL]
dah_pa_frac[, max_channel_recip_yr := max(year), by = c("channel", "recip")]
dah_pa_frac[, max_channel_recip_yr := mean(max_channel_recip_yr, na.rm = T), by = c("channel", "recip")]
dah_pa_frac[, min_keep_year := min(max_channel_recip_yr, 2030), by = c("channel", "recip")]
dah_pa_frac_2030 <- copy(dah_pa_frac)[year == min_keep_year]
for(yr in 2031:2100) {
  dah_pa_frac_yr <- copy(dah_pa_frac_2030)
  dah_pa_frac_yr[, year := yr]
  dah_pa_frac <- rbind(dah_pa_frac, dah_pa_frac_yr)
}

# Apply fractions to channel-recipient amounts
final_dah_crh <- copy(final_dah_cr)
final_dah_crh <- merge(final_dah_crh, dah_hfa_frac, by = c("channel", "recip", "year"), all = T)
missing_merge <- (final_dah_crh[is.na(hfa_fraction_ref) & !is.na(dah_ref)])
missing_merge <- unique(missing_merge[, .(recip, channel, year)])
fill_fracs <- dah_hfa_frac[year == 2031][, year := NULL]
fill_fracs <- merge(fill_fracs, missing_merge, by = c("recip", "channel"), all.y = T)
dah_hfa_frac <- rbind(dah_hfa_frac, fill_fracs)

final_dah_crh <- copy(final_dah_cr)
final_dah_crh <- merge(final_dah_crh, dah_hfa_frac, by = c("channel", "recip", "year"), all = T)
final_dah_crh[, dah_ref := dah_ref * hfa_fraction_ref]
final_dah_crh[, c("hfa_fraction_ref") := NULL]
final_dah_crh <- final_dah_crh[!(is.na(dah_ref))]
fwrite(final_dah_crh, paste0(WORK_DIR, "FILEPATH"))

final_dah_crp <- copy(final_dah_cr)
final_dah_crp <- merge(final_dah_crp, dah_pa_frac, by = c("channel", "recip", "year"), all = T)
missing_merge <- (final_dah_crp[is.na(pa_fraction_ref) & !is.na(dah_ref)])
missing_merge <- unique(missing_merge[, .(recip, channel, year)])
fill_fracs <- dah_pa_frac[year == 2031][, year := NULL]
fill_fracs <- merge(fill_fracs, missing_merge, by = c("recip", "channel"), all.y = T)
dah_pa_frac <- rbind(dah_pa_frac, fill_fracs)

final_dah_crp <- copy(final_dah_cr)
final_dah_crp <- merge(final_dah_crp, dah_pa_frac, by = c("channel", "recip", "year"), all = T)
final_dah_crp[, dah_ref := dah_ref * pa_fraction_ref]
final_dah_crp[, c("pa_fraction_ref") := NULL]
final_dah_crp <- final_dah_crp[!(is.na(dah_ref))]
fwrite(final_dah_crp, paste0(WORK_DIR, "FILEPATH"))
