# Purpose: bring together ODA per GNI reference/better/worse predictions,
# along with GNI time series and retrospective DAH estimates
library(ggplot2)
library(data.table)
library(zoo)
library(scales)
library(ggpubr)

dah_d_folder <- "ID"
root_fold <- 'FILEPATH'

## Source and retrivee location metadata
source('FILEPATH')
locs <- get_location_metadata(location_set_id = 22, release_id = 16)
locs <- locs[level == 3, .(iso3 = ihme_loc_id, location_name, region_name, super_region_name, location_id)]
all_countries <- locs$iso3

# Main 24 DAH-D countries
locs <- c("USA", "GBR", "DEU", "FRA", "CAN", "AUS", "JPN", "NOR", "ESP", "NLD", "AUT", 
          "BEL", "DNK", "FIN", "GRC", "IRL", "ITA", "KOR", "LUX", "NZL", "PRT", "SWE", 
          "CHE", "CHN")

# Countries without ODA per GNI targets, we held forecasts constant into future
constant_locs <- c("AUT", "BEL", "CAN", "CHE", "FIN", "GRC", "JPN", "KOR", "NZL", "PRT", "SWE", "USA","CHN")

# Non-country donors
non_country_donors <- c("DEBT", "GATES", "OTHER", "OTHERPUB_NONGBD", "PRIVATE", "UNALL")

# Remaining countries that used ODA per GNI targets
oda_gni_locs <- locs[!locs %in% constant_locs]

# Countries without any DAH-D in retrospective dataset
missing_country_donors <- c("GRL", "BMU", "VIR", "MNP")

# GNI per capita forecasts
gnipc <- fread(paste0(root_fold, "FILEPATH"))

# ODA per GNI predictions from frontier
preds <- fread(paste0(root_fold, "FILEPATH"))

## Frontier input data
oda0 <- fread(paste0(root_fold, "FILEPATH"))
oda0 <- oda0[, .(iso3, year, oda_disbursed_per_gni)]
preds <- merge(preds, oda0, by = c("iso3", "year"), all.x = T)

# Run a linear regression for reference scenario and add line to this plot
model <- lm(log(oda_disbursed_per_gni) ~ log_gdppc, data = preds)
preds[, lm_pred_reference := predict(model, newdata = preds)]

## Retrospective DAH by donor
donor_dt <- fread(paste0(root_fold, "FILEPATH"))
preds <- merge(preds, donor_dt, by = c("iso3", "year"), all.x = T)
preds <- merge(preds, gnipc, by = c("iso3", "year"), all.x = T)

## Set max ODA disbursed per GNI
global_max_oda_gni <- max(preds$oda_disbursed_per_gni, na.rm = T)

preds <- preds[, .(iso3, year, log_gdppc, 
                   oda_disbursed_per_gni,
                   lm_pred_reference = exp(lm_pred_reference),
                   dah_donor, dah_donated_last_observed_year, final_gni, year_high_income)]

# Reference DAH-D forecasts
ref_dah_forecasts <- fread(paste0(root_fold, "FILEPATH"))[, dah_donated_last_observed_year := NULL]
setnames(ref_dah_forecasts, "dah_donor", "dah_donor_FGH_reference")
preds <- merge(preds, ref_dah_forecasts, by = c("year", "iso3"), all.x = T)


# Countries where oda_disbursed_per_gni is missing for all years
oda_per_gni_countries <- sort(unique(preds[!is.na(oda_disbursed_per_gni), iso3]))
missing_oda_per_gni_countries <- setdiff(unique(preds$iso3), oda_per_gni_countries)
preds[year <= 2000 & is.na(dah_donor), dah_donor := 0]
preds[iso3 %in% missing_oda_per_gni_countries, dah_disbursed_per_gni := dah_donor / final_gni]
preds[iso3 %in% missing_oda_per_gni_countries, oda_disbursed_per_gni := dah_disbursed_per_gni]


# Hold constant DAH/ODA ratio into the future
preds[, dah_oda := dah_donor / (oda_disbursed_per_gni * final_gni)]

for(iso in unique(preds$iso3)) {
  if(iso %in% missing_oda_per_gni_countries) {
    preds[iso3 == iso & year <= 2023, dah_oda := 1]
  }
  first_obs_dah_oda_year <- min(preds[iso3 == iso & year <= 2023 & !is.na(dah_oda), year])
  last_obs_dah_oda_year <- max(preds[iso3 == iso & year <= 2023 & !is.na(dah_oda), year])
  last_obs_dah_oda_value <- preds[iso3 == iso & year == last_obs_dah_oda_year, dah_oda]
  preds[iso3 == iso & year <= 2023 & year >= first_obs_dah_oda_year & is.na(dah_oda), dah_oda := last_obs_dah_oda_value, by = "iso3"]
  preds[iso3 == iso & year <= 2023 & year >= first_obs_dah_oda_year, dah_oda := na.locf(dah_oda, fromLast = T), by = "iso3"]
  preds[iso3 == iso & year >= first_obs_dah_oda_year, dah_oda := na.locf(dah_oda), by = "iso3"]
}

# Make placeholder DAH/ODA for missing countries
preds[iso3 %in% missing_country_donors & year %in% c(2025:2100), dah_oda := 1]

# Use dah_oda from previous last observed year for countries with 0 DAH-D in last observed year
preds[iso3 %in% missing_oda_per_gni_countries & year > 2023, dah_oda := 1]
preds[iso3 %in% missing_oda_per_gni_countries & year <= 2023 & is.na(dah_oda), dah_oda := 1]


# Case 1
case_1_countries <- unique(preds[iso3 %in% locs, iso3])
unique(preds[iso3 %in% case_1_countries, dah_donated_last_observed_year]) # End at 2025 or 2026
unique(preds[iso3 %in% case_1_countries, year_high_income]) 
preds[iso3 %in% case_1_countries, max_scenario_year := 2036]

# CHN is the only case 1 country missing ODA per GNI, set DAH=ODA
new_dah_oda_CHN <- 1
preds[iso3 %in% "CHN" & year %in% c(2025:2035), dah_oda := NA]
preds[iso3 %in% "CHN" & year > 2035, dah_oda := new_dah_oda_CHN]
preds[iso3 %in% "CHN" & year <= 2036 & year >= 2024, dah_oda := stats::approx(year, dah_oda, xout = year, rule = 2)$y, by = c("iso3")]

preds[iso3 %in% oda_gni_locs, dah_donor_ref := dah_donor_FGH_reference]

preds[iso3 %in% constant_locs & year < 2030, dah_donor_ref := dah_donor_FGH_reference]
preds[iso3 %in% constant_locs & year < 2030, final_oda_gni_ref := (dah_donor_FGH_reference / dah_oda) / final_gni, by = "iso3"]
preds[iso3 %in% constant_locs & year >= 2029, final_oda_gni_ref := na.locf(final_oda_gni_ref), by = "iso3"]
preds[iso3 %in% constant_locs & year >= 2030, dah_donor_ref := final_oda_gni_ref * final_gni * dah_oda]
preds[, c("final_oda_gni_ref") := NULL]

# Case 3
case_3_countries <- unique(preds[year_high_income <=2025 & !iso3 %in% locs, iso3])
preds[iso3 %in% c("ASM", "GUM", missing_country_donors), dah_donated_last_observed_year := 2026]
preds[iso3 %in% c("ASM", "GUM", missing_country_donors) & year <= 2026 & is.na(dah_donor), dah_donor := 0]
preds[iso3 %in% case_3_countries, max_scenario_year := dah_donated_last_observed_year + 10]
preds[iso3 %in% case_3_countries, max_scenario_year := mean(max_scenario_year, na.rm = T), by = "iso3"]
preds[iso3 %in% case_3_countries, dah_donated_last_observed_year := mean(dah_donated_last_observed_year, na.rm = T), by = "iso3"]

preds[iso3 %in% case_3_countries & year > dah_donated_last_observed_year & dah_donor == 0, dah_donor := NA]

preds[iso3 %in% case_3_countries & year <= dah_donated_last_observed_year, oda_gni_ref := oda_disbursed_per_gni, by = c("iso3")]

for(ctry in case_3_countries) {
  first_obs_dah_oda_year <- min(preds[iso3 == ctry & year <= 2026 & !is.na(oda_gni_ref), year])
  last_obs_dah_oda_year <- max(preds[iso3 == ctry & year <= 2026 & !is.na(oda_gni_ref), year])
  last_obs_dah_oda_value <- preds[iso3 == ctry & year == last_obs_dah_oda_year, oda_gni_ref]
  preds[iso3 == ctry & year <= dah_donated_last_observed_year & year >= first_obs_dah_oda_year & is.na(oda_gni_ref), 
        oda_gni_ref := last_obs_dah_oda_value, by = "iso3"]
}

preds[iso3 %in% case_3_countries & year <= dah_donated_last_observed_year, dah_donor_ref := dah_donor, by = c("iso3")]

preds[iso3 %in% case_3_countries & year >= (dah_donated_last_observed_year), oda_gni_ref := lm_pred_reference, by = c("iso3", "year")]
preds[iso3 %in% case_3_countries & year <= dah_donated_last_observed_year, dah_donor_ref := dah_donor]
preds[iso3 %in% case_3_countries & year == dah_donated_last_observed_year, oda_gni_ref_baseline := oda_gni_ref, by = "iso3"]
preds[iso3 %in% case_3_countries, oda_gni_ref_baseline := mean(oda_gni_ref_baseline, na.rm = T), by = "iso3"]
preds[iso3 %in% case_3_countries & year >= dah_donated_last_observed_year, pct_change_oda_gni_ref := (oda_gni_ref - oda_gni_ref_baseline) / oda_gni_ref_baseline, by = "iso3"]
preds[iso3 %in% case_3_countries & year >= dah_donated_last_observed_year, new_oda_gni := (dah_donor / dah_oda) / final_gni, by = "iso3"]
preds[iso3 %in% case_3_countries & year == dah_donated_last_observed_year, final_oda_gni_ref := new_oda_gni]
preds[iso3 %in% case_3_countries, final_oda_gni_ref := mean(final_oda_gni_ref, na.rm=T), by = "iso3"]
preds[iso3 %in% case_3_countries & year > dah_donated_last_observed_year, final_oda_gni_ref := (1+pct_change_oda_gni_ref) * final_oda_gni_ref, by = "iso3"]
preds[iso3 %in% case_3_countries & year <= dah_donated_last_observed_year, oda_gni_ref := oda_disbursed_per_gni]
preds[iso3 %in% case_3_countries & year > dah_donated_last_observed_year, oda_gni_ref := final_oda_gni_ref]
preds[iso3 %in% case_3_countries & year > dah_donated_last_observed_year & oda_gni_ref > global_max_oda_gni, oda_gni_ref := global_max_oda_gni]

preds[iso3 %in% case_3_countries & year > dah_donated_last_observed_year, dah_donor_ref := final_oda_gni_ref * final_gni * dah_oda]


preds[, c("final_oda_gni_ref", "pct_change_oda_gni_ref") := NULL]

# For missing countries and ASM, GUM, and PRI, the reference forecast yields 0
# lm() fitted values for these locations
for(ctry in c(missing_country_donors, "ASM", "GUM", "PRI")) {
  print(ctry)
  preds[iso3 %in% ctry & year >= max_scenario_year, oda_gni_ref_miss := lm_pred_reference, by = c("iso3", "year")]
  preds[iso3 %in% ctry & year <= dah_donated_last_observed_year, oda_gni_ref_miss := oda_disbursed_per_gni, by = c("iso3", "year")]
  preds[iso3 %in% ctry & year == dah_donated_last_observed_year & dah_donor == 0, oda_gni_ref_miss := 0, by = c("iso3", "year")]
  last_obs_oda_gni_year <- max(preds[iso3 == ctry & year <= 2026 & !is.na(oda_disbursed_per_gni), year])
  last_obs_oda_gni_value <- preds[iso3 == ctry & year == last_obs_oda_gni_year, oda_disbursed_per_gni]
  if(is.na(last_obs_oda_gni_value)) {
    last_obs_oda_gni_value <- 0
  }
  preds[iso3 %in% ctry  & year == dah_donated_last_observed_year & is.na(oda_gni_ref_miss) & is.na(dah_donor), oda_gni_ref_miss := 0]
  preds[iso3 %in% ctry & year <= max_scenario_year & year >= dah_donated_last_observed_year, oda_gni_ref_miss := stats::approx(year, oda_gni_ref_miss, xout = year, rule = 2)$y, by = c("iso3")]
  
  preds[iso3 %in% ctry, dah_donor_ref := oda_gni_ref_miss * final_gni * dah_oda]
  preds[iso3 %in% ctry, oda_gni_ref := oda_gni_ref_miss]
  preds[iso3 %in% ctry & is.na(dah_donor_ref) & year <= dah_donated_last_observed_year & is.na(dah_donor_ref), dah_donor_ref := 0]
  preds[iso3 %in% ctry & is.na(oda_gni_ref) & year <= dah_donated_last_observed_year, oda_gni_ref := 0]
  
}

# Case 4
case_4_countries <- unique(preds[year_high_income > 2025 & year_high_income < 2101 & !iso3 %in% locs, iso3])
preds[iso3 %in% case_4_countries, max_scenario_year := year_high_income + 10]
preds[iso3 %in% case_4_countries, max_scenario_year := mean(max_scenario_year, na.rm = T), by = "iso3"]

special_inteporlate_countries <- sort(unique(preds[iso3 %in% case_4_countries & year_high_income > 2090, iso3]))
case_4_countries <- case_4_countries[!case_4_countries %in% special_inteporlate_countries]
preds[iso3 %in% case_4_countries, dah_donated_last_observed_year := mean(dah_donated_last_observed_year, na.rm = T), by = c("iso3")]

preds[iso3 %in% case_4_countries & year > dah_donated_last_observed_year & dah_donor == 0, dah_donor := NA]
preds[iso3 %in% case_4_countries & year < year_high_income, oda_gni_ref := oda_disbursed_per_gni, by = c("iso3")]

for(ctry in case_4_countries) {
  first_obs_dah_oda_year <- min(preds[iso3 == ctry & year <= 2023 & !is.na(oda_gni_ref), year])
  preds[iso3 %in% ctry & year < year_high_income & year > first_obs_dah_oda_year, oda_gni_ref := na.locf(oda_gni_ref), by = c("iso3")]
}

preds[iso3 %in% case_4_countries & year <= dah_donated_last_observed_year, c("dah_donor_ref") := dah_donor, by = c("iso3")]
preds[iso3 %in% case_4_countries & year <= dah_donated_last_observed_year, c("oda_gni_ref") := oda_disbursed_per_gni]
preds[iso3 %in% case_4_countries & year < year_high_income & year > dah_donated_last_observed_year, dah_donor_ref := (oda_gni_ref * final_gni) * dah_oda, by = c("iso3")]
preds[iso3 %in% case_4_countries & year <= dah_donated_last_observed_year, c("oda_gni_ref") := oda_disbursed_per_gni]


preds[iso3 %in% case_4_countries & year == (year_high_income-1), oda_gni_ref_orig := oda_gni_ref, by = c("iso3", "year")]

preds[iso3 %in% case_4_countries & year >= ((year_high_income-1)), oda_gni_ref := lm_pred_reference, by = c("iso3", "year")]
preds[iso3 %in% case_4_countries & year == (year_high_income-1), oda_gni_ref_baseline := oda_gni_ref, by = "iso3"]
preds[iso3 %in% case_4_countries, oda_gni_ref_baseline := mean(oda_gni_ref_baseline, na.rm = T), by = "iso3"]
preds[iso3 %in% case_4_countries & year >= (year_high_income-1), pct_change_oda_gni_ref := (oda_gni_ref - oda_gni_ref_baseline) / oda_gni_ref_baseline, by = "iso3"]
preds[iso3 %in% case_4_countries & year >= (year_high_income-1), new_oda_gni := (dah_donor_ref / dah_oda) / final_gni, by = "iso3"]
preds[iso3 %in% case_4_countries & year == (year_high_income-1), final_oda_gni_ref := new_oda_gni]
preds[iso3 %in% case_4_countries, final_oda_gni_ref := mean(final_oda_gni_ref, na.rm=T), by = "iso3"]
preds[iso3 %in% case_4_countries & year > (year_high_income-1), final_oda_gni_ref := (1+pct_change_oda_gni_ref) * final_oda_gni_ref, by = "iso3"]
preds[iso3 %in% case_4_countries & year > (year_high_income-1) & final_oda_gni_ref > global_max_oda_gni, final_oda_gni_ref := global_max_oda_gni]
preds[iso3 %in% case_4_countries & year >= year_high_income, dah_donor_ref := final_oda_gni_ref * final_gni * dah_oda]
preds[iso3 %in% case_4_countries & year == (year_high_income-1), oda_gni_ref := oda_gni_ref_orig, by = c("iso3", "year")]
preds[iso3 %in% case_4_countries & year >= year_high_income, oda_gni_ref := final_oda_gni_ref]


preds[, c("final_oda_gni_ref", "pct_change_oda_gni_ref", "oda_gni_ref_orig") := NULL]

# Case 2
# Last observed DAH-D value carried forward
case_2_countries <- unique(preds[year_high_income == 2101, iso3])
case_2_countries <- c(case_2_countries, special_inteporlate_countries)
preds[iso3 %in% case_2_countries, dah_donor_ref := dah_donor]
preds[iso3 %in% case_2_countries, dah_donated_last_observed_year := mean(dah_donated_last_observed_year, na.rm = T), by = c("iso3")]

preds[iso3 %in% case_2_countries & year < 1995 & is.na(dah_donor_ref), dah_donor_ref := 0]

## Fix Palestine estimates
preds[iso3 %in% "PSE" & year == 2023, dah_donor := 0] 
preds[iso3 %in% "PSE" & year == 2023, oda_disbursed_per_gni := 0] 
preds[iso3 %in% case_2_countries, c("oda_gni_ref") := oda_disbursed_per_gni]

for(ctry in case_2_countries) {
  first_obs_dah_oda_year <- min(preds[iso3 == ctry & year <= 2023 & !is.na(oda_gni_ref), year])
  preds[iso3 %in% ctry & year > first_obs_dah_oda_year, oda_gni_ref := na.locf(oda_gni_ref), by = c("iso3")]
}

preds[iso3 %in% case_2_countries & year >= dah_donated_last_observed_year & oda_gni_ref > global_max_oda_gni, oda_gni_ref := global_max_oda_gni]
preds[iso3 %in% case_2_countries & year > dah_donated_last_observed_year, dah_donor_ref := oda_gni_ref * final_gni * dah_oda]


# Number of country donors
all_case_countries <- c(sort(case_1_countries), sort(case_2_countries), 
                        sort(case_3_countries), sort(case_4_countries))

preds[is.na(oda_gni_ref), oda_gni_ref := (dah_donor_ref / dah_oda) / final_gni]
preds[, oda_disbursed_per_gni_actual := (dah_donor_ref / dah_oda) / final_gni]

# Make table of the data
preds_table <- copy(preds)

preds_table[iso3 %in% c(case_2_countries), y_reference := (dah_donor_ref) / final_gni]
preds_table[iso3 %in% c(case_4_countries, case_3_countries), y_reference := oda_gni_ref]
preds_table[iso3 %in% c(case_4_countries, case_3_countries) & year <= dah_donated_last_observed_year & is.na(oda_disbursed_per_gni), y_reference := NA]

preds_table[iso3 %in% case_1_countries, y_reference := oda_disbursed_per_gni_actual]

preds_table <- preds_table[, .(iso3, year, oda_per_gni_reference = y_reference,
                               dah_reference = dah_donor_ref,
                               final_gni, dah_oda, dah_donated_last_observed_year, year_high_income)]

preds_table[oda_per_gni_reference > global_max_oda_gni, oda_per_gni_reference := global_max_oda_gni]

fwrite(preds_table[year %in% c(2023, 2050, 2100)], "FILEPATH")

## Save out last observed year by donor
donor_last_observed_year <- copy(preds)[,.(iso3, dah_donated_last_observed_year)]
donor_last_observed_year <- unique(donor_last_observed_year)
donor_last_observed_year <- donor_last_observed_year[!is.na(dah_donated_last_observed_year)]
fwrite(donor_last_observed_year, paste0(root_fold, "FILEPATH"))

# Bring in non-country donors and hold same estimates across all scenarios
all_country_forecasts <- copy(preds)
all_country_forecasts[year <= dah_donated_last_observed_year & is.na(dah_donor_ref), dah_donor_ref := 0]
all_country_forecasts <- all_country_forecasts[, .(iso3, year, dah_donor_ref)]

non_country_forecasts <- ref_dah_forecasts
non_country_forecasts <- non_country_forecasts[!iso3 %in% all_case_countries]
unique(non_country_forecasts$iso3)
non_country_forecasts <- non_country_forecasts[, .(iso3, year,dah_donor_ref = dah_donor_FGH_reference)]

# Bring together all data
all_forecasts <- rbind(all_country_forecasts, non_country_forecasts)


# Calculate total aggregated DAH-D across all donors
all_agg_forecasts <- all_forecasts
all_agg_forecasts <- all_agg_forecasts[, .(dah_donor_ref = sum(dah_donor_ref, na.rm = T)),
                                       by = c("year")]
all_agg_forecasts[, iso3 := "AGGREGATE"]

## Append aggregate
all_forecasts_final <- rbind(all_agg_forecasts, all_forecasts)

## Save out forecasts
fwrite(all_forecasts_final, paste0(root_fold, "FILEPATH"))

