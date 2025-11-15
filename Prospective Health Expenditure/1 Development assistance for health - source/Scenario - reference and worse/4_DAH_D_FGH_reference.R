###################################################
### Author: USERNAME
### Purpose: Preparing all the inputs going into DAH D forecasting
###################################################

## Directory structure
rm(list = ls())

require(testthat)
library(zoo)
library(AFModel)
require(ggplot2)

## Open the parser and parse the version
start_year <- 1990
release_id <- ID
location_set_id <- ID
end_FC = 2100
crs_path <- 'FILEPATH'
retro_oda_data <- 'FILEPATH'
oda_gni_countries <- c("AUS", "DEU", "DNK", "ESP", "FRA", "GBR", "IRL", "ITA", "LUX", "NLD", "NOR")
non_oda_gni_countries <- c("AUT", "BEL", "CAN", "CHE", "CHN", "FIN", "GRC", "JPN", "KOR", "NZL", "PRT", "SWE", "GATES", "USA")
non_country_donors <- c("OTHER", "OTHERPUB_NONGBD", "PRIVATE", "UNALL", "DEBT")


dah_d_folder <- "ID"
root_fold <- 'FILEPATH'


repo_path = "FILEPATH"

## Source currency conversion function
source(paste0(repo_path, "FILEPATH"))

###########################
### (0) Get the location metadata
###########################

source('FILEPATH')
locs <- get_location_metadata(location_set_id = location_set_id, release_id = release_id)
locs <- locs[level == 3, .(iso3 = ihme_loc_id, location_name, super_region_name, region_name, location_id)]

###########################
### (1) Get retrospective DAH-D raw data
# We will be forecasting several different groups:
# - a. Country donors that use ODA/GNI metrics: hold DAH/ODA constant to get DAH. Forecast GNI. Use targeted ODA/GNI. Calculate DAH/ODA * ODA/GNI * GNI = DAH
# - b. Country donors that don't use ODA/GNI metrics: Hold DAH constant.
# - c. GATES: Hold DAH constant, then set to 0 starting in 2046 due to announcement of foundation closing.
# - d. All other non-country non-GATES non-PRIVATE donors: Use a linear model.
# - e. Private foundations and NGOs respond to bilateral: use a covariate the sum of all bilateral DAH-D. Two step: 1. Regress private on sum of bilateral, 2. Remove residual and forecast the residual with an Arima, then add forecasted residual plus fitted value.
###########################

# DAH-D retrospective estimates and donor information
fgh_donor <- fread(paste0(root_fold, "FILEPATH"))
main_24_country_donors <- c("AUS", "DEU", "DNK", "ESP", "FRA", "GBR", "IRL", "ITA", "LUX", "NLD", "NOR",
                            "AUT", "BEL", "CAN", "CHE", "CHN", "FIN", "GRC", "JPN", "KOR", "NZL", "PRT", "SWE", "USA")
dah_donors <- sort(c(main_24_country_donors, "GATES", non_country_donors))
fgh_donor <- fgh_donor[iso3 %in% dah_donors]
last_retro_dahd_year <- max(fgh_donor$dah_donated_last_observed_year)

## Expand grid from start_year to end_FC
fgh_donor <- dcast.data.table(fgh_donor, iso3 + dah_donated_last_observed_year ~ year, value.var = "dah_donor")
fgh_donor[, paste0(c(eval(last_retro_dahd_year + 1):end_FC)):= NA]
fgh_donor <- data.table::melt(fgh_donor, id.vars = c('iso3', 'dah_donated_last_observed_year'), value.name = 'dah_donor', variable.name = 'year')
fgh_donor[, year:= as.numeric(as.character(year))]
fgh_donor[iso3 == "GATES" & is.na(dah_donor) & year < 1999, dah_donor := 0]

###########################
### (b) Country donors that don't use ODA/GNI metrics: Hold DAH constant.
###########################

for(iso in non_oda_gni_countries[non_oda_gni_countries != "GATES"]) {
  print(iso)
  fgh_donor[iso3 %in% iso, dah_donor := na.locf(dah_donor), by = "iso3"]
  
}

bmgf_last_value <- fgh_donor[iso3 == "GATES" & year == dah_donated_last_observed_year, dah_donor]
fgh_donor[iso3 %in% "GATES" & year > dah_donated_last_observed_year, dah_donor := bmgf_last_value]
fgh_donor[iso3 == "GATES" & year > 2045, dah_donor := 0]

###########################
### (d) Forecast the non-country donors using ARIMA or linear model over non-COVID years
###########################

non_country_donors_no_private <- non_country_donors
non_country_donors_no_private <- non_country_donors_no_private[non_country_donors_no_private != "PRIVATE"]

## Use LM in level space over non-COVID years for non-country other donors
data_nons <- fgh_donor[iso3 %in% non_country_donors_no_private]
data_nons[dah_donor == 0, dah_donor:= NA]

for(donor in non_country_donors_no_private) {
  lm_nocovid <- lm(data = data_nons[iso3 == donor & year <= 2019 ], formula = dah_donor ~ year)
  data_nons[iso3 == donor & year > dah_donated_last_observed_year, fc_dah:= predict(lm_nocovid, data.frame(year = c(year)))]
  
}

data_nons[iso3 %in% non_country_donors_no_private & year > dah_donated_last_observed_year, dah_donor:= fc_dah]
data_nons <- data_nons[, .(iso3, year, dah_donor, dah_donated_last_observed_year)]
data_nons[is.na(dah_donor), dah_donor:= 0]

# Fix unallocable source - goes negative, hold constant instead
data_nons_unall_lastobs <- data_nons[iso3 == "UNALL" & year == dah_donated_last_observed_year, dah_donor]
data_nons[iso3 == "UNALL" & year > dah_donated_last_observed_year, dah_donor := data_nons_unall_lastobs]

data_nons_otherpub_lastobs <- data_nons[iso3 == "OTHERPUB_NONGBD" & year == dah_donated_last_observed_year, dah_donor]
data_nons[iso3 == "OTHERPUB_NONGBD" & year > dah_donated_last_observed_year, dah_donor := data_nons_otherpub_lastobs]

## Write out non-country, non-private donor forecasts
fwrite(data_nons, paste0(root_fold, "FILEPATH"))


###########################
### (6a) Create retro ODA-D (ODA donated) dataset from raw CRS data
###########################

# Load CRS data and filter to ODA
crs <- fread(crs_path)
crs <- crs[flow_name %in% c("ODA Grants", "ODA Loans")]

# fix inconsistency
crs[sector_code == 998, sector_name := "IX. Unallocated / Unspecified"]

# Aggregate by donor, year, and sector
agg <- crs[,
           .(usd_commitment = sum(usd_commitment * 1e6, na.rm = TRUE),
             usd_disbursement  = sum(usd_disbursement * 1e6, na.rm = TRUE)),
           by = .(donor_name, year, sector_name, sector_code)]


# Merge on country codes and drop non-country donors
agg[, donor_name := fcase(
  donor_name == "Korea", "Republic of Korea",
  donor_name == "Russia", "Russian Federation",
  donor_name == "Slovak Republic", "Slovakia",
  donor_name == "United States", "United States of America",
  rep_len(TRUE, .N), donor_name
)]

agg <- merge(agg, locs[, .(location_name, iso3)],
             by.x = "donor_name", by.y = "location_name", all.x = TRUE)
agg <- agg[!is.na(iso3), ]

# Flag health- and DAH-specific sectors

# Social.Health subsectors
dah_subsectors <- c(121, 122, 130)
other_health_subsectors <- c(123)

agg[, is_health := fifelse(sector_code %in% c(dah_subsectors, other_health_subsectors),
                           TRUE,
                           FALSE)]

agg[, is_dah := fifelse(sector_code %in% dah_subsectors,
                        TRUE,
                        FALSE)]

fwrite(agg, paste0(root_fold, "FILEPATH"))

# save out sector code table
sc <- unique(crs[!is.na(sector_code), .(sector_code,  sector_name)])
setorder(sc, sector_code)
fwrite(sc, paste0(root_fold, "FILEPATH"))

###########################
### (6b) Get retro ODA-D (ODA donated) and ODA-H (ODA donated for health sector)
###########################

# ODA commitments and disbursements from OECD, in nominal USD, by donor
oda0 <- fread(paste0(root_fold, "FILEPATH"))

# Keep necessary years and donors
oda0 <- oda0[year %in% c(start_year:end_FC)]
oda_donors <- sort(unique(oda0$iso3))
setdiff(dah_donors, oda_donors)
oda0 <- oda0[iso3 %in% dah_donors]

# Calculate total ODA donated (ODA-D) and ODA donated for health sectors (ODA-H)
# Aggregate the total of all sub-sectors
# Merge on sector codes dataset, which defines the aggregate sectors, and drop any aggregates


oda_sectors <- fread(paste0(root_fold, "FILEPATH"))
oda0 <- merge(oda0, oda_sectors, by = c("sector_name", "sector_code"), all.x = T)
oda0 <- oda0[sector_name != "Administrative Costs of Donors"]

# Zero out any sectors with negative values
oda0[usd_commitment < 0, usd_commitment := 0][usd_disbursement < 0, usd_disbursement := 0]

# Calculate final total ODAH as the sum across the health sectors
oda0_odah <- copy(oda0)[is_health == TRUE]
oda0_odah <- oda0_odah[, .(odah_usd_commitment = sum(usd_commitment, na.rm = T),
                           odah_usd_disbursement = sum(usd_disbursement, na.rm = T)), 
                       by = c('iso3', 'year')]

# Calculate final total ODA as the sum across all sectors
oda0_oda <- copy(oda0)
oda0_oda <- oda0_oda[, .(oda_usd_commitment = sum(usd_commitment, na.rm = T),
                         oda_usd_disbursement = sum(usd_disbursement, na.rm = T)), 
                     by = c('iso3', 'year')]
oda0_oda <- merge(oda0_oda, oda0_odah, by = c("iso3", "year"), all = T)
oda0_oda[is.na(oda_usd_commitment), oda_usd_commitment := 0][is.na(oda_usd_disbursement), oda_usd_disbursement := 0]
oda0_oda[is.na(odah_usd_commitment), odah_usd_commitment := 0][is.na(odah_usd_disbursement), odah_usd_disbursement := 0]

## Calculate ODAH/ODA for commitments and disbursements
oda0 <- copy(oda0_oda)
setorder(oda0, "iso3", "year")
oda0[, odah_per_oda_commitment := odah_usd_commitment / oda_usd_commitment]
oda0[, odah_per_oda_disbursement := odah_usd_disbursement / oda_usd_disbursement]

# Merge onto modeling variables (GNI and ODA/GNI)
odah_per_oda <- copy(oda0)[,.(iso3, year, odah_per_oda_disbursement)]
odah_per_oda <- odah_per_oda[!is.na(odah_per_oda_disbursement)]
fwrite(odah_per_oda, paste0(root_fold, "FILEPATH"))

gnipc <- fread(paste0(root_fold, "FILEPATH"))
gnipc <- gnipc[!is.na(final_gni)]

fgh_donor_oda_countries <- merge(fgh_donor, odah_per_oda, by = c("iso3", "year"), all.x = T)
fgh_donor_oda_countries[year>=2023, odah_per_oda_disbursement := na.locf(odah_per_oda_disbursement),  by = "iso3"]
fgh_donor_oda_countries <- merge(fgh_donor_oda_countries, gnipc, by = c("iso3", "year"), all.x = T)
fgh_donor_oda_countries <- fgh_donor_oda_countries[iso3 %in% oda_gni_countries]

fgh_donor_oda_countries[, oda_per_gni_historic := (1 / (odah_per_oda_disbursement / dah_donor)) / final_gni]
fgh_donor_oda_countries[, oda_per_gni := oda_per_gni_historic]

# AUS, DEU - constant ODA per GNI ratio, no information on target ratio available
fgh_donor_oda_countries[iso3 %in% c("AUS", "DEU") & year >= 2025, oda_per_gni := na.locf(oda_per_gni), by = "iso3"]

# ESP, ITA - linear increase from 2025 value to 0.07% target in 2030, then constant 0.7% after 2030
fgh_donor_oda_countries[iso3 == "ESP" & year >= 2030, oda_per_gni := 0.007]
ESP_fill <- seq(from = fgh_donor_oda_countries[iso3 == "ESP" & year == 2025, oda_per_gni], 
                to = fgh_donor_oda_countries[iso3 == "ESP" & year == 2030, oda_per_gni], 
                length.out = 6)
fgh_donor_oda_countries[iso3 == "ESP" & year == 2026, oda_per_gni := ESP_fill[2]]
fgh_donor_oda_countries[iso3 == "ESP" & year == 2027, oda_per_gni := ESP_fill[3]]
fgh_donor_oda_countries[iso3 == "ESP" & year == 2028, oda_per_gni := ESP_fill[4]]
fgh_donor_oda_countries[iso3 == "ESP" & year == 2029, oda_per_gni := ESP_fill[5]]

fgh_donor_oda_countries[iso3 == "ITA" & year >= 2030, oda_per_gni := 0.007]
ITA_fill <- seq(from = fgh_donor_oda_countries[iso3 == "ITA" & year == 2025, oda_per_gni], 
                to = fgh_donor_oda_countries[iso3 == "ITA" & year == 2030, oda_per_gni], 
                length.out = 6)
fgh_donor_oda_countries[iso3 == "ITA" & year == 2026, oda_per_gni := ITA_fill[2]]
fgh_donor_oda_countries[iso3 == "ITA" & year == 2027, oda_per_gni := ITA_fill[3]]
fgh_donor_oda_countries[iso3 == "ITA" & year == 2028, oda_per_gni := ITA_fill[4]]
fgh_donor_oda_countries[iso3 == "ITA" & year == 2029, oda_per_gni := ITA_fill[5]]


# DNK, IRL - constant at 0.7%
# These are above 0.7%. Hold last rate constant going forward
fgh_donor_oda_countries[iso3 %in% c("DNK", "IRL") & year >= 2025, oda_per_gni := na.locf(oda_per_gni), by = "iso3"]

# LUX, NOR - constant at 1%
# These are above 1%. Hold last rate constant going forward
fgh_donor_oda_countries[iso3 %in% c("LUX", "NOR") & year >= 2025, oda_per_gni := na.locf(oda_per_gni), by = "iso3"]

# FRA - constant at 0.045%
fgh_donor_oda_countries[iso3 %in% c("FRA") & year >= 2026, oda_per_gni := 0.0045]

# GBR - 2026 at 0.5%, then 0.3% afterward
fgh_donor_oda_countries[iso3 %in% c("GBR") & year == 2026, oda_per_gni := 0.005]
fgh_donor_oda_countries[iso3 %in% c("GBR") & year > 2026, oda_per_gni := 0.003]
# Adjust values using the new ODA totals from Table 5.11: https://www.gov.uk/government/publications/spending-review-2025-document/spending-review-2025-html#:~:text=The%20SR%20announces%20reductions%20in,all%20while%20bolstering%20economic%20stability
fgh_donor_oda_countries[iso3 %in% c("GBR") & year == 2026, new_oda := 10e9]
fgh_donor_oda_countries[iso3 %in% c("GBR") & year == 2027, new_oda := 8.9e9]
fgh_donor_oda_countries[iso3 %in% c("GBR") & year == 2028, new_oda := 9.4e9]
fgh_donor_oda_countries <- currency_conversion(data = fgh_donor_oda_countries, col.loc = 'iso3', col.value = "new_oda", 
                                               currency = 'lcu', currency.year = 2025,
                                               base.unit = 'usd', base.year = .default_base_year)
fgh_donor_oda_countries[iso3 %in% c("GBR") & year %in% c(2026, 2027, 2028), new_oda_per_gni := new_oda / final_gni]
fgh_donor_oda_countries[iso3 %in% c("GBR") & year %in% c(2026, 2027, 2028), oda_per_gni := new_oda_per_gni]

# NLD - 40% decrease from the 2025 value for the 2029 value, then constant
NLD_2029 <- fgh_donor_oda_countries[iso3 == "NLD" & year == 2025, oda_per_gni] * 0.6
fgh_donor_oda_countries[iso3 == "NLD" & year >= 2029, oda_per_gni := NLD_2029]
NLD_fill <- seq(from = fgh_donor_oda_countries[iso3 == "NLD" & year == 2025, oda_per_gni], 
                to = fgh_donor_oda_countries[iso3 == "NLD" & year == 2029, oda_per_gni], 
                length.out = 5)
fgh_donor_oda_countries[iso3 == "NLD" & year == 2026, oda_per_gni := NLD_fill[2]]
fgh_donor_oda_countries[iso3 == "NLD" & year == 2027, oda_per_gni := NLD_fill[3]]
fgh_donor_oda_countries[iso3 == "NLD" & year == 2028, oda_per_gni := NLD_fill[4]]


# Calculate final DAH
fgh_donor_oda_countries[year <= dah_donated_last_observed_year, final_dah := dah_donor]
fgh_donor_oda_countries[year > dah_donated_last_observed_year & !(iso3 == "GBR" & year %in% c(2026, 2027, 2028)), 
                        final_dah := oda_per_gni * final_gni * odah_per_oda_disbursement]
fgh_donor_oda_countries[year > dah_donated_last_observed_year & (iso3 == "GBR" & year %in% c(2026, 2027, 2028)), 
                        final_dah := new_oda * odah_per_oda_disbursement]
fgh_donor_oda_countries[, c('new_oda_per_gni', 'new_oda' ) := NULL]
fgh_donor_oda_countries_final <- copy(fgh_donor_oda_countries)
fgh_donor_oda_countries_final <- fgh_donor_oda_countries_final[, .(iso3, year, dah_donor = final_dah, dah_donated_last_observed_year)]


###################################
# Forecast PRIVATE
### (e) Forecast PRIVATE in two steps: 
# 1. Regress private on sum of bilateral 
# 2. Remove residual and forecast the residual with an Arima model, then add forecasted residual plus fitted value. 
###################################

## Assign private and bilateral donors
data_nons_priv <- fgh_donor[iso3 %in% "PRIVATE", .(iso3, year, dah_donor, dah_donated_last_observed_year)]
data_nons_priv[dah_donor == 0, dah_donor:= NA]
bilateral_donors <- unique(fgh_donor[!iso3 %in% non_country_donors, iso3])
bilateral_donors <- bilateral_donors[bilateral_donors != "GATES"]

fgh_donor_bilat <- dt_plot[iso3 %in% bilateral_donors]
fgh_donor_bilat <- fgh_donor_bilat[, .(dah_donor = sum(dah_donor)), by = "year"]
setnames(fgh_donor_bilat, "dah_donor", "dah_donor_bilat")

data_nons_priv <- merge(data_nons_priv, fgh_donor_bilat, by = "year", all = T)


# step 1 - regress private on sum of bilateral donor DAH
lm_private <- lm(data = data_nons_priv, formula = dah_donor ~ dah_donor_bilat)
data_nons_priv[, pred_dah := predict(lm_private, data.frame(dah_donor_bilat = c(dah_donor_bilat)))]

# intercept shift prediction to observed at 2025 such that residual is 0 in 2025
data_nons_priv[year == dah_donated_last_observed_year, int_shift := dah_donor - pred_dah]
data_nons_priv[, int_shift := mean(int_shift, na.rm = T)]
data_nons_priv[year >= dah_donated_last_observed_year, pred_dah := pred_dah + int_shift]

data_nons_priv[, residual := pred_dah - dah_donor]

# forecast the residual with an ARIMA model
library(forecast)

lm_residual <- lm(data = data_nons_priv, formula = residual ~ year)
data_nons_priv[, pred_resid := predict(lm_residual, data.frame(year = c(year)))]
data_nons_priv[, residual_final := residual]
data_nons_priv[year > dah_donated_last_observed_year, residual_final := pred_resid]

data_nons_priv[, dah_donor_final := pred_dah - residual_final]

data_nons_priv_final <- copy(data_nons_priv)
data_nons_priv_final <- data_nons_priv_final[, .(iso3, year, dah_donor = dah_donor_final, dah_donated_last_observed_year)]

## Bind datasets and save out
dt_plot <- rbind(dt_plot, data_nons_priv_final)
fwrite(dt_plot, paste0(root_fold, "FILEPATH"))
