# Make worse scenario of DAH-D
# Use minimum of (reference, 0.1% ODA/GNI) for forecast years, and linearly transition to that new value over 3 years

library(data.table)
library(ggplot2)

## Get ODA per GNI estimates
oda_gni <- fread("FILEPATH")

## Get reference DAH forecasts
oda_gni$dah_reference <- NULL
dah_reference <- fread("FILEPATH")[iso3 != "AGGREGATE"]
dah_reference <- merge(dah_reference, oda_gni, by = c("iso3", "year"), all = T)
dah_reference[, worse_oda_gni := 0.0005]

dah_reference[, dah_donated_last_observed_year := mean(dah_donated_last_observed_year, na.rm = T), by = "iso3"]
dah_reference[, worse_scenario_start_year := dah_donated_last_observed_year + 3]
dah_reference[year < worse_scenario_start_year,
              worse_oda_gni := NA]
dah_reference[year <= dah_donated_last_observed_year,
              worse_oda_gni := oda_per_gni_reference]
dah_reference[year >= dah_donated_last_observed_year & year <= worse_scenario_start_year,
              worse_oda_gni := oda_per_gni_reference - 
                ((oda_per_gni_reference - 0.0005) / 3) * (year - dah_donated_last_observed_year)] 

dah_reference[worse_oda_gni > oda_per_gni_reference,
              worse_oda_gni := oda_per_gni_reference]
dah_reference[, dah_donated_worse := worse_oda_gni * final_gni * dah_oda]
dah_reference[year <= dah_donated_last_observed_year, dah_donated_worse := dah_donor_ref]

dah_reference[dah_donated_worse > dah_donor_ref, dah_donated_worse := dah_donor_ref]
dah_reference[worse_oda_gni == oda_per_gni_reference & dah_donated_worse != dah_donor_ref, dah_donated_worse := dah_donor_ref]


# Set forecasts for non-country donors equal to reference scenario
non_country_donors <- c("DEBT", "GATES", "OTHER", "OTHERPUB_NONGBD", "UNALL", "PRIVATE")
dah_reference[iso3 %in% non_country_donors, dah_donated_worse := dah_donor_ref]

## Calculate and append aggregate
dah_reference_agg <- dah_reference[, .(dah_donated_worse = sum(dah_donated_worse, na.rm = T), 
                                       dah_donor_ref = sum(dah_donor_ref, na.rm = T)),
                                        by = .(year)]
dah_reference_agg[, iso3 := "AGGREGATE"]
dah_reference <- rbind(dah_reference, dah_reference_agg, fill = T)

# Update with adjustments to 2024
dah_reference[year <= 2024, dah_donated_worse := dah_donor_ref]
setnames(dah_reference, "dah_donor_ref", "dah_donor_ref_ACTUAL")

## Save out forecasts of DAH by donor worse scenario
fwrite(dah_reference, "FILEPATH")
