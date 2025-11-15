# Get retro DAH-D for all donors and countries

## Get location metadata
source('FILEPATH')
locs <- get_location_metadata(location_set_id = 22, release_id = 16)
locs <- locs[level == 3, .(iso3 = ihme_loc_id, location_name, super_region_name, region_name, location_id)]
lox <- unique(locs$iso3)

dah_d_folder <- "ID"
root_fold <- 'FILEPATH'

## Get retrospective DAH estimates
donor_dt <- fread('FILEPATH')
donor_dt[source == "PRIVINK", source := "PRIVATE"]
donor_dt <- donor_dt[, .(iso3 = source, year, dah_donor = dah)]
donor_dt <- donor_dt[, .(dah_donor = sum(dah_donor)), by = c("iso3", "year")]

## Assign type of donor
dah_country_donors <- intersect(unique(donor_dt$iso3), lox)
non_country_donors <- setdiff(unique(donor_dt$iso3), lox)
missing_country_donors <- setdiff(lox, unique(donor_dt$iso3))


# Calculate the maximum year with non-NA dah_donor estimates by country
donor_dt[, dah_donated_last_observed_year := max(year[dah_donor > 0], na.rm = TRUE), by = iso3]
sort(unique(donor_dt$dah_donated_last_observed_year))
unique(donor_dt[dah_donated_last_observed_year==2015, iso3])
unique(donor_dt[dah_donated_last_observed_year==2019, iso3])
unique(donor_dt[dah_donated_last_observed_year==2022, iso3])
unique(donor_dt[dah_donated_last_observed_year==2023, iso3])
unique(donor_dt[dah_donated_last_observed_year==2024, iso3])
unique(donor_dt[dah_donated_last_observed_year==2025, iso3])
unique(donor_dt[dah_donated_last_observed_year==2026, iso3])

donor_dt[dah_donated_last_observed_year==2022 & year == 2023, dah_donor := 0]
donor_dt[dah_donated_last_observed_year < 2023, dah_donated_last_observed_year := 2023]
donor_dt[year < dah_donated_last_observed_year & is.na(dah_donor), dah_donor := 0]
donor_dt <- donor_dt[!(year > dah_donated_last_observed_year & dah_donor == 0)]

## Save out retrospective DAH by donor
fwrite(donor_dt, paste0(root_fold, "FILEPATH"))
