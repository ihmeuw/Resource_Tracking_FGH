# Step 1: Scatter ODA per GNI vs GDP per capita for all DAH donor countries through 2025
# The frontiers will be used to forecast ODA/GNI to 2100 for alternate scenarios,
# And from there we will set DAH/ODA as constant and get DAH through 2100.
# Use GNI forecasts based on GDP through 2100 for all countries.

require(data.table)
require(ggplot2)

## Source currency conversion function
source('FILEPATH')

dah_d_folder <- "ID"
root_fold <- 'FILEPATH'

##############
# Pull main DAH-D donor countries
##############

## Read in retrospective DAH estimates
fgh_donor <- fread('FILEPATH')
retro_dahd_variable <- paste0("DAH_23")
fgh_donor <- fgh_donor[, .(iso3 = income_sector, year, dah_donor = dah)]

dah_donors <- sort(unique(fgh_donor$iso3)) # 24 main donor countries, plus non-country donors BMGF, DEBT, OTHER, OTHERDAC, OTHERPUB, PRIVATE, UNALL
dah_donor_countries <- dah_donors[!(dah_donors %in% c('BMGF', 'DEBT', 'OTHER', 'OTHERDAC', 'OTHERPUB', 'PRIVATE', 'UNALL'))]

##############
# Get ODA per GNI
##############

# ODA commitments and disbursements from OECD, in nominal USD, by donor
oda0 <- fread('FILEPATH')

# Keep needed years and donors
oda0 <- oda0[year %in% c(1990:2025)]
oda_donors <- sort(unique(oda0$iso3))
setdiff(dah_donors, oda_donors)

# Calculate total ODA donated (ODA-D) and ODA donated for health sectors (ODA-H)
# Aggregate the total of all sub-sectors
# Merge on sector codes dataset, which defines the aggregate sectors, and drop any aggregates,

oda_sectors <- fread('FILEPATH')
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

# Calculate ODAH/ODA for commitments and disbursements
oda0 <- copy(oda0_oda)
setorder(oda0, "iso3", "year")
oda0[, odah_per_oda_commitment := odah_usd_commitment / oda_usd_commitment]
oda0[, odah_per_oda_disbursement := odah_usd_disbursement / oda_usd_disbursement]

##############
# Get GNI and GDP
##############

## Read in GNI per capita
gnipc <- fread('FILEPATH')
gnipc <- gnipc[!is.na(final_gni)]

## Read in GDP per capita
gdppc <- fread('FILEPATH')[scenario == "4.5"]
gdppc <- gdppc[, .(iso3, year, gdp = mean)]

## Merge and currency convert
oda0 <- merge(oda0, gnipc, by = c("iso3", "year"), all.x = T)
oda0[, year2 := year]
oda0 <- currency_conversion(data = oda0, col.loc = 'iso3', col.value = "oda_usd_disbursement", 
                            currency = 'usd', col.currency.year = "year2",
                            base.unit = 'usd', base.year = .default_base_year)

## Calculate ODA/GNI disbursements
oda0[, oda_disbursed_per_gni := oda_usd_disbursement / final_gni]
oda0 <- merge(oda0, gdppc, by = c("iso3", "year"), all.x = T)
oda0 <- oda0[, .(iso3, year, oda_disbursed_per_gni, gdppc = gdp)]
oda0 <- oda0[oda_disbursed_per_gni > 0]

# Fit frontier on log-log data with all available countries
oda0[, log_ODA_per_GNI := log(oda_disbursed_per_gni)]
oda0[, negative_log_ODA_per_GNI := log(oda_disbursed_per_gni) * -1]
oda0[, log_gdppc := log(gdppc)]
oda0[, negative_log_gdppc := log_gdppc * -1]
oda0[, test_variance := 0.1 * oda_disbursed_per_gni]
fwrite(oda0, paste0(root_fold, "FILEPATH"))


# Data to use in models and predict estimate based on frontier using GDP per capita and 0 inefficiency for 204 countries through 2100
pred_dt <- copy(gdppc)[year %in% c(1990:2100)]
pred_dt[, log_gdppc := log(gdp)]
pred_dt[, negative_log_gdppc := log_gdppc * -1]
pred_dt[, ineff := 0]
fwrite(pred_dt, paste0(root_fold, "FILEPATH"))
