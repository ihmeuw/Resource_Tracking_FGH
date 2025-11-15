###################################################
### Author: USERNAME
### Purpose: Preparing all the inputs going into GHE forecasting
###################################################

## Directory structure

rm(list = ls())

library(AFModel)

## Open the parser and parse the version
args <- commandArgs(trailingOnly = TRUE)
variable <- args[1]
date <- args[2]
start_year <- as.integer(args[3])
end_fit <- as.integer(args[4])
end_FC <- as.integer(args[5])
release_id <- as.integer(args[6])
location_set_id <- as.integer(args[7])
retro_data_stats <- args[8]
retro_data_draws <- args[9]
new_retro_data_draws <- args[10]
prepped_inputs <- args[11]
project <- args[12]


###########################
### (0) Get the location metadata
###########################

source(paste0("FILEPATH"))
locs <- get_location_metadata(location_set_id = location_set_id, release_id = release_id)
locs <- locs[level == 3, .(iso3 = ihme_loc_id, location_name, super_region_name, region_name, location_id)]

###########################
### (1) Get GDP per capita and GGE per GDP
###########################

## Get best debt-adjusted GGE per GDP
gge_gdp <- fread(paste0('FILEPATH'))
gge_gdp <- gge_gdp[year <= end_FC, .(iso3, year, gge_gdp = mean, logit_gge_gdp = logit_fn(mean, 0, 1.9))]

## Get best debt-adjusted GGE
gge_total <- fread(paste0('FIELPATH'))
gge_total <- gge_total[year <= end_FC, .(iso3, year, debt_gge = mean)]

## Get best GDP 
gdppc <- fread(paste0('FILEPATH'))
gdppc <- gdppc[scenario == 4.5, .(iso3, year, gdppc = mean , ln_gdppc = log(mean))]


###########################
### (2) Get FBD pops
###########################

total_pop <- fread(paste0("FILEPATH"))
total_pop <- total_pop[iso3 %in% locs$iso3]
total_pop <- total_pop[, .(year, iso3, pop = population, pop_over64)]
total_pop[, logit_pop64 := logit(pop_over64 / pop)]

###########################
### (4) Get DAH per GDP
###########################

dah <- fread(paste0("FILEPATH"))
dah <- dah[scenario == 0, .(iso3, year, dah_gdp = mean)]

###########################
### (5) Prep GHE total data, and create GHE/debt-adjusted GGE variable
########################

# Subset through last observed year
ghe <- fread(retro_data_stats)[year >= start_year & year <= 2022, .(iso3, year, ghe_total = mean)]
ghe <- merge(ghe, gge_total, by = c("iso3", "year"), all.x = T)
ghe[, ghe_debt_gge := ghe_total / debt_gge]

###########################
### (6) Merge all data
###########################

data <- gdppc
data <- merge(data, total_pop, c("iso3", "year"), all.x = T)
data <- merge(data, dah, c("iso3", "year"), all.x = T)
data <- merge(data, gge_gdp, c("iso3", "year"), all.x = T)
data <- merge(data, ghe, c("iso3", "year"), all.x = T)
data <- data[year >= start_year & year <= end_FC]
data[, ggepc := gdppc * gge_gdp]

## Zero out DAH/GDP for high income country years
data[is.na(dah_gdp), dah_gdp := 0]
data[, logit_dah_gdp := logit_fn(dah_gdp, 0, 1)]

## Merge to get region info
data <- merge(data, locs, c("iso3"), all = T)

### Country specific max bounds
data[, country_max_bound := 1]
data[, country_max_bound := max(country_max_bound, na.rm = T), by = "iso3"]
data[, logit_ghe_debt_gge := logit_fn(ghe_debt_gge, 0, country_max_bound)]

###########################
### (7) Save out
###########################

fwrite(data, "FILEPATH")
fwrite(data, "FILEPATH")

###########################
### (8) Make new draws of retrospective GHE per debt-adjusted GGE
###########################

retro_data_draws_dt <- data.table(read_feather(retro_data_draws))
retro_data_draws_dt <- merge(retro_data_draws_dt, gge_total, by = c("iso3", "year"), all.x = T)
retro_data_draws_dt <- data.table::melt(retro_data_draws_dt, id.vars = c("iso3", "year", "debt_gge"))
retro_data_draws_dt[, value := value / debt_gge][, debt_gge := NULL]
nrow(retro_data_draws_dt[value > 1])
retro_data_draws_dt[value > 1, value := 0.99]
retro_data_draws_dt <- data.table::dcast(retro_data_draws_dt, iso3 + year ~ variable)
write_feather(retro_data_draws_dt, new_retro_data_draws)
