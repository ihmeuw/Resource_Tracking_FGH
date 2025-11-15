###################################################
### Author: USERNAME
### Purpose: Preparing all the inputs going into PPP forecasting
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
prepped_inputs <- args[9]
project <- args[10]

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
gge_gdp <- fread('FILEPATH')
gge_gdp <- gge_gdp[year <= end_FC, .(iso3, year, gge_gdp = mean, logit_gge_gdp = logit_fn(mean, 0, 1.9))]

## Get best GDP
gdppc <- fread('FILEPATH')
gdppc <- gdppc[scenario == 4.5, .(iso3, year, gdppc = mean, ln_gdppc = log(mean))]
gdppc <- gdppc[!grepl("_", iso3)]

###########################
### (2) Get FBD pops
###########################

total_pop <- fread('FILEPATH')
total_pop <- total_pop[iso3 %in% locs$iso3]
total_pop <- total_pop[, .(year, iso3, population, over64_pop = pop_over64)]
total_pop[, logit_pop64 := logit(over64_pop / population)]


###########################
### (4) Get DAH per GDP and GHE per GDP
###########################

dah <- fread('FILEPATH')
dah <- dah[scenario == 0, .(iso3, year, dah_gdp = mean)]

ghe <- fread("FILEPATH")
ghe <- ghe[, .(iso3, year, ghe_gdp = mean, logit_ghe_gdp = logit(mean))]


###########################
### (5) Get PPP per GDP
###########################

# Note that we subset here through last observed year
ppp <- fread(retro_data_stats)[year >= start_year & year <= 2022, .(iso3, year, ppp_gdp = mean)]
ppp <- ppp[!grepl("_", iso3)]


###########################
### (6) Merge all data
###########################

data <- gdppc
data <- merge(data, total_pop, c("iso3", "year"), all.x = T)
data <- merge(data, dah, c("iso3", "year"), all.x = T)
data <- merge(data, ghe, c("iso3", "year"), all.x = T)
data <- merge(data, ppp, c("iso3", "year"), all.x = T)
data <- merge(data, gge_gdp, c("iso3", "year"), all.x = T)
data <- data[year >= start_year & year <= end_FC]
data[, ggepc := gdppc * gge_gdp]

## Zero out DAH/GDP for high income country years
data[is.na(dah_gdp), dah_gdp := 0]
data[, logit_dah_gdp := logit_fn(dah_gdp, 0, 1)]

## Merge to get region info
data <- merge(data, locs, c("iso3"), all.x = T)

### Create the main depvar ##

### Country specific max bounds
data[, logit_ppp_gdp := logit_fn(ppp_gdp, 0, 1)]


###########################
### (7) Save out for the years we want
###########################

fwrite(data, 'FILEPATH')
fwrite(data, 'FILEPATH')
