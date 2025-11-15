###################################################
### Author: USERNAME
### Purpose: Preparing all the inputs going into DAH R forecasting
###################################################

rm(list = ls())

library(AFModel)

## Open the parser
args <- commandArgs(trailingOnly = TRUE)
source <- args[1]
variable <- args[2]
date <- args[3]
comment <- args[4]
start_year <- as.integer(args[5])
end_FC <- as.integer(args[6])
prepped_inputs <- args[7]
nons_path <- args[8]
retro_data <- args[9]
release_id <- as.integer(args[10])
location_set_id <- as.integer(args[11])
project <- args[12]
last_retro_DAH_year <- as.integer(args[13])
end_fit <- as.integer(args[14])
split_run <- args[15]

root_fold <- 'FILEPATH'

###########################
### (0) Get the location metadata
###########################

source('FILEPATH')
locs <- get_location_metadata(location_set_id = location_set_id, release_id = release_id)
locs <- locs[level == 3, .(iso3 = ihme_loc_id, location_name, super_region_name, region_name, location_id)]
lox_ids <- unique(locs$location_id)

###########################
### (1) Get GDP per capita
###########################

gdppc <- fread('FILEPATH')
gdppc <- gdppc[scenario == "4.5", .(iso3, year, gdppc = mean , ln_gdppc = log(mean))]


###########################
### (2) Get FBD pops
###########################

working_popfrc <- fread('FILEPATH')
working_popfrc <- working_popfrc[iso3 %in% locs$iso3]
working_popfrc <- working_popfrc[, .(year, iso3, pop = population, pop_under15)]
working_popfrc[, logit_pop15 := logit(pop_under15 / pop)]

###########################
### (3) Get health-related covariates
###########################

use_health_covariates=FALSE
if(use_health_covariates) {
  # life expectancy at birth
  source('FILEPATH')
  le <- get_life_table(release_id = 9, life_table_parameter_id = 5, age_group_id = 388, sex_id = 3, year_id = c(1990:2023))
  le <- le[location_id %in% lox_ids]
  le <- le[, .(year = year_id, location_id, life_expectancy = mean)]
  
  # U5M
  source('FILEPATH')
  u5m <- get_outputs("cause", release_id = 9, measure_id = 1, metric_id = 1, cause_id = 294, 
                     location_id = lox_ids, sex_id = 3, age_group_id = 1, year_id = c(1990:2023))
  u5m <- u5m[, .(year = year_id, location_id, u5m = val)]
  u5m[, u5m_share := sum(u5m), by = "year"]
  u5m[, u5m_share := log(u5m/u5m_share), by = "year"]
  
  # total DALYs
  dalys <- get_outputs("cause", release_id = 9, measure_id = 2, metric_id = 1, cause_id = 294, 
                       location_id = lox_ids, sex_id = 3, age_group_id = 22, year_id = c(1990:2023))
  dalys <- dalys[, .(year = year_id, location_id, dalys = val)]
  dalys[, dalys_share := sum(dalys), by = "year"]
  dalys[, dalys_share := log(dalys/dalys_share), by = "year"]
  
  all_health <- merge(le, u5m, by = c("year", "location_id"))
  all_health <- merge(all_health, dalys, by = c("year", "location_id"))
  all_health[, ln_life_expectancy := log(life_expectancy)]
  all_health <- merge(all_health, locs[, .(location_id, iso3)], by = c("location_id"), all.x = T)
  
  # DALY forecasts
  dalys_fc <- fread("FILEPATH")
  dalys_fc <- dalys_fc[location_id %in% lox_ids]
  dalys_fc <- dalys_fc[, .(dalys = value, year = year_id, location_id)]
  dalys_fc[, dalys_share := sum(dalys), by = "year"]
  dalys_fc[, dalys_share := log(dalys/dalys_share), by = "year"]
  
  # Deaths forecasts
  u5m_fc <- fread("FILEPATH")
  u5m_fc <- u5m_fc[location_id %in% lox_ids]
  u5m_fc <- u5m_fc[, .(u5m = value, year = year_id, location_id)]
  u5m_fc[, u5m_share := sum(u5m), by = "year"]
  u5m_fc[, u5m_share := log(u5m/u5m_share), by = "year"]
  
  forecasts <- merge(dalys_fc, u5m_fc, by = c("year", "location_id"))
  forecasts <- forecasts[year > max(all_health$year)]
  forecasts <- merge(forecasts, locs[, .(location_id, iso3)], by = "location_id", all.x=T)
  
  all_health <- rbind(all_health, forecasts, fill = T)
  setorder(all_health, "location_id", "year")
}

###########################
### (4) Get DAH donors total envelope
###########################

if(date %like% "ref") {
  dah_d <- fread("FILEPATH")
} else if(date %like% "better") {
  dah_d <- fread('FILEPATH')
  setnames(dah_d, 'mean', 'dah_donor_ref')
} else if(date %like% "worse") {
  dah_d <- fread('FILEPATH')
  setnames(dah_d, 'dah_donor_ref_ACTUAL', 'dah_donor_ref')
}

dah_d <- dah_d[year == 2025]
dah_d <- dah_d[!iso3 %in% c("AGGREGATE", "AGG", "PRIVATE", "DEBT", "OTHER", "UNALL", "CHN", "OTHERPUB_NONGBD")]
top_10_sources <- dah_d[order(-dah_donor_ref)][1:10, iso3]
all_sources <- c(top_10_sources, "RESID")

if(date %like% "ref") {
  dah_donors <- fread('FILEPATH')[iso3 != "AGGREGATE"]
  setnames(dah_donors, "dah_donor_ref", "dah_donor")
} else if(date %like% "better") {
  dah_donors <- fread('FILEPATH')[iso3 != 'AGGREGATE']
  setnames(dah_donors, "mean", "dah_donor")
} else if(date %like% "worse") {
  dah_donors <- fread('FILEPATH')[iso3 != "AGGREGATE"]
  setnames(dah_donors, "dah_donated_worse", "dah_donor")
}

dah_donors[!iso3 %in% top_10_sources, iso3 := "RESID"]
dah_donors <- dah_donors[, .(env = sum(dah_donor)), by = c("year", "iso3")]
dah_donors <- dah_donors[iso3 %in% source]
dah_donors <- dah_donors[, .(year, env)]

###########################
### (5) Prep recipient Data
########################

fgh_recip <- fread('FILEPATH')[year <= 2030]

if(date %like% "ref") {
  setnames(fgh_recip, "dah_ref", "dah")
} else if(date %like% "better") {
  setnames(fgh_recip, "dah_ref", "dah")
} else if(date %like% "worse") {
  setnames(fgh_recip, "dah_ref", "dah")
}

setnames(fgh_recip, "source", "donor")
fgh_recip[!donor %in% top_10_sources, donor := "RESID"]
fgh_recip <- fgh_recip[donor == source]

fgh_recip <- fgh_recip[, .(dah = sum(dah)), by = c("recip", "year")]
fgh_recip <- fgh_recip[, .(dah_recip = dah, iso3 = recip, year)]
fgh_recip[, dah_recip_total := sum(dah_recip), by = "year"]


#### (2) Merge with location sets so we can identify which are non-GBD locations
fgh_recip <- merge(fgh_recip, locs[, .(iso3, location_name)], 'iso3', all.x=T)

## Expand to end_FC
fgh_recip1 <- data.table::dcast(fgh_recip, location_name + iso3 ~ year, value.var = c('dah_recip'))
fgh_recip2 <- data.table::dcast(fgh_recip, location_name + iso3 ~ year, value.var = c('dah_recip_total'))

fgh_recip1 <- fgh_recip1[, paste0(c(eval(last_retro_DAH_year + 1):end_FC)):= NA]
fgh_recip1 <- melt.data.table(fgh_recip1, c('iso3', 'location_name'), value.name = c('dah_recip'), variable.name = 'year')
fgh_recip1[, year:= as.numeric(as.character(year))]

fgh_recip2 <- fgh_recip2[, paste0(c(eval(last_retro_DAH_year + 1):end_FC)):= NA]
fgh_recip2 <- melt.data.table(fgh_recip2, c('iso3', 'location_name'), value.name = c('dah_recip_total'), variable.name = 'year')
fgh_recip2[, year:= as.numeric(as.character(year))]

fgh_recip <- merge(fgh_recip1, fgh_recip2, by = c("iso3", "location_name", "year"))

#### (4) Zero out all NAs
fgh_recip[is.na(dah_recip) & year <= last_retro_DAH_year, dah_recip := 0]
fgh_recip[is.na(dah_recip_total) & year <= last_retro_DAH_year, dah_recip_total := 0]


#### (3) Take out INKIND, WLD, and OTH to forecast separately as ARIMA processes
non_country_recipients <- c("UNALLOCABLE_DAH_INK", "UNALLOCABLE_DAH_QZA", "UNALLOCABLE_DAH_WLD")
non_country_dah <- fgh_recip[iso3 %in% non_country_recipients]
non_country_dah <- merge(non_country_dah, dah_donors, by = "year", all = T)





###########################
### (5) Forecast non-country recipients using linear models
###########################

setorder(non_country_dah, iso3, year)
non_country_dah[, dah_prop_totes := dah_recip/env]
non_country_dah[, dah_prop_totes := na.locf(dah_prop_totes), by = "iso3"]

non_country_dah[year > 2030, dah_recip := env * dah_prop_totes]

non_country_dah2 <- non_country_dah[, .(iso3, year, dah_prop_totes, dah_recip)]
non_country_dah <- non_country_dah[, .(iso3, year, dah_prop_totes)]


## Save out non-country sources for later compiling
fwrite(non_country_dah[year <= end_FC], gsub(".csv", paste0("_source_", source, ".csv"), nons_path))
fwrite(non_country_dah2[year <= end_FC], paste0(root_fold, "FILEPATH"))


###########################
### (6) Merge all data
###########################

data <- gdppc
data <- merge(data, working_popfrc, c("iso3", "year"), all.x = T)
data <- merge(data, dah_donors, "year", all.x = T)
if(use_health_covariates) {
  data <- merge(data, all_health, c("year", "iso3"), all.x = T)
}

## Left merge on DAH data 
data <- merge(data, fgh_recip, c("iso3", "year"), all.y = T)
data <- data[order(iso3, year)]
data <- data[year >= start_year & year <= end_FC]

## Make Ebola indicator variable for just three countries during years of Ebola outbreak
data[, ebola_dummy := 0]
data[iso3 %in% c("SLE", "LBR", "GIN") & year %in% c(2014,2015), ebola_dummy := 1]

## Merge to get region info
data <- merge(data, locs, c("iso3", "location_name"), all.x = T)


### Create the main depvar (logit_dah_per_totes)
data[, actual_dah_per_totes:= (dah_recip+1e-9) / sum(dah_recip+1e-9), by = c('year')]
data <- data[!(iso3 %in% non_country_recipients)]
data[, dah_per_totes:= actual_dah_per_totes, by = c('year')]
data[, orig_dah_per_totes:= actual_dah_per_totes]
data[, logit_dah_per_totes:= logit(dah_per_totes)]
data[, logit_actual_dah_per_totes:= logit(actual_dah_per_totes)]
data[, orig_logit_actual_dah_per_totes:= logit_actual_dah_per_totes]
data[, log_dah_per_totes:= log(dah_per_totes)]

# Smooth retro data using loess curve over no-COVID years (<= 2019)
data <- data[year <= 2019, dah_per_totes_smooth := (fitted(loess(dah_per_totes ~ year))),  by=iso3]
data <- data[dah_per_totes_smooth < 0, dah_per_totes_smooth := 0.0000001]
data <- data[year >= 2019, dah_per_totes_smooth := dah_per_totes]
data <- data[, logit_dah_per_totes := logit(dah_per_totes_smooth)]

# Create a few more covariates
data[, gdp := gdppc * pop]
data[, ln_gdp_share := log(gdp / sum(gdp)), by = c("year")]
data[, ln_pop15_share := log(pop_under15 / sum(pop_under15)), by = c("year")]


###########################
### (7) Save out for the years we want
###########################

fwrite(data, 'FILEPATH')
fwrite(data, 'FILEPATH')
