############################################
### Author: USERNAME
### Purpose: Make scenarios, create derivatives, and save 
############################################

rm(list = ls())

library(AFModel)

## Open the parser and parse the version
args <- commandArgs(trailingOnly = TRUE)
variable <- args[1]
date <- args[2]
version <- args[3]

root_fold <- 'FILEPATH'
load(paste0(root_fold, 'FILEPATH'))

correlated_output <- data.table(read_feather(paste0(root_fold, "FILEPATH")))

## Get location metadata
source(paste0("FILEPATH"))
lox <- get_location_metadata(location_set_id = metadata_list$location_set_id, release_id = metadata_list$release_id) 
lox <- lox[level == 3, .(iso3 = ihme_loc_id, location_name, super_region_name, region_name, location_id, level)]

########################################
#### (10) Create derivatives (per GDP, per cap, and totes)
########################################

## Get populations
total_pop <- fread('FILEPATH')
total_pop <- total_pop[iso3 %in% lox$iso3]
total_pop <- total_pop[, .(year, iso3, pop_totes = population)]

## Get best GDP
gdppc <- fread('FILEPATH')
gdppc <- gdppc[year %in% c(1990:metadata_list$end_FC) & scenario == 4.5, .(iso3, year, gdppc = mean)]

## Convert to GDP
gdppc <- merge(gdppc, total_pop, c("iso3", "year"), all.x = T)
gdppc[, gdp := gdppc * pop_totes]

## Get debtGGE/GDP
gge_gdp <- fread('FILEPATH')
gge_gdp <- gge_gdp[year %in% c(metadata_list$start_year:metadata_list$end_FC), .(iso3, year, gge_gdp = mean)]

### GHE/GDP = GHE/debtGGE * debtGGE/GDP
ghe_gdp_draws <- merge(correlated_output, gge_gdp, c("iso3", "year"), all.x = T)
ghe_gdp_draws[, paste0("draw_", 1:metadata_list$N_draws) := lapply(paste0("draw_", 1:metadata_list$N_draws), function(x) get(x) * gge_gdp)]
ghe_gdp_draws[, gge_gdp := NULL]

## Calculate means and UIs
ghe_gdp_stats <- stat_maker(ghe_gdp_draws, melt = T, merge = F, idvar = c("iso3", "year"))


########################################
#### (13) Make GHE per capita = GHE/GDP * GDPpc
########################################

ghe_pc_draws <- merge(ghe_gdp_draws, gdppc[, .(iso3, year, gdppc)], c("iso3", "year"), all.x = T)
ghe_pc_draws[, paste0("draw_", c(1:metadata_list$N_draws)) := lapply(paste0("draw_", c(1:metadata_list$N_draws)), function(x) get(x) * gdppc)]
ghe_pc_draws[, gdppc := NULL]

# save out GHEpc draws to use in mean absolute error calculation 
write_feather(ghe_pc_draws, paste0(root_fold, "FILEPATH"))

# finalize new GHEpc
ghe_gge_draws <- correlated_output

#### Save out these steps
save(list = c("ghe_pc_draws", "ghe_gge_draws", "gdppc", "gge_gdp"),
     file = paste0(root_fold, 'FILEAPTH'))

## Calculate means and UIs
ghe_pc_stats <- stat_maker(ghe_pc_draws, melt = T, merge = F, idvar = c("iso3", "year"))


#### Make scenarios in log GHEpc ####

## Get mean scenario lines
scenario_mean_lines <- mean_scenario_model(ghe_pc_stats, metadata_list,
                                           aroc_years = 20, transform = "log"
)

### Propagate draws
scenario_draws <- scenario_uncertainty(
  mean_data = scenario_mean_lines,
  draws_data = ghe_pc_draws,
  metadata_list,
  transform = "log",
  rev_trans = "exp"
)


#### Save out all the files

## GHE per GDP
write_feather(ghe_gdp_draws, paste0(root_fold, "FILEPATH"))
fwrite(ghe_gdp_stats, paste0(root_fold, "FILEPATH"))

## GHE per capita
write_feather(scenario_draws$draws, paste0(root_fold, "FILEPATH"))
fwrite(scenario_draws$stats, paste0(root_fold, "FILEPATH"))

