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
source('FILEPATH')
lox <- get_location_metadata(location_set_id = metadata_list$location_set_id, release_id = metadata_list$release_id) 
lox <- lox[level == 3, .(iso3 = ihme_loc_id, location_name, super_region_name, region_name, location_id, level)]

########################################
#### (10) Create derivatives (per capita)
########################################

total_pop_from_FBD <- fread('FILEPATH')
total_pop_from_FBD <- total_pop_from_FBD[iso3 %in% lox$iso3]
total_pop_from_FBD <- total_pop_from_FBD[, .(year, iso3, pop_totes = population)]

## Get best GDP per capita
gdppc <- fread('FILEPATH')
gdppc <- gdppc[year %in% c(metadata_list$start_year:metadata_list$end_FC) & scenario == 4.5, .(iso3, year, gdppc = mean)]

## Convert to GDP
gdppc <- merge(gdppc, total_pop_from_FBD, c("iso3", "year"), all.x = T)
gdppc[, gdp := gdppc * pop_totes]


########################################
#### (13) Make PPP per capita = PPP/GDP * GDPpc
########################################

ppp_pc_draws <- merge(correlated_output, gdppc[, .(iso3, year, gdppc)], c("iso3", "year"), all.x = T)
ppp_pc_draws[, paste0("draw_", c(1:metadata_list$N_draws)) := lapply(paste0("draw_", c(1:metadata_list$N_draws)), function(x) get(x) * gdppc)]
ppp_pc_draws[, gdppc := NULL]

## Calculate means and UIs
ppp_pc_stats <- stat_maker(ppp_pc_draws, melt = T, merge = F, idvar = c("iso3", "year"))


#### Make scenarios ####

## Get mean scenario lines
scenario_mean_lines <- mean_scenario_model(ppp_pc_stats, metadata_list,
                                           aroc_years = 20, transform = "log"
)

### Propagate draws
scenario_draws <- scenario_uncertainty(
  mean_data = scenario_mean_lines,
  draws_data = ppp_pc_draws,
  metadata_list,
  transform = "log",
  rev_trans = "exp"
)


## PPP per capita
write_feather(scenario_draws$draws, paste0(root_fold, "FILEPATH"))
fwrite(scenario_draws$stats, paste0(root_fold, "FILEPATH"))
