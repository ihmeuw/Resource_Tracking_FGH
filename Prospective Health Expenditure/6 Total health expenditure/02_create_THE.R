############################################
### Author: USERNAME
### Purpose: Create THE from the component forecasts
############################################

rm(list = ls())

library(AFModel)

## Open the parser and parse the version
args <- commandArgs(trailingOnly = TRUE)
variable <- args[1]
version <- args[2]

root_fold <- 'FILEPATH'

load(paste0(root_fold, 'FILEPATH'))

## Get location metadata
source('FILEPATH')
lox <- get_location_metadata(location_set_id = metadata_list$location_set_id, release_id = metadata_list$release_id)
lox <- lox[level == 3, .(iso3 = ihme_loc_id, location_name, super_region_name, region_name, location_id)]


################################################
## (1) Get all financing source per capita data
################################################

print("Get all financing source per capita data")

dah_per_cap <- data.table(read_feather('FILEPATH'))[year %in% c(metadata_list$start_year:metadata_list$end_FC)]
ghes_per_cap <- data.table(read_feather('FILEPATH'))[year %in% c(metadata_list$start_year:metadata_list$end_FC)]
ppp_per_cap <- data.table(read_feather('FILEPATH'))[year %in% c(metadata_list$start_year:metadata_list$end_FC)]
oop_per_cap <- data.table(read_feather('FILEPATH'))[year %in% c(metadata_list$start_year:metadata_list$end_FC)]


################################################
## (2) Sum reference scenarios to generate THE per capita reference scenario
################################################

melt_and_clean <- function(data, varname) {
  melt.data.table(data[scenario %in% c(0), ][, scenario := NULL], c("iso3", "year"), variable.name = "draw", value.name = varname)
}

dah_pc_ref <- melt_and_clean(dah_per_cap, "dah_pc")
ghes_pc_ref <- melt_and_clean(ghes_per_cap, "ghes_pc")
ppp_pc_ref <- melt_and_clean(ppp_per_cap, "ppp_pc")
oop_pc_ref <- melt_and_clean(oop_per_cap, "oop_pc")

key <- c("iso3", "draw", "year")
setkeyv(dah_pc_ref, key)
setkeyv(ghes_pc_ref, key)
setkeyv(ppp_pc_ref, key)
setkeyv(oop_pc_ref, key)

the_pc_ref <- oop_pc_ref[ppp_pc_ref, ][ghes_pc_ref, ]
the_pc_ref <- merge(the_pc_ref, dah_pc_ref, c("iso3", "draw", "year"), all.x = T)

## Zero out high income country years DAH
the_pc_ref[is.na(dah_pc), dah_pc := 0]
gc(T)


print("Make THEpc reference")

the_pc_ref[, data := dah_pc + ghes_pc + ppp_pc + oop_pc]
the_pc_ref <- the_pc_ref[, .(iso3, draw, year, data)]
setkeyv(the_pc_ref, key)

the_pc_ref_stats <- stat_maker(the_pc_ref, melt = F, merge = F)
the_pc_ref <- dcast.data.table(the_pc_ref, iso3 + year ~ draw, value.var = "data")


################################################
## (3) Create scenarios of THE per capita
################################################

## Get mean scenario lines
scenario_mean_lines <- mean_scenario_model(the_pc_ref_stats, metadata_list,
                                           aroc_years = 20, transform = "log"
)

### Propagate draws
scenario_draws <- scenario_uncertainty(
  mean_data = scenario_mean_lines,
  draws_data = the_pc_ref,
  metadata_list,
  transform = "log",
  rev_trans = "exp"
)

the_per_cap_scenarios <- scenario_draws$draws

print("Save out for next script")
save(
  list = c(
    "dah_per_cap",
    "ghes_per_cap",
    "ppp_per_cap",
    "oop_per_cap",
    "the_per_cap_scenarios"
  ),
  file = paste0(root_fold, 'FILEPATH')
)
