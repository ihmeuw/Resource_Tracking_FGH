############################################
### Author: USERNAME
### Purpose: Processes chaos data and correlates draws
############################################

rm(list = ls())

library(AFModel)

## Open the parser and parse the version
args <- commandArgs(trailingOnly = TRUE)
variable <- args[1]
date <- args[2]
version <- args[3]
repo_path <- args[4]

root_fold <- 'FILEPATH'
load(paste0(root_fold, 'FILEPATH'))

########################################
#### (7) Fix past draw data because
########################################

## Custom logit and inverse logit functions
custom_logit_fn <- function(y, y_min = 0, y_max = logit_max, epsilon = 1e-5) {
  log((y - (y_min - epsilon)) / (y_max + epsilon - y))
}

custom_antilogit_fn <- function(antiy, y_min = 0, y_max = logit_max, epsilon = 1e-5) {
  (exp(antiy) * (y_max + epsilon) + y_min - epsilon) /
    (1 + exp(antiy))
}

## Get location metadata
source(paste0("FILEPATH"))
lox <- get_location_metadata(location_set_id = metadata_list$location_set_id, release_id = metadata_list$release_id) 
lox <- lox[level == 3, .(iso3 = ihme_loc_id, location_name, super_region_name, region_name, location_id, level)]

## Get draws
yvar_draws <- data.table(read_feather(metadata_list$new_retro_data_draws,
                                      columns = c("iso3", "year", paste0("draw_", c(1:metadata_list$N_draws)))
))

yvar_draws <- yvar_draws[(year %in% c(metadata_list$start_year:eval(metadata_list$end_fit))) & (iso3 %in% lox$iso3)]

colnames(yvar_draws) <- c("iso3", "year", paste0("draw_", c(1:metadata_list$N_draws)))

## Set zeros to small lower bound
yvar_draws[yvar_draws == 0] <- 0.00001


## Custom logit transform by country
yvar_draws[, country_max_bound := 1]


yvar_draws[, paste0("draw_", c(1:metadata_list$N_draws)) := lapply(
  paste0("draw_", c(1:metadata_list$N_draws)),
  function(v) logit_fn(get(v), 0, country_max_bound)
)]

yvar_draws[, paste0("diff_draw_", c(1:metadata_list$N_draws)) := lapply(
  paste0("draw_", c(1:metadata_list$N_draws)),
  function(v) get(v) - shift(get(v))
), by = "iso3"]

yvar_draws[year > metadata_list$start_year, paste0("draw_", c(1:metadata_list$N_draws)) := lapply(
  paste0("diff_draw_", c(1:metadata_list$N_draws)),
  function(v) (get(v))
)]
yvar_draws[, paste0("diff_draw_", c(1:metadata_list$N_draws)) := NULL]

yvar_draws[, country_max_bound := NULL]

print("Make past draw corrections")



if (metadata_list$chaos) {
  chaos_output <- readRDS(paste0(
    root_fold,
    "FILEPATH"
  ))
  chaos_output <- chaos_output[year >= eval(metadata_list$end_fit + 1)]
  chaos_output <- rbindlist(list(chaos_output, yvar_draws), use.names = T)
  setkeyv(chaos_output, c("iso3", "year"))
  
  saveRDS(chaos_output, paste0(root_fold, "FILEPATH"))
} else {
  chaos_output <- readRDS(paste0(root_fold, "FILEPATH"))
  chaos_output <- chaos_output[year >= eval(metadata_list$end_fit + 1)]
  chaos_output <- rbindlist(list(chaos_output, yvar_draws), use.names = T)
  setkeyv(chaos_output, c("iso3", "year"))
  
  saveRDS(chaos_output, paste0(root_fold, "FILEPATH"))
}


## Cumulative sum all the data and convert to level space
chaos_list <- final_cum_sum(
  root_fold = root_fold,
  chaos = metadata_list$chaos,
  oos_years = metadata_list$oos_years,
  N_draws = metadata_list$N_draws
)


## Antilogit with custom logit
chaos_list$stats <- NULL
chaos_list$draws[, country_max_bound := 1]

chaos_list$draws[, paste0("draw_", c(1:metadata_list$N_draws)) := lapply(
  paste0("draw_", c(1:metadata_list$N_draws)),
  function(v) custom_antilogit_fn(get(v), 0, country_max_bound)
)]
chaos_list$draws[, country_max_bound := NULL ]
chaos_list$stats <- stat_maker(chaos_list$draws, melt = T, merge = F)


########################################
#### (7) Populate draws
########################################

## Correlated draws with AR1 covariance
correlated_output <- correlate_draws_1D(draws = chaos_list[["draws"]], stats = chaos_list[["stats"]], metadata_list = metadata_list, AR1 = T)

## Save out
write_feather(correlated_output[["draws"]], paste0(root_fold, "FILEPATH"))
fwrite(correlated_output[["stats"]], paste0(root_fold, "FILEPATH"))
