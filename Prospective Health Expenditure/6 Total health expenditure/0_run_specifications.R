
require(data.table)

## Set up run info
variable <- "the"
project <- "FGH_2024"
version <- THE_version <- "ID"
start_year <- 1995
end_fit <- 2022
end_FC <- 2050
N_draws <- 500

# GBD round inputs
release_id <- ID
location_set_id <- ID

metadata_list <- list(
  start_year = start_year,
  end_FC = end_FC,
  end_fit = end_fit,
  N_draws = N_draws,
  project = project,
  release_id=release_id,
  location_set_id=location_set_id
)
