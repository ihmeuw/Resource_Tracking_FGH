
require(data.table)

## Set up run info
project <- "FGH_2024"
variable <- "ppp"
version <- "ID"
date <- "ID"
yvv <- "logit_ppp_gdp"

chaos <- T

if (chaos) {
  cha <- "Add Chaos"
} else {
  cha <- "No Chaos"
}

covid_comment <- paste0(version, "_COVID")
comment <- paste0(version, "_COVID/NO_COVID")
comment_long <- paste0(cha, "; ", date, " ", yvv, "; ", gsub("_", "", project), " first run")

# GBD round inputs
release_id <- ID
location_set_id <- ID

## Set forecasting year cutoff info
int_decay <- 1
start_year <- 1995
end_FC <- 2050
oos_years <- 15
N_draws <- 500
chaos_percent <- 15

# Input filepath to retrospective pipeline PPP/GDP outputs
retro_data_stats <- 'FILEPATH'
retro_data_draws <- 'FILEPATH'
input_data_path <- 'FILEPATH'

# Filepath to forecasting input data
prepped_inputs <- paste0(project, "FILEPATH")

if(no_covid_model) {
  end_fit <- 2019 # no-COVID model uses data only from pre-COVID years through 2019
  
  metadata_list <- list(
    variable = variable,
    project = project,
    date = date,
    version = version,
    comment_long = comment_long,
    start_year = start_year,
    end_fit = end_fit,
    end_FC = end_FC,
    oos_years = oos_years,
    chaos = chaos,
    chaos_percent = chaos_percent,
    N_draws = N_draws,
    release_id = release_id,
    location_set_id = location_set_id,
    retro_data_stats = retro_data_stats,
    retro_data_draws = retro_data_draws,
    prepped_inputs_path = 'FILEPATH',
    no_covid_model = no_covid_model
  )
  
} else {
  end_fit <- 2022
  comment <- covid_comment
  
  metadata_list <- list(
    variable = variable,
    project = project,
    date = date,
    version = version,
    comment_long = comment_long,
    start_year = start_year,
    end_fit = end_fit,
    end_FC = end_FC,
    oos_years = oos_years,
    chaos = chaos,
    chaos_percent = chaos_percent,
    N_draws = N_draws,
    release_id = release_id,
    location_set_id = location_set_id,
    retro_data_stats = retro_data_stats,
    retro_data_draws = retro_data_draws,
    prepped_inputs_path = 'FILEPATH',
    no_covid_model = no_covid_model
  )
}


ensemble_metadata <- list(
  yvar = c(yvv),
  xvar = paste0("FE_", c("ln_gdppc", "logit_pop64", "logit_gge_gdp", "logit_dah_gdp", "logit_ghe_gdp")),
  re_coef = NULL,
  ar = c(1),
  ar_mod = c(1),
  ma = c(0),
  ma_mod = c(1),
  weight_decays = seq(0.0, 0.2, length.out = 15),
  global_int = 1, country_int = 1, country_int_dist = 1,
  fdiff = c(1),
  conv = c(0),
  scaled_lev_conv = c(0),
  ar_constrain = 0,
  int_decay = int_decay
)

full_grid <- do.call(create_ensemble_grid, ensemble_metadata)

coef_priors <- list(
  "conv" = -1,
  "scaled_c" = -1,
  "FE_ln_gdppc" = 1,
  "FE_logit_pop64" = 1
)
