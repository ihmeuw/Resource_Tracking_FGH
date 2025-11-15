
require(data.table)

## Set up run info
project <- "FGH_2024"
variable <- "dah_r"
version <- "ID"
date <- "ID"
yvv <- "logit_dah_per_totes"
if(date %like% "ref") {
  split_run <- "ID"
} else if(date %like% "better") {
  split_run <- "ID"
} else if (date %like% "worse") {
  split_run <- "ID"
}

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
start_year <- 1990
end_FC <- 2100
oos_years <- 10
N_draws <- 500
chaos_percent <- 15

# Set last retrospective DAH year
last_retro_DAH_year = 2030
# Input filepath to retrospective DAH
dah_adb_pdb <- "FILEPATH"

# Filepath to forecasting input data
input_data_path <- 'FILEPATH'
prepped_inputs <- paste0(project, "_COVID")

# Filepath to non-country recipients
nons_path <- paste0(input_data_path, "FILEPATH")

# Filepath to non-country recipients after graduation matrix
grads_path <- paste0(input_data_path, "FILEPATH")


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
    retro_data = retro_data,
    nons_path = nons_path,
    last_retro_DAH_year = last_retro_DAH_year,
    grads_path = grads_path,
    dah_adb_pdb = dah_adb_pdb,
    prepped_inputs_path = paste0(input_data_path, prepped_inputs, "FILEPATH"),
    no_covid_model = no_covid_model
  )
} else {
  end_fit <- last_retro_DAH_year
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
    retro_data = retro_data,
    nons_path = nons_path,
    last_retro_DAH_year = last_retro_DAH_year,
    grads_path = grads_path,
    dah_adb_pdb = dah_adb_pdb,
    prepped_inputs_path = paste0(input_data_path, prepped_inputs, "FILEPATH"),
    no_covid_model = no_covid_model
  )
}

## Create a metadata for the ensemble grid innards
ensemble_metadata <- list(
  yvar = c(yvv),
  xvar = paste0("FE_", c("ln_gdppc", "logit_pop15", "ln_gdp_share", "ln_pop15_share")),
  re_coef = NULL,
  ar = c(0),
  ar_mod = c(1),
  ma = c(0),
  ma_mod = c(1),
  weight_decays = round(seq(0.0, 0.8, length.out = 30), digits = 5),
  global_int = 1, country_int = 1, country_int_dist = 1,
  fdiff = c(0),
  conv = c(0),
  scaled_lev_conv = c(0),
  ar_constrain = 0,
  int_decay = 0
)

full_grid <- do.call(create_ensemble_grid, ensemble_metadata)

### Set GDP per capita and remake ID
full_grid <- full_grid[FE_ln_gdp_share == 1]
full_grid[, id:= .I]

## List our coefficient priors
coef_priors <- list("conv" = -1, 
                    "scaled_c" = -1, 
                    "FE_ln_gdppc" = -1,
                    "FE_logit_pop15" = 1,
                    "FE_ln_gdp_share" = -1,
                    "FE_ln_pop15_share" = 1,
                    "ebola_dummy" = 1,
                    "FE_ln_life_expectancy" = -1,
                    "FE_u5m_share" = 1,
                    "FE_dalys_share" = 1)

## Set DAH-D filepath by scenario
if(date %like% "ref") {
  dah_d <- fread('FILEPATH')
} else if(date %like% "better") {
  dah_d <- fread('FILEPATH')
  setnames(dah_d, 'mean', 'dah_donor_ref')
} else if(date %like% "worse") {
  dah_d <- fread('FILEPATH')
  setnames(dah_d, 'dah_donor_ref_ACTUAL', 'dah_donor_ref')
}

## Calculate top 10 sources for individual donor forecasting
dah_d <- dah_d[year == 2025]
dah_d <- dah_d[!iso3 %in% c("AGGREGATE", "AGG", "PRIVATE", "DEBT", "OTHER", "UNALL", "CHN", "OTHERPUB_NONGBD")]
top_10_sources <- dah_d[order(-dah_donor_ref)][1:10, iso3]
all_sources <- c(top_10_sources, "RESID")

