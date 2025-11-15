############################################
### Author: USERNAME
### Purpose: Run submodel mean estimates using TBM, holding out OOS years and assessing submodel goodness of fit 
############################################

rm(list = ls())

library(AFModel)

## Open the parser
args <- commandArgs(trailingOnly = TRUE)
source <- args[1]
variable <- args[2]
date <- args[3]
comment <- args[4]

root_fold <- 'FILEPATH'
load(paste0(root_fold, 'FILEPATH'))

## Get list of models passed
passed_models <- mean_run_check(
  root_fold = root_fold,
  mean_array_grid = full_grid,
  return_fail = F
)


## Bring in all the postfiles
data_models_passed <- mean_postfiles_compile(root_fold, passed_models, mc.cores = 4)
save(list = "data_models_passed", file = paste0(root_fold, "FILEPATH"))

## Apply coefficient direction priors
data_models_passed <- prior_coefs_mean(
  data = data_models_passed,
  coef_prior_list = coef_priors
)


## Distribute draws and chaos RMSE and SRREG
rmse_distro <- chaos_draw_distro_DMC(
  data = data_models_passed,
  chaos_pc = metadata_list$chaos_percent,
  metadata = ensemble_metadata,
  iso_portion = 1,
  region_portion = 0, super_region_portion = 0,
  N_draws = metadata_list$N_draws
)


## Save out metadata and distro
save(list = c("rmse_distro", "metadata_list"), file = paste0(root_fold, "FILEPATH"))
